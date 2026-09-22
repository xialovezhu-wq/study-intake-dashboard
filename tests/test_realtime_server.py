from __future__ import annotations

from contextlib import redirect_stderr
import http.client
import importlib.util
import io
import json
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "scripts/realtime_server.py"
SPEC = importlib.util.spec_from_file_location("realtime_server_under_test", SCRIPT)
assert SPEC and SPEC.loader
SERVER = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = SERVER
SPEC.loader.exec_module(SERVER)


class FakeBuilder:
    def __init__(self) -> None:
        self.revision = "rev-1"
        self.fail = False
        self.calls = 0
        self.lock = threading.Lock()

    def __call__(self, config: object, cutoff_date: str | None = None) -> dict[str, object]:
        with self.lock:
            self.calls += 1
            if self.fail:
                raise RuntimeError("private path /tmp/PRIVATE-STEM must not leak")
            revision = self.revision
        return {
            "schema_version": "fixture-v1",
            "generated_at": "2026-08-27T12:00:00+08:00",
            "revision": revision,
            "update_mode": "live_snapshot",
            "subject_order": ["408", "math", "english"],
            "subjects": {"408": {}, "math": {}, "english": {}},
            "cutoff_date": cutoff_date,
        }

    def set_revision(self, revision: str) -> None:
        with self.lock:
            self.revision = revision

    def set_fail(self, fail: bool) -> None:
        with self.lock:
            self.fail = fail


class BlockingBuilder(FakeBuilder):
    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def __call__(self, config: object, cutoff_date: str | None = None) -> dict[str, object]:
        self.entered.set()
        if not self.release.wait(timeout=5):
            raise RuntimeError("fixture initial refresh timed out")
        return super().__call__(config, cutoff_date=cutoff_date)


class ChangeAwareBuilder(FakeBuilder):
    def __init__(self) -> None:
        super().__init__()
        self.token = "source-1"
        self.token_calls = 0

    def change_token(self, config: object, cutoff_date: str | None = None) -> str:
        with self.lock:
            self.token_calls += 1
            return self.token

    def set_source(self, token: str, revision: str) -> None:
        with self.lock:
            self.token = token
            self.revision = revision


class DegradedBuilder(FakeBuilder):
    def __call__(self, config: object, cutoff_date: str | None = None) -> dict[str, object]:
        snapshot = super().__call__(config, cutoff_date=cutoff_date)
        snapshot["projection_errors"] = {
            "math": {"status": "stale", "code": "subject_projection_failed"}
        }
        return snapshot


class SSESubscriptionCleanupTest(unittest.TestCase):
    def test_header_disconnect_releases_subscription(self) -> None:
        store = SERVER.SnapshotStore(builder=FakeBuilder(), config={})
        handler = object.__new__(SERVER.RealtimeRequestHandler)
        handler.server = SimpleNamespace(snapshot_store=store)
        handler.send_response = lambda *_args: None
        handler._common_headers = lambda: None
        handler.send_header = lambda *_args: None

        def disconnected() -> None:
            raise BrokenPipeError("fixture client closed during headers")

        handler.end_headers = disconnected
        try:
            handler._serve_stream()
        except BrokenPipeError:
            pass
        self.assertEqual(store.state()["subscriber_count"], 0)


class RealtimeServerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.project_root = Path(self.temporary.name)
        (self.project_root / "index.html").write_text(
            "<!doctype html><title>fixture dashboard</title>", encoding="utf-8"
        )
        self.builder = FakeBuilder()
        self.server = SERVER.create_server(
            host="127.0.0.1",
            port=0,
            poll_seconds=30,
            builder=self.builder,
            config={"fixture": True},
            cutoff_date="2026-08-27",
            project_root=self.project_root,
        )
        self.server.start_monitor(initial_refresh=True)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.port = self.server.server_port
        self.base = f"http://127.0.0.1:{self.port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temporary.cleanup()

    def request_json(
        self, path: str, *, method: str = "GET", headers: dict[str, str] | None = None
    ) -> tuple[int, dict[str, object], object]:
        request = Request(self.base + path, method=method, headers=headers or {})
        try:
            response = urlopen(request, timeout=2)
        except HTTPError as exc:
            response = exc
        with response:
            return response.status, json.loads(response.read()), response.headers

    def test_snapshot_refresh_health_and_etag(self) -> None:
        status, snapshot, headers = self.request_json("/api/snapshot")
        self.assertEqual(status, 200)
        self.assertEqual(snapshot["revision"], "rev-1")
        self.assertEqual(headers["ETag"], '"rev-1"')
        self.assertEqual(headers["X-Snapshot-Status"], "fresh")
        self.assertIsNone(headers.get("Access-Control-Allow-Origin"))
        self.assertIn("no-store", headers["Cache-Control"])

        health_status, health, _ = self.request_json("/healthz")
        self.assertEqual(health_status, 200)
        self.assertEqual(health["has_snapshot"], True)
        self.assertEqual(health["revision"], "rev-1")
        self.assertEqual(health["status"], "ok")
        self.assertEqual(health["subscriber_count"], 0)
        self.assertIsInstance(health["last_build_ms"], (int, float))

        self.builder.set_revision("rev-2")
        refresh_status, refreshed, refreshed_headers = self.request_json(
            "/api/refresh", method="POST"
        )
        self.assertEqual(refresh_status, 200)
        self.assertEqual(refreshed["revision"], "rev-2")
        self.assertEqual(refreshed_headers["ETag"], '"rev-2"')
        sequence = self.server.snapshot_store.state()["sequence"]
        second_refresh_status, _, _ = self.request_json("/api/refresh", method="POST")
        self.assertEqual(second_refresh_status, 200)
        self.assertEqual(self.server.snapshot_store.state()["sequence"], sequence)

        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=2)
        connection.request(
            "GET", "/api/snapshot", headers={"If-None-Match": '"rev-2"'}
        )
        response = connection.getresponse()
        self.assertEqual(response.status, 304)
        response.read()
        connection.close()

    def test_asynchronous_initial_refresh_does_not_block_http_startup(self) -> None:
        builder = BlockingBuilder()
        server = SERVER.create_server(
            host="127.0.0.1",
            port=0,
            poll_seconds=30,
            builder=builder,
            config={"fixture": True},
            cutoff_date="2026-08-27",
            project_root=self.project_root,
        )
        server.start_monitor(
            initial_refresh=True, asynchronous_initial_refresh=True
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            self.assertTrue(builder.entered.wait(timeout=1))
            request = Request(
                f"http://127.0.0.1:{server.server_port}/healthz", method="GET"
            )
            with self.assertRaises(HTTPError) as caught:
                urlopen(request, timeout=1)
            self.assertEqual(caught.exception.code, 503)
            body = json.loads(caught.exception.read())
            self.assertEqual(body["status"], "starting")
            self.assertFalse(body["has_snapshot"])

            builder.release.set()
            deadline = time.monotonic() + 2
            while time.monotonic() < deadline:
                state = server.snapshot_store.state()
                if state["snapshot"] is not None:
                    break
                time.sleep(0.01)
            self.assertEqual(
                server.snapshot_store.state()["snapshot"]["revision"], "rev-1"
            )
        finally:
            builder.release.set()
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

    @staticmethod
    def _read_event(response: http.client.HTTPResponse) -> tuple[str, dict[str, object]]:
        event = ""
        data: list[bytes] = []
        while True:
            line = response.fp.readline()  # type: ignore[union-attr]
            if not line:
                raise AssertionError("SSE stream ended before a complete event")
            if line in {b"\n", b"\r\n"}:
                if event:
                    return event, json.loads(b"\n".join(data))
                continue
            if line.startswith(b"event: "):
                event = line[7:].strip().decode("ascii")
            elif line.startswith(b"data: "):
                data.append(line[6:].rstrip(b"\r\n"))

    def test_sse_initial_snapshot_and_change(self) -> None:
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=2)
        connection.request("GET", "/api/stream")
        response = connection.getresponse()
        self.assertEqual(response.status, 200)
        self.assertEqual(response.getheader("Content-Type"), "text/event-stream; charset=utf-8")
        event, data = self._read_event(response)
        self.assertEqual(event, "snapshot")
        self.assertEqual(data["revision"], "rev-1")

        self.builder.set_revision("rev-sse")
        ok, _ = self.server.snapshot_store.refresh()
        self.assertTrue(ok)
        event, data = self._read_event(response)
        self.assertEqual(event, "snapshot")
        self.assertEqual(data["revision"], "rev-sse")
        response.close()
        connection.close()

    def test_error_keeps_last_known_good_and_emits_redacted_stale_event(self) -> None:
        self.builder.set_fail(True)
        status, body, headers = self.request_json("/api/refresh", method="POST")
        self.assertEqual(status, 503)
        self.assertEqual(body["status"], "stale")
        self.assertEqual(body["snapshot"]["revision"], "rev-1")
        self.assertNotIn("PRIVATE", json.dumps(body))
        self.assertEqual(headers["X-Snapshot-Status"], "stale")

        snapshot_status, snapshot, snapshot_headers = self.request_json("/api/snapshot")
        self.assertEqual(snapshot_status, 200)
        self.assertEqual(snapshot["revision"], "rev-1")
        self.assertEqual(snapshot_headers["X-Snapshot-Status"], "stale")
        health_status, health, _ = self.request_json("/healthz")
        self.assertEqual(health_status, 503)
        self.assertTrue(health["has_snapshot"])

        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=2)
        connection.request("GET", "/api/stream")
        response = connection.getresponse()
        first_event, first_data = self._read_event(response)
        second_event, second_data = self._read_event(response)
        self.assertEqual((first_event, first_data["revision"]), ("snapshot", "rev-1"))
        self.assertEqual(second_event, "error")
        self.assertEqual(second_data["status"], "stale")
        self.assertEqual(second_data["snapshot"]["revision"], "rev-1")
        self.assertNotIn("PRIVATE", json.dumps(second_data))
        response.close()
        connection.close()

    def test_local_binding_host_origin_and_paths_are_restricted(self) -> None:
        with self.assertRaises(ValueError):
            SERVER.create_server(
                host="0.0.0.0", port=0, builder=self.builder, config={}, project_root=self.project_root
            )

        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=2)
        connection.putrequest("GET", "/healthz", skip_host=True)
        connection.putheader("Host", f"example.com:{self.port}")
        connection.endheaders()
        response = connection.getresponse()
        self.assertEqual(response.status, 421)
        response.read()
        connection.close()

        status, _, _ = self.request_json("/%2e%2e/index.html")
        self.assertEqual(status, 404)
        status, _, _ = self.request_json("/not-present")
        self.assertEqual(status, 404)
        status, _, _ = self.request_json(
            "/api/refresh",
            method="POST",
            headers={"Origin": f"http://localhost:{self.port}"},
        )
        self.assertEqual(status, 403)

        with urlopen(self.base + "/", timeout=2) as response:
            self.assertEqual(response.status, 200)
            self.assertIn(b"fixture dashboard", response.read())

    def test_server_shutdown_stops_monitor_and_listener(self) -> None:
        monitor = self.server.snapshot_store._monitor_thread
        self.assertIsNotNone(monitor)
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.assertFalse(self.thread.is_alive())
        assert monitor is not None
        self.assertFalse(monitor.is_alive())

    def test_normal_client_disconnect_does_not_print_traceback(self) -> None:
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            try:
                raise ConnectionResetError("normal browser disconnect")
            except ConnectionResetError:
                self.server.handle_error(None, ("127.0.0.1", 0))
        self.assertEqual(stderr.getvalue(), "")

    def test_change_detector_skips_unchanged_rebuilds_and_subscribers_do_not_multiply(self) -> None:
        builder = ChangeAwareBuilder()
        server = SERVER.create_server(
            host="127.0.0.1",
            port=0,
            poll_seconds=0.02,
            debounce_seconds=0.005,
            error_backoff_seconds=(0.2, 0.4),
            builder=builder,
            config={"fixture": True},
            cutoff_date="2026-08-27",
            project_root=self.project_root,
        )
        server.start_monitor(initial_refresh=True)
        try:
            for _ in range(30):
                server.snapshot_store.subscribe()
            time.sleep(0.12)
            self.assertEqual(builder.calls, 1)
            self.assertGreater(builder.token_calls, 1)
            self.assertGreater(
                server.snapshot_store.state()["skipped_refresh_count"], 0
            )

            builder.set_source("source-2", "rev-2")
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline and builder.calls < 2:
                time.sleep(0.01)
            self.assertEqual(builder.calls, 2)
            self.assertEqual(
                server.snapshot_store.state()["snapshot"]["revision"], "rev-2"
            )
        finally:
            for _ in range(30):
                server.snapshot_store.unsubscribe()
            server.server_close()

    def test_failure_backoff_is_bypassed_by_a_new_source_change(self) -> None:
        builder = ChangeAwareBuilder()
        server = SERVER.create_server(
            host="127.0.0.1",
            port=0,
            poll_seconds=0.02,
            debounce_seconds=0.005,
            error_backoff_seconds=(0.3, 0.6),
            builder=builder,
            config={"fixture": True},
            cutoff_date="2026-08-27",
            project_root=self.project_root,
        )
        server.start_monitor(initial_refresh=True)
        server.snapshot_store.subscribe()
        try:
            builder.set_fail(True)
            builder.set_source("source-failed", "rev-failed")
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline and builder.calls < 2:
                time.sleep(0.01)
            self.assertEqual(builder.calls, 2)
            time.sleep(0.12)
            self.assertEqual(builder.calls, 2)
            self.assertGreater(server.snapshot_store.state()["retry_in_seconds"], 0)

            builder.set_fail(False)
            builder.set_source("source-recovered", "rev-recovered")
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                state = server.snapshot_store.state()
                if state["error"] is None and state["snapshot"]["revision"] == "rev-recovered":
                    break
                time.sleep(0.01)
            state = server.snapshot_store.state()
            self.assertIsNone(state["error"])
            self.assertEqual(state["snapshot"]["revision"], "rev-recovered")
        finally:
            server.snapshot_store.unsubscribe()
            server.server_close()

    def test_degraded_subject_snapshot_is_published_and_health_is_fail_closed(self) -> None:
        builder = DegradedBuilder()
        server = SERVER.create_server(
            host="127.0.0.1",
            port=0,
            builder=builder,
            config={"fixture": True},
            cutoff_date="2026-08-27",
            project_root=self.project_root,
        )
        server.start_monitor(initial_refresh=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            state = server.snapshot_store.state()
            self.assertEqual(state["snapshot"]["revision"], "rev-1")
            self.assertEqual(state["error"]["code"], "subject_projection_failed")
            request = Request(
                f"http://127.0.0.1:{server.server_port}/healthz", method="GET"
            )
            with self.assertRaises(HTTPError) as caught:
                urlopen(request, timeout=2)
            self.assertEqual(caught.exception.code, 503)
            body = json.loads(caught.exception.read())
            self.assertEqual(body["status"], "degraded")
            self.assertEqual(body["degraded_subjects"], ["math"])
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()

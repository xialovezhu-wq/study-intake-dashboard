from __future__ import annotations

import argparse
import copy
import json
import logging
import sys
import threading
import time
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlsplit
from urllib.request import Request, urlopen


DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8767
DEFAULT_POLL_SECONDS = 1.0
HEARTBEAT_SECONDS = 15.0
IDLE_POLL_SECONDS = 10.0
DEFAULT_DEBOUNCE_SECONDS = 0.4
DEFAULT_ERROR_BACKOFF_SECONDS = (1.0, 2.0, 4.0, 8.0, 15.0, 30.0)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOGGER = logging.getLogger("quick_intake_realtime")

SnapshotBuilder = Callable[..., dict[str, Any]]


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _safe_etag(revision: Any) -> str | None:
    if not isinstance(revision, (str, int)):
        return None
    value = str(revision)
    if not value or len(value) > 256 or any(ord(char) < 0x20 for char in value):
        return None
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _validate_bind_host(host: str) -> str:
    if host != DEFAULT_HOST:
        raise ValueError("the realtime dashboard may bind only to 127.0.0.1")
    return host


def _safe_error(
    *, has_snapshot: bool, code: str = "snapshot_build_failed"
) -> dict[str, Any]:
    degraded = code == "subject_projection_failed"
    return {
        "status": "degraded" if degraded else ("stale" if has_snapshot else "unavailable"),
        "code": code,
        "message": (
            "One or more subject projections are stale; healthy subjects remain live."
            if degraded
            else "Snapshot refresh failed; serving last-known-good data."
            if has_snapshot
            else "Snapshot refresh failed; no verified snapshot is available."
        ),
    }


class SnapshotStore:
    """Thread-safe last-known-good snapshot and change notification state."""

    def __init__(
        self,
        *,
        builder: SnapshotBuilder,
        config: Any,
        cutoff_date: str | None = None,
        poll_seconds: float = DEFAULT_POLL_SECONDS,
        debounce_seconds: float = DEFAULT_DEBOUNCE_SECONDS,
        error_backoff_seconds: tuple[float, ...] = DEFAULT_ERROR_BACKOFF_SECONDS,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be greater than zero")
        if debounce_seconds < 0:
            raise ValueError("debounce_seconds must not be negative")
        if not error_backoff_seconds or any(value <= 0 for value in error_backoff_seconds):
            raise ValueError("error_backoff_seconds must contain positive values")
        self.builder = builder
        self.config = config
        self.cutoff_date = cutoff_date
        self.poll_seconds = poll_seconds
        self.debounce_seconds = debounce_seconds
        self.error_backoff_seconds = tuple(error_backoff_seconds)
        detector = getattr(builder, "change_token", None)
        self.change_detector = detector if callable(detector) else None
        self.condition = threading.Condition(threading.RLock())
        self.snapshot: dict[str, Any] | None = None
        self.error: dict[str, Any] | None = None
        self.sequence = 0
        self.last_event = "starting"
        self.subscriber_count = 0
        self.last_build_ms: float | None = None
        self.last_source_check_ms: float | None = None
        self.build_count = 0
        self.source_check_count = 0
        self.skipped_refresh_count = 0
        self._last_attempted_source_token: Any = None
        self._failure_count = 0
        self._next_retry_at = 0.0
        self._build_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._wake_event = threading.Event()
        self._monitor_thread: threading.Thread | None = None

    @staticmethod
    def _normalize_snapshot(value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise TypeError("snapshot builder must return a JSON object")
        # Round-tripping makes the stored value immutable with respect to a
        # builder-owned object and rejects non-JSON values before publication.
        normalized = json.loads(_json_bytes(value).decode("utf-8"))
        revision = normalized.get("revision")
        if not isinstance(revision, (str, int)) or not str(revision):
            raise ValueError("snapshot revision is missing")
        return normalized

    def _source_token(self) -> Any:
        if self.change_detector is None:
            return None
        started = time.perf_counter()
        token = self.change_detector(
            self.config,
            cutoff_date=self.cutoff_date,
        )
        with self.condition:
            self.source_check_count += 1
            self.last_source_check_ms = round(
                (time.perf_counter() - started) * 1000, 3
            )
        return token

    def _record_failure_backoff(self) -> None:
        self._failure_count += 1
        index = min(self._failure_count - 1, len(self.error_backoff_seconds) - 1)
        self._next_retry_at = time.monotonic() + self.error_backoff_seconds[index]

    def refresh(self) -> tuple[bool, dict[str, Any]]:
        # Manual refresh and the monitor can arrive together; serializing the
        # builder prevents an older, slower build from overwriting a newer one.
        with self._build_lock:
            try:
                source_token = self._source_token()
            except Exception:
                source_token = None
            return self._refresh_once(source_token=source_token)

    def _refresh_once(self, *, source_token: Any = None) -> tuple[bool, dict[str, Any]]:
        started = time.perf_counter()
        self.build_count += 1
        try:
            candidate = self._normalize_snapshot(
                self.builder(self.config, cutoff_date=self.cutoff_date)
            )
        except Exception as exc:  # The public surface deliberately redacts exc.
            with self.condition:
                self.last_build_ms = round((time.perf_counter() - started) * 1000, 3)
                public_error = _safe_error(has_snapshot=self.snapshot is not None)
                changed = self.error != public_error
                self.error = public_error
                if changed:
                    self.sequence += 1
                    self.last_event = "error"
                    self.condition.notify_all()
                self._last_attempted_source_token = source_token
                self._record_failure_backoff()
                result = self._state_locked()
            LOGGER.warning("snapshot build failed (%s)", type(exc).__name__)
            return False, result

        with self.condition:
            self.last_build_ms = round((time.perf_counter() - started) * 1000, 3)
            prior_revision = None if self.snapshot is None else self.snapshot.get("revision")
            projection_errors = candidate.get("projection_errors")
            degraded = isinstance(projection_errors, dict) and bool(projection_errors)
            public_error = (
                _safe_error(has_snapshot=True, code="subject_projection_failed")
                if degraded
                else None
            )
            changed = (
                prior_revision != candidate.get("revision")
                or self.error != public_error
            )
            self.snapshot = candidate
            self.error = public_error
            self._last_attempted_source_token = source_token
            if degraded:
                self._record_failure_backoff()
            else:
                self._failure_count = 0
                self._next_retry_at = 0.0
            if changed:
                self.sequence += 1
                self.last_event = "snapshot"
                self.condition.notify_all()
            return not degraded, self._state_locked()

    def _state_locked(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "event": self.last_event,
            "snapshot": copy.deepcopy(self.snapshot),
            "error": copy.deepcopy(self.error),
            "stopped": self._stop_event.is_set(),
            "subscriber_count": self.subscriber_count,
            "last_build_ms": self.last_build_ms,
            "last_source_check_ms": self.last_source_check_ms,
            "build_count": self.build_count,
            "source_check_count": self.source_check_count,
            "skipped_refresh_count": self.skipped_refresh_count,
            "retry_in_seconds": max(
                0.0, round(self._next_retry_at - time.monotonic(), 3)
            ) if self.error is not None else 0.0,
        }

    def state(self) -> dict[str, Any]:
        with self.condition:
            return self._state_locked()

    def wait_after(self, sequence: int, timeout: float) -> dict[str, Any]:
        with self.condition:
            self.condition.wait_for(
                lambda: self.sequence > sequence or self._stop_event.is_set(),
                timeout=timeout,
            )
            return self._state_locked()

    def start(self, *, initial_refresh: bool = True) -> None:
        with self.condition:
            if self._monitor_thread is not None and self._monitor_thread.is_alive():
                return
            self._stop_event.clear()
        if initial_refresh:
            self.refresh()
        thread = threading.Thread(
            target=self._monitor,
            name="quick-intake-snapshot-monitor",
            daemon=True,
        )
        self._monitor_thread = thread
        thread.start()

    def _monitor(self) -> None:
        while not self._stop_event.is_set():
            with self.condition:
                delay = self.poll_seconds if self.subscriber_count else IDLE_POLL_SECONDS
            woke = self._wake_event.wait(delay)
            self._wake_event.clear()
            if self._stop_event.is_set():
                return
            if woke:
                with self.condition:
                    if self.subscriber_count == 0:
                        continue
            if self.change_detector is None:
                self.refresh()
                continue
            try:
                source_token = self._source_token()
            except Exception as exc:
                with self._build_lock:
                    self._refresh_once(source_token=None)
                LOGGER.warning("source change check failed (%s)", type(exc).__name__)
                continue
            with self.condition:
                changed = source_token != self._last_attempted_source_token
                retry_due = (
                    self.error is not None
                    and time.monotonic() >= self._next_retry_at
                )
                if not changed and not retry_due:
                    self.skipped_refresh_count += 1
                    continue
            if changed and self.debounce_seconds:
                if self._stop_event.wait(self.debounce_seconds):
                    return
                try:
                    source_token = self._source_token()
                except Exception:
                    source_token = None
            with self._build_lock:
                self._refresh_once(source_token=source_token)

    def subscribe(self) -> None:
        with self.condition:
            self.subscriber_count += 1
        self._wake_event.set()

    def unsubscribe(self) -> None:
        with self.condition:
            self.subscriber_count = max(0, self.subscriber_count - 1)
        self._wake_event.set()

    def stop(self) -> None:
        self._stop_event.set()
        self._wake_event.set()
        with self.condition:
            self.condition.notify_all()
        thread = self._monitor_thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=max(1.0, self.poll_seconds * 2))


class RealtimeRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "QuickIntakeRealtime/1"
    sys_version = ""

    @property
    def realtime_server(self) -> "RealtimeHTTPServer":
        return self.server  # type: ignore[return-value]

    def log_message(self, format: str, *args: Any) -> None:
        # Avoid logging request paths or query strings. Snapshot errors are
        # logged separately by class name only.
        return

    def _common_headers(self) -> None:
        self.send_header("Cache-Control", "no-store, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        self.send_header("X-Frame-Options", "DENY")

    def _send_json(
        self,
        status: int,
        value: Any,
        *,
        etag: str | None = None,
        snapshot_status: str | None = None,
    ) -> None:
        raw = _json_bytes(value) + b"\n"
        self.send_response(status)
        self._common_headers()
        self.send_header("Content-Type", "application/json; charset=utf-8")
        if etag is not None:
            self.send_header("ETag", etag)
        if snapshot_status is not None:
            self.send_header("X-Snapshot-Status", snapshot_status)
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _route(self) -> str | None:
        try:
            raw_path = urlsplit(self.path).path
            path = unquote(raw_path, errors="strict")
        except (UnicodeError, ValueError):
            return None
        if "\\" in path or "\x00" in path or ".." in path.split("/"):
            return None
        allowed = {"/", "/api/snapshot", "/api/stream", "/api/refresh", "/healthz"}
        return path if path in allowed else None

    def do_GET(self) -> None:
        if self._host_authority() is None:
            self._send_json(HTTPStatus.MISDIRECTED_REQUEST, {"error": "host_forbidden"})
            return
        route = self._route()
        if route is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        if route == "/":
            self._serve_index()
        elif route == "/api/snapshot":
            self._serve_snapshot()
        elif route == "/api/stream":
            self._serve_stream()
        elif route == "/healthz":
            self._serve_health()
        else:
            self._send_json(HTTPStatus.METHOD_NOT_ALLOWED, {"error": "method_not_allowed"})

    def do_POST(self) -> None:
        if self._host_authority() is None:
            self._send_json(HTTPStatus.MISDIRECTED_REQUEST, {"error": "host_forbidden"})
            return
        route = self._route()
        if route is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return
        if route != "/api/refresh":
            self._send_json(HTTPStatus.METHOD_NOT_ALLOWED, {"error": "method_not_allowed"})
            return
        if not self._origin_is_local():
            self._send_json(HTTPStatus.FORBIDDEN, {"error": "origin_forbidden"})
            return
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid_content_length"})
            return
        if content_length < 0 or content_length > 1024:
            self._send_json(HTTPStatus.REQUEST_ENTITY_TOO_LARGE, {"error": "body_too_large"})
            return
        if content_length:
            self.rfile.read(content_length)
        ok, state = self.realtime_server.snapshot_store.refresh()
        snapshot = state["snapshot"]
        if ok and snapshot is not None:
            self._send_json(
                HTTPStatus.OK,
                snapshot,
                etag=_safe_etag(snapshot.get("revision")),
                snapshot_status="fresh",
            )
        else:
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {**(state["error"] or _safe_error(has_snapshot=snapshot is not None)), "snapshot": snapshot},
                etag=None if snapshot is None else _safe_etag(snapshot.get("revision")),
                snapshot_status="stale" if snapshot is not None else "unavailable",
            )

    def _origin_is_local(self) -> bool:
        origin = self.headers.get("Origin")
        if origin is None:
            return True
        authority = self._host_authority()
        if authority is None:
            return False
        parsed = urlsplit(origin)
        if parsed.scheme != "http" or parsed.username or parsed.password:
            return False
        try:
            origin_port = parsed.port
        except ValueError:
            return False
        return (
            parsed.hostname is not None
            and parsed.hostname.lower() == authority[0]
            and origin_port == authority[1]
        )

    def _host_authority(self) -> tuple[str, int] | None:
        raw = self.headers.get("Host")
        if not raw or any(char.isspace() for char in raw):
            return None
        parsed = urlsplit("//" + raw)
        if parsed.username or parsed.password or parsed.path or parsed.query or parsed.fragment:
            return None
        try:
            port = parsed.port
        except ValueError:
            return None
        hostname = (parsed.hostname or "").lower()
        if hostname not in {"127.0.0.1", "localhost"}:
            return None
        if port != self.realtime_server.server_port:
            return None
        return hostname, port

    def _serve_index(self) -> None:
        path = self.realtime_server.project_root / "index.html"
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(self.realtime_server.project_root.resolve(strict=True))
            raw = resolved.read_bytes()
        except (OSError, ValueError):
            self._send_json(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "index_unavailable"})
            return
        self.send_response(HTTPStatus.OK)
        self._common_headers()
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self' 'unsafe-inline'; "
            "style-src 'self' 'unsafe-inline'; connect-src 'self'; "
            "img-src 'self' data:; object-src 'none'; base-uri 'none'; form-action 'none'",
        )
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _serve_snapshot(self) -> None:
        state = self.realtime_server.snapshot_store.state()
        snapshot = state["snapshot"]
        if snapshot is None:
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                state["error"] or {"status": "starting", "code": "snapshot_not_ready"},
                snapshot_status="unavailable",
            )
            return
        etag = _safe_etag(snapshot.get("revision"))
        if etag is not None and self.headers.get("If-None-Match") == etag:
            self.send_response(HTTPStatus.NOT_MODIFIED)
            self._common_headers()
            self.send_header("ETag", etag)
            self.send_header("X-Snapshot-Status", "stale" if state["error"] else "fresh")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self._send_json(
            HTTPStatus.OK,
            snapshot,
            etag=etag,
            snapshot_status="stale" if state["error"] else "fresh",
        )

    def _serve_health(self) -> None:
        state = self.realtime_server.snapshot_store.state()
        snapshot = state["snapshot"]
        if state["error"] is not None:
            status = HTTPStatus.SERVICE_UNAVAILABLE
            body = {
                **state["error"],
                "has_snapshot": snapshot is not None,
                "revision": None if snapshot is None else snapshot.get("revision"),
                "subscriber_count": state["subscriber_count"],
                "last_build_ms": state["last_build_ms"],
                "last_source_check_ms": state["last_source_check_ms"],
                "build_count": state["build_count"],
                "source_check_count": state["source_check_count"],
                "skipped_refresh_count": state["skipped_refresh_count"],
                "retry_in_seconds": state["retry_in_seconds"],
                "degraded_subjects": sorted(
                    (snapshot or {}).get("projection_errors", {})
                ),
            }
        elif snapshot is None:
            status = HTTPStatus.SERVICE_UNAVAILABLE
            body = {
                "status": "starting",
                "has_snapshot": False,
                "revision": None,
                "subscriber_count": state["subscriber_count"],
                "last_build_ms": state["last_build_ms"],
                "last_source_check_ms": state["last_source_check_ms"],
                "build_count": state["build_count"],
                "source_check_count": state["source_check_count"],
                "skipped_refresh_count": state["skipped_refresh_count"],
                "retry_in_seconds": state["retry_in_seconds"],
            }
        else:
            status = HTTPStatus.OK
            body = {
                "status": "ok",
                "has_snapshot": True,
                "revision": snapshot.get("revision"),
                "subscriber_count": state["subscriber_count"],
                "last_build_ms": state["last_build_ms"],
                "last_source_check_ms": state["last_source_check_ms"],
                "build_count": state["build_count"],
                "source_check_count": state["source_check_count"],
                "skipped_refresh_count": state["skipped_refresh_count"],
                "retry_in_seconds": state["retry_in_seconds"],
                "degraded_subjects": [],
            }
        self._send_json(status, body)

    def _write_sse(self, event: str, data: Any, *, event_id: int | None = None) -> None:
        parts: list[bytes] = []
        if event_id is not None:
            parts.append(f"id: {event_id}\n".encode("ascii"))
        parts.append(f"event: {event}\n".encode("ascii"))
        for line in _json_bytes(data).splitlines() or [b"null"]:
            parts.append(b"data: " + line + b"\n")
        parts.append(b"\n")
        self.wfile.write(b"".join(parts))
        self.wfile.flush()

    def _serve_stream(self) -> None:
        store = self.realtime_server.snapshot_store
        store.subscribe()
        try:
            self.send_response(HTTPStatus.OK)
            self._common_headers()
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
            state = store.state()
            sequence = state["sequence"]
            if state["snapshot"] is not None:
                self._write_sse("snapshot", state["snapshot"], event_id=sequence)
            if state["error"] is not None:
                self._write_sse(
                    "error",
                    {**state["error"], "snapshot": state["snapshot"]},
                    event_id=sequence,
                )
            while not state["stopped"]:
                state = store.wait_after(sequence, HEARTBEAT_SECONDS)
                if state["stopped"]:
                    return
                if state["sequence"] > sequence:
                    sequence = state["sequence"]
                    if state["event"] == "snapshot" and state["snapshot"] is not None:
                        self._write_sse("snapshot", state["snapshot"], event_id=sequence)
                    elif state["event"] == "error" and state["error"] is not None:
                        self._write_sse(
                            "error",
                            {**state["error"], "snapshot": state["snapshot"]},
                            event_id=sequence,
                        )
                else:
                    self._write_sse(
                        "heartbeat",
                        {
                            "status": "stale" if state["error"] else "ok",
                            "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                        },
                    )
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            return
        finally:
            store.unsubscribe()


class RealtimeHTTPServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        *,
        snapshot_store: SnapshotStore,
        project_root: Path,
    ) -> None:
        self.snapshot_store = snapshot_store
        self.project_root = project_root.resolve()
        super().__init__(server_address, RealtimeRequestHandler)

    def start_monitor(
        self, *, initial_refresh: bool = True, asynchronous_initial_refresh: bool = False
    ) -> None:
        if asynchronous_initial_refresh and initial_refresh:
            # A strict archive-proof replay can be delayed by a sleeping data
            # disk.  Bind and serve the loopback health/static surfaces first,
            # then perform that first replay without blocking HTTP startup.
            self.snapshot_store.start(initial_refresh=False)
            threading.Thread(
                target=self.snapshot_store.refresh,
                name="quick-intake-initial-refresh",
                daemon=True,
            ).start()
            return
        self.snapshot_store.start(initial_refresh=initial_refresh)

    def handle_error(self, request: Any, client_address: Any) -> None:
        error = sys.exc_info()[1]
        if isinstance(
            error,
            (BrokenPipeError, ConnectionResetError, ConnectionAbortedError),
        ):
            return
        LOGGER.error("request handler failed (%s)", type(error).__name__)

    def server_close(self) -> None:
        self.snapshot_store.stop()
        super().server_close()


def create_server(
    *,
    host: str = DEFAULT_HOST,
    port: int = DEFAULT_PORT,
    poll_seconds: float = DEFAULT_POLL_SECONDS,
    builder: SnapshotBuilder,
    config: Any,
    cutoff_date: str | None = None,
    project_root: Path = PROJECT_ROOT,
    debounce_seconds: float = DEFAULT_DEBOUNCE_SECONDS,
    error_backoff_seconds: tuple[float, ...] = DEFAULT_ERROR_BACKOFF_SECONDS,
) -> RealtimeHTTPServer:
    _validate_bind_host(host)
    if not 0 <= port <= 65535:
        raise ValueError("port must be between 0 and 65535")
    store = SnapshotStore(
        builder=builder,
        config=config,
        cutoff_date=cutoff_date,
        poll_seconds=poll_seconds,
        debounce_seconds=debounce_seconds,
        error_backoff_seconds=error_backoff_seconds,
    )
    return RealtimeHTTPServer(
        (host, port), snapshot_store=store, project_root=project_root
    )


def _projector_runtime() -> tuple[SnapshotBuilder, Any]:
    try:
        from project_quick_intake_today import LiveSnapshotBuilder, ProjectorConfig
    except (ImportError, AttributeError) as exc:
        raise RuntimeError("live snapshot projector is unavailable") from exc
    config = ProjectorConfig()
    return LiveSnapshotBuilder(config), config


def _status(host: str, port: int) -> int:
    _validate_bind_host(host)
    request = Request(f"http://{host}:{port}/healthz", method="GET")
    try:
        with urlopen(request, timeout=3) as response:
            raw = response.read()
            status = response.status
    except HTTPError as exc:
        raw = exc.read()
        status = exc.code
    except URLError:
        print(json.dumps({"status": "offline"}, sort_keys=True))
        return 1
    try:
        body = json.loads(raw.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError):
        body = {"status": "invalid_response"}
    print(json.dumps(body, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if status == HTTPStatus.OK else 1


def _add_network_arguments(parser: argparse.ArgumentParser, *, include_poll: bool) -> None:
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    if include_poll:
        parser.add_argument("--poll-seconds", type=float, default=DEFAULT_POLL_SECONDS)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Serve the local realtime intake dashboard.")
    subparsers = parser.add_subparsers(dest="command", required=True)
    serve = subparsers.add_parser("serve", help="serve the dashboard on loopback")
    _add_network_arguments(serve, include_poll=True)
    serve.add_argument("--date", dest="cutoff_date")
    status = subparsers.add_parser("status", help="query the local service health")
    _add_network_arguments(status, include_poll=False)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        _validate_bind_host(args.host)
        if args.command == "status":
            return _status(args.host, args.port)
        builder, config = _projector_runtime()
        server = create_server(
            host=args.host,
            port=args.port,
            poll_seconds=args.poll_seconds,
            builder=builder,
            config=config,
            cutoff_date=args.cutoff_date,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"realtime server error: {exc}", file=sys.stderr)
        return 2
    server.start_monitor(initial_refresh=True, asynchronous_initial_refresh=True)
    LOGGER.info("realtime dashboard listening on http://%s:%s/", args.host, args.port)
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

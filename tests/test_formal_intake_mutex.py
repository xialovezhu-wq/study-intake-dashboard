from __future__ import annotations

import concurrent.futures
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "scripts/formal_intake_mutex.py"


class FormalIntakeMutexTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.lock_dir = Path(self.temporary.name) / "formal-intake.lock"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def run_cli(self, *arguments: str) -> tuple[int, dict[str, object]]:
        completed = subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--lock-dir",
                str(self.lock_dir),
                *arguments,
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertTrue(completed.stdout.strip(), completed.stderr)
        return completed.returncode, json.loads(completed.stdout)

    def acquire(self, run_id: str = "RUN-1") -> dict[str, object]:
        code, payload = self.run_cli(
            "acquire",
            "--thread-id",
            "THREAD-1",
            "--run-id",
            run_id,
            "--phase",
            "preflight",
        )
        self.assertEqual(code, 0)
        self.assertEqual(payload["status"], "acquired")
        return payload

    def test_acquire_busy_status_and_token_redaction(self) -> None:
        acquired = self.acquire()
        token = acquired["owner_token"]
        self.assertNotIn("owner_token", acquired["owner"])

        code, busy = self.run_cli(
            "acquire",
            "--thread-id",
            "THREAD-2",
            "--run-id",
            "RUN-2",
        )
        self.assertEqual(code, 0)
        self.assertEqual(busy["status"], "busy")
        self.assertNotIn("owner_token", busy["owner"])
        self.assertNotIn(token, json.dumps(busy))

        code, status = self.run_cli("status")
        self.assertEqual(code, 0)
        self.assertEqual(status["status"], "busy")

    def test_wrong_token_cannot_heartbeat_or_release(self) -> None:
        self.acquire()
        heartbeat_code, heartbeat = self.run_cli(
            "heartbeat", "--owner-token", "wrong-token", "--phase", "math"
        )
        self.assertEqual(heartbeat_code, 3)
        self.assertEqual(heartbeat["reason"], "owner_token_mismatch")

        release_code, release = self.run_cli(
            "release", "--owner-token", "wrong-token"
        )
        self.assertEqual(release_code, 3)
        self.assertEqual(release["reason"], "owner_token_mismatch")
        self.assertTrue(self.lock_dir.exists())

    def test_same_thread_new_run_is_stale_and_fails_closed(self) -> None:
        self.acquire()
        code, stale = self.run_cli(
            "acquire",
            "--thread-id",
            "THREAD-1",
            "--run-id",
            "RUN-2",
        )
        self.assertEqual(code, 2)
        self.assertEqual(stale["status"], "stale_lock")
        self.assertEqual(stale["reason"], "same_thread_previous_run_not_released")
        self.assertTrue((self.lock_dir / "owner.json").exists())

    def test_heartbeat_release_and_reacquire(self) -> None:
        acquired = self.acquire()
        token = acquired["owner_token"]

        code, heartbeat = self.run_cli(
            "heartbeat", "--owner-token", token, "--phase", "english"
        )
        self.assertEqual(code, 0)
        self.assertEqual(heartbeat["status"], "heartbeat")
        self.assertEqual(heartbeat["owner"]["phase"], "english")

        code, released = self.run_cli("release", "--owner-token", token)
        self.assertEqual(code, 0)
        self.assertEqual(released["status"], "released")
        self.assertFalse(self.lock_dir.exists())

        reacquired = self.acquire("RUN-2")
        self.assertEqual(reacquired["owner"]["run_id"], "RUN-2")

    def test_missing_or_corrupt_owner_is_stale_and_never_deleted(self) -> None:
        self.lock_dir.mkdir(parents=True)
        code, missing = self.run_cli(
            "acquire",
            "--thread-id",
            "THREAD-2",
            "--run-id",
            "RUN-2",
        )
        self.assertEqual(code, 2)
        self.assertEqual(missing["status"], "stale_lock")
        self.assertEqual(missing["reason"], "owner_missing")
        self.assertTrue(self.lock_dir.exists())

        (self.lock_dir / "owner.json").write_text("not-json", encoding="utf-8")
        code, corrupt = self.run_cli("status")
        self.assertEqual(code, 2)
        self.assertEqual(corrupt["status"], "stale_lock")
        self.assertEqual(corrupt["reason"], "owner_invalid_json")
        self.assertTrue(self.lock_dir.exists())

    def test_extra_lock_entry_blocks_release(self) -> None:
        acquired = self.acquire()
        token = acquired["owner_token"]
        (self.lock_dir / "unexpected").write_text("guard", encoding="utf-8")
        code, release = self.run_cli("release", "--owner-token", token)
        self.assertEqual(code, 3)
        self.assertEqual(release["reason"], "unexpected_lock_directory_entries")
        self.assertTrue((self.lock_dir / "owner.json").exists())

    def test_concurrent_acquire_has_exactly_one_owner(self) -> None:
        def attempt(index: int) -> dict[str, object]:
            _, payload = self.run_cli(
                "acquire",
                "--thread-id",
                f"THREAD-{index}",
                "--run-id",
                f"RUN-{index}",
            )
            return payload

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(attempt, range(8)))
        self.assertEqual(sum(result["status"] == "acquired" for result in results), 1)
        self.assertEqual(sum(result["status"] == "busy" for result in results), 7)


if __name__ == "__main__":
    unittest.main()

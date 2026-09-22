#!/usr/bin/env python3
"""Fail-closed single-flight mutex for the three-subject formal intake task.

The mutex is deliberately independent from the Math, 408, and English writer
locks.  It prevents two Codex automation turns from entering semantic planning
at the same time.  It never expires automatically: an abandoned or damaged
lock must be audited before an operator removes it.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import secrets
import sys
import tempfile
import time
from typing import Any
from zoneinfo import ZoneInfo


SCHEMA_VERSION = "three-subject-formal-intake-mutex-v1"
LOCK_KEY = "three-subject-formal-intake"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOCK_DIR = PROJECT_ROOT / "runtime" / "three-subject-formal-intake.lock"
OWNER_FILENAME = "owner.json"
ALLOWED_OWNER_FIELDS = {
    "schema_version",
    "lock_key",
    "thread_id",
    "run_id",
    "owner_token",
    "acquired_at",
    "heartbeat_at",
    "phase",
}


class MutexError(RuntimeError):
    """Raised for a fail-closed mutex operation."""


def shanghai_now() -> str:
    return datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(timespec="seconds")


def canonical_bytes(value: dict[str, Any]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


def validate_label(name: str, value: str) -> str:
    if not value or len(value) > 256 or any(char in value for char in "\r\n\0"):
        raise MutexError(f"invalid_{name}")
    return value


def owner_path(lock_dir: Path) -> Path:
    return lock_dir / OWNER_FILENAME


def atomic_write_owner(lock_dir: Path, owner: dict[str, Any]) -> None:
    data = canonical_bytes(owner)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb", dir=lock_dir, prefix=".owner.", suffix=".tmp", delete=False
        ) as handle:
            temporary_path = Path(handle.name)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, owner_path(lock_dir))
        directory_fd = os.open(lock_dir, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary_path is not None and temporary_path.exists():
            temporary_path.unlink()


def load_owner(lock_dir: Path) -> tuple[dict[str, Any] | None, str | None, str]:
    path = owner_path(lock_dir)
    if not path.is_file():
        return None, "owner_missing", hashlib.sha256(b"").hexdigest()
    try:
        raw = path.read_bytes()
    except OSError as exc:
        return None, f"owner_unreadable:{exc.__class__.__name__}", "unreadable"
    owner_sha256 = hashlib.sha256(raw).hexdigest()
    try:
        parsed = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None, "owner_invalid_json", owner_sha256
    if not isinstance(parsed, dict):
        return None, "owner_not_object", owner_sha256
    if set(parsed) != ALLOWED_OWNER_FIELDS:
        return None, "owner_field_set_invalid", owner_sha256
    required_strings = ALLOWED_OWNER_FIELDS
    if any(not isinstance(parsed.get(field), str) or not parsed[field] for field in required_strings):
        return None, "owner_field_value_invalid", owner_sha256
    if parsed["schema_version"] != SCHEMA_VERSION or parsed["lock_key"] != LOCK_KEY:
        return None, "owner_identity_invalid", owner_sha256
    return parsed, None, owner_sha256


def public_owner(owner: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in owner.items() if key != "owner_token"}


def emit(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def lock_state(lock_dir: Path) -> tuple[str, dict[str, Any]]:
    if not lock_dir.exists():
        return "unlocked", {
            "schema_version": SCHEMA_VERSION,
            "lock_key": LOCK_KEY,
            "status": "unlocked",
            "lock_dir": str(lock_dir),
        }
    if not lock_dir.is_dir() or lock_dir.is_symlink():
        return "stale_lock", {
            "schema_version": SCHEMA_VERSION,
            "lock_key": LOCK_KEY,
            "status": "stale_lock",
            "reason": "lock_path_not_plain_directory",
            "lock_dir": str(lock_dir),
        }
    owner, error, owner_sha256 = load_owner(lock_dir)
    if error is not None:
        return "stale_lock", {
            "schema_version": SCHEMA_VERSION,
            "lock_key": LOCK_KEY,
            "status": "stale_lock",
            "reason": error,
            "owner_sha256": owner_sha256,
            "lock_dir": str(lock_dir),
        }
    assert owner is not None
    return "busy", {
        "schema_version": SCHEMA_VERSION,
        "lock_key": LOCK_KEY,
        "status": "busy",
        "owner": public_owner(owner),
        "owner_sha256": owner_sha256,
        "lock_dir": str(lock_dir),
    }


def acquire_existing_state(lock_dir: Path) -> tuple[str, dict[str, Any]]:
    """Allow only the tiny owner-publication window, never an expiry unlock."""
    status, payload = lock_state(lock_dir)
    for _ in range(50):
        if status != "stale_lock" or payload.get("reason") != "owner_missing":
            break
        time.sleep(0.01)
        status, payload = lock_state(lock_dir)
    return status, payload


def command_acquire(args: argparse.Namespace) -> int:
    lock_dir = Path(args.lock_dir).resolve()
    thread_id = validate_label("thread_id", args.thread_id)
    run_id = validate_label("run_id", args.run_id)
    phase = validate_label("phase", args.phase)
    lock_dir.parent.mkdir(parents=True, exist_ok=True)
    try:
        lock_dir.mkdir(mode=0o700)
    except FileExistsError:
        status, payload = acquire_existing_state(lock_dir)
        existing_owner = payload.get("owner")
        if (
            status == "busy"
            and isinstance(existing_owner, dict)
            and existing_owner.get("thread_id") == thread_id
            and existing_owner.get("run_id") != run_id
        ):
            status = "stale_lock"
            payload = {
                **payload,
                "status": "stale_lock",
                "reason": "same_thread_previous_run_not_released",
            }
        emit(payload)
        return 0 if status == "busy" else 2
    except OSError as exc:
        emit(
            {
                "schema_version": SCHEMA_VERSION,
                "lock_key": LOCK_KEY,
                "status": "failed",
                "reason": f"lock_create_failed:{exc.__class__.__name__}",
                "lock_dir": str(lock_dir),
            }
        )
        return 2

    timestamp = shanghai_now()
    owner = {
        "schema_version": SCHEMA_VERSION,
        "lock_key": LOCK_KEY,
        "thread_id": thread_id,
        "run_id": run_id,
        "owner_token": secrets.token_urlsafe(32),
        "acquired_at": timestamp,
        "heartbeat_at": timestamp,
        "phase": phase,
    }
    try:
        atomic_write_owner(lock_dir, owner)
    except Exception as exc:
        try:
            if lock_dir.exists() and not any(lock_dir.iterdir()):
                lock_dir.rmdir()
        except OSError:
            pass
        emit(
            {
                "schema_version": SCHEMA_VERSION,
                "lock_key": LOCK_KEY,
                "status": "failed",
                "reason": f"owner_write_failed:{exc.__class__.__name__}",
                "lock_dir": str(lock_dir),
            }
        )
        return 2
    emit(
        {
            "schema_version": SCHEMA_VERSION,
            "lock_key": LOCK_KEY,
            "status": "acquired",
            "owner_token": owner["owner_token"],
            "owner": public_owner(owner),
            "lock_dir": str(lock_dir),
        }
    )
    return 0


def require_owner(lock_dir: Path, owner_token: str) -> tuple[dict[str, Any], str]:
    validate_label("owner_token", owner_token)
    if not lock_dir.is_dir() or lock_dir.is_symlink():
        raise MutexError("lock_not_available")
    owner, error, owner_sha256 = load_owner(lock_dir)
    if error is not None or owner is None:
        raise MutexError(f"stale_lock:{error}")
    if not secrets.compare_digest(owner["owner_token"], owner_token):
        raise MutexError("owner_token_mismatch")
    return owner, owner_sha256


def command_heartbeat(args: argparse.Namespace) -> int:
    lock_dir = Path(args.lock_dir).resolve()
    try:
        owner, _ = require_owner(lock_dir, args.owner_token)
        owner["phase"] = validate_label("phase", args.phase)
        owner["heartbeat_at"] = shanghai_now()
        atomic_write_owner(lock_dir, owner)
    except MutexError as exc:
        emit({"status": "failed", "reason": str(exc), "lock_dir": str(lock_dir)})
        return 3
    emit(
        {
            "schema_version": SCHEMA_VERSION,
            "lock_key": LOCK_KEY,
            "status": "heartbeat",
            "owner": public_owner(owner),
            "lock_dir": str(lock_dir),
        }
    )
    return 0


def command_release(args: argparse.Namespace) -> int:
    lock_dir = Path(args.lock_dir).resolve()
    try:
        owner, _ = require_owner(lock_dir, args.owner_token)
        entries = {entry.name for entry in lock_dir.iterdir()}
        if entries != {OWNER_FILENAME}:
            raise MutexError("unexpected_lock_directory_entries")
        owner_path(lock_dir).unlink()
        lock_dir.rmdir()
    except MutexError as exc:
        emit({"status": "failed", "reason": str(exc), "lock_dir": str(lock_dir)})
        return 3
    except OSError as exc:
        emit(
            {
                "status": "failed",
                "reason": f"release_failed:{exc.__class__.__name__}",
                "lock_dir": str(lock_dir),
            }
        )
        return 3
    emit(
        {
            "schema_version": SCHEMA_VERSION,
            "lock_key": LOCK_KEY,
            "status": "released",
            "thread_id": owner["thread_id"],
            "run_id": owner["run_id"],
            "lock_dir": str(lock_dir),
        }
    )
    return 0


def command_status(args: argparse.Namespace) -> int:
    lock_dir = Path(args.lock_dir).resolve()
    status, payload = lock_state(lock_dir)
    emit(payload)
    return 2 if status == "stale_lock" else 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock-dir", default=str(DEFAULT_LOCK_DIR))
    subparsers = parser.add_subparsers(dest="command", required=True)

    acquire = subparsers.add_parser("acquire")
    acquire.add_argument("--thread-id", required=True)
    acquire.add_argument("--run-id", required=True)
    acquire.add_argument("--phase", default="starting")
    acquire.set_defaults(handler=command_acquire)

    heartbeat = subparsers.add_parser("heartbeat")
    heartbeat.add_argument("--owner-token", required=True)
    heartbeat.add_argument("--phase", required=True)
    heartbeat.set_defaults(handler=command_heartbeat)

    release = subparsers.add_parser("release")
    release.add_argument("--owner-token", required=True)
    release.set_defaults(handler=command_release)

    status = subparsers.add_parser("status")
    status.set_defaults(handler=command_status)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.handler(args))
    except MutexError as exc:
        emit({"status": "failed", "reason": str(exc)})
        return 2


if __name__ == "__main__":
    sys.exit(main())

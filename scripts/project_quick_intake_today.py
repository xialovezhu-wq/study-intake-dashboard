from __future__ import annotations

import argparse
import copy
import hashlib
import html
import importlib.util
import json
import os
import re
import sys
import tempfile
import threading
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from types import ModuleType
from typing import Any, Iterable
from zoneinfo import ZoneInfo


SHANGHAI = ZoneInfo("Asia/Shanghai")
SCHEMA_VERSION = "quick-intake-today-v1"
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
SAFE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,95}$")


class ProjectionError(RuntimeError):
    """An authoritative source cannot be projected safely."""


_MODULE_CACHE_LOCK = threading.Lock()
_MODULE_CACHE: dict[tuple[Path, str], ModuleType] = {}


@dataclass(frozen=True)
class ProjectorConfig:
    math_root: Path = Path("/Users/YOUR_USER/Documents/kaoyan-math")
    cs408_root: Path = Path("/Users/YOUR_USER/Documents/kaoyan-408")
    cs408_package_root: Path = Path(
        "/Users/YOUR_USER/.codex/kaoyan-408-conversation-packages"
    )
    english_root: Path = Path("/Users/YOUR_USER/Documents/kaoyan-english")
    t9_root: Path = Path("/Volumes/T9-Data")


class SourceTracker:
    """Bind output to the exact source bytes and detect concurrent drift."""

    def __init__(self) -> None:
        self._entries: dict[Path, str] = {}

    def add_bytes(self, path: Path, raw: bytes) -> None:
        resolved = path.expanduser().resolve()
        digest = hashlib.sha256(raw).hexdigest()
        prior = self._entries.get(resolved)
        if prior is not None and prior != digest:
            raise ProjectionError(f"source changed while reading: {resolved}")
        self._entries[resolved] = digest

    def add_file(self, path: Path) -> bytes:
        raw = path.read_bytes()
        self.add_bytes(path, raw)
        return raw

    def verify_unchanged(self) -> None:
        for path, expected in self._entries.items():
            try:
                current = hashlib.sha256(path.read_bytes()).hexdigest()
            except OSError as exc:
                raise ProjectionError(f"source disappeared before projection: {path}") from exc
            if current != expected:
                raise ProjectionError(f"source changed before projection: {path}")

    def document(self) -> dict[str, Any]:
        rows = sorted((str(path), digest) for path, digest in self._entries.items())
        material = [{"path": path, "sha256": digest} for path, digest in rows]
        return {
            "paths": [path for path, _ in rows],
            "hashes": [digest for _, digest in rows],
            "snapshot_sha256": _object_sha256(material),
        }


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def _object_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _validate_date(value: str) -> str:
    try:
        parsed = date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise ProjectionError("requested date must be YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ProjectionError("requested date must be YYYY-MM-DD")
    return value


def _generated_at(now: datetime | None = None) -> str:
    current = now or datetime.now(SHANGHAI)
    if current.tzinfo is None:
        current = current.replace(tzinfo=SHANGHAI)
    return current.astimezone(SHANGHAI).isoformat(timespec="seconds")


def _captured_at(value: Any) -> tuple[str | None, str]:
    if not isinstance(value, str) or not value.strip():
        return None, "unknown"
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None, "unknown"
    if parsed.tzinfo is None:
        return None, "unknown"
    return parsed.astimezone(SHANGHAI).isoformat(timespec="seconds"), "verified"


def _safe_identifier(value: Any, *, fallback_prefix: str = "item") -> str:
    text = str(value or "").strip()
    if SAFE_ID_RE.fullmatch(text):
        return text[:64]
    return f"{fallback_prefix}-{hashlib.sha256(text.encode('utf-8')).hexdigest()[:10]}"


def _warning(
    warnings: list[dict[str, Any]], code: str, message: str, *, severity: str = "warning"
) -> None:
    for row in warnings:
        if row["code"] == code and row["severity"] == severity:
            row["count"] += 1
            return
    warnings.append(
        {"code": code, "severity": severity, "message": message, "count": 1}
    )


def _load_json(path: Path, tracker: SourceTracker) -> dict[str, Any]:
    try:
        raw = tracker.add_file(path)
        value = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ProjectionError(f"unreadable JSON source: {path}") from exc
    if not isinstance(value, dict):
        raise ProjectionError(f"JSON source must be an object: {path}")
    return value


def _load_jsonl(path: Path, tracker: SourceTracker) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        raw = tracker.add_file(path)
        text = raw.decode("utf-8")
    except (OSError, UnicodeError) as exc:
        raise ProjectionError(f"unreadable JSONL source: {path}") from exc
    rows: list[dict[str, Any]] = []
    for line_no, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ProjectionError(f"invalid JSONL line {line_no}: {path}") from exc
        if not isinstance(row, dict):
            raise ProjectionError(f"JSONL line {line_no} is not an object: {path}")
        rows.append(row)
    return rows


def _jsonl_rows_with_raw(
    path: Path, tracker: SourceTracker
) -> list[tuple[dict[str, Any], bytes]]:
    if not path.is_file():
        return []
    try:
        raw = tracker.add_file(path)
    except OSError as exc:
        raise ProjectionError(f"unreadable JSONL source: {path}") from exc
    result: list[tuple[dict[str, Any], bytes]] = []
    for line_no, line in enumerate(raw.splitlines(keepends=True), start=1):
        if not line.strip():
            continue
        try:
            value = json.loads(line.decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError) as exc:
            raise ProjectionError(f"invalid JSONL line {line_no}: {path}") from exc
        if not isinstance(value, dict):
            raise ProjectionError(f"JSONL line {line_no} is not an object: {path}")
        result.append((value, line))
    return result


def _pretty_object_sha256(value: Any) -> str:
    raw = (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _load_module(name: str, path: Path) -> ModuleType:
    if not path.is_file():
        raise ImportError(path)
    resolved = path.expanduser().resolve()
    try:
        source_sha256 = hashlib.sha256(resolved.read_bytes()).hexdigest()
    except OSError as exc:
        raise ImportError(path) from exc
    cache_key = (resolved, source_sha256)
    with _MODULE_CACHE_LOCK:
        cached = _MODULE_CACHE.get(cache_key)
        if cached is not None:
            return cached
    parent = str(path.parent)
    inserted = parent not in sys.path
    if inserted:
        sys.path.insert(0, parent)
    try:
        spec = importlib.util.spec_from_file_location(name, path)
        if spec is None or spec.loader is None:
            raise ImportError(path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        with _MODULE_CACHE_LOCK:
            _MODULE_CACHE[cache_key] = module
        return module
    finally:
        if inserted and sys.path and sys.path[0] == parent:
            sys.path.pop(0)


def _resolve_inside(root: Path, relative: Any) -> Path:
    value = Path(str(relative or ""))
    if value.is_absolute() or not value.parts or ".." in value.parts:
        raise ProjectionError("unsafe relative source path")
    base = root.expanduser().resolve()
    candidate = (base / value).resolve()
    try:
        candidate.relative_to(base)
    except ValueError as exc:
        raise ProjectionError("source path escaped subject root") from exc
    return candidate


def _attachment_counts(rows: Any) -> dict[str, int]:
    counts = {
        "question_image": 0,
        "solution_image": 0,
        "explanation_image": 0,
        "user_work_image": 0,
        "source_article_image": 0,
        "other_attachment": 0,
        "solution_text": 0,
    }
    if not isinstance(rows, list):
        return counts
    aliases = {
        "question": "question_image",
        "solution": "solution_image",
        "explanation": "explanation_image",
        "user_work": "user_work_image",
        "reference": "other_attachment",
    }
    for row in rows:
        if not isinstance(row, dict):
            continue
        role = aliases.get(str(row.get("role") or ""), str(row.get("role") or ""))
        counts[role if role in counts else "other_attachment"] += 1
    return counts


def _status_basis(rule: str, source_status: str, evidence_ids: Iterable[str]) -> dict[str, Any]:
    return {
        "rule": rule,
        "source_status": source_status,
        "evidence_ids": [_safe_identifier(item, fallback_prefix="evidence") for item in evidence_ids],
    }


def _projection_status(items: list[dict[str, Any]], warnings: list[dict[str, Any]]) -> str:
    if any(row.get("severity") in {"damaged", "partial"} for row in warnings):
        return "partial"
    if not items:
        return "empty"
    return "complete"


def _backlog_summary(
    *,
    cutoff_date: str,
    rows: list[dict[str, Any]],
    plan_sha256: str | None,
    already_consumed_count: int = 0,
    future_excluded_count: int = 0,
    extra_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    normalized = [
        {
            "study_date": str(row["study_date"]),
            "status": str(row["status"]),
            "processable": bool(row.get("processable")),
        }
        for row in rows
        if isinstance(row, dict)
        and isinstance(row.get("study_date"), str)
        and row["study_date"] <= cutoff_date
    ]
    status_counts = Counter(row["status"] for row in normalized)
    per_date: list[dict[str, Any]] = []
    for study_date in sorted({row["study_date"] for row in normalized}):
        day_rows = [row for row in normalized if row["study_date"] == study_date]
        per_date.append(
            {
                "study_date": study_date,
                "unconsumed_count": len(day_rows),
                "processable_count": sum(row["processable"] for row in day_rows),
                "residual_count": sum(not row["processable"] for row in day_rows),
            }
        )
    processable = sum(row["processable"] for row in normalized)
    document = {
        "schema_version": "quick-intake-backlog-through-date-v1",
        "cutoff_date": cutoff_date,
        "unconsumed_count": len(normalized),
        "processable_pending_count": processable,
        "residual_count": len(normalized) - processable,
        "earliest_study_date": min(
            (row["study_date"] for row in normalized), default=None
        ),
        "status_counts": dict(sorted(status_counts.items())),
        "date_counts": per_date,
        "already_consumed_count": int(already_consumed_count),
        "future_excluded_count": int(future_excluded_count),
        "plan_sha256": plan_sha256,
    }
    if extra_counts:
        document["extra_counts"] = {
            key: int(value) for key, value in sorted(extra_counts.items())
        }
    return document


def _finalize_subject(
    *,
    subject: str,
    study_date: str,
    generated_at: str,
    tracker: SourceTracker,
    counts: dict[str, int],
    items: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
    backlog: dict[str, Any],
) -> dict[str, Any]:
    for item in items:
        if item.get("study_date") != study_date:
            raise ProjectionError(
                f"fail-closed date check rejected {subject} item {item.get('subject_item_key')}"
            )
    if int(backlog.get("unconsumed_count", 0)) > 0:
        earliest = backlog.get("earliest_study_date") or "未记录"
        _warning(
            warnings,
            f"{subject}_backlog_through_cutoff",
            (
                f"截至 {study_date} 仍有历史未可信终态条目；"
                f"最早学习日 {earliest}。今日卡片仍只显示 {study_date}。"
            ),
        )
        warnings[-1]["count"] = int(backlog["unconsumed_count"])
    tracker.verify_unchanged()
    items.sort(key=lambda row: (row.get("captured_at") is not None, row.get("captured_at") or "", row["subject_item_key"]), reverse=True)
    return {
        "schema_version": SCHEMA_VERSION,
        "subject": subject,
        "study_date": study_date,
        "generated_at": generated_at,
        "timezone": "Asia/Shanghai",
        "projection_mode": "manual_full_snapshot_replacement",
        "projection_status": _projection_status(items, warnings),
        "source_snapshot": tracker.document(),
        "backlog_through_date": backlog,
        "counts": counts,
        "items": items,
        "warnings": warnings,
    }


def _math_fallback_replay(events: list[dict[str, Any]]) -> dict[str, Any]:
    captures: dict[str, dict[str, Any]] = {}
    closed_by: dict[str, str] = {}
    closeouts: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()
    for row in events:
        event_id = str(row.get("event_id") or "")
        if not event_id or event_id in seen:
            raise ProjectionError("math ledger has duplicate or missing event_id")
        seen.add(event_id)
        event_type = row.get("event_type")
        if event_type == "capture":
            captures[event_id] = row
        elif event_type == "closeout":
            ids = row.get("capture_event_ids")
            if not isinstance(ids, list):
                raise ProjectionError("math closeout capture_event_ids invalid")
            for capture_id in ids:
                if capture_id not in captures or capture_id in closed_by:
                    raise ProjectionError("math closeout references invalid capture")
                closed_by[capture_id] = event_id
            closeouts[event_id] = row
        elif event_type in {
            "amendment", "freeze", "freeze_abort", "closeout_prepare", "closeout_invalidation"
        }:
            continue
        else:
            raise ProjectionError(f"unknown math ledger event type: {event_type}")
    return {"captures": captures, "closed_by": closed_by, "closeouts": closeouts}


def _math_manifest(
    *, root: Path, capture: dict[str, Any], tracker: SourceTracker, warnings: list[dict[str, Any]]
) -> tuple[dict[str, Any] | None, bool]:
    reference = capture.get("conversation_package")
    if not isinstance(reference, dict):
        return None, False
    try:
        path = _resolve_inside(root, reference.get("manifest_path"))
        manifest = _load_json(path, tracker)
        receipt = _load_json(path.parent / "receipt.json", tracker)
        expected_manifest_hash = str(reference.get("manifest_hash") or "")
        expected_package_sha = str(reference.get("package_sha256") or "")
        if (
            _file_sha256(path) != expected_manifest_hash
            or manifest.get("canonical_sha256") != expected_package_sha
            or manifest.get("study_date") != capture.get("study_date")
            or manifest.get("subject") != "math"
            or receipt.get("canonical_sha256") != expected_package_sha
            or receipt.get("study_date") != capture.get("study_date")
            or receipt.get("formal_write_count") != 0
        ):
            raise ProjectionError("math package binding mismatch")
        pointer = path.parent / "archive-pointer.json"
        archived = False
        if pointer.is_file():
            pointer_doc = _load_json(pointer, tracker)
            archived = (
                pointer_doc.get("archive_status") == "verified"
                and pointer_doc.get("raw_archive_package_sha256") == expected_package_sha
            )
            if not archived:
                _warning(
                    warnings,
                    "math_archive_pointer_unverified",
                    "数学归档指针存在，但未达到可验证终态。",
                    severity="partial",
                )
        return manifest, archived
    except (OSError, ProjectionError):
        _warning(
            warnings,
            "math_package_invalid",
            "数学会话包或回执无法按账本绑定重开。",
            severity="damaged",
        )
        return None, False


def _math_backlog(
    *,
    state: dict[str, Any],
    cutoff_date: str,
    native: ModuleType | None,
    native_events: list[dict[str, Any]] | None,
    ledger_sha256: str,
) -> dict[str, Any]:
    if native is not None and native_events is not None and hasattr(native, "build_backlog_plan"):
        try:
            response = native.build_backlog_plan(
                native_events,
                cutoff_date,
                snapshot_hash=ledger_sha256,
            )
            plan = response["plan"]
            rows = [
                {
                    "study_date": item["study_date"],
                    "status": item["status"],
                    "processable": item["status"] in {"pending", "resume"},
                }
                for group in plan.get("date_groups", [])
                for item in group.get("items", [])
            ]
            return _backlog_summary(
                cutoff_date=cutoff_date,
                rows=rows,
                plan_sha256=response.get("plan_sha256"),
                already_consumed_count=len(
                    plan.get("global_gate", {}).get("already_consumed", [])
                ),
                future_excluded_count=len(
                    plan.get("excluded_future_capture_event_ids", [])
                ),
            )
        except Exception as exc:
            raise ProjectionError(f"math backlog planner rejected replay: {exc}") from exc
    rows = []
    already_consumed = 0
    for capture_id, capture in state.get("captures", {}).items():
        date_value = capture.get("study_date")
        if not isinstance(date_value, str) or date_value > cutoff_date:
            continue
        if capture_id in state.get("closed_by", {}):
            already_consumed += 1
            continue
        rows.append(
            {"study_date": date_value, "status": "pending", "processable": True}
        )
    return _backlog_summary(
        cutoff_date=cutoff_date,
        rows=rows,
        plan_sha256=None,
        already_consumed_count=already_consumed,
    )


def project_math(
    config: ProjectorConfig, study_date: str, *, now: datetime | None = None
) -> dict[str, Any]:
    study_date = _validate_date(study_date)
    generated = _generated_at(now)
    tracker = SourceTracker()
    warnings: list[dict[str, Any]] = []
    ledger = config.math_root / "数学一回滚复习系统" / "快速入库事件.jsonl"
    raw_events = _load_jsonl(ledger, tracker)
    state: dict[str, Any]
    script = config.math_root / "数学一回滚复习系统" / "scripts" / "quick_intake.py"
    if script.is_file():
        tracker.add_file(script)
    native: ModuleType | None = None
    events: list[dict[str, Any]] | None = None
    try:
        native = _load_module("quick_board_math_native", script)
        events = native.load_jsonl(ledger)
        state = native.replay(events)
    except (ImportError, AttributeError):
        state = _math_fallback_replay(raw_events)
    except Exception as exc:
        raise ProjectionError(f"math native replay rejected ledger: {exc}") from exc

    backlog = _math_backlog(
        state=state,
        cutoff_date=study_date,
        native=native,
        native_events=events,
        ledger_sha256=_file_sha256(ledger),
    )

    items: list[dict[str, Any]] = []
    package_shas: set[str] = set()
    formal_ids: set[str] = set()
    source_keys: set[str] = set()
    conservative: set[str] = set()
    pending = closed = archived_count = 0
    missing_date = 0
    for capture_id, capture in state.get("captures", {}).items():
        capture_date = capture.get("study_date")
        if not capture_date:
            missing_date += 1
            continue
        if capture_date != study_date:
            continue
        target = capture.get("target") if isinstance(capture.get("target"), dict) else {}
        formal_id = target.get("formal_id")
        source_locator = target.get("source_locator")
        if formal_id:
            formal_ids.add(str(formal_id))
            target_key = f"formal:{formal_id}"
            title = f"正式题 · {_safe_identifier(formal_id, fallback_prefix='formal')}"
        elif source_locator:
            source_hash = hashlib.sha256(str(source_locator).encode("utf-8")).hexdigest()
            source_keys.add(source_hash)
            target_key = f"source:{source_hash}"
            title = f"新来源 · {source_hash[:8]}"
        else:
            target_key = f"capture:{capture_id}"
            title = f"待核对目标 · {_safe_identifier(capture_id)[-10:]}"
        conservative.add(target_key)
        manifest, is_archived = _math_manifest(
            root=config.math_root, capture=capture, tracker=tracker, warnings=warnings
        )
        package_id = manifest.get("package_id") if manifest else None
        package_sha = manifest.get("canonical_sha256") if manifest else None
        if isinstance(package_sha, str) and SHA256_RE.fullmatch(package_sha):
            package_shas.add(package_sha)
        closeout_id = state.get("closed_by", {}).get(capture_id)
        source_status = str(capture.get("initial_state") or (
            "awaiting_sol_formalization"
            if capture.get("capture_schema_version") == "math-fast-intake-capture-v3"
            else "pending_nightly"
        ))
        evidence_ids: list[str] = [capture_id]
        if is_archived:
            display_status = "archived"
            archived_count += 1
            if closeout_id:
                closed += 1
                evidence_ids.append(closeout_id)
        elif closeout_id:
            display_status = "formalized"
            closed += 1
            evidence_ids.append(closeout_id)
        else:
            display_status = "pending"
            pending += 1
        captured_at, time_quality = _captured_at(capture.get("recorded_at"))
        artifacts = manifest.get("artifacts", []) if manifest else []
        items.append(
            {
                "subject_item_key": _object_sha256(["math", capture_id]),
                "safe_title": title,
                "evidence_kind": "capture_event",
                "unit_type": "question",
                "captured_at": captured_at,
                "study_date": capture_date,
                "capture_id": capture_id,
                "package_id": package_id,
                "package_sha256": package_sha,
                "source_bundle_id": (
                    (capture.get("source_bundle") or {}).get("package_id")
                    if isinstance(capture.get("source_bundle"), dict)
                    else None
                ),
                "source_manifest_sha256": (
                    (capture.get("conversation_package") or {}).get("manifest_hash")
                    if isinstance(capture.get("conversation_package"), dict)
                    else (capture.get("source_bundle") or {}).get("manifest_hash")
                    if isinstance(capture.get("source_bundle"), dict)
                    else None
                ),
                "source_status": source_status,
                "display_status": display_status,
                "display_status_basis": _status_basis(
                    "math_ledger_replay_and_verified_archive_pointer_v1",
                    source_status,
                    evidence_ids,
                ),
                "attachment_counts": _attachment_counts(artifacts),
                "time_quality": time_quality,
            }
        )
    if missing_date:
        _warning(
            warnings,
            "math_missing_study_date_excluded",
            "缺少 study_date 的数学事件已排除，未使用时间或 mtime 推断。",
            severity="partial",
        )
        warnings[-1]["count"] = missing_date
    counts = {
        "capture_count": len(items),
        "unique_formal_question_count": len(formal_ids),
        "provisional_unique_source_count": len(source_keys),
        "conservative_unique_target_count": len(conservative),
        "conversation_package_count": len(package_shas),
        "pending_count": pending,
        "closed_count": closed,
        "archived_count": archived_count,
    }
    return _finalize_subject(
        subject="math",
        study_date=study_date,
        generated_at=generated,
        tracker=tracker,
        counts=counts,
        items=items,
        warnings=warnings,
        backlog=backlog,
    )


def _cs408_fallback_replay(events: list[dict[str, Any]]) -> dict[str, Any]:
    captures: dict[str, dict[str, Any]] = {}
    batches: dict[str, dict[str, Any]] = {}
    for event in events:
        kind = event.get("event_type")
        if kind == "fact_captured":
            capture_id = str(event.get("capture_id") or "")
            capture = event.get("capture")
            if not capture_id or capture_id in captures or not isinstance(capture, dict):
                raise ProjectionError("408 fact capture identity invalid")
            captures[capture_id] = {
                "capture_id": capture_id,
                "study_date": capture.get("study_date"),
                "recorded_at": event.get("created_at"),
                "payload_sha256": event.get("payload_sha256"),
                "quality_status": (
                    "awaiting_sol_formalization"
                    if capture.get("formalization_authorized")
                    else "captured_unconfirmed"
                ),
                "formal_id": (capture.get("identity_hint") or {}).get("formal_id") or None,
                "capture": capture,
            }
        elif kind == "curation_batch_started":
            batches[str(event.get("batch_id"))] = {"results": {}}
        elif kind == "curation_item_result":
            capture_id = str(event.get("capture_id"))
            if capture_id not in captures:
                raise ProjectionError("408 result references unknown capture")
            outcome = str(event.get("outcome"))
            captures[capture_id]["quality_status"] = outcome if outcome != "failed" else "curation_failed"
            captures[capture_id]["formal_id"] = event.get("formal_id")
        elif kind in {
            "curation_batch_closed", "capture_authorized", "needs_user_resolved", "neutral_saved"
        }:
            continue
        else:
            raise ProjectionError(f"unknown 408 ledger event type: {kind}")
    return {"captures": captures, "batches": batches}


def _package_reference(capture: dict[str, Any]) -> dict[str, str] | None:
    payload = capture.get("capture") if isinstance(capture.get("capture"), dict) else {}
    refs = payload.get("stable_evidence_refs")
    if not isinstance(refs, list):
        return None
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        if ref.get("kind") == "cs408_conversation_package_v1":
            locator = str(ref.get("locator") or "")
            sha = str(ref.get("sha256") or "")
            if locator.startswith("cs408-conversation-package://") and SHA256_RE.fullmatch(sha):
                return {"locator": locator, "package_sha256": sha}
    return None


def _verified_pointer(
    *,
    pointer_path: Path,
    package_sha: str,
    repo_root: Path,
    t9_root: Path,
    tracker: SourceTracker,
    expected_schema: str,
) -> tuple[bool, Path | None]:
    pointer = _load_json(pointer_path, tracker)
    if pointer.get("schema_version") != expected_schema or pointer.get("package_sha256") != package_sha:
        return False, None
    relative = Path(str(pointer.get("archive_relative_path") or ""))
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        return False, None
    archive_base = t9_root.expanduser().resolve()
    archive_root = (archive_base / relative).resolve()
    try:
        archive_root.relative_to(archive_base)
    except ValueError:
        return False, None
    receipt_path = archive_root / "archive-receipt.json"
    locator_rel = Path(str(pointer.get("locator_note_relative_path") or pointer.get("archive_locator_path") or ""))
    locator_path = (repo_root.resolve() / locator_rel).resolve()
    try:
        locator_path.relative_to(repo_root.resolve())
    except ValueError:
        return False, None
    if not receipt_path.is_file() or not locator_path.is_file():
        return False, archive_root
    tracker.add_file(receipt_path)
    tracker.add_file(locator_path)
    receipt_sha = pointer.get("archive_receipt_sha256")
    locator_sha = pointer.get("locator_note_sha256") or pointer.get("archive_locator_sha256")
    if (
        receipt_sha and _file_sha256(receipt_path) != receipt_sha
    ) or (
        locator_sha and _file_sha256(locator_path) != locator_sha
    ):
        return False, archive_root
    return True, archive_root


def _cs408_package(
    *,
    config: ProjectorConfig,
    reference: dict[str, str],
    study_date: str,
    tracker: SourceTracker,
    warnings: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, bool, bool]:
    package_id = reference["locator"].removeprefix("cs408-conversation-package://")
    if not SAFE_ID_RE.fullmatch(package_id):
        return None, False, False
    local = config.cs408_package_root / study_date / package_id
    manifest_path = local / "manifest.json"
    pointer_path = local / "archive-pointer.json"
    archived = archive_pending = False
    archive_root: Path | None = None
    if pointer_path.is_file():
        try:
            archived, archive_root = _verified_pointer(
                pointer_path=pointer_path,
                package_sha=reference["package_sha256"],
                repo_root=config.cs408_root,
                t9_root=config.t9_root,
                tracker=tracker,
                expected_schema="cs408-local-archive-pointer-v1",
            )
        except ProjectionError:
            archived = False
        archive_pending = not archived
        if archive_pending:
            _warning(
                warnings,
                "cs408_archive_pointer_unverified",
                "408 归档指针存在，但未能重开为已验证归档。",
                severity="partial",
            )
    candidate_manifest = manifest_path
    if (not candidate_manifest.is_file()) and archive_root is not None:
        candidate_manifest = archive_root / "manifest.json"
    if not candidate_manifest.is_file():
        _warning(
            warnings,
            "cs408_package_missing",
            "408 会话包在本地和指针定位中均不可重开。",
            severity="damaged",
        )
        return None, archived, archive_pending
    try:
        manifest = _load_json(candidate_manifest, tracker)
        receipt = _load_json(candidate_manifest.parent / "receipt.json", tracker)
        if (
            manifest.get("package_id") != package_id
            or manifest.get("study_date") != study_date
            or manifest.get("canonical_sha256") != reference["package_sha256"]
            or receipt.get("canonical_sha256") != reference["package_sha256"]
            or receipt.get("study_date") != study_date
            or receipt.get("formal_write_count") != 0
        ):
            raise ProjectionError("408 package receipt binding mismatch")
        return manifest, archived, archive_pending
    except (OSError, ProjectionError):
        _warning(
            warnings,
            "cs408_package_invalid",
            "408 会话包与事实账本或 receipt 绑定不一致。",
            severity="damaged",
        )
        return None, archived, archive_pending


def _cs408_operation_receipts(
    config: ProjectorConfig,
    tracker: SourceTracker,
    warnings: list[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    root = (
        config.cs408_root
        / "wiki/study_vaults/408-full/state/intake-curation"
        / "standalone-question-intake-v1/receipts"
    )
    result: dict[tuple[str, str], dict[str, Any]] = {}
    if not root.is_dir():
        return result
    for path in sorted(root.glob("*.json")):
        try:
            receipt = _load_json(path, tracker)
            capture_id = str(receipt.get("capture_id") or "")
            package_sha = str(receipt.get("conversation_package_sha256") or "")
            identity_sha = str(receipt.get("standalone_item_identity_sha256") or "")
            if (
                receipt.get("schema") != "cs408-standalone-question-intake-receipt-v1"
                or not capture_id
                or not SHA256_RE.fullmatch(package_sha)
                or not SHA256_RE.fullmatch(identity_sha)
                or receipt.get("formal_write_count") != 0
                or receipt.get("background_processing") != "none"
            ):
                raise ProjectionError("standalone operation receipt binding invalid")
            key = (capture_id, package_sha)
            if key in result and result[key] != receipt:
                raise ProjectionError("duplicate standalone operation receipt binding")
            result[key] = receipt
        except ProjectionError:
            _warning(
                warnings,
                "cs408_standalone_receipt_invalid",
                "408 standalone 组合 receipt 无法按 capture/package 绑定验证。",
                severity="damaged",
            )
    return result


def _cs408_identity(
    capture: dict[str, Any],
    manifest: dict[str, Any] | None,
    operation_receipt: dict[str, Any] | None,
) -> tuple[str | None, str]:
    if operation_receipt:
        identity_sha = operation_receipt.get("standalone_item_identity_sha256")
        if isinstance(identity_sha, str) and SHA256_RE.fullmatch(identity_sha):
            return f"standalone:{identity_sha}", "standalone_operation_receipt"
    if manifest:
        candidates = [
            manifest.get("question_identity_sha256"),
            (manifest.get("source_identity") or {}).get("question_identity_sha256")
            if isinstance(manifest.get("source_identity"), dict)
            else None,
        ]
        for value in candidates:
            if isinstance(value, str) and SHA256_RE.fullmatch(value):
                return f"question-sha:{value}", "question_identity_sha256"
    formal_id = capture.get("formal_id")
    if formal_id:
        return f"formal:{formal_id}", "verified_formal_identity"
    payload = capture.get("capture") if isinstance(capture.get("capture"), dict) else {}
    identity = payload.get("identity_hint") if isinstance(payload.get("identity_hint"), dict) else {}
    if identity.get("status") == "existing" and identity.get("formal_id"):
        return f"formal:{identity['formal_id']}", "verified_formal_identity"
    if manifest:
        source_id = manifest.get("source_id") or (manifest.get("source_identity") or {}).get("source_id") if isinstance(manifest.get("source_identity"), dict) else manifest.get("source_id")
        question_sha = manifest.get("question_sha256")
        options_sha = manifest.get("options_sha256")
        if source_id and isinstance(question_sha, str) and SHA256_RE.fullmatch(question_sha):
            return (
                "source-material:" + _object_sha256([source_id, question_sha, options_sha]),
                "source_question_material_hash",
            )
    return None, "unresolved"


def _cs408_backlog(
    *,
    config: ProjectorConfig,
    cutoff_date: str,
    native: ModuleType | None,
    state: dict[str, Any],
) -> dict[str, Any]:
    if native is not None and hasattr(native, "backlog_plan"):
        scripts_path = str((config.cs408_root / "scripts").resolve())
        inserted = scripts_path not in sys.path
        if inserted:
            sys.path.insert(0, scripts_path)
        try:
            plan = native.backlog_plan(
                cutoff_date=cutoff_date,
                repo_root=config.cs408_root,
                package_root=config.cs408_package_root,
            )
            processable = [
                {
                    "study_date": item["study_date"],
                    "status": item["status"],
                    "processable": True,
                }
                for day in plan.get("days", [])
                for item in day.get("processable_items", [])
            ]
            residual = [
                {
                    "study_date": item["study_date"],
                    "status": item["status"],
                    "processable": False,
                }
                for day in plan.get("days", [])
                for item in day.get("residual_items", [])
            ]
            return _backlog_summary(
                cutoff_date=cutoff_date,
                rows=processable + residual,
                plan_sha256=plan.get("canonical_plan_sha256"),
                already_consumed_count=len(plan.get("trusted_terminal_excluded", [])),
                future_excluded_count=len(plan.get("future_excluded", [])),
                extra_counts={
                    "canonical_processable_pending_count": int(
                        plan.get("processable_pending_count", len(processable))
                    ),
                    "canonical_residual_count": int(
                        plan.get("residual_count", len(residual))
                    ),
                },
            )
        except Exception as exc:
            raise ProjectionError(f"408 backlog planner rejected replay: {exc}") from exc
        finally:
            if inserted and sys.path and sys.path[0] == scripts_path:
                sys.path.pop(0)
    rows = []
    pending_by_date = state.get("pending_by_date", {})
    for date_value, capture_ids in pending_by_date.items():
        if isinstance(date_value, str) and date_value <= cutoff_date:
            rows.extend(
                {
                    "study_date": date_value,
                    "status": "pending_start",
                    "processable": True,
                }
                for _ in capture_ids
            )
    if not pending_by_date:
        terminal_statuses = {"curated", "already_current", "concept_only", "skipped", "skip"}
        for capture in state.get("captures", {}).values():
            date_value = capture.get("study_date")
            status = str(capture.get("quality_status") or "pending_start")
            if (
                isinstance(date_value, str)
                and date_value <= cutoff_date
                and status not in terminal_statuses
            ):
                rows.append(
                    {
                        "study_date": date_value,
                        "status": "needs_user" if status == "needs_user" else "pending_start",
                        "processable": status != "needs_user",
                    }
                )
    return _backlog_summary(
        cutoff_date=cutoff_date,
        rows=rows,
        plan_sha256=None,
    )


def project_cs408(
    config: ProjectorConfig, study_date: str, *, now: datetime | None = None
) -> dict[str, Any]:
    study_date = _validate_date(study_date)
    generated = _generated_at(now)
    tracker = SourceTracker()
    warnings: list[dict[str, Any]] = []
    ledger_root = config.cs408_root / "wiki/study_vaults/408-full/state/intake-curation"
    ledger = ledger_root / "events.jsonl"
    raw_events = _load_jsonl(ledger, tracker)
    script = config.cs408_root / "scripts/intake_fact_capture_408.py"
    if script.is_file():
        tracker.add_file(script)
    for helper_name in ("capture_hot_writer_408.py", "capture_commit_index_408.py"):
        helper_path = config.cs408_root / "scripts" / helper_name
        if helper_path.is_file():
            tracker.add_file(helper_path)
    native: ModuleType | None = None
    try:
        native = _load_module("quick_board_cs408_native", script)
        state = native.replay(native._load_events(ledger_root))
    except (ImportError, AttributeError):
        state = _cs408_fallback_replay(raw_events)
    except Exception as exc:
        raise ProjectionError(f"408 native replay rejected ledger: {exc}") from exc

    backlog = _cs408_backlog(
        config=config,
        cutoff_date=study_date,
        native=native,
        state=state,
    )

    operation_receipts = _cs408_operation_receipts(config, tracker, warnings)
    items: list[dict[str, Any]] = []
    package_shas: set[str] = set()
    identities: set[str] = set()
    unresolved = pending = formalized = archived_count = 0
    missing_date = 0
    for capture_id, capture in state.get("captures", {}).items():
        capture_date = capture.get("study_date")
        if not capture_date:
            missing_date += 1
            continue
        if capture_date != study_date:
            continue
        reference = _package_reference(capture)
        manifest = None
        is_archived = archive_pending = False
        if reference:
            manifest, is_archived, archive_pending = _cs408_package(
                config=config,
                reference=reference,
                study_date=study_date,
                tracker=tracker,
                warnings=warnings,
            )
            if manifest:
                package_shas.add(reference["package_sha256"])
        operation_receipt = (
            operation_receipts.get((capture_id, reference["package_sha256"]))
            if reference
            else None
        )
        identity, identity_basis = _cs408_identity(
            capture, manifest, operation_receipt
        )
        if identity is None:
            unresolved += 1
            item_identity = f"capture:{capture_id}"
        else:
            identities.add(identity)
            item_identity = identity
        source_status = str(capture.get("quality_status") or "unknown")
        if is_archived:
            display_status = "archived"
            archived_count += 1
        elif archive_pending:
            display_status = "archive_pending"
        elif source_status in {"curated", "already_current", "concept_only"}:
            display_status = "formalized"
            formalized += 1
        elif source_status in {"needs_user", "curation_failed", "failed"}:
            display_status = "needs_user"
        else:
            display_status = "pending"
            pending += 1
        captured_at, time_quality = _captured_at(
            operation_receipt.get("captured_at")
            if operation_receipt
            else capture.get("recorded_at")
        )
        source_facts = (
            (capture.get("capture") or {}).get("source_facts")
            if isinstance(capture.get("capture"), dict)
            else {}
        ) or {}
        safe_source = _safe_identifier(
            (operation_receipt or {}).get("standalone_item_id")
            or source_facts.get("source_id")
            or capture_id,
            fallback_prefix="source",
        )
        package_id = manifest.get("package_id") if manifest else None
        package_sha = reference.get("package_sha256") if reference and manifest else None
        items.append(
            {
                "subject_item_key": _object_sha256(["408", item_identity, package_sha or capture_id]),
                "safe_title": f"408 题 · {safe_source}",
                "evidence_kind": "conversation_package" if manifest else "capture_event",
                "unit_type": "question",
                "captured_at": captured_at,
                "study_date": capture_date,
                "capture_id": capture_id,
                "package_id": package_id,
                "package_sha256": package_sha,
                "source_bundle_id": None,
                "source_manifest_sha256": (
                    _file_sha256(
                        (config.cs408_package_root / study_date / str(package_id) / "manifest.json")
                    )
                    if package_id
                    and (config.cs408_package_root / study_date / str(package_id) / "manifest.json").is_file()
                    else None
                ),
                "source_status": source_status,
                "display_status": display_status,
                "display_status_basis": _status_basis(
                    f"cs408_replay_{identity_basis}_and_verified_pointer_v1",
                    source_status,
                    [capture_id, package_id or "no-package"],
                ),
                "attachment_counts": _attachment_counts(
                    manifest.get("attachments", []) if manifest else []
                ),
                "time_quality": time_quality,
            }
        )
    if missing_date:
        _warning(
            warnings,
            "cs408_missing_study_date_excluded",
            "缺少 study_date 的 408 条目已排除，未使用时间或 mtime 推断。",
            severity="partial",
        )
        warnings[-1]["count"] = missing_date
    counts = {
        "unique_question_count": len(identities),
        "package_count": len(package_shas),
        "question_identity_unresolved_count": unresolved,
        "pending_count": pending,
        "formalized_count": formalized,
        "archived_count": archived_count,
    }
    return _finalize_subject(
        subject="408",
        study_date=study_date,
        generated_at=generated,
        tracker=tracker,
        counts=counts,
        items=items,
        warnings=warnings,
        backlog=backlog,
    )


class _EnglishFallback:
    @staticmethod
    def validate_conversation_package(package_root: Path) -> dict[str, Any]:
        manifest_path = package_root / "manifest.json"
        receipt_path = package_root / "receipt.json"
        if not manifest_path.is_file() or not receipt_path.is_file():
            raise ProjectionError("english fixture package is incomplete")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        package_sha = str(manifest.get("package_canonical_sha256") or "")
        if (
            manifest.get("subject") != "english"
            or not SHA256_RE.fullmatch(package_sha)
            or receipt.get("package_id") != manifest.get("package_id")
            or receipt.get("package_sha256") != package_sha
            or receipt.get("study_date") != manifest.get("study_date")
            or receipt.get("formal_write_count") != 0
            or receipt.get("background_processing") != "none"
        ):
            raise ProjectionError("english fixture package receipt binding invalid")
        files = manifest.get("files")
        if not isinstance(files, dict):
            raise ProjectionError("english fixture files map invalid")
        for relative, descriptor in files.items():
            path = (package_root / relative).resolve()
            path.relative_to(package_root.resolve())
            if (
                not path.is_file()
                or not isinstance(descriptor, dict)
                or _file_sha256(path) != descriptor.get("sha256")
                or path.stat().st_size != descriptor.get("bytes")
            ):
                raise ProjectionError("english fixture package child drifted")
        return manifest

    @staticmethod
    def processed_package_sha256s(state_dir: Path) -> set[str]:
        processed: set[str] = set()
        for path in sorted((state_dir / "receipts/nightly").glob("**/*.json")):
            try:
                receipt = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            dispositions = receipt.get("package_dispositions") or {}
            for package_id, package_sha in zip(
                receipt.get("package_ids") or [], receipt.get("package_sha256s") or []
            ):
                if dispositions.get(package_id) == "completed" and SHA256_RE.fullmatch(
                    str(package_sha)
                ):
                    processed.add(str(package_sha))
        return processed


def _english_import_module(
    root: Path,
    qualified_name: str,
    module_path: Path,
    dependency_paths: Iterable[Path] = (),
) -> ModuleType:
    paths = [module_path, *dependency_paths]
    digest = _object_sha256(
        [
            [str(path.resolve()), hashlib.sha256(path.read_bytes()).hexdigest()]
            for path in paths
        ]
    )
    root_text = str(root)
    inserted = root_text not in sys.path
    if inserted:
        sys.path.insert(0, root_text)
    try:
        import importlib

        module = sys.modules.get(qualified_name)
        if (
            module is None
            or Path(str(getattr(module, "__file__", ""))).resolve()
            != module_path.resolve()
            or getattr(module, "__dashboard_source_sha256__", None) != digest
        ):
            for name in list(sys.modules):
                if name == "english_pipeline" or name.startswith("english_pipeline."):
                    del sys.modules[name]
            importlib.invalidate_caches()
            module = importlib.import_module(qualified_name)
            setattr(module, "__dashboard_source_sha256__", digest)
        return module
    finally:
        if inserted and sys.path and sys.path[0] == root_text:
            sys.path.pop(0)


def _english_module(root: Path) -> ModuleType | _EnglishFallback:
    package_init = root / "english_pipeline/packages.py"
    if not package_init.is_file():
        return _EnglishFallback()
    return _english_import_module(
        root,
        "english_pipeline.packages",
        package_init,
    )


def _english_backlog_module(root: Path) -> ModuleType | None:
    module_path = root / "english_pipeline/backlog.py"
    if not module_path.is_file():
        return None
    dependencies = [root / "english_pipeline/packages.py"]
    return _english_import_module(
        root,
        "english_pipeline.backlog",
        module_path,
        [path for path in dependencies if path.is_file()],
    )


def _english_unit(source: dict[str, Any], package_sha: str) -> tuple[str, str, str]:
    identity = source.get("identity") if isinstance(source.get("identity"), dict) else {}
    source_id = identity.get("source_id") or identity.get("article_id") or identity.get("reference_id")
    qualifier = _safe_identifier(source_id or f"package-{package_sha[:10]}", fallback_prefix="source")
    priorities = (
        ("question", "question_id", "题目"),
        ("knowledge_point", "knowledge_point_id", "知识点"),
        ("sentence", "sentence_id", "句子"),
        ("paragraph", "paragraph_id", "段落"),
        ("article", "article_id", "文章"),
    )
    for unit_type, field, label in priorities:
        if identity.get(field):
            unit_id = _safe_identifier(identity[field], fallback_prefix=unit_type)
            return unit_type, _object_sha256([unit_type, qualifier, unit_id]), f"{label} · {unit_id}"
    return "segment", _object_sha256(["segment", qualifier, package_sha]), f"会话段 · {package_sha[:8]}"


def _english_backlog(
    *,
    config: ProjectorConfig,
    cutoff_date: str,
    native: ModuleType | _EnglishFallback,
    processed: set[str],
    tracker: SourceTracker,
    warnings: list[dict[str, Any]],
) -> dict[str, Any]:
    backlog_module = _english_backlog_module(config.english_root)
    if backlog_module is not None:
        module_path = config.english_root / "english_pipeline/backlog.py"
        tracker.add_file(module_path)
        try:
            _, plan = backlog_module.build_backlog_plan(
                config.english_root / "intake",
                config.english_root,
                cutoff_date=cutoff_date,
                persist=False,
            )
        except Exception as exc:
            raise ProjectionError(f"english backlog planner rejected state: {exc}") from exc
        rows: list[dict[str, Any]] = []
        already_consumed = 0
        for package in plan.get("packages", []):
            status = str(package.get("status") or "failed")
            if status == "already_consumed":
                already_consumed += 1
                continue
            rows.append(
                {
                    "study_date": package["study_date"],
                    "status": status,
                    "processable": status in {"pending", "recovery_required"},
                }
            )
        legacy = plan.get("legacy_reachability_audit") or {}
        for event in legacy.get("events", []):
            status = str(event.get("status") or "needs_user")
            rows.append(
                {
                    "study_date": event["study_date"],
                    "status": (
                        "legacy_migration"
                        if status == "eligible-for-explicit-migration"
                        else "needs_user"
                    ),
                    "processable": status == "eligible-for-explicit-migration",
                }
            )
        legacy_counts = legacy.get("counts") or {}
        return _backlog_summary(
            cutoff_date=cutoff_date,
            rows=rows,
            plan_sha256=plan.get("plan_canonical_sha256"),
            already_consumed_count=already_consumed,
            future_excluded_count=len(plan.get("future_excluded", [])),
            extra_counts={
                "legacy_event_count": int(legacy.get("event_count", 0)),
                "legacy_eligible_count": int(
                    legacy_counts.get("eligible-for-explicit-migration", 0)
                ),
                "legacy_needs_user_count": int(
                    legacy_counts.get("needs_user", 0)
                ),
            },
        )

    state_dir = config.english_root / "intake"
    archived_shas: set[str] = set()
    pointer_root = state_dir / "archive-pointers"
    if pointer_root.is_dir():
        for pointer_path in sorted(pointer_root.glob("*/*.json")):
            if pointer_path.parent.name > cutoff_date:
                continue
            opened = _english_pointer_package(
                config=config,
                pointer_path=pointer_path,
                study_date=pointer_path.parent.name,
                tracker=tracker,
                native=native,
                warnings=warnings,
            )
            if opened:
                archived_shas.add(opened[1]["package_canonical_sha256"])
    rows = []
    already_consumed = 0
    packages_root = state_dir / "packages"
    if packages_root.is_dir():
        for package_root in sorted(packages_root.glob("*/*")):
            if not package_root.is_dir():
                continue
            try:
                manifest = native.validate_conversation_package(package_root)
                date_value = str(manifest.get("study_date") or "")
                if not date_value or date_value > cutoff_date:
                    continue
                package_sha = str(manifest.get("package_canonical_sha256") or "")
                if package_sha in processed or package_sha in archived_shas:
                    already_consumed += 1
                    continue
                rows.append(
                    {"study_date": date_value, "status": "pending", "processable": True}
                )
            except Exception:
                date_value = package_root.parent.name
                if date_value <= cutoff_date:
                    rows.append(
                        {"study_date": date_value, "status": "failed", "processable": False}
                    )
    legacy_count = 0
    for event_path in sorted((state_dir / "events").glob("*/*.json")):
        date_value = event_path.parent.name
        if date_value <= cutoff_date:
            legacy_count += 1
            rows.append(
                {"study_date": date_value, "status": "needs_user", "processable": False}
            )
    return _backlog_summary(
        cutoff_date=cutoff_date,
        rows=rows,
        plan_sha256=None,
        already_consumed_count=already_consumed,
        extra_counts={
            "legacy_event_count": legacy_count,
            "legacy_eligible_count": 0,
            "legacy_needs_user_count": legacy_count,
        },
    )


def _english_pointer_package(
    *,
    config: ProjectorConfig,
    pointer_path: Path,
    study_date: str,
    tracker: SourceTracker,
    native: ModuleType,
    warnings: list[dict[str, Any]],
) -> tuple[Path, dict[str, Any], dict[str, Any]] | None:
    try:
        pointer = _load_json(pointer_path, tracker)
        if pointer.get("schema_version") != "english_archive_pointer_v1":
            raise ProjectionError("english archive pointer schema mismatch")
        relative = Path(str(pointer.get("archive_relative_path") or ""))
        if relative.is_absolute() or not relative.parts or ".." in relative.parts:
            raise ProjectionError("english archive pointer path invalid")
        package_root = (config.t9_root.resolve() / relative).resolve()
        package_root.relative_to(config.t9_root.resolve())
        archive_receipt_path = Path(str(pointer.get("archive_receipt_path") or "")).resolve()
        archive_locator_path = Path(str(pointer.get("archive_locator_path") or "")).resolve()
        state_dir = pointer_path.parents[2]
        intent_path = (
            state_dir
            / "cleanup-intents"
            / study_date
            / f"{pointer.get('package_id')}.json"
        )
        intent = _load_json(intent_path, tracker)
        if (
            not archive_receipt_path.is_file()
            or not archive_locator_path.is_file()
            or str(archive_receipt_path) != str(pointer.get("archive_receipt_path"))
            or str(archive_locator_path) != str(pointer.get("archive_locator_path"))
            or intent.get("package_sha256") != pointer.get("package_sha256")
            or intent.get("archive_relative_path") != pointer.get("archive_relative_path")
            or intent.get("archive_receipt_path") != str(archive_receipt_path)
            or intent.get("locator_path") != str(archive_locator_path)
            or intent.get("cleanup_authorized") is not True
        ):
            raise ProjectionError("english archive pointer evidence incomplete")
        tracker.add_file(archive_receipt_path)
        tracker.add_file(archive_locator_path)
        if (
            _file_sha256(archive_receipt_path) != intent.get("archive_receipt_sha256")
            or _file_sha256(archive_locator_path) != intent.get("locator_sha256")
        ):
            raise ProjectionError("english archive pointer evidence hash mismatch")
        manifest = native.validate_conversation_package(package_root)
        if (
            manifest.get("study_date") != study_date
            or manifest.get("package_canonical_sha256") != pointer.get("package_sha256")
        ):
            raise ProjectionError("english archive package binding mismatch")
        tracker.add_file(package_root / "manifest.json")
        receipt = _load_json(package_root / "receipt.json", tracker)
        return package_root, manifest, receipt
    except Exception:
        _warning(
            warnings,
            "english_archive_pointer_invalid",
            "英语归档指针存在，但无法按精确路径重开并验证。",
            severity="damaged",
        )
        return None


def project_english(
    config: ProjectorConfig, study_date: str, *, now: datetime | None = None
) -> dict[str, Any]:
    study_date = _validate_date(study_date)
    generated = _generated_at(now)
    tracker = SourceTracker()
    warnings: list[dict[str, Any]] = []
    state_dir = config.english_root / "intake"
    package_module_path = config.english_root / "english_pipeline/packages.py"
    if package_module_path.is_file():
        tracker.add_file(package_module_path)
    try:
        native = _english_module(config.english_root)
    except Exception as exc:
        raise ProjectionError(f"english package validator unavailable: {exc}") from exc
    try:
        processed = native.processed_package_sha256s(state_dir)
    except Exception as exc:
        raise ProjectionError(f"english processed-package replay failed: {exc}") from exc

    backlog = _english_backlog(
        config=config,
        cutoff_date=study_date,
        native=native,
        processed=processed,
        tracker=tracker,
        warnings=warnings,
    )

    candidates: dict[str, tuple[Path, dict[str, Any], dict[str, Any], bool]] = {}
    local_day = state_dir / "packages" / study_date
    if local_day.is_dir():
        for package_root in sorted(path for path in local_day.iterdir() if path.is_dir()):
            try:
                manifest = native.validate_conversation_package(package_root)
                tracker.add_file(package_root / "manifest.json")
                receipt = _load_json(package_root / "receipt.json", tracker)
                if manifest.get("study_date") != study_date:
                    _warning(
                        warnings,
                        "english_wrong_study_date_excluded",
                        "英语会话包 study_date 与请求日不一致，已排除。",
                        severity="partial",
                    )
                    continue
                package_sha = manifest["package_canonical_sha256"]
                candidates[package_sha] = (package_root, manifest, receipt, False)
            except Exception:
                _warning(
                    warnings,
                    "english_local_package_invalid",
                    "英语本地会话包或 receipt 验证失败。",
                    severity="damaged",
                )
    pointer_day = state_dir / "archive-pointers" / study_date
    if pointer_day.is_dir():
        for pointer_path in sorted(pointer_day.glob("*.json")):
            opened = _english_pointer_package(
                config=config,
                pointer_path=pointer_path,
                study_date=study_date,
                tracker=tracker,
                native=native,
                warnings=warnings,
            )
            if opened:
                package_root, manifest, receipt = opened
                candidates[manifest["package_canonical_sha256"]] = (
                    package_root,
                    manifest,
                    receipt,
                    True,
                )

    items: list[dict[str, Any]] = []
    question_keys: set[str] = set()
    learning_keys: set[str] = set()
    for package_sha, (package_root, manifest, receipt, archived) in candidates.items():
        source = _load_json(package_root / "source.json", tracker)
        unit_type, subject_key, safe_title = _english_unit(source, package_sha)
        learning_keys.add(subject_key)
        if unit_type == "question":
            question_keys.add(subject_key)
        source_status = str(receipt.get("status") or "unknown")
        if archived:
            display_status = "archived"
        elif package_sha in processed:
            display_status = "formalized"
        elif receipt.get("missing_fields"):
            display_status = "needs_user"
        else:
            display_status = "pending"
        receipt_schema = str(receipt.get("schema_version") or "")
        captured_at, time_quality = _captured_at(receipt.get("captured_at"))
        if receipt_schema not in {"english_package_receipt_v2", "english_package_receipt_v3"} and captured_at is not None:
            # A v1 compatibility receipt may carry a value synthesized from
            # manifest.created_at. It is useful activity time, but it is not
            # the durable v2 package-publication timestamp.
            time_quality = "activity_time_only"
        items.append(
            {
                "subject_item_key": subject_key,
                "safe_title": safe_title,
                "evidence_kind": "archive_pointer" if archived else "conversation_package",
                "unit_type": unit_type,
                "captured_at": captured_at,
                "study_date": manifest.get("study_date"),
                "capture_id": None,
                "package_id": manifest.get("package_id"),
                "package_sha256": package_sha,
                "source_bundle_id": None,
                "source_manifest_sha256": _file_sha256(package_root / "manifest.json"),
                "source_status": source_status,
                "display_status": display_status,
                "display_status_basis": _status_basis(
                    "english_package_receipt_writer_closeout_and_verified_pointer_v1",
                    source_status,
                    [manifest.get("package_id") or "unknown-package"],
                ),
                "attachment_counts": _attachment_counts(manifest.get("attachments", [])),
                "time_quality": time_quality,
            }
        )
    counts = {
        "question_count": len(question_keys),
        "learning_unit_count": len(learning_keys),
        "segment_count": len(candidates),
        "pending_count": sum(row["display_status"] == "pending" for row in items),
        "formalized_count": sum(row["display_status"] == "formalized" for row in items),
        "archived_count": sum(row["display_status"] == "archived" for row in items),
    }
    return _finalize_subject(
        subject="english",
        study_date=study_date,
        generated_at=generated,
        tracker=tracker,
        counts=counts,
        items=items,
        warnings=warnings,
        backlog=backlog,
    )


def build_all(
    config: ProjectorConfig,
    study_date: str,
    *,
    now: datetime | None = None,
) -> dict[str, dict[str, Any]]:
    """Build all projections in fixed UI order without writing any source."""

    return {
        "408": project_cs408(config, study_date, now=now),
        "math": project_math(config, study_date, now=now),
        "english": project_english(config, study_date, now=now),
    }


LIVE_SCHEMA_VERSION = "three-subject-quick-intake-live-v1"
LIVE_SUBJECT_ORDER = ("408", "math", "english")
LIVE_DISPLAY_NAMES = {"408": "408", "math": "数学", "english": "英语"}
MATH_CONVERSATION_ARCHIVE_RECEIPT_SCHEMAS = {
    "math-conversation-package-archive-receipt-v1",
    "math-conversation-package-archive-receipt-v2",
}
MATH_LEGACY_ARCHIVE_RECEIPT_SCHEMAS = {
    "math-legacy-evidence-archive-receipt-v1",
    "math-legacy-evidence-archive-receipt-v2",
}
MATH_CONVERSATION_ARCHIVE_POINTER_SCHEMAS = {
    "math-conversation-package-archive-pointer-v1",
    "math-conversation-package-archive-pointer-v2",
}
MATH_LEGACY_ARCHIVE_POINTER_SCHEMAS = {
    "math-legacy-evidence-archive-pointer-v1",
    "math-legacy-evidence-archive-pointer-v2",
}
MATH_DISPLAY_CLOSURE_KEYS = (
    "display_closure_receipt_id",
    "display_closure_receipt_path",
    "display_closure_receipt_sha256",
    "formal_reference_scan_sha256",
    "stable_asset_count",
    "no_display_proof",
)


def _empty_attachment_counts() -> dict[str, int]:
    return _attachment_counts([])


def _short_identifier(value: Any) -> str | None:
    if value is None or str(value).strip() == "":
        return None
    safe = _safe_identifier(value, fallback_prefix="id")
    if len(safe) <= 18:
        return safe
    return f"{safe[:6]}…{safe[-8:]}"


def _live_id_metadata(
    *, capture_id: Any = None, package_id: Any = None, event_id: Any = None
) -> list[str]:
    rows: list[str] = []
    for label, value in (
        ("capture", capture_id),
        ("package", package_id),
        ("event", event_id),
    ):
        short = _short_identifier(value)
        if short is not None and f"{label} · {short}" not in rows:
            rows.append(f"{label} · {short}")
    return rows


def _live_display_status(status: Any, queue_class: str) -> str:
    source = str(status or "unknown")
    if queue_class == "processable":
        return source
    if source in {"needs_user", "archive_pending", "damaged", "failed"}:
        return source
    return "needs_user" if "needs_user" in source else source


def _live_item(
    *,
    subject: str,
    identity: Any,
    safe_title: str,
    study_date: Any,
    captured_at: Any,
    queue_class: str,
    source_status: Any,
    display_status: Any,
    capture_id: Any = None,
    package_id: Any = None,
    event_id: Any = None,
    attachment_counts: dict[str, int] | None = None,
    time_quality: str | None = None,
) -> dict[str, Any]:
    date_value = _validate_date(str(study_date or ""))
    normalized_at, inferred_quality = _captured_at(captured_at)
    capture_value = (
        _safe_identifier(capture_id, fallback_prefix="capture")
        if capture_id is not None
        else None
    )
    package_value = (
        _safe_identifier(package_id, fallback_prefix="package")
        if package_id is not None
        else None
    )
    event_value = (
        _safe_identifier(event_id, fallback_prefix="event")
        if event_id is not None
        else None
    )
    return {
        "subject_item_key": _object_sha256([subject, str(identity)]),
        "safe_title": str(safe_title)[:96],
        "study_date": date_value,
        "captured_at": normalized_at,
        "queue_class": queue_class,
        "capture_id": capture_value,
        "package_id": package_value,
        "event_id": event_value,
        "id_metadata": _live_id_metadata(
            capture_id=capture_value,
            package_id=package_value,
            event_id=event_value,
        ),
        "source_status": str(source_status or "unknown")[:96],
        "display_status": str(display_status or "unknown")[:96],
        "time_quality": time_quality or inferred_quality,
        "attachment_counts": {
            key: int(value)
            for key, value in (attachment_counts or _empty_attachment_counts()).items()
            if key in _empty_attachment_counts()
        },
    }


def _dedupe_and_sort_live_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    chosen: dict[str, dict[str, Any]] = {}
    for item in items:
        key = item["subject_item_key"]
        prior = chosen.get(key)
        if prior is None:
            chosen[key] = item
            continue
        # A residual is the safer representation when one planner row appears
        # in both a closeout action queue and an unresolved residual list.
        if prior["queue_class"] == "processable" and item["queue_class"] == "residual":
            chosen[key] = item
            continue
        if prior["queue_class"] == item["queue_class"]:
            prior_order = (
                prior.get("captured_at")
                or f"{prior['study_date']}T00:00:00+08:00",
                prior["study_date"],
            )
            item_order = (
                item.get("captured_at")
                or f"{item['study_date']}T00:00:00+08:00",
                item["study_date"],
            )
            if item_order > prior_order:
                chosen[key] = item
    return sorted(
        chosen.values(),
        key=lambda row: (
            row.get("captured_at") or f"{row['study_date']}T00:00:00+08:00",
            row["study_date"],
            row["subject_item_key"],
        ),
        reverse=True,
    )


def _live_counts(
    items: list[dict[str, Any]], *, already_consumed: int, future_excluded: int,
    administrative_terminal_excluded: int = 0,
) -> dict[str, Any]:
    status_counts = Counter(str(row["display_status"]) for row in items)
    return {
        "item_count": len(items),
        "processable_count": sum(
            row["queue_class"] == "processable" for row in items
        ),
        "residual_count": sum(row["queue_class"] == "residual" for row in items),
        "waiting_web_review_count": sum(
            row["queue_class"] == "waiting_web_review" for row in items
        ),
        "already_consumed_excluded_count": int(already_consumed),
        "administrative_terminal_excluded_count": int(
            administrative_terminal_excluded
        ),
        "future_excluded_count": int(future_excluded),
        "status_counts": dict(sorted(status_counts.items())),
    }


def _live_subject(
    subject: str,
    items: list[dict[str, Any]],
    *,
    already_consumed: int,
    future_excluded: int,
    administrative_terminal_excluded: int = 0,
) -> dict[str, Any]:
    normalized = _dedupe_and_sort_live_items(items)
    return {
        "display_name": LIVE_DISPLAY_NAMES[subject],
        "counts": _live_counts(
            normalized,
            already_consumed=already_consumed,
            future_excluded=future_excluded,
            administrative_terminal_excluded=(
                administrative_terminal_excluded
            ),
        ),
        "items": normalized,
    }


def _math_local_archive_receipts(
    root: Path, tracker: SourceTracker
) -> dict[str, dict[str, Any]]:
    path = root / "数学一回滚复习系统/原始会话归档回执.jsonl"
    rows = _load_jsonl(path, tracker)
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        receipt_id = str(row.get("receipt_id") or "")
        if (
            row.get("schema_version")
            not in (
                MATH_CONVERSATION_ARCHIVE_RECEIPT_SCHEMAS
                | MATH_LEGACY_ARCHIVE_RECEIPT_SCHEMAS
            )
            or not receipt_id
            or (receipt_id in result and result[receipt_id] != row)
        ):
            raise ProjectionError("math local archive receipt ledger is invalid")
        result[receipt_id] = row
    return result


def _math_effective_package_reference(
    capture: dict[str, Any], amendments: list[dict[str, Any]]
) -> dict[str, Any] | None:
    reference = capture.get("source_bundle")
    if not isinstance(reference, dict):
        reference = capture.get("conversation_package")
    for amendment in amendments:
        patch = amendment.get("target_patch")
        if isinstance(patch, dict) and isinstance(patch.get("source_bundle"), dict):
            reference = patch["source_bundle"]
    return reference if isinstance(reference, dict) else None


def _math_verified_local_file(
    root: Path, relative: Any, expected_sha256: Any, tracker: SourceTracker
) -> bool:
    if not isinstance(relative, str) or not SHA256_RE.fullmatch(
        str(expected_sha256 or "")
    ):
        return False
    try:
        path = _resolve_inside(root, relative)
        if path.is_symlink() or not path.is_file():
            return False
        raw = tracker.add_file(path)
    except (OSError, ProjectionError):
        return False
    return hashlib.sha256(raw).hexdigest() == expected_sha256


def _math_conversation_archive_complete(
    *,
    root: Path,
    capture_id: str,
    capture: dict[str, Any],
    amendments: list[dict[str, Any]],
    closeout_id: str,
    receipts: dict[str, dict[str, Any]],
    tracker: SourceTracker,
) -> bool:
    reference = _math_effective_package_reference(capture, amendments)
    if not isinstance(reference, dict):
        return False
    try:
        manifest_path = _resolve_inside(root, reference.get("manifest_path"))
        if manifest_path.is_symlink() or not manifest_path.is_file():
            return False
        manifest_raw = tracker.add_file(manifest_path)
        manifest = json.loads(manifest_raw.decode("utf-8"))
        package_receipt = _load_json(manifest_path.parent / "receipt.json", tracker)
    except (OSError, UnicodeError, json.JSONDecodeError, ProjectionError):
        return False
    package_id = str(reference.get("package_id") or "")
    manifest_sha256 = str(reference.get("manifest_hash") or "")
    package_sha256 = str(reference.get("package_sha256") or "")
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != "math-conversation-package-v1"
        or not package_id
        or not SHA256_RE.fullmatch(manifest_sha256)
        or not SHA256_RE.fullmatch(package_sha256)
        or hashlib.sha256(manifest_raw).hexdigest() != manifest_sha256
        or manifest.get("package_id") != package_id
        or manifest.get("canonical_sha256") != package_sha256
        or manifest.get("study_date") != capture.get("study_date")
        or manifest.get("subject") != "math"
        or package_receipt.get("schema_version")
        != "math-conversation-package-receipt-v1"
        or package_receipt.get("package_id") != package_id
        or package_receipt.get("canonical_sha256") != package_sha256
        or package_receipt.get("study_date") != capture.get("study_date")
        or package_receipt.get("formal_write_count") != 0
    ):
        return False
    pointer_path = manifest_path.parent / "archive-pointer.json"
    if pointer_path.is_symlink() or not pointer_path.is_file():
        return False
    try:
        pointer = _load_json(pointer_path, tracker)
    except ProjectionError:
        return False
    pointer_schema = pointer.get("schema_version")
    if (
        pointer_schema not in MATH_CONVERSATION_ARCHIVE_POINTER_SCHEMAS
        or pointer.get("package_id") != package_id
        or pointer.get("archive_volume") != "T9-Data"
        or pointer.get("archive_status") != "verified"
        or pointer.get("raw_archive_manifest_sha256") != manifest_sha256
        or pointer.get("raw_archive_package_sha256") != package_sha256
    ):
        return False
    receipt = receipts.get(str(pointer.get("archive_receipt_id") or ""))
    expected_receipt_schema = (
        "math-conversation-package-archive-receipt-v2"
        if pointer_schema == "math-conversation-package-archive-pointer-v2"
        else "math-conversation-package-archive-receipt-v1"
    )
    capture_ids = receipt.get("capture_ids") if isinstance(receipt, dict) else None
    if (
        not isinstance(receipt, dict)
        or receipt.get("schema_version") != expected_receipt_schema
        or receipt.get("package_id") != package_id
        or receipt.get("closeout_id") != closeout_id
        or not isinstance(capture_ids, list)
        or capture_id not in capture_ids
        or receipt.get("archive_intent_id") != pointer.get("archive_intent_id")
        or receipt.get("archive_status") != "verified"
        or receipt.get("obsidian_path_status") != "verified"
        or any(
            receipt.get(field) != pointer.get(field)
            for field in (
                "raw_archive_relpath",
                "raw_archive_manifest_sha256",
                "raw_archive_package_sha256",
                "obsidian_locator_path",
                "obsidian_locator_sha256",
            )
        )
    ):
        return False
    if pointer_schema == "math-conversation-package-archive-pointer-v2":
        if (
            pointer.get("pending_component") is not None
            or receipt.get("pending_component") is not None
            or any(
                pointer.get(key) != receipt.get(key)
                for key in MATH_DISPLAY_CLOSURE_KEYS
            )
            or pointer.get("cleanup_intent")
            != "remove_local_attachments_after_verified_display_archive_locator_receipt"
            or not _math_verified_local_file(
                root,
                pointer.get("display_closure_receipt_path"),
                pointer.get("display_closure_receipt_sha256"),
                tracker,
            )
        ):
            return False
    elif pointer.get("cleanup_intent") not in {
        "remove_local_attachments_after_verified_archive_and_locator_receipt",
        "remove_local_attachments_after_verified_display_archive_locator_receipt",
    }:
        return False
    return _math_verified_local_file(
        root,
        pointer.get("obsidian_locator_path"),
        pointer.get("obsidian_locator_sha256"),
        tracker,
    )


def _math_capture_pointer_candidates(
    root: Path,
    capture_id: str,
    capture: dict[str, Any],
    amendments: list[dict[str, Any]],
    tracker: SourceTracker,
) -> list[Path]:
    ledger_pointer = (
        root / "数学一回滚复习系统/历史证据归档指针" / f"{capture_id}.json"
    )
    result = [ledger_pointer] if ledger_pointer.is_file() else []
    declared: list[Any] = [
        capture.get("source_bundle"),
        capture.get("conversation_package"),
    ]
    declared.extend(
        patch.get("source_bundle")
        for amendment in amendments
        for patch in [amendment.get("target_patch")]
        if isinstance(patch, dict)
    )
    for value in declared:
        if not isinstance(value, dict):
            continue
        manifest_value = value.get("manifest_path")
        if not isinstance(manifest_value, str) or not manifest_value:
            continue
        try:
            manifest = _resolve_inside(root, manifest_value)
        except ProjectionError:
            continue
        capture_specific = (
            manifest.parent / "legacy-archive-pointers" / f"{capture_id}.json"
        )
        if capture_specific.is_file():
            result.append(capture_specific)
            continue
        single = manifest.parent / "legacy-archive-pointer.json"
        if single.is_file() and not single.is_symlink():
            try:
                owner = _load_json(single, tracker).get("capture_event_id")
            except ProjectionError:
                continue
            if owner == capture_id:
                result.append(single)
    return result


def _math_local_archive_complete(
    *,
    root: Path,
    capture_id: str,
    capture: dict[str, Any],
    amendments: list[dict[str, Any]],
    closeout_id: str,
    receipts: dict[str, dict[str, Any]],
    tracker: SourceTracker,
) -> bool:
    reference = _math_effective_package_reference(capture, amendments)
    if isinstance(reference, dict):
        try:
            manifest_path = _resolve_inside(root, reference.get("manifest_path"))
            manifest = _load_json(manifest_path, tracker)
        except ProjectionError:
            manifest = {}
        if manifest.get("schema_version") == "math-conversation-package-v1":
            return _math_conversation_archive_complete(
                root=root,
                capture_id=capture_id,
                capture=capture,
                amendments=amendments,
                closeout_id=closeout_id,
                receipts=receipts,
                tracker=tracker,
            )
    pointer_paths = [
        path
        for path in _math_capture_pointer_candidates(
            root, capture_id, capture, amendments, tracker
        )
        if path.is_file() and not path.is_symlink()
    ]
    if len(pointer_paths) != 1:
        return False
    try:
        pointer_path = pointer_paths[0]
        pointer = _load_json(pointer_path, tracker)
        pointer_schema = pointer.get("schema_version")
        if (
            pointer_schema not in MATH_LEGACY_ARCHIVE_POINTER_SCHEMAS
            or pointer.get("capture_event_id") != capture_id
            or pointer.get("closeout_id") != closeout_id
            or pointer.get("archive_status") != "verified"
            or pointer.get("canonical_package") is not False
            or pointer.get("archive_volume") != "T9-Data"
        ):
            return False
        pointer_relative = pointer_path.resolve().relative_to(root.resolve()).as_posix()
        if (
            pointer.get("local_pointer_path") is not None
            and pointer.get("local_pointer_path") != pointer_relative
        ):
            return False
        cleanup_proof = pointer.get("cleanup_proof")
        if (
            not isinstance(cleanup_proof, dict)
            or cleanup_proof.get("schema_version")
            != "math-legacy-source-cleanup-proof-v1"
            or cleanup_proof.get("target_capture_event_id") != capture_id
            or not isinstance(cleanup_proof.get("items"), list)
        ):
            return False
        if pointer.get("evidence_mode") == "ledger_only":
            if (
                pointer.get("cleanup_intent") != "none_ledger_only"
                or cleanup_proof.get("overall_decision")
                != "not_applicable_ledger_only"
                or cleanup_proof.get("items") != []
            ):
                return False
        elif pointer.get("cleanup_intent") != (
            "remove_local_source_artifacts_after_verified_legacy_archive_locator_receipt"
        ):
            return False
        receipt = receipts.get(str(pointer.get("archive_receipt_id") or ""))
        expected_receipt_schema = (
            "math-legacy-evidence-archive-receipt-v2"
            if pointer_schema == "math-legacy-evidence-archive-pointer-v2"
            else "math-legacy-evidence-archive-receipt-v1"
        )
        if (
            not isinstance(receipt, dict)
            or receipt.get("schema_version") != expected_receipt_schema
            or receipt.get("capture_event_id") != capture_id
            or receipt.get("closeout_id") != closeout_id
            or receipt.get("legacy_package_id") != pointer.get("legacy_package_id")
            or receipt.get("archive_intent_id") != pointer.get("archive_intent_id")
            or receipt.get("authorization") != pointer.get("authorization")
            or receipt.get("cleanup_proof") != cleanup_proof
            or receipt.get("archive_status") != "verified"
            or receipt.get("obsidian_path_status") != "verified"
            or any(
                receipt.get(field) != pointer.get(field)
                for field in (
                    "raw_archive_relpath",
                    "raw_archive_manifest_sha256",
                    "raw_archive_package_sha256",
                    "archive_tree_sha256",
                    "obsidian_locator_path",
                    "obsidian_locator_sha256",
                )
            )
        ):
            return False
        if pointer_schema == "math-legacy-evidence-archive-pointer-v2":
            if (
                pointer.get("pending_component") is not None
                or receipt.get("pending_component") is not None
                or any(
                    pointer.get(key) != receipt.get(key)
                    for key in MATH_DISPLAY_CLOSURE_KEYS
                )
                or not _math_verified_local_file(
                    root,
                    pointer.get("display_closure_receipt_path"),
                    pointer.get("display_closure_receipt_sha256"),
                    tracker,
                )
            ):
                return False
        intent_path = (
            root
            / "数学一回滚复习系统/原始会话归档意图"
            / f"{pointer.get('legacy_package_id')}.json"
        )
        intent = _load_json(intent_path, tracker)
        if (
            intent.get("schema_version")
            != "math-legacy-evidence-archive-intent-v1"
            or intent.get("intent_id") != pointer.get("archive_intent_id")
            or intent.get("authorization") != pointer.get("authorization")
            or intent.get("capture_event_id") != capture_id
            or intent.get("closeout_id") != closeout_id
        ):
            return False
        locator_value = pointer.get("obsidian_locator_path")
        locator_sha = pointer.get("obsidian_locator_sha256")
        if not isinstance(locator_value, str) or not SHA256_RE.fullmatch(
            str(locator_sha or "")
        ):
            return False
        locator = _resolve_inside(root, locator_value)
        locator_raw = tracker.add_file(locator)
        if hashlib.sha256(locator_raw).hexdigest() != locator_sha:
            return False
        return True
    except (OSError, ValueError, ProjectionError):
        return False


def _math_live_subject(config: ProjectorConfig, cutoff_date: str) -> dict[str, Any]:
    tracker = SourceTracker()
    ledger = config.math_root / "数学一回滚复习系统" / "快速入库事件.jsonl"
    raw_events = _load_jsonl(ledger, tracker)
    script = config.math_root / "数学一回滚复习系统/scripts/quick_intake.py"
    if script.is_file():
        tracker.add_file(script)
        try:
            state = _load_module("quick_board_math_live_replay", script).replay(
                raw_events
            )
        except Exception:
            state = _math_fallback_replay(raw_events)
    else:
        state = _math_fallback_replay(raw_events)
    receipts = _math_local_archive_receipts(config.math_root, tracker)
    planner_rows: list[dict[str, Any]] = []
    already_consumed = 0
    future_excluded = 0
    for capture_id, capture in state.get("captures", {}).items():
        study_date = capture.get("study_date")
        if not isinstance(study_date, str):
            continue
        if study_date > cutoff_date:
            future_excluded += 1
            continue
        target = capture.get("target") if isinstance(capture.get("target"), dict) else {}
        closeout_id = state.get("closed_by", {}).get(capture_id)
        status = "pending"
        amendments = list(state.get("amendments", {}).get(capture_id, []))
        package_reference = _math_effective_package_reference(capture, amendments)
        if isinstance(closeout_id, str):
            if _math_local_archive_complete(
                root=config.math_root,
                capture_id=capture_id,
                capture=capture,
                amendments=amendments,
                closeout_id=closeout_id,
                receipts=receipts,
                tracker=tracker,
            ):
                already_consumed += 1
                continue
            status = "legacy_archive_pending"
            if isinstance(package_reference, dict):
                try:
                    package_manifest = _load_json(
                        _resolve_inside(
                            config.math_root, package_reference.get("manifest_path")
                        ),
                        tracker,
                    )
                except ProjectionError:
                    package_manifest = {}
                if package_manifest.get("schema_version") == "math-conversation-package-v1":
                    status = "archive_pending"
        planner_rows.append(
            {
                "capture_event_id": capture_id,
                "study_date": study_date,
                "recorded_at": capture.get("recorded_at"),
                "package_id": package_reference.get("package_id")
                if isinstance(package_reference, dict)
                else None,
                "formal_id": target.get("formal_id"),
                "source_locator": target.get("source_locator"),
                "status": status,
            }
        )

    items: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    captures = state.get("captures", {})
    for row in planner_rows:
        capture_id = str(row.get("capture_event_id") or "")
        if not capture_id:
            continue
        capture = captures.get(capture_id, {})
        package_id = row.get("package_id")
        attachment_counts = _empty_attachment_counts()
        if isinstance(capture, dict):
            manifest, _ = _math_manifest(
                root=config.math_root,
                capture=capture,
                tracker=tracker,
                warnings=warnings,
            )
            if manifest:
                package_id = package_id or manifest.get("package_id")
                attachment_counts = _attachment_counts(manifest.get("artifacts", []))
        formal_id = row.get("formal_id")
        if formal_id:
            title = f"正式题 · {_short_identifier(formal_id)}"
        elif row.get("source_locator"):
            locator_hash = hashlib.sha256(
                str(row["source_locator"]).encode("utf-8")
            ).hexdigest()[:8]
            title = f"新来源 · {locator_hash}"
        else:
            title = f"数学待处理 · {_short_identifier(capture_id)}"
        status = str(row.get("status") or "failed")
        queue_class = "processable" if status in {"pending", "resume"} else "residual"
        source_status = (
            capture.get("initial_state")
            if isinstance(capture, dict)
            else status
        ) or status
        items.append(
            _live_item(
                subject="math",
                identity=capture_id,
                safe_title=title,
                study_date=row.get("study_date"),
                captured_at=row.get("recorded_at"),
                queue_class=queue_class,
                source_status=source_status,
                display_status=_live_display_status(status, queue_class),
                capture_id=capture_id,
                package_id=package_id,
                event_id=capture_id,
                attachment_counts=attachment_counts,
            )
        )
    tracker.verify_unchanged()
    return _live_subject(
        "math",
        items,
        already_consumed=already_consumed,
        future_excluded=future_excluded,
    )


def _cs408_local_archive_complete_ids(
    *,
    config: ProjectorConfig,
    state: dict[str, Any],
    ledger_rows: list[tuple[dict[str, Any], bytes]],
    tracker: SourceTracker,
) -> set[str]:
    by_event_id: dict[str, tuple[dict[str, Any], bytes]] = {}
    for event, raw in ledger_rows:
        event_id = str(event.get("event_id") or "")
        if event_id:
            if event_id in by_event_id:
                return set()
            by_event_id[event_id] = (event, raw)
    pointer_roots = [
        config.cs408_package_root / "legacy-archive-pointers",
        config.cs408_package_root / "legacy-verified/legacy-archive-pointers",
    ]
    verified: set[str] = set()
    conflicted: set[str] = set()
    pointer_paths = sorted(
        path
        for root in pointer_roots
        if root.is_dir() and not root.is_symlink()
        for path in root.glob("*.json")
    )
    for pointer_path in pointer_paths:
        try:
            if pointer_path.is_symlink() or not pointer_path.is_file():
                continue
            pointer = _load_json(pointer_path, tracker)
            capture_id = str(pointer.get("capture_id") or "")
            capture = state.get("captures", {}).get(capture_id)
            material = pointer.get("authorization_material")
            if (
                pointer.get("schema_version")
                != "cs408-legacy-evidence-archive-pointer-v1"
                or pointer.get("status") != "archive_verified"
                or pointer.get("canonical_package") is not False
                or not isinstance(capture, dict)
                or not isinstance(material, dict)
                or material.get("capture_id") != capture_id
                or material.get("capture_payload_sha256")
                != capture.get("payload_sha256")
                or pointer.get("authorization")
                != "CS408-LEGACY-ARCHIVE-" + _pretty_object_sha256(material)
                or pointer.get("source_files") != material.get("source_files")
                or not SHA256_RE.fullmatch(
                    str(pointer.get("archive_receipt_sha256") or "")
                )
            ):
                continue
            proof_files = material.get("proof_files")
            if not isinstance(proof_files, list):
                continue
            proof_events: dict[str, tuple[dict[str, Any], bytes]] = {}
            valid = True
            for role in (
                "canonical_capture_event",
                "canonical_result_event",
                "canonical_closeout_event",
            ):
                matches = [
                    row
                    for row in proof_files
                    if isinstance(row, dict) and row.get("role") == role
                ]
                if len(matches) != 1:
                    valid = False
                    break
                row = matches[0]
                event_id = str(row.get("event_id") or "")
                event_raw = by_event_id.get(event_id)
                if (
                    event_raw is None
                    or hashlib.sha256(event_raw[1]).hexdigest() != row.get("sha256")
                    or len(event_raw[1]) != row.get("byte_count")
                ):
                    valid = False
                    break
                proof_events[role] = event_raw
            if not valid:
                continue
            capture_event = proof_events["canonical_capture_event"][0]
            result_event, result_raw = proof_events["canonical_result_event"]
            close_event = proof_events["canonical_closeout_event"][0]
            formal_ids = [result_event.get("formal_id")] if result_event.get("formal_id") else []
            if (
                capture_event.get("capture_id") != capture_id
                or result_event.get("capture_id") != capture_id
                or result_event.get("outcome") != material.get("outcome")
                or formal_ids != material.get("formal_ids")
                or close_event.get("batch_id") != result_event.get("batch_id")
                or pointer.get("outcome") != material.get("outcome")
                or pointer.get("formal_ids") != material.get("formal_ids")
                or pointer.get("ledger_terminal_event_sha256")
                != hashlib.sha256(result_raw).hexdigest()
            ):
                continue
            locator_value = pointer.get("locator_note_relative_path")
            locator_sha = pointer.get("locator_note_sha256")
            if not isinstance(locator_value, str) or not SHA256_RE.fullmatch(
                str(locator_sha or "")
            ):
                continue
            locator = _resolve_inside(config.cs408_root, locator_value)
            locator_raw = tracker.add_file(locator)
            if hashlib.sha256(locator_raw).hexdigest() != locator_sha:
                continue
            if capture_id in verified:
                conflicted.add(capture_id)
            verified.add(capture_id)
        except (OSError, ValueError, ProjectionError):
            continue
    legacy_verified = verified - conflicted
    canonical_verified: set[str] = set()
    for capture_id, capture in state.get("captures", {}).items():
        if not isinstance(capture, dict):
            continue
        reference = _package_reference(capture)
        study_date = capture.get("study_date")
        if reference is None or not isinstance(study_date, str):
            continue
        package_id = reference["locator"].removeprefix(
            "cs408-conversation-package://"
        )
        if not SAFE_ID_RE.fullmatch(package_id):
            continue
        local_root = config.cs408_package_root / study_date / package_id
        pointer_path = local_root / "archive-pointer.json"
        if not pointer_path.is_file() or pointer_path.is_symlink():
            continue
        try:
            pointer = _load_json(pointer_path, tracker)
            current_sha = str(pointer.get("package_sha256") or "")
            if (
                pointer.get("schema_version") != "cs408-local-archive-pointer-v1"
                or pointer.get("status") != "local_heavy_content_cleaned"
                or pointer.get("package_id") != package_id
                or pointer.get("archive_volume") != "T9-Data"
                or pointer.get("pending_component") is not None
                or not SHA256_RE.fullmatch(current_sha)
                or not SHA256_RE.fullmatch(
                    str(pointer.get("formal_terminal_sha256") or "")
                )
            ):
                continue
            archived, archive_root = _verified_pointer(
                pointer_path=pointer_path,
                package_sha=current_sha,
                repo_root=config.cs408_root,
                t9_root=config.t9_root,
                tracker=tracker,
                expected_schema="cs408-local-archive-pointer-v1",
            )
            if not archived or archive_root is None:
                continue
            manifest = _load_json(archive_root / "manifest.json", tracker)
            receipt = _load_json(archive_root / "receipt.json", tracker)
            archive_receipt = _load_json(
                archive_root / "archive-receipt.json", tracker
            )
            formal_id = capture.get("formal_id")
            expected_formal_ids = [] if capture.get("quality_status") == "concept_only" else [formal_id] if formal_id else []
            if (
                manifest.get("package_id") != package_id
                or manifest.get("study_date") != study_date
                or manifest.get("canonical_sha256") != current_sha
                or receipt.get("package_id") != package_id
                or receipt.get("study_date") != study_date
                or receipt.get("canonical_sha256") != current_sha
                or receipt.get("formal_write_count") != 0
                or archive_receipt.get("schema_version")
                != "cs408-package-archive-receipt-v1"
                or archive_receipt.get("package_id") != package_id
                or archive_receipt.get("package_sha256") != current_sha
                or archive_receipt.get("formal_ids") != expected_formal_ids
                or archive_receipt.get("formal_terminal_outcome")
                != capture.get("quality_status")
                or archive_receipt.get("formal_terminal_sha256")
                != pointer.get("formal_terminal_sha256")
                or archive_receipt.get("status")
                != "ARCHIVED_AND_LOCATOR_BOUND"
            ):
                continue
            captured_sha = reference["package_sha256"]
            if captured_sha == current_sha:
                captured_manifest = manifest
                captured_receipt = receipt
            else:
                revision_root = archive_root / ".revisions" / captured_sha
                captured_manifest = _load_json(
                    revision_root / "manifest.json", tracker
                )
                captured_receipt = _load_json(
                    revision_root / "receipt.json", tracker
                )
            if (
                captured_manifest.get("package_id") != package_id
                or captured_manifest.get("study_date") != study_date
                or captured_manifest.get("canonical_sha256") != captured_sha
                or captured_receipt.get("package_id") != package_id
                or captured_receipt.get("study_date") != study_date
                or captured_receipt.get("canonical_sha256") != captured_sha
                or captured_receipt.get("formal_write_count") != 0
            ):
                continue
            canonical_verified.add(str(capture_id))
        except (OSError, ValueError, ProjectionError):
            continue
    return legacy_verified | canonical_verified


def _cs408_local_package_manifest(
    *,
    config: ProjectorConfig,
    reference: dict[str, str],
    study_date: str,
    tracker: SourceTracker,
) -> dict[str, Any] | None:
    package_id = reference["locator"].removeprefix("cs408-conversation-package://")
    root = config.cs408_package_root / study_date / package_id
    manifest_path = root / "manifest.json"
    receipt_path = root / "receipt.json"
    if not manifest_path.is_file() or not receipt_path.is_file():
        return None
    try:
        manifest = _load_json(manifest_path, tracker)
        receipt = _load_json(receipt_path, tracker)
        if (
            manifest.get("package_id") != package_id
            or manifest.get("study_date") != study_date
            or manifest.get("canonical_sha256") != reference["package_sha256"]
            or receipt.get("package_id") != package_id
            or receipt.get("study_date") != study_date
            or receipt.get("canonical_sha256") != reference["package_sha256"]
            or receipt.get("formal_write_count") != 0
        ):
            return None
        return manifest
    except ProjectionError:
        return None


def _cs408_live_subject(config: ProjectorConfig, cutoff_date: str) -> dict[str, Any]:
    tracker = SourceTracker()
    ledger_root = config.cs408_root / "wiki/study_vaults/408-full/state/intake-curation"
    ledger = ledger_root / "events.jsonl"
    raw_events = _load_jsonl(ledger, tracker)
    ledger_rows = _jsonl_rows_with_raw(ledger, tracker)
    script = config.cs408_root / "scripts/intake_fact_capture_408.py"
    state: dict[str, Any]
    if script.is_file():
        tracker.add_file(script)
        try:
            native = _load_module("quick_board_cs408_live_native", script)
            state = native.replay(raw_events)
        except Exception:
            state = _cs408_fallback_replay(raw_events)
    else:
        state = _cs408_fallback_replay(raw_events)
    verified_archives = _cs408_local_archive_complete_ids(
        config=config,
        state=state,
        ledger_rows=ledger_rows,
        tracker=tracker,
    )
    planner_rows: list[tuple[dict[str, Any], str]] = []
    already_consumed = 0
    administrative_terminal_excluded = 0
    future_excluded = 0
    terminal = {"curated", "already_current", "concept_only", "skipped", "skip"}
    for capture_id, capture in state.get("captures", {}).items():
        study_date = capture.get("study_date")
        status = str(capture.get("quality_status") or "pending_start")
        if not isinstance(study_date, str):
            continue
        if study_date > cutoff_date:
            future_excluded += 1
            continue
        if status == "user_removed":
            administrative_terminal_excluded += 1
            continue
        if status in terminal:
            if capture_id in verified_archives:
                already_consumed += 1
                continue
            planner_status = "legacy_archive_pending"
            queue_class = "residual"
        elif status == "needs_user":
            planner_status = "needs_user"
            queue_class = "residual"
        else:
            planner_status = "pending_start"
            queue_class = "processable"
        planner_rows.append(
            (
                {
                    "capture_id": capture_id,
                    "study_date": study_date,
                    "status": planner_status,
                    "package_locators": [],
                },
                queue_class,
            )
        )

    raw_event_ids = {
        str(row.get("capture_id")): row.get("event_id")
        for row in raw_events
        if row.get("event_type") == "fact_captured" and row.get("capture_id")
    }
    items: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    for row, queue_class in planner_rows:
        capture_id = str(row.get("capture_id") or "")
        if not capture_id:
            continue
        capture = state.get("captures", {}).get(capture_id, {})
        reference = _package_reference(capture) if isinstance(capture, dict) else None
        package_id = None
        attachment_counts = _empty_attachment_counts()
        if reference:
            package_id = reference["locator"].removeprefix(
                "cs408-conversation-package://"
            )
            manifest = _cs408_local_package_manifest(
                config=config,
                reference=reference,
                study_date=str(row.get("study_date") or ""),
                tracker=tracker,
            )
            if manifest:
                attachment_counts = _attachment_counts(manifest.get("attachments", []))
        if package_id is None:
            locators = row.get("package_locators")
            if isinstance(locators, list) and locators:
                locator = str(locators[0])
                package_id = locator.rsplit("/", 1)[-1].removeprefix(
                    "cs408-conversation-package://"
                )
        event_id = raw_event_ids.get(capture_id)
        status = str(row.get("status") or "damaged")
        captured_at = capture.get("recorded_at") if isinstance(capture, dict) else None
        source_status = (
            capture.get("quality_status")
            if isinstance(capture, dict)
            else None
        ) or status
        items.append(
            _live_item(
                subject="408",
                identity=capture_id,
                safe_title=f"408 待处理 · {_short_identifier(capture_id)}",
                study_date=row.get("study_date"),
                captured_at=captured_at,
                queue_class=queue_class,
                source_status=source_status,
                display_status=_live_display_status(status, queue_class),
                capture_id=capture_id,
                package_id=package_id,
                event_id=event_id,
                attachment_counts=attachment_counts,
            )
        )
    tracker.verify_unchanged()
    return _live_subject(
        "408",
        items,
        already_consumed=already_consumed,
        future_excluded=future_excluded,
        administrative_terminal_excluded=administrative_terminal_excluded,
    )


def _english_live_package_metadata(
    *, config: ProjectorConfig, row: dict[str, Any], tracker: SourceTracker
) -> tuple[Any, str, dict[str, int], str]:
    captured_at = None
    time_quality = "unknown"
    attachment_counts = _empty_attachment_counts()
    source_status = str(row.get("status") or "failed")
    path_value = row.get("path")
    if not path_value:
        return captured_at, time_quality, attachment_counts, source_status
    package_root = Path(str(path_value)).expanduser().resolve()
    allowed_root = (config.english_root / "intake/packages").resolve()
    try:
        package_root.relative_to(allowed_root)
    except ValueError as exc:
        raise ProjectionError("english live package path escaped intake root") from exc
    manifest_path = package_root / "manifest.json"
    receipt_path = package_root / "receipt.json"
    if manifest_path.is_file():
        manifest = _load_json(manifest_path, tracker)
        attachment_counts = _attachment_counts(manifest.get("attachments", []))
    if receipt_path.is_file():
        receipt = _load_json(receipt_path, tracker)
        captured_at, time_quality = _captured_at(receipt.get("captured_at"))
        if (
            receipt.get("schema_version") not in {"english_package_receipt_v2", "english_package_receipt_v3"}
            and captured_at is not None
        ):
            time_quality = "activity_time_only"
        source_status = str(receipt.get("status") or source_status)
    return captured_at, time_quality, attachment_counts, source_status


def _english_live_subject(config: ProjectorConfig, cutoff_date: str) -> dict[str, Any]:
    tracker = SourceTracker()
    state_dir = config.english_root / "intake"
    backlog_module = _english_backlog_module(config.english_root)
    if backlog_module is not None:
        module_path = config.english_root / "english_pipeline/backlog.py"
        tracker.add_file(module_path)
        try:
            _, plan = backlog_module.build_backlog_plan(
                state_dir,
                config.english_root,
                cutoff_date=cutoff_date,
                persist=False,
            )
        except Exception as exc:
            raise ProjectionError(f"english live backlog planner rejected state: {exc}") from exc
        package_rows = [
            row
            for row in [*plan.get("packages", []), *plan.get("waiting_web_review", [])]
            if row.get("status") != "already_consumed"
        ]
        all_legacy_rows = list(
            (plan.get("legacy_reachability_audit") or {}).get("events", [])
        )
        administrative_terminal_excluded = sum(
            str(row.get("status") or "") == "user_removed"
            for row in all_legacy_rows
        )
        legacy_rows = [
            row
            for row in all_legacy_rows
            if str(row.get("status") or "") != "user_removed"
        ]
        already_consumed = sum(
            row.get("status") == "already_consumed"
            for row in plan.get("packages", [])
        )
        future_excluded = len(plan.get("future_excluded", []))
    else:
        try:
            native = _english_module(config.english_root)
            processed = native.processed_package_sha256s(state_dir)
        except Exception as exc:
            raise ProjectionError(f"english live fallback replay failed: {exc}") from exc
        package_rows = []
        already_consumed = 0
        administrative_terminal_excluded = 0
        future_excluded = 0
        for package_root in sorted((state_dir / "packages").glob("*/*")):
            if not package_root.is_dir():
                continue
            study_date = package_root.parent.name
            if study_date > cutoff_date:
                future_excluded += 1
                continue
            try:
                manifest = native.validate_conversation_package(package_root)
                package_sha = manifest.get("package_canonical_sha256")
                if package_sha in processed:
                    already_consumed += 1
                    continue
                status = "pending"
            except Exception:
                package_sha = None
                status = "failed"
            package_rows.append(
                {
                    "package_id": package_root.name,
                    "study_date": study_date,
                    "path": str(package_root),
                    "status": status,
                    "package_sha256": package_sha,
                }
            )
        legacy_rows = [
            {
                "event_id": path.stem,
                "study_date": path.parent.name,
                "status": "needs_user",
                "attachment_count": 0,
            }
            for path in sorted((state_dir / "events").glob("*/*.json"))
            if path.parent.name <= cutoff_date
        ]

    items: list[dict[str, Any]] = []
    for row in package_rows:
        package_id = str(row.get("package_id") or "")
        if not package_id:
            continue
        status = str(row.get("status") or "failed")
        queue_class = (
            "waiting_web_review"
            if status == "waiting_web_review"
            else "processable"
            if status in {"pending", "recovery_required"}
            else "residual"
        )
        captured_at, time_quality, attachment_counts, source_status = (
            _english_live_package_metadata(
                config=config,
                row=row,
                tracker=tracker,
            )
        )
        items.append(
            _live_item(
                subject="english",
                identity=f"package:{package_id}",
                safe_title=f"英语会话包 · {_short_identifier(package_id)}",
                study_date=row.get("study_date"),
                captured_at=captured_at,
                queue_class=queue_class,
                source_status=source_status,
                display_status=_live_display_status(status, queue_class),
                package_id=package_id,
                attachment_counts=attachment_counts,
                time_quality=time_quality,
            )
        )
    for row in legacy_rows:
        event_id = str(row.get("event_id") or "")
        if not event_id:
            continue
        status = str(row.get("status") or "needs_user")
        queue_class = (
            "processable"
            if status == "eligible-for-explicit-migration"
            else "residual"
        )
        attachment_counts = _empty_attachment_counts()
        attachment_counts["other_attachment"] = int(row.get("attachment_count") or 0)
        items.append(
            _live_item(
                subject="english",
                identity=f"legacy:{event_id}",
                safe_title=f"英语历史事件 · {_short_identifier(event_id)}",
                study_date=row.get("study_date"),
                captured_at=None,
                queue_class=queue_class,
                source_status=status,
                display_status=(
                    "legacy_migration" if queue_class == "processable" else "needs_user"
                ),
                event_id=event_id,
                attachment_counts=attachment_counts,
            )
        )
    tracker.verify_unchanged()
    return _live_subject(
        "english",
        items,
        already_consumed=already_consumed,
        future_excluded=future_excluded,
        administrative_terminal_excluded=administrative_terminal_excluded,
    )


def build_live_snapshot(
    config: ProjectorConfig, cutoff_date: str | None = None
) -> dict[str, Any]:
    """Build a read-only cross-date view of every untrusted-terminal item.

    The snapshot intentionally exposes identifiers and small operational
    metadata only. Titles never depend on question, option, explanation,
    passage, or conversation bodies.
    """

    cutoff = _validate_date(
        cutoff_date or datetime.now(SHANGHAI).date().isoformat()
    )
    subjects = {
        "408": _cs408_live_subject(config, cutoff),
        "math": _math_live_subject(config, cutoff),
        "english": _english_live_subject(config, cutoff),
    }
    revision_material = {
        "schema_version": LIVE_SCHEMA_VERSION,
        "update_mode": "live_backlog_stream",
        "cutoff_date": cutoff,
        "subject_order": list(LIVE_SUBJECT_ORDER),
        "subjects": subjects,
    }
    return {
        "schema_version": LIVE_SCHEMA_VERSION,
        "generated_at": _generated_at(),
        "revision": _object_sha256(revision_material),
        "update_mode": "live_backlog_stream",
        "cutoff_date": cutoff,
        "subject_order": list(LIVE_SUBJECT_ORDER),
        "subjects": subjects,
    }


def _stat_signature(path: Path) -> list[Any]:
    resolved = path.expanduser().resolve()
    try:
        stat = resolved.lstat()
    except OSError:
        return [str(resolved), "missing"]
    return [
        str(resolved),
        stat.st_mode,
        stat.st_dev,
        stat.st_ino,
        stat.st_size,
        stat.st_mtime_ns,
    ]


def _tree_signature(root: Path, *, max_depth: int) -> list[list[Any]]:
    resolved = root.expanduser().resolve()
    rows = [_stat_signature(resolved)]
    if not resolved.is_dir() or resolved.is_symlink():
        return rows
    stack: list[tuple[Path, int]] = [(resolved, 0)]
    while stack:
        current, depth = stack.pop()
        if depth >= max_depth:
            continue
        try:
            entries = sorted(os.scandir(current), key=lambda entry: entry.name)
        except OSError:
            rows.append([str(current), "unreadable"])
            continue
        for entry in entries:
            path = Path(entry.path)
            rows.append(_stat_signature(path))
            try:
                if entry.is_dir(follow_symlinks=False):
                    stack.append((path, depth + 1))
            except OSError:
                continue
    return rows


def _glob_signature(root: Path, pattern: str) -> list[list[Any]]:
    resolved = root.expanduser().resolve()
    rows = [_stat_signature(resolved)]
    if not resolved.is_dir() or resolved.is_symlink():
        return rows
    try:
        paths = sorted(resolved.glob(pattern))
    except OSError:
        return rows
    rows.extend(_stat_signature(path) for path in paths)
    return rows


def live_subject_source_signatures(
    config: ProjectorConfig, cutoff_date: str | None = None
) -> dict[str, str]:
    cutoff = _validate_date(
        cutoff_date or datetime.now(SHANGHAI).date().isoformat()
    )
    math_state = config.math_root / "数学一回滚复习系统"
    cs408_state = config.cs408_root / "wiki/study_vaults/408-full/state/intake-curation"
    english_state = config.english_root / "intake"
    material: dict[str, list[Any]] = {
        "math": [
            cutoff,
            _stat_signature(math_state / "快速入库事件.jsonl"),
            _stat_signature(math_state / "原始会话归档回执.jsonl"),
            _stat_signature(math_state / "scripts/quick_intake.py"),
        ],
        "408": [
            cutoff,
            _stat_signature(cs408_state / "events.jsonl"),
            _stat_signature(config.cs408_root / "scripts/intake_fact_capture_408.py"),
            _tree_signature(
                config.cs408_package_root / "legacy-archive-pointers", max_depth=2
            ),
            _tree_signature(
                config.cs408_package_root
                / "legacy-verified/legacy-archive-pointers",
                max_depth=2,
            ),
            _glob_signature(
                config.cs408_package_root,
                "????-??-??/*/archive-intent.json",
            ),
            _glob_signature(
                config.cs408_package_root,
                "????-??-??/*/archive-pointer.json",
            ),
        ],
        "english": [
            cutoff,
            _stat_signature(config.english_root / "english_pipeline/backlog.py"),
            _stat_signature(config.english_root / "english_pipeline/packages.py"),
            _tree_signature(english_state / "packages", max_depth=3),
            _tree_signature(english_state / "events", max_depth=2),
            _tree_signature(english_state / "receipts", max_depth=3),
            _tree_signature(english_state / "nightly", max_depth=3),
            _tree_signature(
                english_state / "legacy-event-retirements", max_depth=2
            ),
        ],
    }
    return {
        subject: _object_sha256(rows)
        for subject, rows in material.items()
    }


class LiveSnapshotBuilder:
    """Cache unchanged subjects while preserving fail-closed source replay."""

    def __init__(self, config: ProjectorConfig | None = None) -> None:
        self.config = config or ProjectorConfig()
        self._lock = threading.Lock()
        self._subjects: dict[str, dict[str, Any]] = {}
        self._successful_signatures: dict[str, str] = {}
        self._last_success_at: dict[str, str] = {}
        self._errors: set[str] = set()

    def change_token(
        self, config: ProjectorConfig | None = None, cutoff_date: str | None = None
    ) -> str:
        active = config if isinstance(config, ProjectorConfig) else self.config
        return _object_sha256(
            live_subject_source_signatures(active, cutoff_date=cutoff_date)
        )

    @staticmethod
    def _unavailable_subject(subject: str) -> dict[str, Any]:
        return _live_subject(
            subject,
            [],
            already_consumed=0,
            future_excluded=0,
        )

    def __call__(
        self, config: ProjectorConfig | None = None, cutoff_date: str | None = None
    ) -> dict[str, Any]:
        active = config if isinstance(config, ProjectorConfig) else self.config
        cutoff = _validate_date(
            cutoff_date or datetime.now(SHANGHAI).date().isoformat()
        )
        builders = {
            "408": _cs408_live_subject,
            "math": _math_live_subject,
            "english": _english_live_subject,
        }
        with self._lock:
            before = live_subject_source_signatures(active, cutoff)
            errors: dict[str, dict[str, str]] = {}
            subjects: dict[str, dict[str, Any]] = {}
            for subject in LIVE_SUBJECT_ORDER:
                should_build = (
                    subject not in self._subjects
                    or self._successful_signatures.get(subject) != before[subject]
                    or subject in self._errors
                )
                if should_build:
                    try:
                        candidate = builders[subject](active, cutoff)
                        after = live_subject_source_signatures(active, cutoff)[subject]
                        if after != before[subject]:
                            raise ProjectionError(
                                f"{subject} source changed during live projection"
                            )
                    except Exception:
                        self._errors.add(subject)
                        candidate = copy.deepcopy(
                            self._subjects.get(
                                subject, self._unavailable_subject(subject)
                            )
                        )
                        status = "stale" if subject in self._subjects else "unavailable"
                        candidate["freshness"] = {
                            "status": status,
                            "last_success_at": self._last_success_at.get(subject),
                            "error_code": "subject_projection_failed",
                        }
                        errors[subject] = {
                            "status": status,
                            "code": "subject_projection_failed",
                        }
                    else:
                        success_at = _generated_at()
                        candidate = copy.deepcopy(candidate)
                        candidate["freshness"] = {
                            "status": "fresh",
                            "last_success_at": success_at,
                            "error_code": None,
                        }
                        self._subjects[subject] = copy.deepcopy(candidate)
                        self._successful_signatures[subject] = after
                        self._last_success_at[subject] = success_at
                        self._errors.discard(subject)
                else:
                    candidate = copy.deepcopy(self._subjects[subject])
                subjects[subject] = candidate

            for subject in sorted(self._errors):
                if subject not in errors:
                    status = "stale" if subject in self._subjects else "unavailable"
                    errors[subject] = {
                        "status": status,
                        "code": "subject_projection_failed",
                    }
                    subjects[subject]["freshness"] = {
                        "status": status,
                        "last_success_at": self._last_success_at.get(subject),
                        "error_code": "subject_projection_failed",
                    }
            revision_material = {
                "schema_version": LIVE_SCHEMA_VERSION,
                "update_mode": "live_backlog_stream",
                "cutoff_date": cutoff,
                "subject_order": list(LIVE_SUBJECT_ORDER),
                "subjects": subjects,
                "projection_errors": errors,
            }
            return {
                "schema_version": LIVE_SCHEMA_VERSION,
                "generated_at": _generated_at(),
                "revision": _object_sha256(revision_material),
                "update_mode": "live_backlog_stream",
                "refresh_interval_seconds": 1,
                "cutoff_date": cutoff,
                "subject_order": list(LIVE_SUBJECT_ORDER),
                "subjects": subjects,
                "projection_errors": errors,
            }


SUBJECT_OUTPUTS = {
    "408": Path("wiki/operation_center/quick-intake-today.json"),
    "math": Path("错题知识网络/wiki/operation_center/quick-intake-today.json"),
    "english": Path("wiki/operation_center/quick-intake-today.json"),
}
SUMMARY_OUTPUTS = {
    "408": Path("wiki/operation_center/dashboard-summary.json"),
    "math": Path("错题知识网络/wiki/operation_center/dashboard-summary.json"),
    "english": Path("wiki/operation_center/dashboard-summary.json"),
}


def _atomic_write(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _json_output(value: Any) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode(
        "utf-8"
    )


def _compact_summary(projection: dict[str, Any], output_path: Path) -> dict[str, Any]:
    return {
        "schema_version": "quick-intake-summary-v1",
        "subject": projection["subject"],
        "study_date": projection["study_date"],
        "generated_at": projection["generated_at"],
        "projection_status": projection["projection_status"],
        "projection_path": str(output_path),
        "source_snapshot_sha256": projection["source_snapshot"]["snapshot_sha256"],
        "counts": projection["counts"],
        "backlog_through_date": projection["backlog_through_date"],
        "warning_count": sum(int(row.get("count", 0)) for row in projection["warnings"]),
    }


def _updated_summary(path: Path, compact: dict[str, Any]) -> dict[str, Any]:
    if not path.is_file():
        raise ProjectionError(f"dashboard-summary.json is missing: {path}")
    try:
        current = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ProjectionError(f"dashboard-summary.json is unreadable: {path}") from exc
    if not isinstance(current, dict):
        raise ProjectionError(f"dashboard-summary.json is not an object: {path}")
    return {**current, "quick_intake": compact}


def _status_label(status: str) -> str:
    return {
        "pending": "待正式入库",
        "formalized": "已正式处理",
        "archived": "已归档 T9",
        "archive_pending": "归档待恢复",
        "needs_user": "需补充",
    }.get(status, "状态未记录")


def _subject_heading(subject: str, counts: dict[str, int]) -> str:
    if subject == "408":
        return f"408 · 题目 {counts['unique_question_count']} · 包 {counts['package_count']}"
    if subject == "math":
        return (
            f"数学 · 保守题目 {counts['conservative_unique_target_count']} "
            f"· Capture {counts['capture_count']}"
        )
    return (
        f"英语 · 题目 {counts['question_count']} · "
        f"学习单元 {counts['learning_unit_count']} · 会话段 {counts['segment_count']}"
    )


def _time_label(value: str | None) -> str:
    if value is None:
        return "时间未记录"
    try:
        return datetime.fromisoformat(value).strftime("%H:%M")
    except ValueError:
        return "时间未记录"


def _short(value: Any) -> str:
    text = str(value or "")
    if not text:
        return "未记录"
    return text if len(text) <= 24 else f"{text[:12]}…{text[-8:]}"


def _render_card(item: dict[str, Any]) -> str:
    counts = item.get("attachment_counts") or {}
    attachment_total = sum(int(value) for value in counts.values())
    metadata = [
        ("Capture", _short(item.get("capture_id"))),
        ("Package", _short(item.get("package_id"))),
        ("Package SHA", _short(item.get("package_sha256"))),
        ("来源状态", str(item.get("source_status") or "unknown")),
        ("附件", str(attachment_total)),
        ("时间质量", str(item.get("time_quality") or "unknown")),
    ]
    rows = "".join(
        f"<dt>{html.escape(label)}</dt><dd>{html.escape(value)}</dd>"
        for label, value in metadata
    )
    status = str(item.get("display_status") or "unknown")
    return (
        '<article class="timeline-card">'
        '<span class="timeline-dot" aria-hidden="true"></span>'
        f'<p class="capture-time">{html.escape(_time_label(item.get("captured_at")))}</p>'
        f'<h3>{html.escape(str(item.get("safe_title") or "未命名条目"))}</h3>'
        f'<span class="status-chip status-{html.escape(status)}">{html.escape(_status_label(status))}</span>'
        '<details><summary>展开元数据</summary><dl>'
        f"{rows}</dl></details></article>"
    )


def _render_column(subject: str, projection: dict[str, Any]) -> str:
    warnings = projection.get("warnings") or []
    warning_html = ""
    if warnings:
        warning_rows = "".join(
            f"<li><span>{html.escape(str(row.get('message') or ''))}</span>"
            f"<b aria-label=\"数量\">{int(row.get('count', 0))}</b></li>"
            for row in warnings
        )
        warning_html = (
            '<aside class="warning-box" role="status"><p>数据警告</p>'
            f"<ul>{warning_rows}</ul></aside>"
        )
    items = projection.get("items") or []
    cards = "".join(_render_card(item) for item in items)
    if not items:
        cards = (
            '<div class="empty-state"><p>今日暂无可验证的快速入库条目。</p>'
            '<span>运行页面上方的刷新命令后，快照会被整体替换。</span></div>'
        )
    pending = int(projection["counts"].get("pending_count", 0))
    backlog = projection["backlog_through_date"]
    historical = int(backlog.get("unconsumed_count", 0))
    processable = int(backlog.get("processable_pending_count", 0))
    residual = int(backlog.get("residual_count", 0))
    earliest = backlog.get("earliest_study_date") or "无"
    return (
        f'<section class="subject-column subject-{html.escape(subject)}" aria-labelledby="heading-{html.escape(subject)}">'
        '<header class="column-header">'
        f'<h2 id="heading-{html.escape(subject)}">{html.escape(_subject_heading(subject, projection["counts"]))}</h2>'
        f'<p>今日待正式入库 {pending} · 截止日未可信终态 {historical} '
        f'（可处理 {processable} · 残留 {residual}） · 最早 {html.escape(str(earliest))} '
        f'· 最后刷新 {html.escape(projection["generated_at"][11:16])}</p>'
        f'<span class="projection-state">{html.escape(projection["projection_status"])}</span>'
        "</header>"
        f"{warning_html}<div class=\"timeline\">{cards}</div></section>"
    )


def render_index(
    *, combined: dict[str, Any], template_text: str, refresh_command: str
) -> str:
    columns = "".join(
        _render_column(subject, combined["subjects"][subject])
        for subject in ("408", "math", "english")
    )
    warning_total = sum(
        sum(int(row.get("count", 0)) for row in combined["subjects"][subject]["warnings"])
        for subject in ("408", "math", "english")
    )
    backlog_total = sum(
        int(combined["subjects"][subject]["backlog_through_date"].get("unconsumed_count", 0))
        for subject in ("408", "math", "english")
    )
    if backlog_total:
        global_note = (
            f"今日卡片只显示当日；截至今日三科共有 {backlog_total} 条未可信终态记录。"
        )
    elif warning_total:
        global_note = f"当前快照包含 {warning_total} 条警告；请展开对应学科核对。"
    else:
        global_note = "三科权威来源均已按当日口径投影，且截至今日无未可信终态记录。"
    replacements = {
        "{{STUDY_DATE}}": html.escape(combined["study_date"]),
        "{{GENERATED_AT}}": html.escape(combined["generated_at"]),
        "{{GLOBAL_NOTE}}": html.escape(global_note),
        "{{COLUMNS}}": columns,
        "{{REFRESH_COMMAND_JSON}}": json.dumps(refresh_command, ensure_ascii=False),
        "{{REFRESH_COMMAND_TEXT}}": html.escape(refresh_command),
        "{{COMBINED_JSON}}": (
            json.dumps(combined, ensure_ascii=False, separators=(",", ":"))
            .replace("&", "\\u0026")
            .replace("<", "\\u003c")
            .replace(">", "\\u003e")
        ),
    }
    rendered = template_text
    for token, value in replacements.items():
        rendered = rendered.replace(token, value)
    if re.search(r"\{\{[A-Z_]+\}\}", rendered):
        raise ProjectionError("HTML template has unresolved tokens")
    return rendered


def write_outputs(
    *,
    config: ProjectorConfig,
    projections: dict[str, dict[str, Any]],
    project_root: Path,
    template_path: Path,
    refresh_command: str,
) -> dict[str, Any]:
    generated_at = projections["408"]["generated_at"]
    study_date = projections["408"]["study_date"]
    combined = {
        "schema_version": "three-subject-quick-intake-board-v1",
        "study_date": study_date,
        "timezone": "Asia/Shanghai",
        "generated_at": generated_at,
        "projection_mode": "manual_full_snapshot_replacement",
        "subject_order": ["408", "math", "english"],
        "subjects": projections,
    }
    roots = {
        "408": config.cs408_root,
        "math": config.math_root,
        "english": config.english_root,
    }
    staged: dict[Path, bytes] = {}
    for subject in ("408", "math", "english"):
        projection_path = roots[subject] / SUBJECT_OUTPUTS[subject]
        summary_path = roots[subject] / SUMMARY_OUTPUTS[subject]
        compact = _compact_summary(projections[subject], projection_path)
        staged[projection_path] = _json_output(projections[subject])
        staged[summary_path] = _json_output(_updated_summary(summary_path, compact))
    combined_path = project_root / "data/quick-intake-today.json"
    index_path = project_root / "index.html"
    staged[combined_path] = _json_output(combined)
    template_text = template_path.read_text(encoding="utf-8")
    staged[index_path] = render_index(
        combined=combined,
        template_text=template_text,
        refresh_command=refresh_command,
    ).encode("utf-8")
    # All documents are fully built before the first replacement. Each target
    # then uses same-directory fsync + os.replace, so consumers never see a
    # half-written JSON or HTML file.
    for path, raw in staged.items():
        _atomic_write(path, raw)
    return {
        "study_date": study_date,
        "generated_at": generated_at,
        "outputs": [str(path) for path in staged],
        "counts": {subject: projections[subject]["counts"] for subject in projections},
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Project today's read-only math/408/English quick-intake board."
    )
    parser.add_argument("--date", dest="study_date")
    parser.add_argument("--math-root", type=Path, default=ProjectorConfig.math_root)
    parser.add_argument("--cs408-root", type=Path, default=ProjectorConfig.cs408_root)
    parser.add_argument(
        "--cs408-package-root", type=Path, default=ProjectorConfig.cs408_package_root
    )
    parser.add_argument("--english-root", type=Path, default=ProjectorConfig.english_root)
    parser.add_argument("--t9-root", type=Path, default=ProjectorConfig.t9_root)
    parser.add_argument(
        "--project-root", type=Path, default=Path(__file__).resolve().parents[1]
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    study_date = args.study_date or datetime.now(SHANGHAI).date().isoformat()
    config = ProjectorConfig(
        math_root=args.math_root,
        cs408_root=args.cs408_root,
        cs408_package_root=args.cs408_package_root,
        english_root=args.english_root,
        t9_root=args.t9_root,
    )
    projections = build_all(config, study_date)
    if args.dry_run:
        print(json.dumps(projections, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    project_root = args.project_root.resolve()
    command = (
        f"cd {json.dumps(str(project_root), ensure_ascii=False)} && "
        "PYTHONDONTWRITEBYTECODE=1 python3 scripts/project_quick_intake_today.py"
    )
    result = write_outputs(
        config=config,
        projections=projections,
        project_root=project_root,
        template_path=project_root / "templates/index.template.html",
        refresh_command=command,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

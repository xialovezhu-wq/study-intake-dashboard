from __future__ import annotations

import json
import hashlib
import unittest
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from tests import test_project_quick_intake_today as fixture_module


PRIVATE_MARKER = fixture_module.PRIVATE_MARKER
PROJECTOR = fixture_module.PROJECTOR
STUDY_DATE = fixture_module.STUDY_DATE
write_json = fixture_module.write_json
write_jsonl = fixture_module.write_jsonl


FUTURE_DATE = (date.fromisoformat(STUDY_DATE) + timedelta(days=1)).isoformat()


class LiveProjectionFixtureTest(unittest.TestCase):
    def test_concept_only_is_terminal_but_still_requires_verified_archive(self) -> None:
        state={"captures":{"CAP-concept":{"study_date":STUDY_DATE,"quality_status":"concept_only",
               "formal_id":None,"capture":{"stable_evidence_refs":[]}}}}
        with mock.patch.object(PROJECTOR,"_cs408_fallback_replay",return_value=state), mock.patch.object(
                PROJECTOR,"_cs408_local_archive_complete_ids",return_value={"CAP-concept"}):
            result=PROJECTOR._cs408_live_subject(self.config,STUDY_DATE)
            self.assertEqual(0,result['counts']['item_count'])
            self.assertEqual(1,result['counts']['already_consumed_excluded_count'])
        with mock.patch.object(PROJECTOR,"_cs408_fallback_replay",return_value=state), mock.patch.object(
                PROJECTOR,"_cs408_local_archive_complete_ids",return_value=set()):
            result=PROJECTOR._cs408_live_subject(self.config,STUDY_DATE)
            self.assertEqual(0,result['counts']['processable_count'])
            self.assertEqual(1,result['counts']['residual_count'])

    def setUp(self) -> None:
        fixture = fixture_module.QuickIntakeProjectorFixtureTest(methodName="runTest")
        fixture.setUp()
        self.addCleanup(fixture.tearDown)
        self.fixture = fixture
        self.config = fixture.config
        self._extend_math_fixture()
        self._extend_cs408_fixture()
        self._extend_english_fixture()

    def _extend_math_fixture(self) -> None:
        ledger = self.fixture.math / "数学一回滚复习系统/快速入库事件.jsonl"
        rows = [json.loads(line) for line in ledger.read_text().splitlines() if line]
        rows.append(
            {
                "schema_version": "math-fast-intake-ledger-v1",
                "event_id": "MFI-CAP-" + "F" * 24,
                "event_type": "capture",
                "study_date": FUTURE_DATE,
                "recorded_at": f"{FUTURE_DATE}T08:00:00+08:00",
                "initial_state": "pending_nightly",
                "target": {"kind": "new_source", "source_locator": PRIVATE_MARKER},
            }
        )
        write_jsonl(ledger, rows)

    def _extend_cs408_fixture(self) -> None:
        ledger = (
            self.fixture.cs408
            / "wiki/study_vaults/408-full/state/intake-curation/events.jsonl"
        )
        rows = [json.loads(line) for line in ledger.read_text().splitlines() if line]
        next(
            row
            for row in rows
            if row.get("capture_id") == "CAP-20260827-AAAA"
        )["payload_sha256"] = "a" * 64
        rows.extend(
            [
                {
                    "schema": "intake_fact_capture_event_v1",
                    "event_id": "CE-RESULT-AAAA",
                    "event_type": "curation_item_result",
                    "batch_id": "CUR-AAAA",
                    "capture_id": "CAP-20260827-AAAA",
                    "outcome": "curated",
                    "formal_id": "GS-999",
                },
                {
                    "schema": "intake_fact_capture_event_v1",
                    "event_id": "CE-CLOSE-AAAA",
                    "event_type": "curation_batch_closed",
                    "batch_id": "CUR-AAAA",
                },
                {
                    "schema": "intake_fact_capture_event_v1",
                    "event_id": "CE-RESULT-UNRESOLVED",
                    "event_type": "curation_item_result",
                    "capture_id": "CAP-20260827-UNRESOLVED",
                    "outcome": "needs_user",
                    "formal_id": None,
                },
                {
                    "schema": "intake_fact_capture_event_v1",
                    "event_id": "CE-FUTURE",
                    "event_type": "fact_captured",
                    "capture_id": "CAP-FUTURE",
                    "created_at": f"{FUTURE_DATE}T09:00:00+08:00",
                    "capture": {
                        "study_date": FUTURE_DATE,
                        "formalization_authorized": True,
                        "stable_evidence_refs": [],
                        "source_facts": {"source_id": PRIVATE_MARKER},
                    },
                },
            ]
        )
        write_jsonl(ledger, rows)

    def _extend_english_fixture(self) -> None:
        self.fixture._english_package(
            package_id="EN-PKG-20260828-FFFFFFFFFFFFFFFF",
            package_sha="a" * 64,
            source_id="ARTICLE-FUTURE",
            unit_field="question_id",
            unit_id="Q-FUTURE",
            captured_at=f"{FUTURE_DATE}T10:00:00+08:00",
            day=FUTURE_DATE,
        )
        for study_date in ("2026-08-26", STUDY_DATE):
            write_json(
                self.fixture.english
                / "intake/events"
                / study_date
                / "EN-LEGACY-SAME.json",
                {
                    "occurred_at": f"{study_date}T12:00:00+08:00",
                    "private_conversation": PRIVATE_MARKER,
                },
            )
        write_json(
            self.fixture.english
            / "intake/events"
            / FUTURE_DATE
            / "EN-LEGACY-FUTURE.json",
            {
                "occurred_at": f"{FUTURE_DATE}T12:00:00+08:00",
                "private_conversation": PRIVATE_MARKER,
            },
        )

    def _install_math_local_archive_proof(self) -> dict[str, object]:
        capture_id = "MFI-CAP-" + "A" * 24
        closeout_id = "MFI-CLOSE-" + "C" * 20
        package_id = "MATHLEGACY-" + "1" * 24
        receipt_id = "MATH-LEGACY-ARCHIVE-" + "2" * 24
        intent_id = "MATH-LEGACY-ARCHIVE-INTENT-" + "3" * 24
        authorization = "MATH-LEGACY-ARCHIVE-APPLY-" + "4" * 64
        locator_rel = f"错题知识网络/wiki/sources/raw_archives/{package_id}.md"
        locator = self.fixture.math / locator_rel
        locator.parent.mkdir(parents=True, exist_ok=True)
        locator.write_bytes(b"local math locator proof\n")
        locator_sha = hashlib.sha256(locator.read_bytes()).hexdigest()
        cleanup_proof = {
            "schema_version": "math-legacy-source-cleanup-proof-v1",
            "target_capture_event_id": capture_id,
            "source_bundle_manifest_path": None,
            "source_bundle_manifest_sha256": None,
            "reference_capture_ids": [],
            "reference_count": 0,
            "unprovable_capture_ids": [],
            "overall_decision": "not_applicable_ledger_only",
            "items": [],
        }
        common = {
            "legacy_package_id": package_id,
            "capture_event_id": capture_id,
            "closeout_id": closeout_id,
            "authorization": authorization,
            "raw_archive_relpath": f"03_数学/资料库/原始会话资料/{STUDY_DATE}/{package_id}",
            "raw_archive_manifest_sha256": "5" * 64,
            "raw_archive_package_sha256": "6" * 64,
            "archive_tree_sha256": "7" * 64,
        }
        pointer_path = (
            self.fixture.math
            / "数学一回滚复习系统/历史证据归档指针"
            / f"{capture_id}.json"
        )
        pointer_rel = pointer_path.relative_to(self.fixture.math).as_posix()
        pointer = {
            "schema_version": "math-legacy-evidence-archive-pointer-v1",
            **common,
            "freeze_id": "MFI-FREEZE-" + "8" * 24,
            "formal_ids": ["GS-700"],
            "terminal_outcome": "updated",
            "evidence_mode": "ledger_only",
            "conversation_complete": False,
            "canonical_package": False,
            "archive_volume": "T9-Data",
            "archive_receipt_id": receipt_id,
            "archive_intent_id": intent_id,
            "obsidian_locator_path": locator_rel,
            "obsidian_locator_sha256": locator_sha,
            "source_bundle_manifest_path": None,
            "source_bundle_manifest_sha256": None,
            "cleanup_proof": cleanup_proof,
            "archive_status": "verified",
            "cleanup_intent": "none_ledger_only",
            "local_pointer_path": pointer_rel,
        }
        receipt = {
            "schema_version": "math-legacy-evidence-archive-receipt-v1",
            "receipt_id": receipt_id,
            **common,
            "archive_intent_id": intent_id,
            "cleanup_proof": cleanup_proof,
            "archive_status": "verified",
            "obsidian_path_status": "verified",
            "obsidian_locator_path": locator_rel,
            "obsidian_locator_sha256": locator_sha,
        }
        intent = {
            "schema_version": "math-legacy-evidence-archive-intent-v1",
            "intent_id": intent_id,
            **common,
        }
        write_json(pointer_path, pointer)
        write_jsonl(
            self.fixture.math / "数学一回滚复习系统/原始会话归档回执.jsonl",
            [receipt],
        )
        intent_path = (
            self.fixture.math
            / "数学一回滚复习系统/原始会话归档意图"
            / f"{package_id}.json"
        )
        write_json(intent_path, intent)
        return {
            "pointer_path": pointer_path,
            "pointer": pointer,
            "receipt_path": self.fixture.math
            / "数学一回滚复习系统/原始会话归档回执.jsonl",
            "receipt": receipt,
            "locator_path": locator,
            "locator_bytes": locator.read_bytes(),
        }

    def _install_math_conversation_v2_archive_proof(self) -> dict[str, Path]:
        capture_id = "MFI-CAP-" + "A" * 24
        closeout_id = "MFI-CLOSE-" + "C" * 20
        ledger = self.fixture.math / "数学一回滚复习系统/快速入库事件.jsonl"
        rows = [json.loads(line) for line in ledger.read_text().splitlines() if line]
        capture = next(row for row in rows if row.get("event_id") == capture_id)
        reference = capture["conversation_package"]
        manifest_path = self.fixture.math / str(reference["manifest_path"])
        manifest = json.loads(manifest_path.read_text())
        manifest["schema_version"] = "math-conversation-package-v1"
        write_json(manifest_path, manifest)
        manifest_sha = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        package_id = str(reference["manifest_path"]).split("/")[-2]
        for row in rows:
            package = row.get("conversation_package")
            if isinstance(package, dict) and package.get("manifest_path") == reference["manifest_path"]:
                package["manifest_hash"] = manifest_sha
                package["package_id"] = package_id
        write_jsonl(ledger, rows)

        package_sha = str(reference["package_sha256"])
        receipt_id = "MATH-ARCHIVE-" + "2" * 24
        intent_id = "MATH-ARCHIVE-INTENT-" + "3" * 24
        locator_rel = f"错题知识网络/wiki/sources/raw_archives/{package_id}.md"
        locator = self.fixture.math / locator_rel
        locator.parent.mkdir(parents=True, exist_ok=True)
        locator.write_bytes(b"conversation archive locator\n")
        locator_sha = hashlib.sha256(locator.read_bytes()).hexdigest()
        closure_rel = (
            f"数学一回滚复习系统/展示资产闭环/{package_id}/binding-test/"
            "display-asset-closure-receipt.json"
        )
        closure = self.fixture.math / closure_rel
        closure.parent.mkdir(parents=True, exist_ok=True)
        closure.write_bytes(b"display closure receipt\n")
        closure_sha = hashlib.sha256(closure.read_bytes()).hexdigest()
        common = {
            "package_id": package_id,
            "archive_intent_id": intent_id,
            "archive_volume": "T9-Data",
            "raw_archive_relpath": f"03_数学/资料库/原始会话资料/{STUDY_DATE}/{package_id}",
            "raw_archive_manifest_sha256": manifest_sha,
            "raw_archive_package_sha256": package_sha,
            "archive_status": "verified",
            "obsidian_locator_path": locator_rel,
            "obsidian_locator_sha256": locator_sha,
            "display_closure_receipt_id": "MATH-DISPLAY-CLOSE-" + "4" * 24,
            "display_closure_receipt_path": closure_rel,
            "display_closure_receipt_sha256": closure_sha,
            "formal_reference_scan_sha256": "5" * 64,
            "stable_asset_count": 1,
            "no_display_proof": False,
            "pending_component": None,
        }
        pointer = {
            "schema_version": "math-conversation-package-archive-pointer-v2",
            **common,
            "archive_receipt_id": receipt_id,
            "cleanup_intent": "remove_local_attachments_after_verified_display_archive_locator_receipt",
        }
        receipt = {
            "schema_version": "math-conversation-package-archive-receipt-v2",
            "receipt_id": receipt_id,
            **common,
            "capture_ids": [capture_id],
            "closeout_id": closeout_id,
            "obsidian_path_status": "verified",
        }
        pointer_path = manifest_path.parent / "archive-pointer.json"
        write_json(pointer_path, pointer)
        receipt_path = self.fixture.math / "数学一回滚复习系统/原始会话归档回执.jsonl"
        write_jsonl(receipt_path, [receipt])
        return {
            "pointer_path": pointer_path,
            "receipt_path": receipt_path,
            "locator_path": locator,
            "closure_path": closure,
        }

    def _install_cs408_local_archive_proof(self) -> dict[str, Path]:
        ledger = (
            self.fixture.cs408
            / "wiki/study_vaults/408-full/state/intake-curation/events.jsonl"
        )
        by_id: dict[str, tuple[dict[str, object], bytes]] = {}
        for line in ledger.read_bytes().splitlines(keepends=True):
            row = json.loads(line)
            if row.get("event_id"):
                by_id[str(row["event_id"])] = (row, line)
        roles = {
            "canonical_capture_event": next(
                event_id
                for event_id, (row, _raw) in by_id.items()
                if row.get("capture_id") == "CAP-20260827-AAAA"
                and row.get("event_type") == "fact_captured"
            ),
            "canonical_result_event": "CE-RESULT-AAAA",
            "canonical_closeout_event": "CE-CLOSE-AAAA",
        }
        proof_files = []
        for role, event_id in roles.items():
            _row, raw = by_id[event_id]
            proof_files.append(
                {
                    "role": role,
                    "relative_path": f"proof/{role}.jsonl",
                    "event_id": event_id,
                    "sha256": hashlib.sha256(raw).hexdigest(),
                    "byte_count": len(raw),
                }
            )
        result_raw = by_id["CE-RESULT-AAAA"][1]
        material = {
            "capture_id": "CAP-20260827-AAAA",
            "capture_payload_sha256": "a" * 64,
            "outcome": "curated",
            "formal_ids": ["GS-999"],
            "proof_files": proof_files,
            "source_files": [],
        }
        locator_rel = "wiki/sources/raw_archives/CS408-LOCAL-PROOF.md"
        locator = self.fixture.cs408 / locator_rel
        locator.parent.mkdir(parents=True, exist_ok=True)
        locator.write_bytes(b"local 408 locator proof\n")
        pointer = {
            "schema_version": "cs408-legacy-evidence-archive-pointer-v1",
            "status": "archive_verified",
            "canonical_package": False,
            "capture_id": "CAP-20260827-AAAA",
            "authorization_material": material,
            "authorization": "CS408-LEGACY-ARCHIVE-"
            + PROJECTOR._pretty_object_sha256(material),
            "source_files": [],
            "archive_receipt_sha256": "b" * 64,
            "outcome": "curated",
            "formal_ids": ["GS-999"],
            "ledger_terminal_event_sha256": hashlib.sha256(result_raw).hexdigest(),
            "locator_note_relative_path": locator_rel,
            "locator_note_sha256": hashlib.sha256(locator.read_bytes()).hexdigest(),
        }
        pointer_path = (
            self.fixture.cs408_packages
            / "legacy-archive-pointers"
            / ("e" * 64 + ".json")
        )
        write_json(pointer_path, pointer)
        return {"pointer_path": pointer_path, "locator_path": locator}

    def _install_cs408_canonical_archive_proof(self) -> dict[str, Path]:
        capture_id = "CAP-20260827-AAAA"
        package_id = "CS408-20260827-PKGA"
        captured_sha = "2" * 64
        current_sha = "4" * 64
        local_root = self.fixture.cs408_packages / STUDY_DATE / package_id
        captured_manifest = json.loads((local_root / "manifest.json").read_text())
        captured_receipt = json.loads((local_root / "receipt.json").read_text())
        current_manifest = {**captured_manifest, "canonical_sha256": current_sha}
        current_receipt = {**captured_receipt, "canonical_sha256": current_sha}
        write_json(local_root / "manifest.json", current_manifest)
        write_json(local_root / "receipt.json", current_receipt)

        archive_relative = (
            Path("01_408/资料库/原始会话资料") / STUDY_DATE / package_id
        )
        archive_root = self.fixture.t9 / archive_relative
        write_json(archive_root / "manifest.json", current_manifest)
        write_json(archive_root / "receipt.json", current_receipt)
        write_json(
            archive_root / ".revisions" / captured_sha / "manifest.json",
            captured_manifest,
        )
        write_json(
            archive_root / ".revisions" / captured_sha / "receipt.json",
            captured_receipt,
        )
        terminal_sha = "5" * 64
        archive_receipt = {
            "schema_version": "cs408-package-archive-receipt-v1",
            "status": "ARCHIVED_AND_LOCATOR_BOUND",
            "package_id": package_id,
            "package_sha256": current_sha,
            "formal_ids": ["GS-999"],
            "formal_terminal_outcome": "curated",
            "formal_terminal_sha256": terminal_sha,
        }
        write_json(archive_root / "archive-receipt.json", archive_receipt)
        locator_rel = f"wiki/sources/raw_archives/{package_id}.md"
        locator = self.fixture.cs408 / locator_rel
        locator.parent.mkdir(parents=True, exist_ok=True)
        locator.write_bytes(b"canonical 408 locator proof\n")
        pointer = {
            "schema_version": "cs408-local-archive-pointer-v1",
            "status": "local_heavy_content_cleaned",
            "package_id": package_id,
            "package_sha256": current_sha,
            "archive_volume": "T9-Data",
            "archive_relative_path": archive_relative.as_posix(),
            "archive_receipt_sha256": hashlib.sha256(
                (archive_root / "archive-receipt.json").read_bytes()
            ).hexdigest(),
            "locator_note_relative_path": locator_rel,
            "locator_note_sha256": hashlib.sha256(locator.read_bytes()).hexdigest(),
            "formal_terminal_sha256": terminal_sha,
            "pending_component": None,
        }
        pointer_path = local_root / "archive-pointer.json"
        write_json(pointer_path, pointer)
        return {
            "pointer_path": pointer_path,
            "locator_path": locator,
            "archive_receipt_path": archive_root / "archive-receipt.json",
        }

    def test_live_snapshot_is_cross_date_private_and_stably_revisioned(self) -> None:
        first = PROJECTOR.build_live_snapshot(self.config, STUDY_DATE)
        second = PROJECTOR.build_live_snapshot(self.config, STUDY_DATE)

        self.assertEqual(first["schema_version"], "three-subject-quick-intake-live-v1")
        self.assertEqual(first["update_mode"], "live_backlog_stream")
        self.assertEqual(first["subject_order"], ["408", "math", "english"])
        self.assertEqual(list(first["subjects"]), first["subject_order"])
        self.assertRegex(first["revision"], r"^[a-f0-9]{64}$")
        self.assertEqual(first["revision"], second["revision"])

        math = first["subjects"]["math"]
        self.assertEqual(math["counts"]["item_count"], 3)
        self.assertEqual(math["counts"]["already_consumed_excluded_count"], 0)
        self.assertEqual(math["counts"]["residual_count"], 1)
        self.assertEqual(math["counts"]["future_excluded_count"], 1)
        self.assertEqual(
            {row["study_date"] for row in math["items"]},
            {"2026-08-26", STUDY_DATE},
        )

        cs408 = first["subjects"]["408"]
        self.assertEqual(cs408["counts"]["processable_count"], 1)
        self.assertEqual(cs408["counts"]["residual_count"], 2)
        self.assertEqual(cs408["counts"]["already_consumed_excluded_count"], 0)
        self.assertEqual(cs408["counts"]["future_excluded_count"], 1)
        self.assertEqual(
            next(row for row in cs408["items"] if row["queue_class"] == "residual")[
                "display_status"
            ],
            "needs_user",
        )

        english = first["subjects"]["english"]
        self.assertEqual(english["counts"]["residual_count"], 1)
        self.assertEqual(
            sum(row["event_id"] == "EN-LEGACY-SAME" for row in english["items"]),
            1,
        )
        self.assertEqual(
            next(
                row
                for row in english["items"]
                if row["event_id"] == "EN-LEGACY-SAME"
            )["study_date"],
            STUDY_DATE,
        )

        serialized = json.dumps(first, ensure_ascii=False)
        self.assertNotIn(PRIVATE_MARKER, serialized)
        for forbidden in (
            "private_conversation",
            "private_dialogue",
            "full_passage",
            "source_sentence",
            "messages",
            "options",
        ):
            self.assertNotIn(forbidden, serialized)

        for subject in first["subject_order"]:
            rows = first["subjects"][subject]["items"]
            self.assertEqual(
                len(rows), len({row["subject_item_key"] for row in rows})
            )
            sort_keys = [
                (
                    row["captured_at"]
                    or f"{row['study_date']}T00:00:00+08:00",
                    row["study_date"],
                    row["subject_item_key"],
                )
                for row in rows
            ]
            self.assertEqual(sort_keys, sorted(sort_keys, reverse=True))
            for row in rows:
                self.assertEqual(
                    set(row["attachment_counts"]),
                    set(PROJECTOR._empty_attachment_counts()),
                )

    def test_user_removed_legacy_event_is_excluded_but_counted(self) -> None:
        module_path = self.fixture.english / "english_pipeline/backlog.py"
        module_path.parent.mkdir(parents=True, exist_ok=True)
        module_path.write_text("# fixture backlog module\n", encoding="utf-8")
        fake_backlog = SimpleNamespace(
            build_backlog_plan=lambda *args, **kwargs: (
                None,
                {
                    "packages": [],
                    "future_excluded": [],
                    "legacy_reachability_audit": {
                        "events": [
                            {
                                "event_id": "EVT-USER-REMOVED",
                                "study_date": STUDY_DATE,
                                "status": "user_removed",
                                "actionable": False,
                            }
                        ]
                    },
                },
            )
        )
        with mock.patch.object(
            PROJECTOR, "_english_backlog_module", return_value=fake_backlog
        ):
            subject = PROJECTOR._english_live_subject(self.config, STUDY_DATE)

        self.assertEqual(subject["counts"]["item_count"], 0)
        self.assertEqual(subject["counts"]["residual_count"], 0)
        self.assertEqual(
            subject["counts"]["administrative_terminal_excluded_count"], 1
        )
        self.assertEqual(subject["items"], [])

    def test_english_web_capture_visible_once_until_trusted_terminal(self) -> None:
        module_path = self.fixture.english / "english_pipeline/backlog.py"
        module_path.parent.mkdir(parents=True, exist_ok=True)
        module_path.write_text("# fixture backlog module\n", encoding="utf-8")
        row = {
            "package_id": "EN-PKG-WEB-CAPTURE",
            "study_date": STUDY_DATE,
            "status": "waiting_web_review",
        }
        plan = {"packages": [], "waiting_web_review": [row, dict(row)]}
        fake_backlog = SimpleNamespace(
            build_backlog_plan=lambda *args, **kwargs: (None, plan)
        )
        with mock.patch.object(PROJECTOR, "_english_backlog_module", return_value=fake_backlog):
            for _ in range(2):
                subject = PROJECTOR._english_live_subject(self.config, STUDY_DATE)
                self.assertEqual(subject["counts"]["item_count"], 1)
                self.assertEqual(subject["counts"]["waiting_web_review_count"], 1)
                self.assertEqual(subject["counts"]["processable_count"], 0)
                self.assertEqual(subject["counts"]["residual_count"], 0)
                self.assertEqual(subject["items"][0]["study_date"], STUDY_DATE)
                self.assertEqual(subject["items"][0]["display_status"], "waiting_web_review")
            plan["waiting_web_review"] = []
            plan["packages"] = [{**row, "status": "archive_pending"}]
            subject = PROJECTOR._english_live_subject(self.config, STUDY_DATE)
            self.assertEqual(subject["counts"]["item_count"], 1)
            self.assertEqual(subject["counts"]["waiting_web_review_count"], 0)
            self.assertEqual(subject["items"][0]["display_status"], "archive_pending")
            plan["packages"] = [{**row, "status": "already_consumed"}]
            subject = PROJECTOR._english_live_subject(self.config, STUDY_DATE)
            self.assertEqual(subject["counts"]["item_count"], 0)

    def test_local_archive_proof_excludes_closed_items_without_reading_t9(self) -> None:
        self._install_math_local_archive_proof()
        self._install_cs408_local_archive_proof()
        original_read_bytes = Path.read_bytes
        original_read_text = Path.read_text
        t9 = self.fixture.t9.resolve()

        def guard_bytes(path: Path) -> bytes:
            resolved = path.resolve()
            if resolved == t9 or t9 in resolved.parents:
                raise AssertionError(f"live projection read T9 bytes: {resolved}")
            return original_read_bytes(path)

        def guard_text(path: Path, *args, **kwargs) -> str:
            resolved = path.resolve()
            if resolved == t9 or t9 in resolved.parents:
                raise AssertionError(f"live projection read T9 text: {resolved}")
            return original_read_text(path, *args, **kwargs)

        with (
            mock.patch.object(Path, "read_bytes", guard_bytes),
            mock.patch.object(Path, "read_text", guard_text),
        ):
            snapshot = PROJECTOR.build_live_snapshot(self.config, STUDY_DATE)

        math = snapshot["subjects"]["math"]
        self.assertEqual(math["counts"]["already_consumed_excluded_count"], 1)
        self.assertEqual(math["counts"]["residual_count"], 0)
        cs408 = snapshot["subjects"]["408"]
        self.assertEqual(cs408["counts"]["already_consumed_excluded_count"], 1)
        self.assertEqual(cs408["counts"]["residual_count"], 1)

    def test_canonical_cs408_archive_pointer_consumes_revision_advanced_package(self) -> None:
        proof = self._install_cs408_canonical_archive_proof()
        snapshot = PROJECTOR.build_live_snapshot(self.config, STUDY_DATE)
        cs408 = snapshot["subjects"]["408"]
        self.assertEqual(cs408["counts"]["already_consumed_excluded_count"], 1)
        self.assertEqual(cs408["counts"]["residual_count"], 1)

        receipt = json.loads(proof["archive_receipt_path"].read_text())
        receipt["formal_terminal_sha256"] = "6" * 64
        write_json(proof["archive_receipt_path"], receipt)
        drifted = PROJECTOR.build_live_snapshot(self.config, STUDY_DATE)
        self.assertEqual(drifted["subjects"]["408"]["counts"]["residual_count"], 2)

    def test_live_builder_rebuilds_408_after_canonical_archive_pointer_appears(self) -> None:
        builder = PROJECTOR.LiveSnapshotBuilder(self.config)
        before = builder(self.config, STUDY_DATE)
        self.assertEqual(before["subjects"]["408"]["counts"]["residual_count"], 2)

        self._install_cs408_canonical_archive_proof()

        after = builder(self.config, STUDY_DATE)
        self.assertEqual(after["subjects"]["408"]["counts"]["residual_count"], 1)

    def test_math_conversation_v2_archive_proof_is_consumed(self) -> None:
        proof = self._install_math_conversation_v2_archive_proof()
        snapshot = PROJECTOR.build_live_snapshot(self.config, STUDY_DATE)
        math = snapshot["subjects"]["math"]
        self.assertEqual(math["counts"]["already_consumed_excluded_count"], 1)
        self.assertEqual(math["counts"]["residual_count"], 0)
        self.assertNotIn(
            "MFI-CAP-" + "A" * 24,
            {row["capture_id"] for row in math["items"]},
        )

        pointer = json.loads(proof["pointer_path"].read_text())
        pointer["display_closure_receipt_sha256"] = "f" * 64
        write_json(proof["pointer_path"], pointer)
        drifted = PROJECTOR.build_live_snapshot(self.config, STUDY_DATE)
        self.assertEqual(drifted["subjects"]["math"]["counts"]["residual_count"], 1)
        self.assertEqual(
            next(
                row
                for row in drifted["subjects"]["math"]["items"]
                if row["capture_id"] == "MFI-CAP-" + "A" * 24
            )["display_status"],
            "archive_pending",
        )

    def test_live_builder_isolates_and_recovers_one_subject_failure(self) -> None:
        builder = PROJECTOR.LiveSnapshotBuilder(self.config)
        first = builder(self.config, STUDY_DATE)
        self.assertEqual(first["projection_errors"], {})
        receipt_ledger = self.fixture.math / "数学一回滚复习系统/原始会话归档回执.jsonl"
        receipt_ledger.parent.mkdir(parents=True, exist_ok=True)
        receipt_ledger.write_text("\n", encoding="utf-8")
        with mock.patch.object(
            PROJECTOR, "_math_live_subject", side_effect=PROJECTOR.ProjectionError("private")
        ):
            degraded = builder(self.config, STUDY_DATE)
        self.assertEqual(set(degraded["projection_errors"]), {"math"})
        self.assertEqual(degraded["subjects"]["math"]["freshness"]["status"], "stale")
        self.assertEqual(degraded["subjects"]["408"]["freshness"]["status"], "fresh")
        self.assertEqual(degraded["subjects"]["english"]["freshness"]["status"], "fresh")
        self.assertNotIn("private", json.dumps(degraded))

        recovered = builder(self.config, STUDY_DATE)
        self.assertEqual(recovered["projection_errors"], {})
        self.assertEqual(recovered["subjects"]["math"]["freshness"]["status"], "fresh")

    def test_local_archive_pointer_locator_and_receipt_drift_remain_residual(self) -> None:
        math_proof = self._install_math_local_archive_proof()
        cs408_proof = self._install_cs408_local_archive_proof()

        pointer = dict(math_proof["pointer"])
        pointer["capture_event_id"] = "MFI-CAP-" + "Z" * 24
        write_json(math_proof["pointer_path"], pointer)
        snapshot = PROJECTOR.build_live_snapshot(self.config, STUDY_DATE)
        self.assertEqual(snapshot["subjects"]["math"]["counts"]["residual_count"], 1)
        write_json(math_proof["pointer_path"], math_proof["pointer"])

        math_proof["locator_path"].write_bytes(b"drifted math locator\n")
        snapshot = PROJECTOR.build_live_snapshot(self.config, STUDY_DATE)
        self.assertEqual(snapshot["subjects"]["math"]["counts"]["residual_count"], 1)
        math_proof["locator_path"].write_bytes(math_proof["locator_bytes"])

        receipt = dict(math_proof["receipt"])
        receipt["archive_status"] = "drifted"
        write_jsonl(math_proof["receipt_path"], [receipt])
        snapshot = PROJECTOR.build_live_snapshot(self.config, STUDY_DATE)
        self.assertEqual(snapshot["subjects"]["math"]["counts"]["residual_count"], 1)

        cs408_proof["locator_path"].write_bytes(b"drifted 408 locator\n")
        snapshot = PROJECTOR.build_live_snapshot(self.config, STUDY_DATE)
        self.assertEqual(snapshot["subjects"]["408"]["counts"]["residual_count"], 2)

    def test_math_single_owner_pointer_selection_prefers_capture_specific(self) -> None:
        ledger = self.fixture.math / "数学一回滚复习系统/快速入库事件.jsonl"
        capture = next(
            row
            for row in (json.loads(line) for line in ledger.read_text().splitlines())
            if row.get("event_id") == "MFI-CAP-" + "A" * 24
        )
        capture_id = str(capture["event_id"])
        manifest = self.fixture.math / str(
            capture["conversation_package"]["manifest_path"]
        )
        single = manifest.parent / "legacy-archive-pointer.json"
        specific = manifest.parent / "legacy-archive-pointers" / f"{capture_id}.json"
        write_json(single, {"capture_event_id": "MFI-CAP-" + "B" * 24})
        self.assertEqual(
            PROJECTOR._math_capture_pointer_candidates(
                self.fixture.math,
                capture_id,
                capture,
                [],
                PROJECTOR.SourceTracker(),
            ),
            [],
        )
        write_json(specific, {"capture_event_id": capture_id})
        self.assertEqual(
            PROJECTOR._math_capture_pointer_candidates(
                self.fixture.math,
                capture_id,
                capture,
                [],
                PROJECTOR.SourceTracker(),
            ),
            [specific.resolve()],
        )
        specific.unlink()
        write_json(single, {"capture_event_id": capture_id})
        self.assertEqual(
            PROJECTOR._math_capture_pointer_candidates(
                self.fixture.math,
                capture_id,
                capture,
                [],
                PROJECTOR.SourceTracker(),
            ),
            [single.resolve()],
        )


if __name__ == "__main__":
    unittest.main()

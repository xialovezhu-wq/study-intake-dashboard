from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import shutil
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "scripts/project_quick_intake_today.py"
SPEC = importlib.util.spec_from_file_location("quick_intake_projector_under_test", SCRIPT)
assert SPEC and SPEC.loader
PROJECTOR = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = PROJECTOR
SPEC.loader.exec_module(PROJECTOR)


STUDY_DATE = "2026-08-27"
PREVIOUS_DATE = "2026-08-26"
NOW = datetime(2026, 8, 27, 15, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
PRIVATE_MARKER = "PRIVATE-FULL-STEM-ANSWER-AND-DIALOGUE-MUST-NOT-LEAK"


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


class QuickIntakeProjectorFixtureTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.math = self.root / "math"
        self.cs408 = self.root / "cs408"
        self.cs408_packages = self.root / "cs408-private"
        self.english = self.root / "english"
        self.t9 = self.root / "T9-Data"
        self.output = self.root / "board"
        self._dashboard_summaries()
        self._math_fixture()
        self._cs408_fixture()
        self._english_fixture()
        self.config = PROJECTOR.ProjectorConfig(
            math_root=self.math,
            cs408_root=self.cs408,
            cs408_package_root=self.cs408_packages,
            english_root=self.english,
            t9_root=self.t9,
        )

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def _dashboard_summaries(self) -> None:
        write_json(
            self.math / "错题知识网络/wiki/operation_center/dashboard-summary.json",
            {"system": "math", "legacy_consumer_field": {"keep": True}},
        )
        write_json(
            self.cs408 / "wiki/operation_center/dashboard-summary.json",
            {"system": "408", "legacy_consumer_field": {"keep": True}},
        )
        write_json(
            self.english / "wiki/operation_center/dashboard-summary.json",
            {"system": "english", "legacy_consumer_field": {"keep": True}},
        )

    def _math_package(self) -> dict[str, str]:
        package_id = "MATHPKG-" + "a" * 24
        package_sha = "1" * 64
        relative = Path(
            f"数学一回滚复习系统/快速入库来源/{STUDY_DATE}/{package_id}/manifest.json"
        )
        path = self.math / relative
        manifest = {
            "schema_version": "math-conversation-package-manifest-v1",
            "package_id": package_id,
            "subject": "math",
            "study_date": STUDY_DATE,
            "timezone": "Asia/Shanghai",
            "canonical_sha256": package_sha,
            "artifacts": [
                {"role": "question_image"},
                {"role": "explanation_image"},
                {"role": "user_work_image"},
            ],
        }
        write_json(path, manifest)
        write_json(
            path.parent / "receipt.json",
            {
                "schema_version": "math-conversation-package-receipt-v1",
                "package_id": package_id,
                "study_date": STUDY_DATE,
                "canonical_sha256": package_sha,
                "formal_write_count": 0,
                "background_processing": "none",
            },
        )
        return {
            "manifest_path": relative.as_posix(),
            "manifest_hash": sha(path.read_bytes()),
            "package_sha256": package_sha,
        }

    def _math_fixture(self) -> None:
        package = self._math_package()
        captures: list[dict[str, object]] = []
        for suffix, at in (("A", "2026-08-27T09:10:00+08:00"), ("B", "2026-08-27T10:20:00+08:00")):
            captures.append(
                {
                    "schema_version": "math-fast-intake-ledger-v1",
                    "event_id": f"MFI-CAP-{suffix * 24}",
                    "event_type": "capture",
                    "study_date": STUDY_DATE,
                    "recorded_at": at,
                    "initial_state": "awaiting_sol_formalization",
                    "capture_schema_version": "math-fast-intake-capture-v3",
                    "target": {"kind": "formal_card", "formal_id": "GS-700"},
                    "conversation_package": package,
                    "private_payload": PRIVATE_MARKER,
                }
            )
        captures.extend(
            [
                {
                    "schema_version": "math-fast-intake-ledger-v1",
                    "event_id": "MFI-CAP-" + "P" * 24,
                    "event_type": "capture",
                    "study_date": PREVIOUS_DATE,
                    "recorded_at": "2026-08-26T21:00:00+08:00",
                    "initial_state": "pending_nightly",
                    "target": {"kind": "new_source", "source_locator": "previous"},
                },
                {
                    "schema_version": "math-fast-intake-ledger-v1",
                    "event_id": "MFI-CAP-" + "M" * 24,
                    "event_type": "capture",
                    "recorded_at": "2026-08-27T11:00:00+08:00",
                    "initial_state": "pending_nightly",
                    "target": {"kind": "new_source", "source_locator": "missing-date"},
                },
                {
                    "schema_version": "math-fast-intake-ledger-v1",
                    "event_id": "MFI-CLOSE-" + "C" * 20,
                    "event_type": "closeout",
                    "capture_event_ids": ["MFI-CAP-" + "A" * 24],
                },
            ]
        )
        write_jsonl(
            self.math / "数学一回滚复习系统/快速入库事件.jsonl",
            captures,
        )

    def _cs408_package(self, package_id: str, package_sha: str) -> None:
        root = self.cs408_packages / STUDY_DATE / package_id
        write_json(
            root / "manifest.json",
            {
                "schema_version": "cs408-conversation-package-manifest-v1",
                "package_id": package_id,
                "study_date": STUDY_DATE,
                "canonical_sha256": package_sha,
                "attachments": [{"role": "question_image"}],
            },
        )
        write_json(
            root / "receipt.json",
            {
                "schema_version": "cs408-conversation-package-receipt-v1",
                "package_id": package_id,
                "study_date": STUDY_DATE,
                "canonical_sha256": package_sha,
                "formal_write_count": 0,
                "background_processing": "none",
            },
        )

    def _cs408_fixture(self) -> None:
        identity_sha = "9" * 64
        rows: list[dict[str, object]] = []
        receipts = (
            ("CAP-20260827-AAAA", "CS408-20260827-PKGA", "2" * 64, "08:15"),
            ("CAP-20260827-BBBB", "CS408-20260827-PKGB", "3" * 64, "09:45"),
        )
        for capture_id, package_id, package_sha, local_time in receipts:
            self._cs408_package(package_id, package_sha)
            rows.append(
                {
                    "schema": "intake_fact_capture_event_v1",
                    "event_id": "CE-" + hashlib.sha256(capture_id.encode()).hexdigest()[:20],
                    "event_type": "fact_captured",
                    "capture_id": capture_id,
                    "created_at": f"2026-08-27T{local_time}:00+08:00",
                    "capture": {
                        "study_date": STUDY_DATE,
                        "formalization_authorized": True,
                        "stable_evidence_refs": [
                            {
                                "kind": "cs408_conversation_package_v1",
                                "locator": f"cs408-conversation-package://{package_id}",
                                "sha256": package_sha,
                            }
                        ],
                        "source_facts": {"source_id": "DS-2015"},
                        "private_dialogue": PRIVATE_MARKER,
                    },
                }
            )
            write_json(
                self.cs408
                / "wiki/study_vaults/408-full/state/intake-curation"
                / "standalone-question-intake-v1/receipts"
                / f"{capture_id}.json",
                {
                    "schema": "cs408-standalone-question-intake-receipt-v1",
                    "status": "awaiting_sol_formalization",
                    "standalone_item_id": "SQI-SAME-QUESTION",
                    "standalone_item_identity_sha256": identity_sha,
                    "conversation_package_sha256": package_sha,
                    "capture_id": capture_id,
                    "captured_at": f"2026-08-27T{local_time}:00+08:00",
                    "formal_write_count": 0,
                    "background_processing": "none",
                },
            )
        rows.append(
            {
                "schema": "intake_fact_capture_event_v1",
                "event_id": "CE-UNRESOLVED",
                "event_type": "fact_captured",
                "capture_id": "CAP-20260827-UNRESOLVED",
                "created_at": "2026-08-27T10:30:00+08:00",
                "capture": {
                    "study_date": STUDY_DATE,
                    "formalization_authorized": True,
                    "stable_evidence_refs": [],
                    "source_facts": {"source_id": "SOURCE-WITH-MULTIPLE-QUESTIONS"},
                },
            }
        )
        write_jsonl(
            self.cs408 / "wiki/study_vaults/408-full/state/intake-curation/events.jsonl",
            rows,
        )

    def _english_package(
        self,
        *,
        package_id: str,
        package_sha: str,
        source_id: str,
        unit_field: str,
        unit_id: str,
        captured_at: str,
        day: str = STUDY_DATE,
        parent: Path | None = None,
    ) -> Path:
        package_root = (parent or self.english / "intake/packages" / day) / package_id
        source = {
            "schema_version": "english_conversation_source_v1",
            "identity": {
                "source_id": source_id,
                unit_field: unit_id,
                "full_passage": PRIVATE_MARKER,
            },
        }
        source_raw = json.dumps(source, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        conversation_raw = json.dumps(
            {"messages": [{"role": "user", "content": PRIVATE_MARKER}]},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        package_root.mkdir(parents=True, exist_ok=True)
        (package_root / "source.json").write_bytes(source_raw)
        (package_root / "conversation.json").write_bytes(conversation_raw)
        files = {
            "source.json": {"sha256": sha(source_raw), "bytes": len(source_raw)},
            "conversation.json": {
                "sha256": sha(conversation_raw),
                "bytes": len(conversation_raw),
            },
        }
        write_json(
            package_root / "manifest.json",
            {
                "schema_version": "english_conversation_package_v1",
                "package_id": package_id,
                "subject": "english",
                "study_date": day,
                "created_at": "2000-01-01T00:00:00+08:00",
                "files": files,
                "attachments": [],
                "package_canonical_sha256": package_sha,
            },
        )
        write_json(
            package_root / "receipt.json",
            {
                "schema_version": "english_package_receipt_v2",
                "package_id": package_id,
                "package_sha256": package_sha,
                "study_date": day,
                "captured_at": captured_at,
                "status": "created",
                "missing_fields": [],
                "formal_write_count": 0,
                "background_processing": "none",
            },
        )
        return package_root

    def _english_fixture(self) -> None:
        packages = (
            ("EN-PKG-20260827-AAAAAAAAAAAAAAAA", "4" * 64, "ARTICLE-A", "question_id", "Q01", "2026-08-27T08:00:00+08:00"),
            ("EN-PKG-20260827-BBBBBBBBBBBBBBBB", "5" * 64, "ARTICLE-A", "question_id", "Q01", "2026-08-27T09:00:00+08:00"),
            ("EN-PKG-20260827-CCCCCCCCCCCCCCCC", "6" * 64, "ARTICLE-A", "sentence_id", "S01", "2026-08-27T10:00:00+08:00"),
            ("EN-PKG-20260827-DDDDDDDDDDDDDDDD", "7" * 64, "ARTICLE-B", "sentence_id", "S01", "2026-08-27T11:00:00+08:00"),
        )
        local_roots: dict[str, Path] = {}
        for args in packages:
            local_roots[args[0]] = self._english_package(
                package_id=args[0],
                package_sha=args[1],
                source_id=args[2],
                unit_field=args[3],
                unit_id=args[4],
                captured_at=args[5],
            )
        self._english_package(
            package_id="EN-PKG-20260826-EEEEEEEEEEEEEEEE",
            package_sha="8" * 64,
            source_id="ARTICLE-OLD",
            unit_field="question_id",
            unit_id="Q-OLD",
            captured_at="2026-08-26T20:00:00+08:00",
            day=PREVIOUS_DATE,
        )
        archived_id = packages[0][0]
        archived_sha = packages[0][1]
        relative = Path("02_English/raw") / STUDY_DATE / archived_id
        archived_root = self.t9 / relative
        shutil.copytree(local_roots[archived_id], archived_root)
        archive_receipt = archived_root / "archive-receipt.json"
        locator = self.english / "wiki/raw_archives" / f"{archived_id}.md"
        archive_receipt.write_text(
            json.dumps({"package_id": archived_id, "package_sha256": archived_sha}),
            encoding="utf-8",
        )
        locator.parent.mkdir(parents=True, exist_ok=True)
        locator.write_text(f"package_id: {archived_id}\n", encoding="utf-8")
        write_json(
            self.english / "intake/archive-pointers" / STUDY_DATE / f"{archived_id}.json",
            {
                "schema_version": "english_archive_pointer_v1",
                "package_id": archived_id,
                "package_sha256": archived_sha,
                "archive_relative_path": relative.as_posix(),
                "archive_receipt_path": str(archive_receipt.resolve()),
                "archive_locator_path": str(locator.resolve()),
                "created_at": "2026-08-27T12:00:00+08:00",
            },
        )
        write_json(
            self.english / "intake/cleanup-intents" / STUDY_DATE / f"{archived_id}.json",
            {
                "schema_version": "english_archive_cleanup_intent_v1",
                "package_id": archived_id,
                "package_sha256": archived_sha,
                "archive_relative_path": relative.as_posix(),
                "archive_receipt_path": str(archive_receipt.resolve()),
                "archive_receipt_sha256": sha(archive_receipt.read_bytes()),
                "locator_path": str(locator.resolve()),
                "locator_sha256": sha(locator.read_bytes()),
                "cleanup_authorized": True,
            },
        )

    def _build(self) -> dict[str, dict[str, object]]:
        return PROJECTOR.build_all(self.config, STUDY_DATE, now=NOW)

    def test_three_subject_counts_date_filter_and_deduplication(self) -> None:
        projections = self._build()
        math = projections["math"]
        self.assertEqual(math["counts"]["capture_count"], 2)
        self.assertEqual(math["counts"]["unique_formal_question_count"], 1)
        self.assertEqual(math["counts"]["conservative_unique_target_count"], 1)
        self.assertEqual(math["counts"]["conversation_package_count"], 1)
        self.assertEqual(math["counts"]["pending_count"], 1)
        self.assertEqual(math["counts"]["closed_count"], 1)
        self.assertEqual(math["backlog_through_date"]["unconsumed_count"], 2)
        self.assertEqual(
            math["backlog_through_date"]["processable_pending_count"], 2
        )
        self.assertEqual(
            math["backlog_through_date"]["earliest_study_date"], PREVIOUS_DATE
        )
        self.assertTrue(any(row["code"] == "math_missing_study_date_excluded" for row in math["warnings"]))

        cs408 = projections["408"]
        self.assertEqual(cs408["counts"]["unique_question_count"], 1)
        self.assertEqual(cs408["counts"]["package_count"], 2)
        self.assertEqual(cs408["counts"]["question_identity_unresolved_count"], 1)
        self.assertEqual(len(cs408["items"]), 3)
        self.assertEqual(cs408["backlog_through_date"]["unconsumed_count"], 3)
        self.assertEqual(
            cs408["backlog_through_date"]["processable_pending_count"], 3
        )

        english = projections["english"]
        self.assertEqual(english["counts"]["question_count"], 1)
        self.assertEqual(english["counts"]["learning_unit_count"], 3)
        self.assertEqual(english["counts"]["segment_count"], 4)
        self.assertEqual(english["counts"]["archived_count"], 1)
        self.assertEqual(len(english["items"]), 4)
        self.assertEqual(english["backlog_through_date"]["unconsumed_count"], 4)
        self.assertEqual(
            english["backlog_through_date"]["processable_pending_count"], 4
        )
        self.assertEqual(
            english["backlog_through_date"]["earliest_study_date"], PREVIOUS_DATE
        )
        self.assertEqual(
            next(row for row in english["items"] if row["package_id"].endswith("AAAAAAAAAAAAAAAA"))["captured_at"],
            "2026-08-27T08:00:00+08:00",
        )
        for projection in projections.values():
            self.assertTrue(all(item["study_date"] == STUDY_DATE for item in projection["items"]))

    def test_outputs_are_private_self_contained_and_preserve_old_summary_fields(self) -> None:
        projections = self._build()
        result = PROJECTOR.write_outputs(
            config=self.config,
            projections=projections,
            project_root=self.output,
            template_path=PROJECT_ROOT / "templates/index.template.html",
            refresh_command="cd /fixture/board && python3 scripts/project_quick_intake_today.py",
        )
        self.assertEqual(len(result["outputs"]), 8)
        combined = json.loads((self.output / "data/quick-intake-today.json").read_text())
        self.assertEqual(combined["subject_order"], ["408", "math", "english"])
        page = (self.output / "index.html").read_text(encoding="utf-8")
        embedded = re.search(
            r'<script type="application/json" id="snapshot-data">(.*?)</script>',
            page,
            re.DOTALL,
        )
        self.assertIsNotNone(embedded)
        self.assertEqual(json.loads(embedded.group(1)), combined)
        serialized = json.dumps(combined, ensure_ascii=False) + page
        self.assertNotIn(PRIVATE_MARKER, serialized)
        for forbidden in ("完整题干", "source_sentence", "private_dialogue"):
            self.assertNotIn(forbidden, json.dumps(combined, ensure_ascii=False))
        self.assertNotIn("{{", page)
        self.assertLess(page.index("subject-408"), page.index("subject-math"))
        self.assertLess(page.index("subject-math"), page.index("subject-english"))
        self.assertIn("快速入库实时流", page)
        self.assertIn("new EventSource('/api/stream')", page)
        self.assertIn("fetch('/api/refresh', { method: 'POST' })", page)
        self.assertNotIn("此按钮只复制命令", page)
        self.assertNotIn("数据警告", page)
        self.assertIn("prefers-reduced-motion", page)
        self.assertIn("focus-visible", page)
        self.assertIn("min-height: 44px", page)
        self.assertNotIn("<canvas", page.lower())
        self.assertNotIn("webgl", page.lower())

        summaries = (
            self.math / "错题知识网络/wiki/operation_center/dashboard-summary.json",
            self.cs408 / "wiki/operation_center/dashboard-summary.json",
            self.english / "wiki/operation_center/dashboard-summary.json",
        )
        for path, subject in zip(summaries, ("math", "408", "english")):
            summary = json.loads(path.read_text())
            self.assertEqual(summary["legacy_consumer_field"], {"keep": True})
            self.assertEqual(summary["quick_intake"]["study_date"], STUDY_DATE)
            self.assertEqual(
                summary["quick_intake"]["backlog_through_date"]["unconsumed_count"],
                projections[subject]["backlog_through_date"]["unconsumed_count"],
            )
            self.assertNotIn("items", summary["quick_intake"])

    def test_v3_independent_english_receipt_keeps_verified_time(self) -> None:
        package = self.english / "intake/packages" / STUDY_DATE / "EN-PKG-20260827-DDDDDDDDDDDDDDDD"
        receipt = json.loads((package / "receipt.json").read_text())
        receipt.update(schema_version="english_package_receipt_v3", capture_mode="independent_unit")
        write_json(package / "receipt.json", receipt)
        projection = PROJECTOR.project_english(self.config, STUDY_DATE, now=NOW)
        item = next(row for row in projection["items"] if row["package_id"].endswith("DDDDDDDDDDDDDDDD"))
        self.assertEqual(item["captured_at"], receipt["captured_at"])
        self.assertEqual(item["time_quality"], "verified")
        stamp, quality, _, _ = PROJECTOR._english_live_package_metadata(
            config=self.config, row={"path": str(package)}, tracker=PROJECTOR.SourceTracker()
        )
        self.assertEqual(stamp, receipt["captured_at"])
        self.assertEqual(quality, "verified")

    def test_v1_english_receipt_does_not_inherit_manifest_time(self) -> None:
        package = self.english / "intake/packages" / STUDY_DATE / "EN-PKG-20260827-DDDDDDDDDDDDDDDD"
        receipt = json.loads((package / "receipt.json").read_text())
        receipt["schema_version"] = "english_package_receipt_v1"
        receipt.pop("captured_at")
        write_json(package / "receipt.json", receipt)
        projection = PROJECTOR.project_english(self.config, STUDY_DATE, now=NOW)
        item = next(row for row in projection["items"] if row["package_id"].endswith("DDDDDDDDDDDDDDDD"))
        self.assertIsNone(item["captured_at"])
        self.assertEqual(item["time_quality"], "unknown")

        compatible = self.english / "intake/packages" / STUDY_DATE / "EN-PKG-20260827-CCCCCCCCCCCCCCCC"
        compatible_receipt = json.loads((compatible / "receipt.json").read_text())
        compatible_receipt["schema_version"] = "english_package_receipt_v1"
        write_json(compatible / "receipt.json", compatible_receipt)
        projection = PROJECTOR.project_english(self.config, STUDY_DATE, now=NOW)
        compatible_item = next(
            row
            for row in projection["items"]
            if row["package_id"].endswith("CCCCCCCCCCCCCCCC")
        )
        self.assertEqual(compatible_item["captured_at"], "2026-08-27T10:00:00+08:00")
        self.assertEqual(compatible_item["time_quality"], "activity_time_only")


if __name__ == "__main__":
    unittest.main()

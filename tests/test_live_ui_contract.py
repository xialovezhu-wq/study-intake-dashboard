from __future__ import annotations

import re
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = PROJECT_ROOT / "templates/index.template.html"
README = PROJECT_ROOT / "README.md"


class LiveUIContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.page = TEMPLATE.read_text(encoding="utf-8")
        cls.readme = README.read_text(encoding="utf-8")

    def test_title_fixed_columns_and_no_retired_ui(self) -> None:
        self.assertIn("<title>快速入库实时流</title>", self.page)
        self.assertIn("<h1>快速入库实时流</h1>", self.page)
        positions = [self.page.index(f"subject-{subject}") for subject in ("408", "math", "english")]
        self.assertEqual(positions, sorted(positions))
        self.assertIn("const SUBJECT_ORDER = ['408', 'math', 'english'];", self.page)
        for retired in (
            "今日",
            "手动全量快照",
            "命令已复制",
            "数据警告",
            "copy-refresh",
            "navigator.clipboard",
            "execCommand('copy')",
            "warning-box",
        ):
            self.assertNotIn(retired, self.page)

    def test_compact_live_status_and_refresh_action(self) -> None:
        for label in ("连接状态", "最后更新", "自动更新频率", "立即刷新"):
            self.assertIn(label, self.page)
        self.assertIn('id="live-status"', self.page)
        self.assertIn('id="refresh-now"', self.page)
        self.assertRegex(
            self.page,
            r"fetch\('/api/refresh',\s*\{\s*method:\s*'POST'",
        )
        self.assertIn("const degraded = render(await response.json())", self.page)
        self.assertIn("待归档历史证据", self.page)
        self.assertIn("其他残留", self.page)
        self.assertIn("legacy_archive_pending", self.page)
        self.assertIn("部分科目数据陈旧", self.page)
        self.assertIn("freshness.status === 'unavailable'", self.page)

    def test_file_redirect_snapshot_then_named_sse_events(self) -> None:
        self.assertIn("window.location.protocol === 'file:'", self.page)
        self.assertIn("window.location.replace('http://127.0.0.1:8767/')", self.page)
        snapshot_fetch = self.page.index("fetch('/api/snapshot'")
        stream_connect = self.page.index("new EventSource('/api/stream')")
        bootstrap = self.page.index("loadInitialSnapshot().finally(connectStream)")
        self.assertLess(snapshot_fetch, bootstrap)
        self.assertLess(stream_connect, bootstrap)
        for event_name in ("snapshot", "heartbeat", "error"):
            self.assertIn(f"addEventListener('{event_name}'", self.page)

    def test_last_known_good_stale_and_exponential_reconnect(self) -> None:
        self.assertIn("let lastKnownGood = null", self.page)
        self.assertIn("重连中 · 数据可能陈旧", self.page)
        self.assertIn("payload.status === 'stale'", self.page)
        self.assertIn("payload.snapshot", self.page)
        self.assertIn("reconnectDelay * 2", self.page)
        self.assertIn("Math.min(reconnectDelay * 2, 30000)", self.page)
        self.assertIn("stream.close()", self.page)

    def test_render_is_safe_textual_and_preserves_view_state(self) -> None:
        self.assertIn("处理状态：", self.page)
        self.assertIn("残留状态：", self.page)
        self.assertIn("node.textContent =", self.page)
        self.assertNotIn("innerHTML", self.page)
        self.assertIn("captureViewState", self.page)
        self.assertIn("expandedKeys", self.page)
        self.assertIn("timelineScroll", self.page)
        self.assertIn("renderSignature", self.page)
        self.assertIn("requestAnimationFrame", self.page)
        self.assertIn("replaceChildren", self.page)
        for private_field in (
            "private_dialogue",
            "private_conversation",
            "source_sentence",
            "full_passage",
            "question_text",
            "answer_text",
            "explanation_text",
        ):
            self.assertNotIn(private_field, self.page)

    def test_keyboard_access_motion_and_responsive_contract(self) -> None:
        self.assertIn("document.createElement('details')", self.page)
        self.assertIn("text('summary'", self.page)
        self.assertIn("summary:focus-visible", self.page)
        self.assertIn('aria-live="polite"', self.page)
        self.assertGreaterEqual(self.page.count("min-height: 44px"), 2)
        self.assertIn("prefers-reduced-motion: reduce", self.page)
        for width in (1440, 1024, 768, 390):
            self.assertIn(f"@media (max-width: {width}px)", self.page)

    def test_readme_describes_live_loopback_and_read_only_boundary(self) -> None:
        self.assertIn("http://127.0.0.1:8767/", self.readme)
        self.assertIn("GET /api/snapshot", self.readme)
        self.assertIn("GET /api/stream", self.readme)
        self.assertIn("POST /api/refresh", self.readme)
        self.assertIn("last-known-good", self.readme)
        self.assertIn("不执行三科正式写入", self.readme)


if __name__ == "__main__":
    unittest.main()

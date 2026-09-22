from __future__ import annotations

import plistlib
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PLIST = (
    ROOT
    / "launchd"
    / "com.xiazhibin.three-subject-quick-intake-dashboard.plist"
)
LABEL = "com.xiazhibin.three-subject-quick-intake-dashboard"


class LoginLaunchAgentContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with PLIST.open("rb") as handle:
            cls.config = plistlib.load(handle)

    def test_exact_loopback_viewer_identity(self) -> None:
        self.assertEqual(self.config["Label"], LABEL)
        self.assertEqual(
            self.config["ProgramArguments"],
            [
                "/opt/miniconda3/bin/python3",
                "-B",
                str(ROOT / "scripts/realtime_server.py"),
                "serve",
                "--host",
                "127.0.0.1",
                "--port",
                "8767",
                "--poll-seconds",
                "1",
            ],
        )
        self.assertEqual(self.config["WorkingDirectory"], str(ROOT))

    def test_login_and_recovery_contract(self) -> None:
        self.assertIs(self.config["RunAtLoad"], True)
        self.assertEqual(
            self.config["KeepAlive"], {"SuccessfulExit": False}
        )
        self.assertEqual(self.config["LimitLoadToSessionType"], "Aqua")
        self.assertEqual(self.config["ProcessType"], "Background")

    def test_has_no_formal_writer_or_external_binding(self) -> None:
        serialized = repr(self.config)
        for forbidden in (
            "0.0.0.0",
            "apply-nightly",
            "intake_apply_408",
            "quick_intake.py close",
            "/Volumes/T9-Data",
            "open -a",
        ):
            self.assertNotIn(forbidden, serialized)
        self.assertEqual(
            self.config["EnvironmentVariables"]["PYTHONDONTWRITEBYTECODE"],
            "1",
        )


if __name__ == "__main__":
    unittest.main()

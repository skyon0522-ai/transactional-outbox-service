"""Safety contracts for the public demo runner; no Go build required."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import run_demo


class DemoContracts(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name) / "new-run"

    def demo(self):
        return run_demo.Demo(Path(sys.executable), self.output)

    def test_existing_directory_is_refused_before_execution(self):
        self.output.mkdir()
        sentinel = self.output / "keep.txt"
        sentinel.write_bytes(b"user-owned bytes")
        with patch.object(run_demo.Demo, "execute") as execute:
            with self.assertRaises(FileExistsError):
                self.demo().run()
            execute.assert_not_called()
        self.assertEqual(sentinel.read_bytes(), b"user-owned bytes")
        self.assertEqual(list(self.output.iterdir()), [sentinel])

    def test_existing_output_symlink_is_refused(self):
        target = Path(self.temp.name) / "target"
        target.mkdir()
        self.output.symlink_to(target, target_is_directory=True)
        with self.assertRaises(FileExistsError):
            self.demo().run()
        self.assertEqual(list(target.iterdir()), [])

    def test_failure_retains_failed_receipt_and_stops_owned_child(self):
        demo = self.demo()

        def fail_after_start():
            demo.start("owned-child", "-c", "import time; time.sleep(60)")
            raise run_demo.DemoError("synthetic failure <script>alert(1)</script>")

        with patch.object(demo, "execute", fail_after_start):
            self.assertEqual(demo.run(), 1)
        receipt = json.loads((self.output / "result.json").read_text())
        self.assertEqual(receipt["status"], "failed")
        self.assertIsNotNone(demo.children[0][1].poll())
        self.assertEqual(len(receipt["cleanup"]), 1)
        html = (self.output / "report.html").read_text()
        self.assertIn("Demo incomplete", html)
        self.assertNotIn("<script>", html)
        self.assertIn("&lt;script&gt;", html)

    def test_unexpected_clean_process_exit_is_not_success(self):
        demo = self.demo()

        def exited_child():
            demo.start("early-exit", "-c", "pass").wait(timeout=5)

        with patch.object(demo, "execute", exited_child):
            self.assertEqual(demo.run(), 1)
        receipt = json.loads((self.output / "result.json").read_text())
        self.assertEqual(receipt["status"], "failed")
        self.assertTrue(receipt["cleanup"][0]["unexpected_exit"])

    def test_report_failure_cannot_leave_passed_receipt(self):
        demo = self.demo()
        original = Path.write_text

        def fail_report(path, *args, **kwargs):
            if path.name == "report.html":
                raise OSError("synthetic disk error")
            return original(path, *args, **kwargs)

        with patch.object(demo, "execute"), patch.object(Path, "write_text", fail_report):
            self.assertEqual(demo.run(), 1)
        receipt = json.loads((self.output / "result.json").read_text())
        self.assertEqual(receipt["status"], "failed")
        self.assertIn("report write failed", receipt["failure"])

    def test_running_marker_survives_incomplete_final_write(self):
        demo = self.demo()
        original = Path.write_text

        def fail_final(path, *args, **kwargs):
            if path.name == "result.final.json":
                raise OSError("synthetic disk error")
            return original(path, *args, **kwargs)

        with patch.object(demo, "execute"), patch.object(Path, "write_text", fail_final):
            with self.assertRaises(OSError):
                demo.run()
        self.assertEqual(json.loads((self.output / "result.json").read_text())["status"], "running")

    def test_report_escapes_check_fields_and_observations(self):
        html = run_demo.report_html({"status": "passed", "checks": [{"name": "<script>", "meaning": "<img onerror=bad>", "observed": "</pre><script>bad</script>"}]})
        self.assertNotIn("<script>", html)
        self.assertNotIn("<img ", html)
        self.assertIn("&lt;/pre&gt;", html)
        self.assertIn("default-src 'none'", html)


if __name__ == "__main__":
    unittest.main()

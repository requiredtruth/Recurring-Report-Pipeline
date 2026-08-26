from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

try:
    from PySide6.QtWidgets import QApplication
    from recurring_report_pipeline.gui import MainWindow
except ImportError:
    QApplication = None
    MainWindow = None


@unittest.skipIf(QApplication is None, "PySide6 not installed in core-only test environment")
class GuiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = QApplication.instance() or QApplication([])

    def test_full_window_constructs_offscreen(self):
        with tempfile.TemporaryDirectory() as temp:
            window = MainWindow(Path(temp))
            self.assertIn("Recurring Report", window.windowTitle())
            self.assertEqual(window.pipelines.columnCount(), 3)
            self.assertEqual(window.runs.columnCount(), 5)
            window.close()

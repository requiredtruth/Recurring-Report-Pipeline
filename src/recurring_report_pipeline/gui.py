"""PySide6 desktop control and status interface."""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

from PySide6.QtCore import QObject, QThread, Signal
from PySide6.QtWidgets import QApplication, QFileDialog, QHBoxLayout, QLabel, QMainWindow, QMessageBox, QPlainTextEdit, QProgressBar, QPushButton, QTableWidget, QTableWidgetItem, QVBoxLayout, QWidget

from .demo import run_demo
from .pipeline import safe_error
from .service import Service


class Worker(QObject):
    progress = Signal(str, int, int, str)
    finished = Signal(object)
    failed = Signal(str)

    def __init__(self, action):
        super().__init__()
        self.action = action

    def run(self) -> None:
        try:
            self.finished.emit(self.action(lambda p, d, t, i: self.progress.emit(p, d, -1 if t is None else t, i)))
        except Exception as exc:
            code, message = safe_error(exc)
            self.failed.emit(f"{code}: {message}")


class MainWindow(QMainWindow):
    """Desktop dashboard for pipeline registration, execution, and evidence."""

    def __init__(self, data_dir: Path | None = None):
        super().__init__()
        self.data_dir = data_dir or Path.cwd() / ".report-pipeline"
        self.service = Service(self.data_dir)
        self.thread: QThread | None = None
        self.setWindowTitle("Recurring Report Pipeline")
        self.resize(960, 680)
        root = QWidget(); layout = QVBoxLayout(root)
        title = QLabel("Recurring Report Pipeline — local scheduled reports with quality gates")
        title.setStyleSheet("font-size: 18px; font-weight: 600")
        layout.addWidget(title)
        buttons = QHBoxLayout()
        self.demo_button = QPushButton("Run safe demo")
        self.import_button = QPushButton("Import pipeline")
        self.due_button = QPushButton("Run due now")
        self.refresh_button = QPushButton("Refresh")
        for button in (self.demo_button, self.import_button, self.due_button, self.refresh_button): buttons.addWidget(button)
        layout.addLayout(buttons)
        self.status = QLabel("Ready")
        self.progress = QProgressBar(); self.progress.setRange(0, 1); self.progress.setValue(0)
        layout.addWidget(self.status); layout.addWidget(self.progress)
        layout.addWidget(QLabel("Registered pipelines"))
        self.pipelines = QTableWidget(0, 3); self.pipelines.setHorizontalHeaderLabels(["Name", "Next due (UTC)", "Enabled"])
        layout.addWidget(self.pipelines)
        layout.addWidget(QLabel("Recent runs"))
        self.runs = QTableWidget(0, 5); self.runs.setHorizontalHeaderLabels(["Pipeline", "Status", "Scheduled for", "Error", "Run ID"])
        layout.addWidget(self.runs)
        self.log = QPlainTextEdit(); self.log.setReadOnly(True); self.log.setMaximumBlockCount(500)
        layout.addWidget(self.log)
        self.setCentralWidget(root)
        self.demo_button.clicked.connect(self.run_demo)
        self.import_button.clicked.connect(self.import_config)
        self.due_button.clicked.connect(self.run_due)
        self.refresh_button.clicked.connect(self.refresh)
        self.refresh()

    def refresh(self) -> None:
        pipelines = self.service.store.pipelines(); self.pipelines.setRowCount(len(pipelines))
        for row, item in enumerate(pipelines):
            for col, value in enumerate((item["name"], item["next_due"], "yes" if item["enabled"] else "no")):
                self.pipelines.setItem(row, col, QTableWidgetItem(str(value)))
        runs = self.service.store.runs(); self.runs.setRowCount(len(runs))
        for row, item in enumerate(runs):
            for col, value in enumerate((item["pipeline_name"], item["status"], item["scheduled_for"], item["error_code"] or "", item["id"])):
                self.runs.setItem(row, col, QTableWidgetItem(str(value)))

    def _start(self, action) -> None:
        if self.thread is not None:
            return
        for button in (self.demo_button, self.import_button, self.due_button): button.setEnabled(False)
        self.status.setText("Starting…"); self.progress.setRange(0, 0)
        thread = QThread(self); worker = Worker(action); worker.moveToThread(thread)
        thread.started.connect(worker.run); worker.progress.connect(self._progress)
        worker.finished.connect(self._done); worker.failed.connect(self._failed)
        worker.finished.connect(thread.quit); worker.failed.connect(thread.quit)
        thread.finished.connect(lambda: self._clear_thread(thread))
        thread.worker = worker  # type: ignore[attr-defined]
        self.thread = thread; thread.start()

    def _progress(self, phase: str, done: int, total: int, item: str) -> None:
        self.status.setText(f"{phase}: {item}")
        if total < 0: self.progress.setRange(0, 0)
        else: self.progress.setRange(0, max(total, 1)); self.progress.setValue(done)
        self.log.appendPlainText(f"{phase}: {item}")

    def _done(self, result: object) -> None:
        self.status.setText("Complete"); self.progress.setRange(0, 1); self.progress.setValue(1)
        self.log.appendPlainText(json.dumps(result, sort_keys=True)); self.refresh()

    def _failed(self, message: str) -> None:
        self.status.setText("Failed — see status below"); self.progress.setRange(0, 1); self.progress.setValue(0)
        self.log.appendPlainText(message); QMessageBox.critical(self, "Pipeline failed", message)

    def _clear_thread(self, thread: QThread) -> None:
        thread.deleteLater(); self.thread = None
        for button in (self.demo_button, self.import_button, self.due_button): button.setEnabled(True)

    def run_demo(self) -> None:
        demo_root = self.data_dir / "demo"
        self._start(lambda progress: run_demo(demo_root, progress))

    def run_due(self) -> None:
        self._start(lambda progress: self.service.run_due(progress=progress))

    def import_config(self) -> None:
        selected, _ = QFileDialog.getOpenFileName(self, "Import pipeline JSON", "", "JSON (*.json)")
        if not selected: return
        try:
            config = self.service.register(Path(selected)); self.log.appendPlainText(f"registered: {config.name}"); self.refresh()
        except Exception as exc:
            code, message = safe_error(exc)
            QMessageBox.critical(self, "Import failed", f"{code}: {message}")


def main() -> int:
    app = QApplication(sys.argv)
    window = MainWindow(); window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())

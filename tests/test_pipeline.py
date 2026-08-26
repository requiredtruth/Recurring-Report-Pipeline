from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from recurring_report_pipeline.config import ConfigError, load_config
from recurring_report_pipeline.demo import DEMO_CONFIG, create_demo, run_demo
from recurring_report_pipeline.pipeline import PipelineError, execute_pipeline, safe_error
from recurring_report_pipeline.schedule import next_due
from recurring_report_pipeline.service import Service
from recurring_report_pipeline.store import Store, iso


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def config(self, edits=None):
        raw = json.loads(json.dumps(DEMO_CONFIG))
        if edits:
            raw.update(edits)
        path = self.root / "config.json"
        path.write_text(json.dumps(raw), encoding="utf-8")
        return path

    def test_config_rejects_path_escape_and_unknown_fields(self):
        with self.assertRaisesRegex(ConfigError, "contained relative"):
            load_config(self.config({"source": "../private.csv"}))
        with self.assertRaisesRegex(ConfigError, "unknown field"):
            load_config(self.config({"mystery": True}))

    def test_config_rejects_sum_of_text(self):
        raw = json.loads(json.dumps(DEMO_CONFIG))
        raw["aggregates"] = [{"column": "region", "op": "sum", "name": "bad_sum"}]
        with self.assertRaisesRegex(ConfigError, "integer or number"):
            load_config(self.config(raw))

    def test_schedule_interval_daily_weekly_monthly(self):
        point = datetime(2026, 1, 5, 10, tzinfo=timezone.utc)
        self.assertEqual(next_due({"kind": "interval", "seconds": 60}, point), point + timedelta(minutes=1))
        self.assertEqual(next_due({"kind": "daily", "time": "09:00"}, point).day, 6)
        self.assertEqual(next_due({"kind": "weekly", "weekday": 0, "time": "09:00"}, point).day, 12)
        self.assertEqual(next_due({"kind": "monthly", "day": 5, "time": "09:00"}, point).month, 2)

    def test_demo_is_complete_and_delivered(self):
        manifest = run_demo(self.root)
        self.assertEqual(manifest["row_count"], 2)
        names = {item["name"] for item in manifest["files"]}
        self.assertEqual(names, {"report.csv", "report.json", "report.html"})
        run_id = manifest["run_id"]
        self.assertTrue((self.root / "runs" / run_id / "manifest.json").is_file())
        self.assertTrue((self.root / "deliveries" / "review_outbox" / run_id / "report.html").is_file())
        rows = json.loads((self.root / "runs" / run_id / "report.json").read_text())
        self.assertEqual(rows[0], {"order_count": 2, "region": "North", "total_amount": "20.00"})

    def test_quality_gate_blocks_blank_and_does_not_publish(self):
        path = create_demo(self.root)
        raw = json.loads(path.read_text())
        raw["filters"] = [{"column": "status", "op": "eq", "value": "never"}]
        path.write_text(json.dumps(raw))
        service = Service(self.root)
        service.register(path)
        with self.assertRaises(PipelineError) as caught:
            service.run("weekly_summary")
        self.assertEqual(caught.exception.code, "MIN_ROWS")
        self.assertEqual(list((self.root / "runs").iterdir()), [])
        self.assertEqual(service.store.runs()[0]["status"], "failed")

    def test_missing_column_is_detected_before_output(self):
        path = create_demo(self.root)
        (self.root / "inputs" / "synthetic_orders.csv").write_text("Region,Amount\nNorth,1\n")
        service = Service(self.root); service.register(path)
        with self.assertRaises(PipelineError) as caught:
            service.run("weekly_summary")
        self.assertEqual(caught.exception.code, "MISSING_COLUMNS")

    def test_row_and_input_bounds(self):
        path = create_demo(self.root)
        raw = json.loads(path.read_text()); raw["limits"] = {"max_input_bytes": 10, "max_rows": 1, "max_columns": 3, "max_cell_chars": 10}
        path.write_text(json.dumps(raw)); service = Service(self.root); service.register(path)
        with self.assertRaises(PipelineError) as caught: service.run("weekly_summary")
        self.assertEqual(caught.exception.code, "INPUT_LIMIT")

    def test_due_slot_is_claimed_once_and_advanced(self):
        path = create_demo(self.root); service = Service(self.root)
        slot = datetime(2026, 1, 5, 9, tzinfo=timezone.utc); service.register(path, now=slot)
        first = service.run_due(slot); second = service.run_due(slot)
        self.assertEqual(first[0]["status"], "complete")
        self.assertEqual(second, [])
        self.assertEqual(len(service.store.runs()), 1)

    def test_store_recovers_stale_run(self):
        path = create_demo(self.root); service = Service(self.root)
        slot = datetime(2026, 1, 5, 9, tzinfo=timezone.utc); service.register(path, now=slot)
        run_id = service.store.claim("weekly_summary", slot)
        service.store.db.execute("UPDATE runs SET started_at=? WHERE id=?", (iso(datetime.now(timezone.utc)-timedelta(days=1)), run_id))
        self.assertEqual(service.store.recover_stale(), 1)
        row = service.store.runs()[0]
        self.assertEqual((row["status"], row["error_code"]), ("failed", "STALE_RUN"))

    def test_register_is_repeat_safe_and_preserves_due_slot(self):
        path = create_demo(self.root); service = Service(self.root)
        first = datetime(2026, 1, 5, 9, tzinfo=timezone.utc); second = first + timedelta(days=1)
        service.register(path, now=first); service.register(path, now=second)
        self.assertEqual(service.store.pipelines()[0]["next_due"], iso(first))

    def test_safe_error_redacts_absolute_paths(self):
        code, message = safe_error(RuntimeError("failed at /var/private/example.csv"))
        self.assertEqual(code, "RUNTIMEERROR")
        self.assertNotIn("/var/private", message)

    def test_status_does_not_expose_config_path_or_body(self):
        path = create_demo(self.root); service = Service(self.root); service.register(path)
        from recurring_report_pipeline.cli import main
        from contextlib import redirect_stdout
        from io import StringIO
        output = StringIO()
        with redirect_stdout(output):
            self.assertEqual(main(["--data-dir", str(self.root), "status"]), 0)
        status = json.loads(output.getvalue())
        self.assertNotIn("config_path", status["pipelines"][0])
        self.assertNotIn("config_json", status["pipelines"][0])

    def test_schema_version_guard(self):
        store = Store(self.root / "state.sqlite3"); store.close()
        db = sqlite3.connect(self.root / "state.sqlite3"); db.execute("UPDATE metadata SET value='99' WHERE key='schema_version'"); db.commit(); db.close()
        with self.assertRaisesRegex(RuntimeError, "unsupported"):
            Store(self.root / "state.sqlite3")


if __name__ == "__main__":
    unittest.main()

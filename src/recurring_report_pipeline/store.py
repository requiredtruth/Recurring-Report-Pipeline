"""SQLite control plane with atomic run claims and append-only events."""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 1


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


class Store:
    """Durable pipeline/run state stored in one local SQLite database."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.db = sqlite3.connect(path, timeout=10, isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA busy_timeout=10000")
        self.db.execute("PRAGMA journal_mode=WAL")
        self._migrate()

    def _migrate(self) -> None:
        self.db.executescript("""
        BEGIN IMMEDIATE;
        CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS pipelines(
          name TEXT PRIMARY KEY, config_path TEXT NOT NULL, config_json TEXT NOT NULL,
          enabled INTEGER NOT NULL DEFAULT 1, next_due TEXT NOT NULL,
          created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS runs(
          id TEXT PRIMARY KEY, pipeline_name TEXT NOT NULL REFERENCES pipelines(name),
          scheduled_for TEXT NOT NULL, status TEXT NOT NULL,
          attempt INTEGER NOT NULL DEFAULT 1, started_at TEXT, finished_at TEXT,
          input_sha256 TEXT, output_manifest TEXT, error_code TEXT, error_message TEXT,
          UNIQUE(pipeline_name, scheduled_for)
        );
        CREATE TABLE IF NOT EXISTS events(
          id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, event TEXT NOT NULL,
          target TEXT NOT NULL, outcome TEXT NOT NULL, run_id TEXT, detail TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS runs_pipeline_status ON runs(pipeline_name,status);
        INSERT OR IGNORE INTO metadata(key,value) VALUES('schema_version','1');
        COMMIT;
        """)
        version = int(self.db.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()[0])
        if version != SCHEMA_VERSION:
            raise RuntimeError(f"unsupported database schema version: {version}")

    def close(self) -> None:
        self.db.close()

    def register(self, name: str, config_path: Path, config_json: dict[str, Any], next_due_at: datetime) -> None:
        now = iso(utc_now())
        with self.db:
            self.db.execute("""INSERT INTO pipelines(name,config_path,config_json,next_due,created_at,updated_at)
              VALUES(?,?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET config_path=excluded.config_path,
              config_json=excluded.config_json, updated_at=excluded.updated_at""",
              (name, str(config_path), json.dumps(config_json, sort_keys=True), iso(next_due_at), now, now))
            self.event("pipeline_registered", name, "ok", detail="configuration validated")

    def event(self, event: str, target: str, outcome: str, run_id: str | None = None, detail: str = "") -> None:
        self.db.execute("INSERT INTO events(at,event,target,outcome,run_id,detail) VALUES(?,?,?,?,?,?)",
                        (iso(utc_now()), event, target, outcome, run_id, detail[:512]))

    def claim(self, name: str, scheduled_for: datetime) -> str | None:
        run_id = uuid.uuid4().hex
        try:
            with self.db:
                row = self.db.execute("SELECT enabled FROM pipelines WHERE name=?", (name,)).fetchone()
                if row is None or not row["enabled"]:
                    return None
                self.db.execute("INSERT INTO runs(id,pipeline_name,scheduled_for,status,started_at) VALUES(?,?,?,?,?)",
                                (run_id, name, iso(scheduled_for), "running", iso(utc_now())))
                self.event("run_started", name, "running", run_id)
            return run_id
        except sqlite3.IntegrityError:
            return None

    def finish(self, run_id: str, manifest: dict[str, Any], input_sha256: str) -> None:
        with self.db:
            row = self.db.execute("SELECT pipeline_name FROM runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                raise KeyError(run_id)
            self.db.execute("UPDATE runs SET status='complete',finished_at=?,input_sha256=?,output_manifest=? WHERE id=?",
                            (iso(utc_now()), input_sha256, json.dumps(manifest, sort_keys=True), run_id))
            self.event("run_completed", row["pipeline_name"], "ok", run_id, f"rows={manifest['row_count']}")

    def fail(self, run_id: str, code: str, message: str) -> None:
        with self.db:
            row = self.db.execute("SELECT pipeline_name FROM runs WHERE id=?", (run_id,)).fetchone()
            if row is None:
                return
            self.db.execute("UPDATE runs SET status='failed',finished_at=?,error_code=?,error_message=? WHERE id=?",
                            (iso(utc_now()), code[:64], message[:512], run_id))
            self.event("run_failed", row["pipeline_name"], "failed", run_id, code[:64])

    def set_next_due(self, name: str, value: datetime) -> None:
        self.db.execute("UPDATE pipelines SET next_due=?,updated_at=? WHERE name=?", (iso(value), iso(utc_now()), name))

    def due(self, now: datetime, limit: int = 32) -> list[sqlite3.Row]:
        return list(self.db.execute("SELECT * FROM pipelines WHERE enabled=1 AND next_due<=? ORDER BY next_due LIMIT ?", (iso(now), limit)))

    def pipelines(self) -> list[sqlite3.Row]:
        return list(self.db.execute("SELECT * FROM pipelines ORDER BY name"))

    def runs(self, limit: int = 100) -> list[sqlite3.Row]:
        return list(self.db.execute("SELECT * FROM runs ORDER BY COALESCE(started_at,scheduled_for) DESC LIMIT ?", (limit,)))

    def recover_stale(self, older_than: timedelta = timedelta(hours=6)) -> int:
        cutoff = iso(utc_now() - older_than)
        with self.db:
            rows = list(self.db.execute("SELECT id,pipeline_name FROM runs WHERE status='running' AND started_at<?", (cutoff,)))
            for row in rows:
                self.db.execute("UPDATE runs SET status='failed',finished_at=?,error_code='STALE_RUN',error_message='worker stopped before completion' WHERE id=?", (iso(utc_now()), row["id"]))
                self.event("run_recovered", row["pipeline_name"], "failed", row["id"], "STALE_RUN")
        return len(rows)

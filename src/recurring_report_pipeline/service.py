"""Application service joining configuration, storage, schedules, and execution."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .config import PipelineConfig, load_config
from .pipeline import execute_pipeline, safe_error
from .schedule import next_due
from .store import Store, iso


class Service:
    """Local control service for registering and executing recurring reports."""

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir.resolve()
        for name in ("configs", "inputs", "runs", "deliveries"):
            (self.data_dir / name).mkdir(parents=True, exist_ok=True)
        self.store = Store(self.data_dir / "state.sqlite3")
        self.store.recover_stale()

    def register(self, config_path: Path, now: datetime | None = None) -> PipelineConfig:
        config = load_config(config_path)
        raw = json.loads(config_path.read_text(encoding="utf-8"))
        target = self.data_dir / "configs" / f"{config.name}.json"
        target.write_text(json.dumps(raw, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        point = now or datetime.now(timezone.utc)
        self.store.register(config.name, target, raw, point)
        return config

    def _config(self, row: object) -> PipelineConfig:
        return load_config(Path(row["config_path"]))  # type: ignore[index]

    def run(self, name: str, *, scheduled_for: datetime | None = None, progress: Callable[[str, int, int | None, str], None] | None = None) -> dict | None:
        row = next((x for x in self.store.pipelines() if x["name"] == name), None)
        if row is None:
            raise KeyError(name)
        config = self._config(row)
        slot = scheduled_for or datetime.now(timezone.utc)
        run_id = self.store.claim(name, slot)
        if run_id is None:
            return None
        try:
            manifest = execute_pipeline(config, input_root=self.data_dir / "inputs", output_root=self.data_dir / "runs", delivery_root=self.data_dir / "deliveries", run_id=run_id, progress=progress)
            self.store.finish(run_id, manifest, manifest["input_sha256"])
            return manifest
        except Exception as exc:
            code, message = safe_error(exc)
            self.store.fail(run_id, code, message)
            raise

    def run_due(self, now: datetime | None = None, progress: Callable[[str, int, int | None, str], None] | None = None) -> list[dict]:
        point = now or datetime.now(timezone.utc)
        results: list[dict] = []
        for row in self.store.due(point):
            slot = datetime.fromisoformat(row["next_due"].replace("Z", "+00:00"))
            config = self._config(row)
            try:
                manifest = self.run(row["name"], scheduled_for=slot, progress=progress)
                results.append({"pipeline": row["name"], "scheduled_for": iso(slot), "status": "complete" if manifest else "duplicate"})
            except Exception as exc:
                code, _ = safe_error(exc)
                results.append({"pipeline": row["name"], "scheduled_for": iso(slot), "status": "failed", "error_code": code})
            finally:
                self.store.set_next_due(row["name"], next_due(config.schedule, slot))
        return results

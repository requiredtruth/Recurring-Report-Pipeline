"""Safe synthetic end-to-end demonstration."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from .service import Service

DEMO_CONFIG = {
    "name": "weekly_summary",
    "source": "synthetic_orders.csv",
    "columns": [
        {"source": "Region", "name": "region", "type": "string", "required": True},
        {"source": "Amount", "name": "amount", "type": "number", "required": True},
        {"source": "Status", "name": "status", "type": "string", "required": True},
    ],
    "filters": [{"column": "status", "op": "eq", "value": "complete"}],
    "group_by": ["region"],
    "aggregates": [
        {"column": "amount", "op": "count", "name": "order_count"},
        {"column": "amount", "op": "sum", "name": "total_amount"},
    ],
    "sort": [{"column": "total_amount", "descending": True}],
    "outputs": ["csv", "json", "html"],
    "deliveries": [{"type": "local_copy", "destination": "review_outbox"}],
    "quality": {"min_rows": 1, "max_rows": 10, "reject_blank_output": True, "required_source_columns": ["Region", "Amount", "Status"]},
    "schedule": {"kind": "weekly", "weekday": 0, "time": "09:00"},
}


def create_demo(data_dir: Path) -> Path:
    """Write only neutral synthetic demo inputs and return the config path."""
    (data_dir / "inputs").mkdir(parents=True, exist_ok=True)
    (data_dir / "configs").mkdir(parents=True, exist_ok=True)
    (data_dir / "inputs" / "synthetic_orders.csv").write_text(
        "Region,Amount,Status\nNorth,12.50,complete\nSouth,8.25,pending\nNorth,7.50,complete\nSouth,11.00,complete\n",
        encoding="utf-8",
    )
    path = data_dir / "configs" / "weekly_summary.json"
    path.write_text(json.dumps(DEMO_CONFIG, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def run_demo(data_dir: Path, progress=None) -> dict:
    """Create and execute the complete deterministic local demo."""
    service = Service(data_dir)
    config = service.register(create_demo(data_dir), now=datetime(2026, 1, 5, 9, tzinfo=timezone.utc))
    manifest = service.run(config.name, progress=progress)
    if manifest is None:
        raise RuntimeError("demo slot was already executed")
    return manifest

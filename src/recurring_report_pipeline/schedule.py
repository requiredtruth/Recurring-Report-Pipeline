"""Deterministic UTC schedule calculations."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def next_due(schedule: dict[str, Any], after: datetime) -> datetime:
    """Return the first scheduled UTC instant strictly after ``after``."""
    after = after.astimezone(timezone.utc)
    kind = schedule["kind"]
    if kind == "interval":
        return after + timedelta(seconds=schedule["seconds"])
    hour, minute = map(int, schedule["time"].split(":"))
    candidate = after.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if kind == "daily":
        return candidate if candidate > after else candidate + timedelta(days=1)
    if kind == "weekly":
        candidate += timedelta(days=(schedule["weekday"] - candidate.weekday()) % 7)
        return candidate if candidate > after else candidate + timedelta(days=7)
    candidate = candidate.replace(day=schedule["day"])
    if candidate > after:
        return candidate
    year = candidate.year + (1 if candidate.month == 12 else 0)
    month = 1 if candidate.month == 12 else candidate.month + 1
    return candidate.replace(year=year, month=month)

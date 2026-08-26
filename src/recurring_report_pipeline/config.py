"""Strict, bounded JSON configuration loading."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{0,62}$")
TYPES = {"string", "integer", "number", "boolean", "date"}
OPS = {"eq", "ne", "gt", "ge", "lt", "le", "contains"}
AGGREGATES = {"count", "sum", "average", "min", "max"}


class ConfigError(ValueError):
    """Raised when a pipeline configuration violates its public schema."""


@dataclass(frozen=True)
class Limits:
    max_input_bytes: int = 25_000_000
    max_rows: int = 250_000
    max_columns: int = 128
    max_cell_chars: int = 32_768


@dataclass(frozen=True)
class PipelineConfig:
    name: str
    source: str
    columns: tuple[dict[str, Any], ...]
    filters: tuple[dict[str, Any], ...]
    group_by: tuple[str, ...]
    aggregates: tuple[dict[str, str], ...]
    sort: tuple[dict[str, Any], ...]
    outputs: tuple[str, ...]
    deliveries: tuple[dict[str, str], ...]
    quality: dict[str, Any]
    schedule: dict[str, Any]
    limits: Limits


def _keys(obj: dict[str, Any], allowed: set[str], where: str) -> None:
    unknown = sorted(set(obj) - allowed)
    if unknown:
        raise ConfigError(f"{where}: unknown field(s): {', '.join(unknown)}")


def _positive(value: Any, name: str, ceiling: int) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 1 <= value <= ceiling:
        raise ConfigError(f"{name} must be an integer from 1 to {ceiling}")
    return value


def load_config(path: Path) -> PipelineConfig:
    """Load and validate one UTF-8 JSON config, capped at 256 KiB."""
    if path.stat().st_size > 262_144:
        raise ConfigError("configuration exceeds 256 KiB")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ConfigError(f"configuration is not valid UTF-8 JSON: {exc.__class__.__name__}") from exc
    if not isinstance(raw, dict):
        raise ConfigError("configuration root must be an object")
    _keys(raw, {"name", "source", "columns", "filters", "group_by", "aggregates", "sort", "outputs", "deliveries", "quality", "schedule", "limits"}, "configuration")
    name = raw.get("name")
    if not isinstance(name, str) or not NAME_RE.fullmatch(name):
        raise ConfigError("name must match ^[a-z][a-z0-9_-]{0,62}$")
    source = raw.get("source")
    if not isinstance(source, str) or not source or Path(source).is_absolute() or ".." in Path(source).parts:
        raise ConfigError("source must be a contained relative path")
    cols = raw.get("columns")
    if not isinstance(cols, list) or not 1 <= len(cols) <= 128:
        raise ConfigError("columns must contain 1 to 128 definitions")
    seen: set[str] = set()
    column_types: dict[str, str] = {}
    for i, col in enumerate(cols):
        if not isinstance(col, dict):
            raise ConfigError(f"columns[{i}] must be an object")
        _keys(col, {"source", "name", "type", "required", "default"}, f"columns[{i}]")
        if not isinstance(col.get("source"), str) or not isinstance(col.get("name"), str):
            raise ConfigError(f"columns[{i}] source and name must be strings")
        if not NAME_RE.fullmatch(col["name"]):
            raise ConfigError(f"columns[{i}].name is invalid")
        if col["name"] in seen:
            raise ConfigError(f"duplicate output column: {col['name']}")
        seen.add(col["name"])
        if col.get("type", "string") not in TYPES:
            raise ConfigError(f"columns[{i}].type is unsupported")
        column_types[col["name"]] = col.get("type", "string")
        if "required" in col and not isinstance(col["required"], bool):
            raise ConfigError(f"columns[{i}].required must be boolean")
    filters = raw.get("filters", [])
    if not isinstance(filters, list) or len(filters) > 32:
        raise ConfigError("filters must be a list of at most 32 items")
    for i, item in enumerate(filters):
        if not isinstance(item, dict):
            raise ConfigError(f"filters[{i}] must be an object")
        _keys(item, {"column", "op", "value"}, f"filters[{i}]")
        if item.get("column") not in seen or item.get("op") not in OPS or "value" not in item:
            raise ConfigError(f"filters[{i}] has an invalid column, operation, or value")
    group_by = raw.get("group_by", [])
    if not isinstance(group_by, list) or any(x not in seen for x in group_by) or len(set(group_by)) != len(group_by):
        raise ConfigError("group_by must contain distinct normalized column names")
    aggs = raw.get("aggregates", [])
    if not isinstance(aggs, list) or len(aggs) > 32:
        raise ConfigError("aggregates must be a list of at most 32 items")
    agg_names: set[str] = set()
    for i, item in enumerate(aggs):
        if not isinstance(item, dict):
            raise ConfigError(f"aggregates[{i}] must be an object")
        _keys(item, {"column", "op", "name"}, f"aggregates[{i}]")
        if item.get("op") not in AGGREGATES or item.get("column") not in seen or not NAME_RE.fullmatch(str(item.get("name", ""))):
            raise ConfigError(f"aggregates[{i}] is invalid")
        if item["op"] in {"sum", "average"} and column_types[item["column"]] not in {"integer", "number"}:
            raise ConfigError(f"aggregates[{i}] requires an integer or number column")
        if item["name"] in seen or item["name"] in agg_names:
            raise ConfigError(f"duplicate aggregate name: {item['name']}")
        agg_names.add(item["name"])
    if group_by and not aggs:
        raise ConfigError("group_by requires at least one aggregate")
    sort = raw.get("sort", [])
    result_names = (set(group_by) | agg_names) if aggs else seen
    if not isinstance(sort, list) or len(sort) > 16:
        raise ConfigError("sort must be a list of at most 16 items")
    for i, item in enumerate(sort):
        if not isinstance(item, dict) or item.get("column") not in result_names or item.get("descending", False) not in (True, False):
            raise ConfigError(f"sort[{i}] is invalid")
        _keys(item, {"column", "descending"}, f"sort[{i}]")
    outputs = raw.get("outputs", ["csv", "json", "html"])
    if not isinstance(outputs, list) or not outputs or len(set(outputs)) != len(outputs) or any(x not in {"csv", "json", "html"} for x in outputs):
        raise ConfigError("outputs must be a nonempty distinct subset of csv, json, html")
    deliveries = raw.get("deliveries", [])
    if not isinstance(deliveries, list) or len(deliveries) > 16:
        raise ConfigError("deliveries must be a list of at most 16 local-copy adapters")
    for i, item in enumerate(deliveries):
        if not isinstance(item, dict):
            raise ConfigError(f"deliveries[{i}] must be an object")
        _keys(item, {"type", "destination"}, f"deliveries[{i}]")
        dest = item.get("destination")
        if item.get("type") != "local_copy" or not isinstance(dest, str) or not dest or Path(dest).is_absolute() or ".." in Path(dest).parts:
            raise ConfigError(f"deliveries[{i}] must be a contained local_copy destination")
    quality = raw.get("quality", {})
    if not isinstance(quality, dict):
        raise ConfigError("quality must be an object")
    _keys(quality, {"min_rows", "max_rows", "reject_blank_output", "required_source_columns"}, "quality")
    min_rows = quality.get("min_rows", 1)
    max_quality_rows = quality.get("max_rows", 250_000)
    if not isinstance(min_rows, int) or not isinstance(max_quality_rows, int) or min_rows < 0 or max_quality_rows < min_rows:
        raise ConfigError("quality row bounds are invalid")
    required_source = quality.get("required_source_columns", [])
    if not isinstance(required_source, list) or any(not isinstance(x, str) for x in required_source):
        raise ConfigError("required_source_columns must be a string list")
    schedule = raw.get("schedule", {"kind": "interval", "seconds": 86400})
    _validate_schedule(schedule)
    limits_raw = raw.get("limits", {})
    if not isinstance(limits_raw, dict):
        raise ConfigError("limits must be an object")
    _keys(limits_raw, {"max_input_bytes", "max_rows", "max_columns", "max_cell_chars"}, "limits")
    limits = Limits(
        _positive(limits_raw.get("max_input_bytes", 25_000_000), "limits.max_input_bytes", 1_000_000_000),
        _positive(limits_raw.get("max_rows", 250_000), "limits.max_rows", 5_000_000),
        _positive(limits_raw.get("max_columns", 128), "limits.max_columns", 1024),
        _positive(limits_raw.get("max_cell_chars", 32_768), "limits.max_cell_chars", 1_000_000),
    )
    return PipelineConfig(name, source, tuple(cols), tuple(filters), tuple(group_by), tuple(aggs), tuple(sort), tuple(outputs), tuple(deliveries), quality, schedule, limits)


def _validate_schedule(schedule: Any) -> None:
    if not isinstance(schedule, dict):
        raise ConfigError("schedule must be an object")
    kind = schedule.get("kind")
    if kind == "interval":
        _keys(schedule, {"kind", "seconds"}, "schedule")
        _positive(schedule.get("seconds"), "schedule.seconds", 31_536_000)
        if schedule["seconds"] < 60:
            raise ConfigError("schedule.seconds must be at least 60")
    elif kind in {"daily", "weekly", "monthly"}:
        allowed = {"kind", "time"} | ({"weekday"} if kind == "weekly" else set()) | ({"day"} if kind == "monthly" else set())
        _keys(schedule, allowed, "schedule")
        if not isinstance(schedule.get("time"), str) or not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", schedule["time"]):
            raise ConfigError("schedule.time must be UTC HH:MM")
        if kind == "weekly" and schedule.get("weekday") not in range(7):
            raise ConfigError("schedule.weekday must be 0 (Monday) through 6")
        if kind == "monthly" and schedule.get("day") not in range(1, 29):
            raise ConfigError("schedule.day must be 1 through 28")
    else:
        raise ConfigError("schedule.kind must be interval, daily, weekly, or monthly")

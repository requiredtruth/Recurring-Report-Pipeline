"""Bounded CSV normalization, transformation, reporting, and local delivery."""

from __future__ import annotations

import csv
import hashlib
import html
import io
import json
import os
import re
import shutil
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable

from .config import PipelineConfig

Progress = Callable[[str, int, int | None, str], None]


class PipelineError(RuntimeError):
    """Expected bounded pipeline failure with a stable error code."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _contained(root: Path, relative: str) -> Path:
    root = root.resolve()
    path = (root / relative).resolve()
    if path != root and root not in path.parents:
        raise PipelineError("PATH_ESCAPE", "configured path escapes its allowed root")
    return path


def _convert(value: str, kind: str) -> Any:
    value = value.strip()
    if kind == "string":
        return value
    if kind == "integer":
        try:
            return int(value)
        except ValueError as exc:
            raise PipelineError("TYPE_ERROR", "value is not an integer") from exc
    if kind == "number":
        try:
            return Decimal(value)
        except InvalidOperation as exc:
            raise PipelineError("TYPE_ERROR", "value is not a decimal number") from exc
    if kind == "boolean":
        lowered = value.lower()
        if lowered in {"true", "yes", "1"}:
            return True
        if lowered in {"false", "no", "0"}:
            return False
        raise PipelineError("TYPE_ERROR", "value is not a recognized boolean")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise PipelineError("TYPE_ERROR", "value is not an ISO date") from exc


def _json_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, date):
        return value.isoformat()
    return value


def _display(value: Any) -> str:
    value = _json_value(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _match(left: Any, op: str, right_raw: Any) -> bool:
    if isinstance(left, Decimal):
        try:
            right = Decimal(str(right_raw))
        except InvalidOperation as exc:
            raise PipelineError("FILTER_TYPE", "filter value is not numeric") from exc
    elif isinstance(left, int) and not isinstance(left, bool):
        try:
            right = int(right_raw)
        except (ValueError, TypeError) as exc:
            raise PipelineError("FILTER_TYPE", "filter value is not an integer") from exc
    elif isinstance(left, bool):
        right = str(right_raw).lower() in {"true", "yes", "1"}
    elif isinstance(left, date):
        try:
            right = date.fromisoformat(str(right_raw))
        except ValueError as exc:
            raise PipelineError("FILTER_TYPE", "filter value is not an ISO date") from exc
    else:
        right = str(right_raw)
    if op == "contains":
        return str(right) in str(left)
    return {"eq": left == right, "ne": left != right, "gt": left > right, "ge": left >= right, "lt": left < right, "le": left <= right}[op]


def normalize_rows(config: PipelineConfig, raw_rows: Iterable[dict[str, str]], source_headers: list[str]) -> list[dict[str, Any]]:
    """Normalize bounded raw CSV rows according to explicit column definitions."""
    required_headers = set(config.quality.get("required_source_columns", [])) | {c["source"] for c in config.columns if c.get("required", False)}
    missing_headers = sorted(required_headers - set(source_headers))
    if missing_headers:
        raise PipelineError("MISSING_COLUMNS", "missing required source column(s): " + ", ".join(missing_headers))
    result: list[dict[str, Any]] = []
    for row_number, raw in enumerate(raw_rows, 2):
        if len(result) >= config.limits.max_rows:
            raise PipelineError("ROW_LIMIT", f"input exceeds {config.limits.max_rows} rows")
        normalized: dict[str, Any] = {}
        for col in config.columns:
            if None in raw:
                raise PipelineError("MALFORMED_CSV", f"row {row_number} has more cells than the header")
            value = raw.get(col["source"], "") or ""
            if len(value) > config.limits.max_cell_chars:
                raise PipelineError("CELL_LIMIT", f"row {row_number} contains an oversized cell")
            if not value.strip():
                if "default" in col:
                    value = str(col["default"])
                elif col.get("required", False):
                    raise PipelineError("REQUIRED_VALUE", f"row {row_number} is missing {col['source']}")
            normalized[col["name"]] = _convert(value, col.get("type", "string")) if value.strip() else ""
        if all(_match(normalized[f["column"]], f["op"], f["value"]) for f in config.filters):
            result.append(normalized)
    return result


def aggregate_rows(config: PipelineConfig, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Apply deterministic grouping/aggregates, preserving first-seen group order."""
    if not config.aggregates:
        return rows
    groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[tuple(row[k] for k in config.group_by)].append(row)
    output: list[dict[str, Any]] = []
    for key, members in groups.items():
        item = dict(zip(config.group_by, key))
        for agg in config.aggregates:
            values = [row[agg["column"]] for row in members if row[agg["column"]] != ""]
            op = agg["op"]
            if op == "count":
                value: Any = len(values)
            elif not values:
                value = ""
            elif op == "sum":
                value = sum(values)
            elif op == "average":
                value = sum(values) / len(values)
            elif op == "min":
                value = min(values)
            else:
                value = max(values)
            item[agg["name"]] = value
        output.append(item)
    return output


def sort_rows(config: PipelineConfig, rows: list[dict[str, Any]]) -> None:
    for rule in reversed(config.sort):
        rows.sort(key=lambda row: (row[rule["column"]] == "", row[rule["column"]]), reverse=bool(rule.get("descending", False)))


def _write_outputs(stage: Path, config: PipelineConfig, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    stage.mkdir(parents=True)
    columns = list(rows[0]) if rows else ([*config.group_by, *(a["name"] for a in config.aggregates)] if config.aggregates else [c["name"] for c in config.columns])
    files: list[Path] = []
    if "csv" in config.outputs:
        path = stage / "report.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            writer.writerows({k: _display(row.get(k, "")) for k in columns} for row in rows)
        files.append(path)
    if "json" in config.outputs:
        path = stage / "report.json"
        path.write_text(json.dumps([{k: _json_value(v) for k, v in row.items()} for row in rows], indent=2, sort_keys=True) + "\n", encoding="utf-8")
        files.append(path)
    if "html" in config.outputs:
        path = stage / "report.html"
        head = "".join(f"<th>{html.escape(k)}</th>" for k in columns)
        body = "".join("<tr>" + "".join(f"<td>{html.escape(_display(row.get(k, '')))}</td>" for k in columns) + "</tr>" for row in rows)
        path.write_text(f"<!doctype html><meta charset=\"utf-8\"><title>{html.escape(config.name)} report</title><style>body{{font:14px system-ui;margin:2rem}}table{{border-collapse:collapse}}th,td{{border:1px solid #bbb;padding:.45rem;text-align:left}}th{{background:#eef}}</style><h1>{html.escape(config.name)} report</h1><p>Generated from validated local input.</p><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>\n", encoding="utf-8")
        files.append(path)
    manifest = []
    for path in files:
        manifest.append({"name": path.name, "bytes": path.stat().st_size, "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    return manifest


def execute_pipeline(config: PipelineConfig, *, input_root: Path, output_root: Path, delivery_root: Path, run_id: str, progress: Progress | None = None) -> dict[str, Any]:
    """Execute one bounded run and atomically publish verified outputs."""
    report = progress or (lambda phase, done, total, item: None)
    source = _contained(input_root, config.source)
    report("discovering", 0, None, source.name)
    if not source.is_file():
        raise PipelineError("SOURCE_MISSING", "configured source file is missing")
    size = source.stat().st_size
    if size > config.limits.max_input_bytes:
        raise PipelineError("INPUT_LIMIT", f"source exceeds {config.limits.max_input_bytes} bytes")
    payload = source.read_bytes()
    input_sha = hashlib.sha256(payload).hexdigest()
    report("validating", 0, None, "CSV header")
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise PipelineError("ENCODING", "source must be UTF-8 CSV") from exc
    reader = csv.DictReader(io.StringIO(text, newline=""))
    headers = reader.fieldnames or []
    if not headers:
        raise PipelineError("EMPTY_INPUT", "source has no CSV header")
    if len(headers) > config.limits.max_columns:
        raise PipelineError("COLUMN_LIMIT", f"source exceeds {config.limits.max_columns} columns")
    if len(set(headers)) != len(headers):
        raise PipelineError("DUPLICATE_HEADER", "source contains duplicate column names")
    report("processing", 0, None, "normalize and filter")
    rows = normalize_rows(config, reader, headers)
    rows = aggregate_rows(config, rows)
    sort_rows(config, rows)
    minimum = int(config.quality.get("min_rows", 1))
    maximum = int(config.quality.get("max_rows", config.limits.max_rows))
    if len(rows) < minimum:
        raise PipelineError("MIN_ROWS", f"output has {len(rows)} rows; minimum is {minimum}")
    if len(rows) > maximum:
        raise PipelineError("MAX_ROWS", f"output has {len(rows)} rows; maximum is {maximum}")
    if config.quality.get("reject_blank_output", True) and not rows:
        raise PipelineError("BLANK_OUTPUT", "blank output was blocked")
    output_root.mkdir(parents=True, exist_ok=True)
    final = output_root / run_id
    stage = output_root / f".stage-{run_id}"
    if final.exists() or stage.exists():
        raise PipelineError("RUN_EXISTS", "run output already exists")
    report("writing", 0, len(config.outputs), "stage outputs")
    delivery_stages: list[Path] = []
    try:
        files = _write_outputs(stage, config, rows)
        manifest: dict[str, Any] = {
            "schema_version": 1,
            "pipeline": config.name,
            "run_id": run_id,
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "input_sha256": input_sha,
            "row_count": len(rows),
            "files": files,
            "deliveries": [],
        }
        (stage / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        report("verifying", len(files), len(files), "hash outputs")
        os.replace(stage, final)
        delivered_paths: list[Path] = []
        for adapter in config.deliveries:
            destination = _contained(delivery_root, adapter["destination"]) / run_id
            destination.parent.mkdir(parents=True, exist_ok=True)
            temp = destination.parent / f".stage-{run_id}"
            if destination.exists() or temp.exists():
                raise PipelineError("DELIVERY_EXISTS", "delivery target already exists")
            delivery_stages.append(temp)
            shutil.copytree(final, temp)
            os.replace(temp, destination)
            delivery_stages.remove(temp)
            delivered_paths.append(destination)
            manifest["deliveries"].append({"type": "local_copy", "destination": adapter["destination"], "status": "complete"})
        (final / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        for destination in delivered_paths:
            (destination / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        report("complete", len(rows), len(rows), f"{len(rows)} rows")
        return manifest
    except Exception:
        if stage.exists():
            shutil.rmtree(stage)
        for delivery_stage in delivery_stages:
            if delivery_stage.exists():
                shutil.rmtree(delivery_stage)
        raise


def safe_error(exc: Exception) -> tuple[str, str]:
    """Return a bounded path-redacted error code/message for persistence."""
    code = exc.code if isinstance(exc, PipelineError) else exc.__class__.__name__.upper()
    message = str(exc) or exc.__class__.__name__
    message = re.sub(r"(?:[A-Za-z]:)?[/\\](?:[^\s:/\\]+[/\\])+[^\s:]+", "[path]", message)
    return code[:64], message[:512]

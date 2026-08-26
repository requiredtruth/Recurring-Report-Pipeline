"""Machine-readable command line interface."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from .demo import run_demo
from .pipeline import safe_error
from .service import Service


def _emit(value: object) -> None:
    print(json.dumps(value, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="report-pipeline", description="Run bounded recurring CSV report jobs locally.")
    parser.add_argument("--data-dir", type=Path, default=Path(".report-pipeline"))
    sub = parser.add_subparsers(dest="command", required=True)
    register = sub.add_parser("register", help="validate and register a JSON pipeline")
    register.add_argument("config", type=Path)
    run = sub.add_parser("run", help="run one registered pipeline now")
    run.add_argument("name")
    sub.add_parser("run-due", help="run all pipelines whose UTC schedule is due")
    sub.add_parser("status", help="emit pipelines and recent runs")
    sub.add_parser("demo", help="run the bundled synthetic workflow")
    daemon = sub.add_parser("scheduler", help="poll due jobs in the foreground")
    daemon.add_argument("--poll-seconds", type=int, default=30)
    daemon.add_argument("--once", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "demo":
            manifest = run_demo(args.data_dir, lambda phase, done, total, item: print(f"{phase}: {item}", file=sys.stderr))
            _emit(manifest)
            return 0
        service = Service(args.data_dir)
        if args.command == "register":
            config = service.register(args.config)
            _emit({"name": config.name, "status": "registered"})
        elif args.command == "run":
            result = service.run(args.name)
            _emit(result or {"status": "duplicate"})
        elif args.command == "run-due":
            _emit(service.run_due())
        elif args.command == "status":
            pipelines = [{key: row[key] for key in ("name", "enabled", "next_due", "updated_at")} for row in service.store.pipelines()]
            runs = [{key: row[key] for key in ("id", "pipeline_name", "scheduled_for", "status", "started_at", "finished_at", "input_sha256", "error_code", "error_message")} for row in service.store.runs()]
            _emit({"schema_version": 1, "pipelines": pipelines, "runs": runs})
        else:
            if not 1 <= args.poll_seconds <= 3600:
                raise ValueError("poll-seconds must be from 1 to 3600")
            while True:
                _emit({"at": datetime.now(timezone.utc).isoformat(), "results": service.run_due()})
                if args.once:
                    break
                time.sleep(args.poll_seconds)
        return 0
    except (ValueError, KeyError) as exc:
        code, message = safe_error(exc)
        print(json.dumps({"error_code": code, "message": message}, sort_keys=True), file=sys.stderr)
        return 2
    except Exception as exc:
        code, message = safe_error(exc)
        print(json.dumps({"error_code": code, "message": message}, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

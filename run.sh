#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ ! -f "$ROOT/.venv/.report-pipeline-ready" ]]; then "$ROOT/install.sh"; fi
exec "$ROOT/.venv/bin/python" -m recurring_report_pipeline.gui "$@"

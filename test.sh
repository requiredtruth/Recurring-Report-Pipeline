#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PYTHONPATH="$ROOT/src" python3 -m unittest discover -s "$ROOT/tests" -v
bash -n "$ROOT/install.sh" "$ROOT/run.sh" "$ROOT/cli.sh" "$ROOT/test.sh"

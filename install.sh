#!/usr/bin/env bash
set -Eeuo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
VENV="$ROOT/.venv"
MARKER="$VENV/.report-pipeline-ready"
echo "[install] checking Python"
command -v python3 >/dev/null || { echo "python3 is required" >&2; exit 1; }
if ! python3 -c 'import ctypes; ctypes.CDLL("libEGL.so.1")' >/dev/null 2>&1; then
  echo "[install] installing Qt system libraries"
  if command -v apt-get >/dev/null && [[ "$(id -u)" -eq 0 ]]; then
    apt-get update
    apt-get install -y --no-install-recommends libegl1 libxkbcommon-x11-0 libxcb-cursor0
  elif command -v sudo >/dev/null && sudo -n true 2>/dev/null; then
    sudo apt-get update
    sudo apt-get install -y --no-install-recommends libegl1 libxkbcommon-x11-0 libxcb-cursor0
  else
    echo "Qt needs libegl1, libxkbcommon-x11-0, and libxcb-cursor0; install them, then rerun install.sh" >&2
    exit 1
  fi
fi
if [[ ! -x "$VENV/bin/python" ]]; then
  echo "[install] creating local virtual environment"
  python3 -m venv "$VENV"
fi
echo "[install] installing application and GUI dependency"
"$VENV/bin/python" -m pip install --disable-pip-version-check -e "$ROOT[gui]"
echo "[install] verifying imports"
QT_QPA_PLATFORM=offscreen "$VENV/bin/python" -c 'from PySide6.QtWidgets import QApplication; from recurring_report_pipeline.gui import MainWindow; app=QApplication([]); window=MainWindow(); assert window.windowTitle()'
printf '0.1.0\n' > "$MARKER"
echo "[install] ready"

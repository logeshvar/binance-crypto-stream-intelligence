#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON:-python3}"
VENV_DIR="${VENV_DIR:-.venv}"

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "Python executable not found: ${PYTHON_BIN}" >&2
  echo "Install Python 3.11+ or run with PYTHON=/path/to/python3." >&2
  exit 1
fi

PYTHON_VERSION="$("${PYTHON_BIN}" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
PYTHON_OK="$("${PYTHON_BIN}" -c 'import sys; print(int(sys.version_info >= (3, 11)))')"
if [[ "${PYTHON_OK}" != "1" ]]; then
  echo "Python 3.11+ is required. Found ${PYTHON_VERSION} at ${PYTHON_BIN}." >&2
  exit 1
fi

if [[ ! -d "${VENV_DIR}" ]]; then
  "${PYTHON_BIN}" -m venv "${VENV_DIR}"
fi

if [[ "${UPGRADE_PIP:-0}" == "1" ]]; then
  "${VENV_DIR}/bin/python" -m pip install --upgrade pip
fi

"${VENV_DIR}/bin/python" -m pip install -r requirements.txt
"${VENV_DIR}/bin/python" - <<'PY'
import aiokafka
import websockets

print("Producer dependencies verified:")
print(f"  aiokafka={aiokafka.__version__}")
print(f"  websockets={websockets.__version__}")
PY

echo
echo "Activate the virtual environment with:"
echo "  source ${VENV_DIR}/bin/activate"

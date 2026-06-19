#!/usr/bin/env bash
# Run the Sandbox API on the host (not in Docker) for local testing — especially
# SANDBOX_ISOLATION=lima / colima where ``limactl`` must be on the host PATH.
#
# Usage (from anywhere):
#   ./scripts/run_api_host.sh
#   SANDBOX_ISOLATION=lima ./scripts/run_api_host.sh
#
# Config: put variables in ``api_server/.env`` (loaded by ``main.py``) or export them before running.
# Default DB file avoids clobbering a DB used inside Docker: ``sandboxes.host.db`` in this directory.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export DATABASE_PATH="${DATABASE_PATH:-$ROOT/sandboxes.host.db}"

VENV="${ROOT}/.venv"
if [[ ! -d "$VENV" ]]; then
  echo "Creating virtualenv at $VENV ..."
  python3 -m venv "$VENV"
fi

PY="$VENV/bin/python"
PIP="$VENV/bin/pip"
if ! "$PY" -c "import uvicorn" 2>/dev/null; then
  echo "Installing dependencies from requirements.txt ..."
  "$PIP" install -r "$ROOT/requirements.txt"
fi

echo "----------------------------------------------------------------"
echo "  API root:     $ROOT"
echo "  Database:     $DATABASE_PATH"
echo "  Isolation:    ${SANDBOX_ISOLATION:-docker}  (set SANDBOX_ISOLATION=lima to test Lima VMs)"
echo "  Lima remote:  ${LIMA_REMOTE_HOST:-<unset — limactl on this host PATH>}"
echo "  URL:          http://127.0.0.1:8000"
echo "----------------------------------------------------------------"

exec "$VENV/bin/uvicorn" main:app --reload --host 127.0.0.1 --port 8000

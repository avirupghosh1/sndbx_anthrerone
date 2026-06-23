#!/usr/bin/env bash
# Register Custodian agentlib template against in-cluster api-service (Branch B, no sudo).
#
# Do NOT run ``minikube tunnel`` — it wedges the docker driver.
# Uses host ``docker build`` (not Kaniko) to avoid killing minikube etcd.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
PF_SCRIPT="$(cd "$(dirname "$0")" && pwd)/branch-b-no-sudo.sh"

if ! curl -sf -m 3 -H "X-API-Key: ${SANDBOX_API_KEY:-test-key-12345}" http://127.0.0.1:8001/health >/dev/null; then
  echo "api-service not reachable on :8001."
  echo "Start port-forwards in another terminal:"
  echo "  $PF_SCRIPT"
  exit 1
fi

export SANDBOX_API_URL="${SANDBOX_API_URL:-http://127.0.0.1:8001}"
export SANDBOX_API_KEY="${SANDBOX_API_KEY:-test-key-12345}"
export MY_SDK_REQUEST_TIMEOUT="${MY_SDK_REQUEST_TIMEOUT:-3600}"

cd "$ROOT/custodian"
PY="${ROOT}/custodian/backend/.venv/bin/python"
if [[ ! -x "$PY" ]]; then
  PY=python3
fi

echo "Registering template (--host-docker, no Kaniko)…"
exec "$PY" deployment/local/register_local_template.py --env dev --host-docker "$@"

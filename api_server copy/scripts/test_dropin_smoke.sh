#!/usr/bin/env bash
# Quick curl-based smoke test for E2B drop-in REST + timeout (curl + python3 for JSON).
#
# Usage (from repo root, API already running with Docker + secrets):
#   export API_KEY="${API_KEY:-test-key-12345}"
#   export API_BASE="${API_BASE:-http://127.0.0.1:8000}"
#   export E2B_DROPIN_WS_SECRET="$(python3 -c 'import secrets; print(secrets.token_hex(24))')"   # on server only
#   bash api_server/scripts/test_dropin_smoke.sh
#
# This script only validates HTTP; WebSocket is covered by test_dropin_integration.py.
#
# If POST /sandboxes returns 503, the API could not start a workload — see stderr body below.
# Typical causes: Docker not running, bad DOCKER_HOST, image pull blocked, disk full, warm-pool build failed.

set -euo pipefail
API_BASE="${API_BASE:-http://127.0.0.1:8000}"
API_KEY="${API_KEY:-test-key-12345}"
HDR=(-H "X-API-Key: ${API_KEY}" -H "Content-Type: application/json")

echo "== GET /health"
curl -fsS "${API_BASE}/health" | head -c 200 || true
echo

echo "== POST /sandboxes"
CREATE_JSON='{"template_id":"python:3.11","timeout":600}'
HTTP=$(curl -sS -o /tmp/smoke_create.json -w "%{http_code}" "${API_BASE}/sandboxes" "${HDR[@]}" -d "${CREATE_JSON}")
if [[ "${HTTP}" != "200" ]]; then
  echo "POST /sandboxes failed: HTTP ${HTTP}"
  echo "Response body:"
  cat /tmp/smoke_create.json 2>/dev/null || echo "(empty)"
  echo
  echo "Hints:"
  echo "  - Is Docker running?  docker info"
  echo "  - Can the API pull images?  docker pull python:3.11"
  echo "  - If the API runs in a container, is the Docker socket mounted?"
  echo "  - Check API logs for create_sandbox / create_container errors."
  exit 1
fi

SID=$(python3 -c "import json; print(json.load(open('/tmp/smoke_create.json'))['sandbox_id'])")
echo "sandbox_id=${SID}"

echo "== GET /sandboxes/${SID}/status"
curl -fsS "${API_BASE}/sandboxes/${SID}/status" "${HDR[@]}"
echo

echo "== POST /sandboxes/${SID}/timeout"
curl -fsS "${API_BASE}/sandboxes/${SID}/timeout" "${HDR[@]}" -d '{"timeout_seconds":1200}'
echo

echo "== GET /sandboxes/${SID}/e2b-connection (needs E2B_DROPIN_WS_SECRET on server)"
set +e
EC=$(curl -sS -w "%{http_code}" -o /tmp/e2b_conn.json "${API_BASE}/sandboxes/${SID}/e2b-connection" "${HDR[@]}")
set -e
echo "HTTP ${EC}"
head -c 400 /tmp/e2b_conn.json || true
echo

echo "== POST /sandboxes/${SID}/commands/run"
curl -fsS "${API_BASE}/sandboxes/${SID}/commands/run" "${HDR[@]}" -d '{"command":"echo dropin-smoke","cwd":"/","timeout":30}'
echo

echo "== POST /sandboxes/${SID}/kill"
# curl defaults to GET when no -d/-X; kill is POST-only → 405 without a body.
curl -fsS "${API_BASE}/sandboxes/${SID}/kill" "${HDR[@]}" -d '{}'
echo
echo "OK smoke finished."

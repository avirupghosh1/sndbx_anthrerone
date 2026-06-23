#!/usr/bin/env bash
# Mac minikube: build Custodian agentlib template on the HOST (not Kaniko).
#
# Kaniko inside minikube OOMs/kills etcd on large Dockerfiles (playwright, awscli, …).
# This builds into minikube's Docker daemon, then registers via a fast POST /templates.
#
# Prereqs:
#   minikube start --cpus=4 --memory=6144
#   ./branch-b-no-sudo.sh   (api reachable on :8001)
#
# Usage:
#   ./please_work/deploy/mac-minikube/build-template-host.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
export SANDBOX_API_URL="${SANDBOX_API_URL:-http://127.0.0.1:8001}"
export SANDBOX_API_KEY="${SANDBOX_API_KEY:-test-key-12345}"
export CUSTODIAN_TEMPLATE_HOST_DOCKER=1

if ! curl -sf -m 3 -H "X-API-Key: $SANDBOX_API_KEY" "$SANDBOX_API_URL/health" >/dev/null; then
  echo "api-service not on $SANDBOX_API_URL — run branch-b-no-sudo.sh first."
  exit 1
fi

cd "$ROOT/custodian"
PY="${ROOT}/custodian/backend/.venv/bin/python"
[[ -x "$PY" ]] || PY=python3

echo "Building template on host docker (minikube docker-env)…"
exec "$PY" deployment/local/register_local_template.py --env dev --host-docker "$@"

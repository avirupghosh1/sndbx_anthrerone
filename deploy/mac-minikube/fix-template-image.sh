#!/usr/bin/env bash
# Fix template image after ImagePullBackOff (kubelet pulls :latest from Docker Hub).
set -euo pipefail

IMG="custodian-custodian-agentlib-sandbox-dev-avirup-ghosh"
eval "$(minikube docker-env)"

if docker image inspect "${IMG}:latest" >/dev/null 2>&1; then
  docker tag "${IMG}:latest" "${IMG}:v1"
  echo "Tagged ${IMG}:v1"
elif docker image inspect "${IMG}:v1" >/dev/null 2>&1; then
  echo "Image ${IMG}:v1 already exists"
else
  echo "Missing ${IMG} in minikube docker. Run build-template-host.sh first."
  exit 1
fi

kubectl delete pods -n sandboxes -l app=sandbox --field-selector=status.phase!=Running 2>/dev/null || true
kubectl delete pods -n sandboxes --field-selector=status.phase=Failed 2>/dev/null || true
kubectl delete jobs -n sandboxes -l app=kaniko-builder 2>/dev/null || true
kubectl delete pods -n sandboxes -l app=kaniko-builder 2>/dev/null || true

echo "Re-register template (sets start_cmd=agentlib-e2b-server):"
echo "  ./please_work/deploy/mac-minikube/build-template-host.sh"
echo ""
echo "Or register only (no rebuild):"
API_URL="${SANDBOX_API_URL:-http://127.0.0.1:8001}"
API_KEY="${SANDBOX_API_KEY:-test-key-12345}"
TID="custodian-agentlib-sandbox-dev-$(whoami | tr '[:upper:]' '[:lower:]' | tr -cd 'a-z0-9-' | sed 's/^-//;s/-$//')"
curl -sS -X POST "$API_URL/templates" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d "{\"template_id\":\"$TID\",\"base_image\":\"${IMG}:v1\",\"warm_snapshot_image\":\"${IMG}:v1\",\"start_cmd\":\"agentlib-e2b-server\",\"env\":{\"PORT\":\"8765\",\"AGENTLIB_SANDBOX_WS_PORT\":\"8765\"},\"settle_seconds\":5}"

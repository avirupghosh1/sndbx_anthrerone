#!/usr/bin/env bash
# Port-forward the active data-plane service to localhost:18080 (no sudo).
#
# Runtime-gateway architecture:
#   kubectl port-forward -n sandboxes svc/api-service 8001:8000
#   ./deploy/mac-minikube/port-forward.sh
#   export SANDBOX_API_URL=http://127.0.0.1:8001
#   export SANDBOX_DATA_PLANE_URL=http://127.0.0.1:18080
#
# Legacy K8s-per-sandbox architecture:
#   this script falls back to svc/proxy-service when runtime-gateway is not present.
set -euo pipefail

NAMESPACE="${NAMESPACE:-sandboxes}"
SERVICE=""
ARCH=""

if kubectl get svc -n "$NAMESPACE" runtime-gateway >/dev/null 2>&1; then
  SERVICE="runtime-gateway"
  ARCH="runtime-gateway"
elif kubectl get svc -n "$NAMESPACE" proxy-service >/dev/null 2>&1; then
  SERVICE="proxy-service"
  ARCH="legacy-k8s"
else
  echo "No data-plane service found in namespace $NAMESPACE (expected runtime-gateway or proxy-service)." >&2
  exit 1
fi

echo "Architecture: $ARCH"
echo "Forwarding $SERVICE → http://127.0.0.1:18080 (Ctrl+C to stop)"
echo "Control plane remains separate: forward api-service on :8001 if your client uses localhost access."
kubectl port-forward -n "$NAMESPACE" "svc/$SERVICE" 18080:8080

#!/usr/bin/env bash
# Port-forward minikube ingress to localhost (no sudo). Use when ``minikube tunnel`` is unavailable.
#
#   ./please_work/deploy/mac-minikube/port-forward.sh
#
# Then set in custodian/.env:
#   SANDBOX_DATA_PLANE_URL=http://127.0.0.1:18080
#
# Cluster control plane (branch B) without /etc/hosts — forward api-service separately:
#   kubectl port-forward -n sandboxes svc/api-service 8001:8000
#   SANDBOX_API_URL=http://127.0.0.1:8001
set -euo pipefail

echo "Forwarding proxy-service → http://127.0.0.1:18080 (Ctrl+C to stop)"
echo "Routes via X-Sandbox-Id + X-Guest-Port (no nginx Host required on Mac dev)."
kubectl port-forward -n sandboxes svc/proxy-service 18080:8080

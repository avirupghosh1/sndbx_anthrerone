#!/usr/bin/env bash
set -euo pipefail

cleanup() { kill "${PF_API:-}" "${PF_SSH:-}" "${PF_ING:-}" 2>/dev/null || true; }
trap cleanup EXIT INT TERM

echo "Branch B (no sudo) port-forwards:"
echo "  control plane  http://127.0.0.1:8001  → api-service:8000"
echo "  ssh gateway    ssh://127.0.0.1:2222     → api-service:2222"
echo "  data plane     http://127.0.0.1:18080 → proxy-service:8080"
echo ""
echo "Data plane goes directly to proxy-service (debug headers X-Sandbox-Id / X-Guest-Port)."
echo "Do NOT run minikube tunnel at the same time (TLS timeouts on docker driver)."
echo "Press Ctrl+C to stop."
echo ""

kubectl port-forward -n sandboxes svc/api-service 8001:8000 &
PF_API=$!
kubectl port-forward -n sandboxes svc/api-service 2222:2222 &
PF_SSH=$!
kubectl port-forward -n sandboxes svc/runtime-gateway 18080:8080 &
PF_ING=$!

wait

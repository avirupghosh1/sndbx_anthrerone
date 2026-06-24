#!/usr/bin/env bash
# Deploy api-service + runtime-gateway + HTTP ingress on Mac minikube.
#
# Prerequisites: minikube, kubectl, docker (minikube driver or eval minikube docker-env)
#
# Usage:
#   ./please_work/deploy/mac-minikube/up.sh
#   ./please_work/deploy/mac-minikube/up.sh --tunnel   # also start minikube tunnel (needs sudo)
#
# After deploy:
#   1. Run ``minikube tunnel`` in a separate terminal (sudo) unless you passed --tunnel
#   2. Add to /etc/hosts:  127.0.0.1 api.sndbx.com
#   3. Custodian cluster mode:
#        SANDBOX_API_URL=http://api.sndbx.com
#        SANDBOX_DATA_PLANE_URL=http://127.0.0.1
#        SANDBOX_INGRESS_DEBUG=false
#   4. Custodian direct mode (api on Mac :8001, pods still in cluster):
#        SANDBOX_API_URL=http://127.0.0.1:8001
#        SANDBOX_DATA_PLANE_URL=http://127.0.0.1:18080
#        SANDBOX_INGRESS_DEBUG=false
#        kubectl port-forward -n sandboxes svc/runtime-gateway 18080:8080
#   5. Production-style local ingress mode (additional mode; direct port-forwards still work):
#        ./deploy/mac-minikube/enable-ingress-domain.sh
#        export SANDBOX_API_URL=http://api.127-0-0-1.sslip.io
#        unset SANDBOX_DATA_PLANE_URL
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
API_DIR="$ROOT/api_server copy"
PROXY_DIR="$ROOT/proxy_service"
RUNTIME_DIR="$ROOT/deploy/runtime-gateway"
START_TUNNEL=false

for arg in "$@"; do
  case "$arg" in
    --tunnel) START_TUNNEL=true ;;
    -h|--help)
      sed -n '2,22p' "$0"
      exit 0
      ;;
  esac
done

echo "==> Starting minikube (if needed)"
minikube start --cpus=2 --memory=4096 2>/dev/null || minikube start

echo "==> Enabling ingress addon"
minikube addons enable ingress

echo "==> Building images inside minikube Docker"
eval "$(minikube docker-env)"
docker build -t api-service:latest -f "$API_DIR/Dockerfile.api-service" "$API_DIR"
docker build -t runtime-gateway:latest -f "$PROXY_DIR/Dockerfile" "$ROOT"

echo "==> Applying Kubernetes manifests"
kubectl apply -f "$API_DIR/deploy/k8s/api-service.yaml"
kubectl apply -f "$API_DIR/deploy/k8s/secrets.yaml"
kubectl apply -f "$RUNTIME_DIR/runtime-gateway.yaml"
kubectl apply -f "$RUNTIME_DIR/ingress-http-minikube.yaml"

echo "==> Waiting for deployments"
kubectl rollout status deployment/api-service -n sandboxes --timeout=180s
kubectl rollout status deployment/runtime-gateway -n sandboxes --timeout=180s

echo ""
kubectl get pods,svc,ingress -n sandboxes
echo ""
echo "Direct local mode remains:"
echo "  api   -> kubectl port-forward -n sandboxes svc/api-service 8001:8000"
echo "  data  -> kubectl port-forward -n sandboxes svc/runtime-gateway 18080:8080"
echo ""
echo "Production-style ingress mode:"
echo "  ./deploy/mac-minikube/enable-ingress-domain.sh"
echo ""

if ! grep -q 'api\.sndbx\.com' /etc/hosts 2>/dev/null; then
  echo "NOTE: add to /etc/hosts (sudo):"
  echo "  127.0.0.1 api.sndbx.com"
fi

if $START_TUNNEL; then
  echo "==> Starting minikube tunnel (Ctrl+C to stop)"
  minikube tunnel
else
  echo "Run in another terminal:  minikube tunnel"
  echo "Then verify:  curl -sS -H 'X-API-Key: test-key-12345' http://api.sndbx.com/health"
fi

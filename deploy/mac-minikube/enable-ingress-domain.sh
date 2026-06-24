#!/usr/bin/env bash
# Configure the runtime-gateway deployment for production-style ingress testing on local minikube.
#
# This keeps the same control/data split as production:
#   - http://api.<domain>                -> api-service
#   - http://{port}-{sandbox_id}.<domain> -> runtime-gateway
#
# Default domain uses sslip.io so wildcard DNS resolves to 127.0.0.1 while ``minikube tunnel``
# exposes the ingress-nginx LoadBalancer locally. This is the most reliable production-like path
# on Mac. No service port-forward is required in this mode, but the direct 8001/18080 path remains
# available and unchanged.
#
# Usage:
#   ./deploy/mac-minikube/enable-ingress-domain.sh
#   DOMAIN=127-0-0-1.sslip.io ./deploy/mac-minikube/enable-ingress-domain.sh --tunnel
#   DOMAIN=my-dev.example.com ./deploy/mac-minikube/enable-ingress-domain.sh
#
# Client env in ingress mode:
#   SANDBOX_API_URL=http://api.${DOMAIN}
#   unset SANDBOX_DATA_PLANE_URL
#
set -euo pipefail

NAMESPACE="${NAMESPACE:-sandboxes}"
DOMAIN="${DOMAIN:-127-0-0-1.sslip.io}"
SCHEME="${SCHEME:-http}"
START_TUNNEL=false

for arg in "$@"; do
  case "$arg" in
    --tunnel) START_TUNNEL=true ;;
    -h|--help)
      sed -n '2,26p' "$0"
      exit 0
      ;;
  esac
done

echo "==> Configuring ingress domain"
echo "namespace: $NAMESPACE"
echo "domain:    $DOMAIN"
echo "scheme:    $SCHEME"

echo "==> Ensuring ingress-nginx controller is exposed as LoadBalancer"
kubectl patch svc ingress-nginx-controller -n ingress-nginx --type merge -p \
  '{"spec":{"type":"LoadBalancer"}}'

kubectl patch configmap api-service-config -n "$NAMESPACE" --type merge -p \
  "{\"data\":{\"SANDBOX_DATA_PLANE_DOMAIN\":\"$DOMAIN\",\"SANDBOX_DATA_PLANE_SCHEME\":\"$SCHEME\",\"SANDBOX_DATA_PLANE_DEBUG\":\"false\"}}"

kubectl patch configmap runtime-gateway-config -n "$NAMESPACE" --type merge -p \
  "{\"data\":{\"SANDBOX_DOMAIN\":\"$DOMAIN\",\"SANDBOX_INGRESS_DEBUG\":\"false\"}}"

cat <<EOF | kubectl apply -f -
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: sandbox-ingress
  namespace: ${NAMESPACE}
  annotations:
    nginx.ingress.kubernetes.io/proxy-read-timeout: "600"
    nginx.ingress.kubernetes.io/proxy-send-timeout: "600"
    nginx.ingress.kubernetes.io/proxy-body-size: "100m"
spec:
  ingressClassName: nginx
  rules:
    - host: api.${DOMAIN}
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: api-service
                port:
                  number: 8000
    - host: "*.${DOMAIN}"
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: runtime-gateway
                port:
                  number: 8080
EOF

echo "==> Restarting deployments"
kubectl rollout restart deployment/api-service -n "$NAMESPACE"
kubectl rollout restart deployment/runtime-gateway -n "$NAMESPACE"
kubectl rollout status deployment/api-service -n "$NAMESPACE" --timeout=180s
kubectl rollout status deployment/runtime-gateway -n "$NAMESPACE" --timeout=180s

echo
kubectl get ingress -n "$NAMESPACE"
echo
kubectl get svc -n ingress-nginx ingress-nginx-controller
echo
echo "Ingress mode client settings:"
echo "  SANDBOX_API_URL=${SCHEME}://api.${DOMAIN}"
echo "  SANDBOX_DATA_PLANE_URL should be unset"
echo "  direct mode remains available:"
echo "    SANDBOX_API_URL=http://127.0.0.1:8001"
echo "    SANDBOX_DATA_PLANE_URL=http://127.0.0.1:18080"
echo
echo "Verification:"
echo "  curl -sS -H 'X-API-Key: test-key-12345' ${SCHEME}://api.${DOMAIN}/health"
echo "  curl -sS -H 'X-API-Key: test-key-12345' ${SCHEME}://49983-<sandbox_id>.${DOMAIN}/health"
echo
if $START_TUNNEL; then
  echo "==> Starting minikube tunnel (Ctrl+C to stop)"
  minikube tunnel
else
  echo "Run in another terminal if not already running:"
  echo "  minikube tunnel"
fi

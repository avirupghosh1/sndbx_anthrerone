#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-sandboxes}"
DOMAIN="${SANDBOX_DOMAIN:-sndbx.example.com}"
API_HOST="${API_HOST:-api.${DOMAIN}}"
SECRET_NAME="${TLS_SECRET_NAME:-sndbx-example-com-tls}"
WORKDIR="${TMPDIR:-/tmp}/sndbx-local-tls"
CERT_FILE="${WORKDIR}/${DOMAIN}.crt"
KEY_FILE="${WORKDIR}/${DOMAIN}.key"

mkdir -p "${WORKDIR}"

kubectl get namespace "${NAMESPACE}" >/dev/null 2>&1 || kubectl create namespace "${NAMESPACE}"

if command -v mkcert >/dev/null 2>&1; then
  mkcert -install
  mkcert -cert-file "${CERT_FILE}" -key-file "${KEY_FILE}" "${API_HOST}" "*.${DOMAIN}"
elif [[ "${ALLOW_SELF_SIGNED_TLS:-false}" == "true" ]]; then
  echo "mkcert not found; creating an explicitly untrusted self-signed cert." >&2
  echo "Python/httpx will reject this unless you explicitly trust this exact cert." >&2
  openssl req -x509 -nodes -newkey rsa:2048 -days 365 \
    -keyout "${KEY_FILE}" \
    -out "${CERT_FILE}" \
    -subj "/CN=*.${DOMAIN}" \
    -addext "subjectAltName=DNS:${API_HOST},DNS:*.${DOMAIN}"
else
  echo "mkcert is required for local E2B HTTPS data-plane tests." >&2
  echo "Install mkcert, then rerun this script. Set ALLOW_SELF_SIGNED_TLS=true only for manual debugging." >&2
  exit 1
fi

kubectl -n "${NAMESPACE}" create secret tls "${SECRET_NAME}" \
  --cert="${CERT_FILE}" \
  --key="${KEY_FILE}" \
  --dry-run=client \
  -o yaml | kubectl apply -f -

echo "Created/updated TLS secret ${NAMESPACE}/${SECRET_NAME}"
echo "Certificate hosts: ${API_HOST}, *.${DOMAIN}"
openssl x509 -in "${CERT_FILE}" -noout -subject -issuer -ext subjectAltName
if command -v mkcert >/dev/null 2>&1; then
  echo "If Python/httpx still reports CERTIFICATE_VERIFY_FAILED, run the smoke test with:"
  echo "  SSL_CERT_FILE=\"$(mkcert -CAROOT)/rootCA.pem\" ./e2b_control_plane_smoke.py"
fi

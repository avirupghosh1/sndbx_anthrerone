#!/bin/sh
# Fail the image build if security-sensitive packages are below patched versions.
# Jenkins flags these OpenSSL CVEs when libssl3/libcrypto3/openssl stay < 3.5.6-r0:
#   CVE-2025-15467, CVE-2025-69419, CVE-2025-69420, CVE-2025-69421
#   CVE-2026-28387, CVE-2026-28388, CVE-2026-28389, CVE-2026-28390
#   CVE-2026-31789 (CRITICAL), CVE-2026-31790
# Docker CLI must match the pinned patched docker:29.6.1-dind-alpine3.24 base.
set -eu

MIN_OPENSSL="3.5.6-r0"
MIN_DOCKER_CLI="29.6.1"

fail=0

apk_pkg_version() {
  pkg="$1"
  apk list -I "$pkg" 2>/dev/null | head -n1 | sed -n "s/^[^-]*-\([^ ]*\).*/\1/p"
}

require_apk_min() {
  pkg="$1"
  min="$2"
  ver="$(apk_pkg_version "$pkg")"
  if [ -z "$ver" ]; then
    echo "VALIDATION FAIL: package $pkg is not installed"
    fail=1
    return
  fi
  if [ "$(apk version -t "$ver" "$min")" = '<' ]; then
    echo "VALIDATION FAIL: $pkg $ver < required $min"
    fail=1
    return
  fi
  echo "VALIDATION OK: $pkg $ver >= $min"
}

require_docker_cli_min() {
  min="$1"
  ver="$(docker version --format '{{.Client.Version}}' 2>/dev/null || true)"
  ver="${ver%%-*}"
  if [ -z "$ver" ]; then
    echo "VALIDATION FAIL: docker client version unavailable"
    fail=1
    return
  fi
  if [ "$(printf '%s\n' "$min" "$ver" | sort -V | tail -n1)" != "$ver" ]; then
    echo "VALIDATION FAIL: docker client $ver < required $min"
    fail=1
    return
  fi
  echo "VALIDATION OK: docker client $ver >= $min"
}

echo "=== gVisor DinD image security validation ==="
echo "Alpine release: $(cat /etc/alpine-release 2>/dev/null || echo unknown)"
echo "OpenSSL runtime: $(openssl version 2>/dev/null || echo unavailable)"

require_apk_min openssl "$MIN_OPENSSL"
require_apk_min libssl3 "$MIN_OPENSSL"
require_apk_min libcrypto3 "$MIN_OPENSSL"
require_docker_cli_min "$MIN_DOCKER_CLI"

if [ ! -x /usr/local/bin/runsc ]; then
  echo "VALIDATION FAIL: /usr/local/bin/runsc is missing or not executable"
  fail=1
else
  echo "VALIDATION OK: runsc present ($(/usr/local/bin/runsc --version 2>&1 | head -n1))"
fi

if [ "$fail" -ne 0 ]; then
  echo "=== validation failed; refusing to publish vulnerable image ==="
  exit 1
fi

echo "=== validation passed ==="

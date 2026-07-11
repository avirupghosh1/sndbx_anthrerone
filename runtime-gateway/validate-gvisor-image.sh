#!/bin/sh
# Fail the image build if security-sensitive packages are below patched versions.
set -eu

MIN_OPENSSL="3.5.6-r0"

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

echo "=== gVisor DinD image security validation ==="
echo "Alpine release: $(cat /etc/alpine-release 2>/dev/null || echo unknown)"

require_apk_min openssl "$MIN_OPENSSL"
require_apk_min libssl3 "$MIN_OPENSSL"
require_apk_min libcrypto3 "$MIN_OPENSSL"

if [ -x /usr/local/bin/docker ]; then
  echo "VALIDATION FAIL: docker CLI should be removed from this image"
  fail=1
else
  echo "VALIDATION OK: docker CLI not present"
fi

if [ ! -x /usr/local/bin/dockerd ]; then
  echo "VALIDATION FAIL: dockerd is missing"
  fail=1
else
  echo "VALIDATION OK: dockerd present"
fi

if [ ! -x /usr/local/bin/runsc ]; then
  echo "VALIDATION FAIL: /usr/local/bin/runsc is missing or not executable"
  fail=1
else
  echo "VALIDATION OK: runsc present ($(/usr/local/bin/runsc --version 2>&1 | head -n1))"
fi

if ! curl -fsS http://127.0.0.1:2375/_ping >/dev/null 2>&1; then
  echo "VALIDATION SKIP: dockerd not running during image build (expected)"
fi

if [ "$fail" -ne 0 ]; then
  echo "=== validation failed; refusing to publish vulnerable image ==="
  exit 1
fi

echo "=== validation passed ==="

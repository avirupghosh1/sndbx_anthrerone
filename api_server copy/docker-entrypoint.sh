#!/bin/sh
# Pre-pull the default sandbox image before Uvicorn accepts traffic so the first
# POST /sandboxes does not run a multi-minute pull inside a single request (which
# can OOM-kill the API process or otherwise drop the client connection).
set -e
img="${PREPULL_TEMPLATE:-${DEFAULT_TEMPLATE:-python:3.11}}"
if command -v docker >/dev/null 2>&1 && [ -S /var/run/docker.sock ]; then
  echo "docker-entrypoint: pre-pulling ${img} (set PREPULL_TEMPLATE or DEFAULT_TEMPLATE to override)"
  docker pull "${img}" || echo "docker-entrypoint: WARNING: pull failed; first create may pull on demand"
else
  echo "docker-entrypoint: WARNING: docker CLI or /var/run/docker.sock missing; skipping pre-pull"
fi
exec "$@"

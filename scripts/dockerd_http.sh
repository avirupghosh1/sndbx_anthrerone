#!/usr/bin/env sh
# Minimal Docker Engine HTTP API helpers for the dockerd sidecar (no docker CLI).
# Uses curl against DOCKER_HOST (default tcp://127.0.0.1:2375).

set -eu

DOCKER_HOST="${DOCKER_HOST:-tcp://127.0.0.1:2375}"
case "$DOCKER_HOST" in
  tcp://*)
    DOCKER_API_BASE="http://${DOCKER_HOST#tcp://}"
    ;;
  unix://*)
    echo "dockerd_http.sh: unix sockets are not supported (set DOCKER_HOST=tcp://127.0.0.1:2375)" >&2
    exit 1
    ;;
  *)
    DOCKER_API_BASE="$DOCKER_HOST"
    ;;
esac

dockerd_ping() {
  curl -fsS "${DOCKER_API_BASE}/_ping" >/dev/null
}

dockerd_get() {
  path="$1"
  curl -fsS "${DOCKER_API_BASE}${path}"
}

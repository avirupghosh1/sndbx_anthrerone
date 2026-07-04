#!/usr/bin/env bash
set -euo pipefail

# Show, per runtime-gateway pod, which sandboxes are running and which
# templates (images) the pod has on its local dockerd.
#
# Usage:
#   scripts/inspect_pod_contents.sh
#
# Env overrides:
#   NAMESPACE         kube namespace                   (default: sandboxes)
#   SELECTOR          pod label selector               (default: app=runtime-gateway)
#   DOCKER_CONTAINER  dockerd container name           (default: dockerd)
#   GATEWAY_CONTAINER gateway (app) container name     (default: gateway)
#   IMAGE_FILTER      optional grep -E filter for images (default: none)
#   ALL_CONTAINERS    1 = include stopped sandboxes too  (default: 1)
#   LIMIT_RATIO       disk usage limit ratio; if unset, read from the
#                     live api-service env RUNTIME_GATEWAY_DISK_USAGE_LIMIT_RATIO
#                     (fallback 0.80)

NAMESPACE="${NAMESPACE:-sandboxes}"
SELECTOR="${SELECTOR:-app=runtime-gateway}"
DOCKER_CONTAINER="${DOCKER_CONTAINER:-dockerd}"
GATEWAY_CONTAINER="${GATEWAY_CONTAINER:-gateway}"
IMAGE_FILTER="${IMAGE_FILTER:-}"
ALL_CONTAINERS="${ALL_CONTAINERS:-1}"
LIMIT_RATIO="${LIMIT_RATIO:-}"

section() { printf '\n===== %s =====\n' "$1"; }

# Resolve the disk usage limit ratio from the live api-service if not given.
if [ -z "$LIMIT_RATIO" ]; then
  api_pod="$(kubectl -n "$NAMESPACE" get pods -l app=api-service \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
  if [ -n "$api_pod" ]; then
    LIMIT_RATIO="$(kubectl -n "$NAMESPACE" exec "$api_pod" -c api-service -- \
      printenv RUNTIME_GATEWAY_DISK_USAGE_LIMIT_RATIO 2>/dev/null || true)"
  fi
fi
LIMIT_RATIO="${LIMIT_RATIO:-0.80}"
echo "RUNTIME_GATEWAY_DISK_USAGE_LIMIT_RATIO = ${LIMIT_RATIO}"

fmt() {
  if command -v column >/dev/null 2>&1; then
    column -t -s "$(printf '\t')"
  else
    cat
  fi
}

pods="$(kubectl -n "$NAMESPACE" get pods -l "$SELECTOR" \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"

if [ -z "$pods" ]; then
  echo "No pods found in namespace '${NAMESPACE}' matching selector '${SELECTOR}'." >&2
  exit 1
fi

ps_flag=""
[ "$ALL_CONTAINERS" = "1" ] && ps_flag="-a"

for pod in $pods; do
  section "POD: ${pod}"

  echo "--- Docker graph usage in this pod ---"
  graph_meta="$(kubectl -n "$NAMESPACE" exec "$pod" -c "$GATEWAY_CONTAINER" -- \
    sh -lc 'printf "%s\t%s\n" "${DOCKER_GRAPH_PATH:-/var/lib/docker}" "${DOCKER_GRAPH_CAPACITY_BYTES:-}"' \
    2>/dev/null || true)"
  if [ -n "$graph_meta" ]; then
    graph="$(printf '%s' "$graph_meta" | cut -f1)"
    capacity="$(printf '%s' "$graph_meta" | cut -f2)"
    # Measure from the gateway container's read-only mount of the Docker graph.
    # Measuring inside dockerd crosses active overlay "merged" mounts and can
    # double-count running container filesystems.
    used="$(kubectl -n "$NAMESPACE" exec "$pod" -c "$GATEWAY_CONTAINER" -- \
      sh -lc 'du -sb '"$graph"' 2>/dev/null | awk '"'"'{print $1}'"'"'' 2>/dev/null || true)"
    if [ -n "$capacity" ] && [ "${capacity:-0}" -gt 0 ] 2>/dev/null && [ -n "$used" ]; then
      awk -v total="$capacity" -v used="$used" -v limit="$LIMIT_RATIO" -v graph="$graph" '
        function hr(b,  u,i,s){ split("B KB MB GB TB PB",u," "); s=b; i=1; while(s>=1024 && i<6){s/=1024; i++} return sprintf((i==1?"%d %s":"%.1f %s"), s, u[i]) }
        BEGIN{
          ur = total>0 ? used/total : 0;
          allowed = total*limit;
          headroom = allowed-used; if(headroom<0) headroom=0;
          hpct = total>0 ? (headroom/total)*100 : 0;
          status = (ur<limit) ? "OK (accepting new containers)" : "FULL (blocked for new containers)";
          printf "  path:               %s\n", graph;
          printf "  capacity:           %s\n", hr(total);
          printf "  docker used:        %s (%.1f%%)\n", hr(used), ur*100;
          printf "  scheduler limit:    %.0f%% of capacity (%s)\n", limit*100, hr(allowed);
          printf "  headroom to limit:  %s (%.1f%% of capacity)\n", hr(headroom), hpct;
          printf "  status:             %s\n", status;
        }'
    else
      echo "  (docker graph capacity unavailable — set DOCKER_GRAPH_CAPACITY_BYTES on this pod)"
    fi
  else
    echo "  (docker graph metadata unavailable)"
  fi

  echo
  echo "--- Docker usage on this pod (real per-shard consumption) ---"
  dfp="$(kubectl -n "$NAMESPACE" exec "$pod" -c "$DOCKER_CONTAINER" -- \
    docker system df 2>/dev/null || true)"
  if [ -n "$dfp" ]; then
    printf '%s\n' "$dfp" | sed 's/^/  /'
  else
    echo "  (docker usage: unavailable)"
  fi

  echo
  echo "--- Sandboxes (containers named sandbox-*) ---"
  sandboxes="$(kubectl -n "$NAMESPACE" exec "$pod" -c "$DOCKER_CONTAINER" -- \
    docker ps $ps_flag --filter 'name=sandbox-' \
    --format '{{.Names}}\t{{.Status}}\t{{.Image}}' 2>/dev/null || true)"
  if [ -n "$sandboxes" ]; then
    {
      printf 'SANDBOX\tSTATUS\tTEMPLATE_IMAGE\n'
      printf '%s\n' "$sandboxes"
    } | fmt
    echo "(sandboxes: $(printf '%s\n' "$sandboxes" | grep -c .))"
  else
    echo "(none)"
  fi

  echo
  echo "--- Templates (images on this pod) ---"
  images="$(kubectl -n "$NAMESPACE" exec "$pod" -c "$DOCKER_CONTAINER" -- \
    docker images --format '{{.Repository}}:{{.Tag}}\t{{.Size}}\t{{.CreatedSince}}' 2>/dev/null || true)"
  if [ -n "$IMAGE_FILTER" ] && [ -n "$images" ]; then
    images="$(printf '%s\n' "$images" | grep -E "$IMAGE_FILTER" || true)"
  fi
  if [ -n "$images" ]; then
    {
      printf 'TEMPLATE_IMAGE\tSIZE\tCREATED\n'
      printf '%s\n' "$images"
    } | fmt
    echo "(images: $(printf '%s\n' "$images" | grep -c .))"
  else
    echo "(none)"
  fi
done

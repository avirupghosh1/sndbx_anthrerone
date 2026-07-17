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
#   DOCKER_HOST_IN_POD dockerd endpoint inside pod     (default: tcp://127.0.0.1:2375)
#   GATEWAY_CONTAINER gateway (app) container name     (default: runtime-gateway; falls back to gateway)
#   IMAGE_FILTER      optional grep -E filter for images (default: none)
#   ALL_CONTAINERS    1 = include stopped sandboxes too  (default: 1)

NAMESPACE="${NAMESPACE:-sandboxes}"
SELECTOR="${SELECTOR:-app=runtime-gateway}"
DOCKER_HOST_IN_POD="${DOCKER_HOST_IN_POD:-tcp://127.0.0.1:2375}"
GATEWAY_CONTAINER="${GATEWAY_CONTAINER:-runtime-gateway}"
IMAGE_FILTER="${IMAGE_FILTER:-}"
ALL_CONTAINERS="${ALL_CONTAINERS:-1}"

section() { printf '\n===== %s =====\n' "$1"; }

echo "Docker endpoint inside dockerd sidecars = ${DOCKER_HOST_IN_POD}"

fmt() {
  if command -v column >/dev/null 2>&1; then
    column -t -s "$(printf '\t')"
  else
    cat
  fi
}

first_line() {
  printf '%s' "$1" | sed -n '1p'
}

pod_containers() {
  kubectl -n "$NAMESPACE" get pod "$1" \
    -o jsonpath='{range .spec.containers[*]}{.name}{"\n"}{end}' 2>/dev/null || true
}

resolve_container() {
  local pod="$1"
  local requested="$2"
  local fallback="$3"
  local role="$4"
  local containers

  containers="$(pod_containers "$pod")"
  if printf '%s\n' "$containers" | grep -qx "$requested"; then
    printf '%s' "$requested"
    return 0
  fi

  if printf '%s\n' "$containers" | grep -qx "$fallback"; then
    echo "  warning: ${role} container '${requested}' is not present in ${pod}; using '${fallback}'." >&2
    echo "           available containers: $(printf '%s\n' "$containers" | paste -sd ', ' -)" >&2
    printf '%s' "$fallback"
    return 0
  fi

  echo "  warning: ${role} container '${requested}' is not present in ${pod}; available containers: $(printf '%s\n' "$containers" | paste -sd ', ' -)" >&2
  printf '%s' "$requested"
}

pods="$(kubectl -n "$NAMESPACE" get pods -l "$SELECTOR" \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)"

if [ -z "$pods" ]; then
  echo "No pods found in namespace '${NAMESPACE}' matching selector '${SELECTOR}'." >&2
  exit 1
fi

for pod in $pods; do
  section "POD: ${pod}"
  gateway_container="$(resolve_container "$pod" "$GATEWAY_CONTAINER" "gateway" "gateway/app")"
  echo "containers: gateway/app=${gateway_container}"

  echo "--- Docker graph usage in this pod ---"
  graph_meta_status=0
  graph_meta="$(kubectl -n "$NAMESPACE" exec "$pod" -c "$gateway_container" -- \
    sh -lc 'printf "%s\t%s\n" "${DOCKER_GRAPH_PATH:-/var/lib/docker}" "${DOCKER_GRAPH_CAPACITY_BYTES:-}"' \
    2>&1)" || graph_meta_status=$?
  if [ -n "$graph_meta" ]; then
    graph="$(printf '%s' "$graph_meta" | cut -f1)"
    capacity="$(printf '%s' "$graph_meta" | cut -f2)"
    # Measure from the gateway container's read-only mount of the Docker graph.
    # Measuring inside dockerd crosses active overlay "merged" mounts and can
    # double-count running container filesystems.
    used="$(kubectl -n "$NAMESPACE" exec "$pod" -c "$gateway_container" -- \
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
    echo "  (docker graph metadata unavailable: $(first_line "$graph_meta"))"
    if [ "$graph_meta_status" -ne 0 ]; then
      echo "  hint: check GATEWAY_CONTAINER; this pod's gateway container should usually be 'gateway'."
    fi
  fi

  echo
  echo "--- Docker usage on this pod (real per-shard consumption) ---"
  dfp="$(kubectl -n "$NAMESPACE" exec "$pod" -c "$gateway_container" -- env DOCKER_HOST="$DOCKER_HOST_IN_POD" python -c "
import docker, os
c = docker.DockerClient(base_url=os.environ['DOCKER_HOST'])
df = c.df()
for key in ('Containers', 'Images', 'Volumes', 'BuildCache'):
    block = df.get(key) or {}
    print(f\"{key}: count={block.get('Count', 0)} active={block.get('Active', 0)} size={block.get('Size', 0)} reclaimable={block.get('Reclaimable', 0)}\")
" 2>/dev/null || true)"
  if [ -n "$dfp" ]; then
    printf '%s\n' "$dfp" | sed 's/^/  /'
  else
    echo "  (docker usage: unavailable)"
    echo "  hint: dockerd may still be starting, or DOCKER_HOST_IN_POD/GATEWAY_CONTAINER is wrong."
  fi

  echo
  echo "--- Sandboxes (containers named sandbox-*) ---"
  sandboxes="$(kubectl -n "$NAMESPACE" exec "$pod" -c "$gateway_container" -- env DOCKER_HOST="$DOCKER_HOST_IN_POD" ALL_CONTAINERS="$ALL_CONTAINERS" python -c "
import docker, os
all_flag = os.environ.get('ALL_CONTAINERS', '1') == '1'
c = docker.DockerClient(base_url=os.environ['DOCKER_HOST'])
for cont in c.containers.list(all=all_flag):
    name = (cont.name or '').lstrip('/')
    if not name.startswith('sandbox-'):
        continue
    image = (cont.image.tags[0] if cont.image.tags else cont.image.short_id)
    print(f\"{name}\t{cont.status}\t{image}\")
" 2>/dev/null || true)"
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
  images="$(kubectl -n "$NAMESPACE" exec "$pod" -c "$gateway_container" -- env DOCKER_HOST="$DOCKER_HOST_IN_POD" python -c "
import datetime as dt
import docker, os
c = docker.DockerClient(base_url=os.environ['DOCKER_HOST'])
now = dt.datetime.now(dt.timezone.utc)
for image in c.images.list():
    tags = image.tags or [image.short_id]
    for tag in tags:
        created = image.attrs.get('Created', '')
        try:
            created_dt = dt.datetime.fromisoformat(created.replace('Z', '+00:00'))
            age = now - created_dt
            if age.days:
                since = f\"{age.days} days ago\"
            else:
                hours = int(age.total_seconds() // 3600)
                since = f\"{hours} hours ago\" if hours else 'just now'
        except Exception:
            since = created
        size = image.attrs.get('Size', 0)
        print(f\"{tag}\t{size}\t{since}\")
" 2>/dev/null || true)"
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

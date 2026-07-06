#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-sandboxes}"
DB_URL="${DB_URL:-postgresql://avirup.ghosh@localhost:5433/postgres}"
SHARDS="${SHARDS:-3}"
IMAGE_FILTER="${IMAGE_FILTER:-mysandbox|custodian|python|agentlib|REPOSITORY}"
DOCKER_HOST_IN_POD="${DOCKER_HOST_IN_POD:-tcp://127.0.0.1:2375}"

section() {
  printf '\n===== %s =====\n' "$1"
}

section "Kubernetes pods"
kubectl -n "$NAMESPACE" get pods -o wide
echo "Docker endpoint inside dockerd sidecars: ${DOCKER_HOST_IN_POD}"

section "Warm-pool segments"
psql "$DB_URL" -c "
select
  w.template_id,
  w.cpu_limit,
  w.memory_limit,
  w.timeout,
  w.desired_size,
  w.inflight_count,
  coalesce(w.preferred_gateway_instance_id, '') as preferred_gateway,
  count(s.sandbox_id) filter (where s.state = 'running' and s.is_warm_pool = 1) as ready_running,
  min(s.lease_expires_at) filter (where s.state = 'running' and s.is_warm_pool = 1) as earliest_warm_lease,
  coalesce(w.ready_image_ref, '') as ready_image_ref,
  coalesce(w.last_error, '') as last_error
from warm_pool_segments w
left join sandboxes s on s.warm_pool_key = w.warm_pool_key
group by
  w.template_id,
  w.cpu_limit,
  w.memory_limit,
  w.timeout,
  w.desired_size,
  w.inflight_count,
  w.preferred_gateway_instance_id,
  w.ready_image_ref,
  w.last_error
order by w.template_id;
"

section "Running warm-pool rows"
psql "$DB_URL" -c "
select
  sandbox_id,
  template_id,
  gateway_instance_id,
  container_id,
  lease_expires_at,
  warm_pool_key
from sandboxes
where state = 'running'
  and is_warm_pool = 1
order by template_id, gateway_instance_id, created_at;
"

section "Running non-warm sandboxes"
psql "$DB_URL" -c "
select
  sandbox_id,
  template_id,
  gateway_instance_id,
  container_id,
  lease_expires_at
from sandboxes
where state = 'running'
  and coalesce(is_warm_pool, 0) = 0
order by created_at desc
limit 50;
"

section "Lost sandbox rows waiting for retention purge"
psql "$DB_URL" -c "
select state, gateway_instance_id, count(*) as count
from sandboxes
where state = 'lost'
group by state, gateway_instance_id
order by count desc;
"

for i in $(seq 0 "$((SHARDS - 1))"); do
  pod="runtime-gateway-${i}"
  section "Shard ${pod}: containers"
  if ! kubectl -n "$NAMESPACE" get pod "$pod" >/dev/null 2>&1; then
    echo "missing pod: ${pod}"
    continue
  fi
  kubectl -n "$NAMESPACE" exec "$pod" -c dockerd -- \
    docker --host "$DOCKER_HOST_IN_POD" ps -a --format 'table {{.Names}}\t{{.Status}}\t{{.Image}}'

  section "Shard ${pod}: relevant images"
  kubectl -n "$NAMESPACE" exec "$pod" -c dockerd -- sh -lc \
    "docker --host '$DOCKER_HOST_IN_POD' images --format 'table {{.Repository}}:{{.Tag}}\t{{.Size}}\t{{.CreatedSince}}' | grep -E '${IMAGE_FILTER}' || true"
done

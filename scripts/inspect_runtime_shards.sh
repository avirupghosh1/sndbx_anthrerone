#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-sandboxes}"
DB_URL="${DB_URL:-mongodb://127.0.0.1:27017/sandboxes}"
DB_TYPE="${DB_TYPE:-}"
SHARDS="${SHARDS:-3}"
IMAGE_FILTER="${IMAGE_FILTER:-mysandbox|custodian|python|agentlib|REPOSITORY}"
DOCKER_HOST_IN_POD="${DOCKER_HOST_IN_POD:-tcp://127.0.0.1:2375}"

section() {
  printf '\n===== %s =====\n' "$1"
}

is_mongo_url() {
  local db_type_lc
  db_type_lc="$(printf '%s' "$DB_TYPE" | tr '[:upper:]' '[:lower:]')"
  case "$db_type_lc" in
    mongo|mongodb) return 0 ;;
    postgres|postgresql|pg) return 1 ;;
  esac
  case "$DB_URL" in
    mongodb://*|mongodb+srv://*) return 0 ;;
    *) return 1 ;;
  esac
}

run_mongo() {
  mongosh "$DB_URL" --quiet --eval "$1"
}

require_tool() {
  local tool="$1"
  local hint="${2:-}"
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "Required command not found: $tool" >&2
    if [ -n "$hint" ]; then
      echo "$hint" >&2
    fi
    exit 2
  fi
}

section "Kubernetes pods"
kubectl -n "$NAMESPACE" get pods -o wide
echo "Docker endpoint inside dockerd sidecars: ${DOCKER_HOST_IN_POD}"

section "Warm-pool segments"
if is_mongo_url; then
  require_tool mongosh "Install MongoDB Shell or set DB_URL to a PostgreSQL DSN for Postgres inspection."
  run_mongo '
    const rows = db.warm_pool_segments.find({}).sort({template_id: 1}).toArray().map((w) => {
      const ready = db.sandboxes.countDocuments({
        warm_pool_key: w.warm_pool_key,
        state: "running",
        is_warm_pool: true,
      });
      const earliest = db.sandboxes.find({
        warm_pool_key: w.warm_pool_key,
        state: "running",
        is_warm_pool: true,
      }).sort({lease_expires_at: 1}).limit(1).toArray()[0];
      return {
        template_id: w.template_id,
        cpu_limit: w.cpu_limit,
        memory_limit: w.memory_limit,
        timeout: w.timeout,
        desired_size: w.desired_size,
        inflight_count: w.inflight_count || 0,
        preferred_gateway: w.preferred_gateway_instance_id || "",
        ready_running: ready,
        earliest_warm_lease: earliest ? earliest.lease_expires_at : null,
        ready_image_ref: w.ready_image_ref || "",
        last_error: w.last_error || "",
      };
    });
    printjson(rows);
  '
else
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
fi

section "Running warm-pool rows"
if is_mongo_url; then
  run_mongo '
    printjson(db.sandboxes.find(
      {state: "running", is_warm_pool: true},
      {_id: 0, sandbox_id: 1, template_id: 1, gateway_instance_id: 1, container_id: 1, lease_expires_at: 1, warm_pool_key: 1}
    ).sort({template_id: 1, gateway_instance_id: 1, created_at: 1}).toArray());
  '
else
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
fi

section "Running non-warm sandboxes"
if is_mongo_url; then
  run_mongo '
    printjson(db.sandboxes.find(
      {state: "running", $or: [{is_warm_pool: false}, {is_warm_pool: 0}, {is_warm_pool: null}, {is_warm_pool: {$exists: false}}]},
      {_id: 0, sandbox_id: 1, template_id: 1, gateway_instance_id: 1, container_id: 1, lease_expires_at: 1}
    ).sort({created_at: -1}).limit(50).toArray());
  '
else
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
fi

section "Lost sandbox rows waiting for retention purge"
if is_mongo_url; then
  run_mongo '
    printjson(db.sandboxes.aggregate([
      {$match: {state: "lost"}},
      {$group: {_id: {state: "$state", gateway_instance_id: "$gateway_instance_id"}, count: {$sum: 1}}},
      {$sort: {count: -1}},
    ]).toArray());
  '
else
psql "$DB_URL" -c "
select state, gateway_instance_id, count(*) as count
from sandboxes
where state = 'lost'
group by state, gateway_instance_id
order by count desc;
"
fi

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

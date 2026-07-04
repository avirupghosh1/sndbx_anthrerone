#!/usr/bin/env bash
set -euo pipefail

# Destructive local cleanup for the raw minikube deployment.
# It removes runtime containers/images, runtime/template DB rows, and local PVCs.

NAMESPACE="${NAMESPACE:-sandboxes}"
DB_URL="${DB_URL:-postgresql://avirup.ghosh@localhost:5433/postgres}"
EXPECTED_CONTEXT="${EXPECTED_CONTEXT:-minikube}"
CLEAN_SLATE_CONFIRM="${CLEAN_SLATE_CONFIRM:-}"

API_DEPLOY="${API_DEPLOY:-api-service}"
REGISTRY_DEPLOY="${REGISTRY_DEPLOY:-registry}"
RUNTIME_STS="${RUNTIME_STS:-runtime-gateway}"
DOCKER_CONTAINER="${DOCKER_CONTAINER:-dockerd}"

PRUNE_DOCKER="${PRUNE_DOCKER:-1}"
TRUNCATE_DB="${TRUNCATE_DB:-1}"
DELETE_PVCS="${DELETE_PVCS:-1}"
WAIT_TIMEOUT_SEC="${WAIT_TIMEOUT_SEC:-180}"

section() {
  printf '\n===== %s =====\n' "$1"
}

exists() {
  kubectl -n "$NAMESPACE" get "$1" "$2" >/dev/null 2>&1
}

replicas_for() {
  local kind="$1"
  local name="$2"
  kubectl -n "$NAMESPACE" get "$kind" "$name" -o jsonpath='{.spec.replicas}' 2>/dev/null || printf '0'
}

pod_names() {
  kubectl -n "$NAMESPACE" get pods -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true
}

runtime_pods() {
  pod_names | grep -E "^${RUNTIME_STS}-[0-9]+$" || true
}

wait_for_local_pods_gone() {
  local deadline=$((SECONDS + WAIT_TIMEOUT_SEC))
  while true; do
    local remaining
    remaining="$(
      pod_names | grep -E "^(${API_DEPLOY}-|${REGISTRY_DEPLOY}-|${RUNTIME_STS}-[0-9]+$)" || true
    )"
    if [ -z "$remaining" ]; then
      return 0
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      echo "Timed out waiting for pods to terminate:" >&2
      printf '%s\n' "$remaining" >&2
      return 1
    fi
    sleep 3
  done
}

delete_pvc_if_exists() {
  local name="$1"
  kubectl -n "$NAMESPACE" delete pvc "$name" --ignore-not-found=true
}

require_confirmation() {
  if [ "$CLEAN_SLATE_CONFIRM" != "yes" ]; then
    cat >&2 <<EOF
Refusing to clean without explicit confirmation.

Run:
  CLEAN_SLATE_CONFIRM=yes $0

Defaults:
  NAMESPACE=$NAMESPACE
  DB_URL=$DB_URL
  EXPECTED_CONTEXT=$EXPECTED_CONTEXT

This will scale local workloads to 0, truncate sandbox/template runtime rows,
delete runtime Docker graph PVCs, and delete registry/api stale PVCs if present.
EOF
    exit 2
  fi
}

require_context() {
  local context
  context="$(kubectl config current-context)"
  if [ -n "$EXPECTED_CONTEXT" ] && [ "$context" != "$EXPECTED_CONTEXT" ]; then
    cat >&2 <<EOF
Refusing to run on Kubernetes context '$context'; expected '$EXPECTED_CONTEXT'.

Override only when intentional:
  EXPECTED_CONTEXT=$context CLEAN_SLATE_CONFIRM=yes $0
EOF
    exit 2
  fi
}

require_confirmation
require_context
command -v kubectl >/dev/null
command -v psql >/dev/null

api_replicas="$(replicas_for deployment "$API_DEPLOY")"
registry_replicas="$(replicas_for deployment "$REGISTRY_DEPLOY")"
runtime_replicas="$(replicas_for statefulset "$RUNTIME_STS")"
runtime_replicas="${runtime_replicas:-0}"

section "Target"
printf 'context=%s\n' "$(kubectl config current-context)"
printf 'namespace=%s\n' "$NAMESPACE"
printf 'database=%s\n' "$DB_URL"
printf 'api_replicas=%s registry_replicas=%s runtime_replicas=%s\n' \
  "$api_replicas" "$registry_replicas" "$runtime_replicas"

if [ "$PRUNE_DOCKER" = "1" ]; then
  section "Prune runtime Docker daemons"
  while IFS= read -r pod; do
    [ -n "$pod" ] || continue
    if ! kubectl -n "$NAMESPACE" get pod "$pod" >/dev/null 2>&1; then
      continue
    fi
    phase="$(kubectl -n "$NAMESPACE" get pod "$pod" -o jsonpath='{.status.phase}')"
    if [ "$phase" != "Running" ]; then
      echo "skip $pod phase=$phase"
      continue
    fi
    echo "prune $pod"
    kubectl -n "$NAMESPACE" exec "$pod" -c "$DOCKER_CONTAINER" -- sh -lc '
      ids="$(docker ps -aq)"
      if [ -n "$ids" ]; then
        docker rm -f $ids >/dev/null
      fi
      docker system prune -a --volumes -f
    '
  done < <(runtime_pods)
fi

section "Scale workloads down"
if exists deployment "$API_DEPLOY"; then
  kubectl -n "$NAMESPACE" scale deployment "$API_DEPLOY" --replicas=0
fi
if exists deployment "$REGISTRY_DEPLOY"; then
  kubectl -n "$NAMESPACE" scale deployment "$REGISTRY_DEPLOY" --replicas=0
fi
if exists statefulset "$RUNTIME_STS"; then
  kubectl -n "$NAMESPACE" scale statefulset "$RUNTIME_STS" --replicas=0
fi
wait_for_local_pods_gone

if [ "$TRUNCATE_DB" = "1" ]; then
  section "Truncate runtime database rows"
  psql "$DB_URL" -v ON_ERROR_STOP=1 -c "
    TRUNCATE
      agent_messages,
      commands_history,
      agents,
      sandbox_snapshots,
      sandboxes,
      sandbox_templates,
      template_builds,
      warm_pool_segments,
      service_leases
    RESTART IDENTITY CASCADE;
  "
fi

if [ "$DELETE_PVCS" = "1" ]; then
  section "Delete local PVCs"
  if [ "$runtime_replicas" -gt 0 ] 2>/dev/null; then
    for i in $(seq 0 "$((runtime_replicas - 1))"); do
      delete_pvc_if_exists "docker-graph-${RUNTIME_STS}-${i}"
    done
  fi
  delete_pvc_if_exists "runtime-gateway-docker-graph"
  delete_pvc_if_exists "registry-data"
  delete_pvc_if_exists "api-service-data"

  if [ -n "${EXTRA_PVCS:-}" ]; then
    for pvc in $EXTRA_PVCS; do
      delete_pvc_if_exists "$pvc"
    done
  fi
fi

section "Post-clean checks"
kubectl -n "$NAMESPACE" get pods,pvc || true
psql "$DB_URL" -v ON_ERROR_STOP=1 -c "
  SELECT 'sandboxes' AS table_name, count(*) FROM sandboxes
  UNION ALL SELECT 'sandbox_templates', count(*) FROM sandbox_templates
  UNION ALL SELECT 'template_builds', count(*) FROM template_builds
  UNION ALL SELECT 'warm_pool_segments', count(*) FROM warm_pool_segments
  UNION ALL SELECT 'service_leases', count(*) FROM service_leases
  ORDER BY table_name;
"

section "Next"
cat <<EOF
Clean slate complete.

For the raw local manifests, recreate PVCs and restart pods with:
  kubectl apply -f deploy/api-service.yaml
  kubectl apply -f deploy/runtime-gateway.yaml

Or restore previous replica counts manually:
  kubectl -n $NAMESPACE scale deployment $API_DEPLOY --replicas=$api_replicas
  kubectl -n $NAMESPACE scale deployment $REGISTRY_DEPLOY --replicas=$registry_replicas
  kubectl -n $NAMESPACE scale statefulset $RUNTIME_STS --replicas=$runtime_replicas
EOF

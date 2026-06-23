#!/usr/bin/env bash
# Delete stale sandbox pods (frees CPU on small minikube clusters).
set -euo pipefail
kubectl delete pods -n sandboxes -l app=sandbox --ignore-not-found
kubectl delete jobs -n sandboxes -l app=kaniko-builder --ignore-not-found 2>/dev/null || true
echo "Sandbox pods cleared. Running:"
kubectl get pods -n sandboxes

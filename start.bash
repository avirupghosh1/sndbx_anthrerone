minikube start
kubectl -n sandboxes get pods
kubectl -n sandboxes port-forward deploy/api-service -n sandboxes 8000:8000
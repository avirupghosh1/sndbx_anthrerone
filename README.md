# sndbx_anthrerone
cd "intern_1strepo/api_server copy"
cp deploy/k8s/secrets.yaml.example deploy/k8s/secrets.yaml
# Edit deploy/k8s/secrets.yaml — set the same strong value for API_KEY and CONTROL_PLANE_API_KEY
kubectl apply -f deploy/k8s/secrets.yaml
4. Apply manifests (order matters)
# Control plane + RBAC + PVC + api-service
kubectl apply -f deploy/k8s/api-service.yaml
# Data plane
kubectl apply -f ../proxy_service/deploy/k8s/proxy-service.yaml
kubectl apply -f ../proxy_service/deploy/k8s/ingress.yaml
Patch images if needed:

kubectl -n sandboxes set image deployment/api-service api-service=YOUR_REGISTRY/api-service:latest
kubectl -n sandboxes set image deployment/proxy-service proxy-service=YOUR_REGISTRY/proxy-service:latest
5. Verify pods
kubectl -n sandboxes get pods,svc,ingress,pvc
kubectl -n sandboxes logs deploy/api-service --tail=50
kubectl -n sandboxes logs deploy/proxy-service --tail=50
curl -s -H "X-API-Key: YOUR_KEY" https://api.sndbx.com/health | jq .
curl -s https://YOUR_PROXY_POD_IP:8080/health   # in-cluster smoke test
Expect api-service /health to show "execution_plane_ok": true when the pod has RBAC and in-cluster K8s access.

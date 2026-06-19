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
minikube start
minikube addons enable ingress
kubectl get nodes    # must show Ready
2. Build images into minikube
On Linux, from your repo:

eval $(minikube docker-env)
cd "intern_1strepo/api_server copy"
docker build -f Dockerfile.api-service -t api-service:latest .
cd ../proxy_service
docker build -t proxy-service:latest .
Using minikube docker-env avoids pushing to a registry.

3. Deploy
cd "intern_1strepo/api_server copy"
cp deploy/k8s/secrets.yaml.example deploy/k8s/secrets.yaml
# edit: set API_KEY and CONTROL_PLANE_API_KEY to the same value, e.g. test-key-12345
kubectl apply -f deploy/k8s/secrets.yaml
kubectl apply -f deploy/k8s/api-service.yaml
kubectl apply -f ../proxy_service/deploy/k8s/proxy-service.yaml
kubectl apply -f ../proxy_service/deploy/k8s/ingress.yaml
Wait until pods are up:

kubectl -n sandboxes get pods
kubectl -n sandboxes wait --for=condition=ready pod -l app=api-service --timeout=120s
kubectl -n sandboxes wait --for=condition=ready pod -l app=proxy-service --timeout=120s
4. Smoke-test api-service (no ingress needed yet)
kubectl -n sandboxes port-forward svc/api-service 8000:8000
From Linux (or Mac with SSH tunnel to Linux):

curl -s -H "X-API-Key: test-key-12345" http://127.0.0.1:8000/health
Fix on your Linux minikube box
Run everything on the Linux machine where minikube runs.

1. Confirm the error
kubectl -n sandboxes describe pod -l app=api-service | tail -20
You’ll likely see something like: Failed to pull image "api-service:latest" ... not found.

2. Build images inside minikube’s Docker
This is the important step — build while pointed at minikube’s Docker, not your host’s:

eval $(minikube docker-env)
cd "/path/to/please_work/api_server copy"
docker build -f Dockerfile.api-service -t api-service:latest .
cd "../proxy_service"
docker build -t proxy-service:latest .
docker images | grep -E 'api-service|proxy-service'
You should see both images listed.

3. Restart pods so they pick up the images
kubectl -n sandboxes rollout restart deployment/api-service
kubectl -n sandboxes rollout restart deployment/proxy-service
kubectl -n sandboxes get pods -w
Wait until all show Running and READY 1/1.

4. Verify
kubectl -n sandboxes get pods
kubectl -n sandboxes logs deploy/api-service --tail=30
kubectl -n sandboxes logs deploy/proxy-service --tail=30
If it still fails
PVC stuck (api-service):

kubectl -n sandboxes get pvc
kubectl -n sandboxes describe pod -l app=api-service
If the PVC is Pending, minikube may need a default storage class:

minikube addons enable default-storageclass
kubectl -n sandboxes delete pod -l app=api-service
Secret missing:

kubectl -n sandboxes get secret sandbox-secrets
If missing, re-apply secrets after creating the namespace.

Force local-only pulls (minikube dev only):

If images exist in minikube but Kube still tries to pull from the internet, patch:

kubectl -n sandboxes patch deployment api-service -p \
  '{"spec":{"template":{"spec":{"containers":[{"name":"api-service","imagePullPolicy":"Never"}]}}}}'
kubectl -n sandboxes patch deployment proxy-service -p \
  '{"spec":{"template":{"spec":{"containers":[{"name":"proxy-service","imagePullPolicy":"Never"}]}}}}'
Checklist
Step	Command	Expected
Minikube running
minikube status
Running
Images built in minikube
eval $(minikube docker-env) then docker images
api-service:latest, proxy-service:latest
Pods healthy
kubectl -n sandboxes get pods
Running, not ErrImagePull
Why this happened
The YAML references:

image: api-service:latest
image: proxy-service:latest
Those images only exist after you build them on the minikube node. kubectl apply deploys the manifests; it does not build images.

After pods are Running, test with:

kubectl -n sandboxes port-forward svc/api-service 8000:8000
curl -s -H "X-API-Key: YOUR_KEY" http://127.0.0.1:8000/health
Paste kubectl -n sandboxes get pods and the last few lines of kubectl describe pod for one failing pod if it’s still not working.
6. Create a sandbox (needs a template image)
You need a container image minikube can pull, e.g. python:3.11, registered as a template in api-service (via its templates API or DB). Simplest path if you already have templates in SQLite from before — otherwise create one:

curl -s -X POST "http://127.0.0.1:8000/sandboxes" \
  -H "X-API-Key: test-key-12345" \
  -H "Content-Type: application/json" \
  -d '{
    "template_id": "python:3.11",
    "metadata": { "guest_ports": [8765] }
  }'


"""Kaniko-based Dockerfile builder for Kubernetes production environments."""

import io
import os
import tarfile
import time
import uuid
import logging
import base64

from kubernetes import client, config as k8s_config
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)

def build_with_kaniko(
    dockerfile: str,
    template_id: str,
    context_tar_gzip: bytes | None,
    image_tag: str | None = None,
    registry_host: str = "registry.kube-system.svc.cluster.local:80",
    kubelet_registry_host: str = "localhost:5000",
    namespace: str = "sandboxes",
    api_service_host: str = "api-service.sandboxes.svc.cluster.local:8000"
) -> str:
    """Build a Dockerfile using Kaniko inside a K8s Job."""
    
    try:
        try:
            k8s_config.load_incluster_config()
        except k8s_config.ConfigException:
            k8s_config.load_kube_config()
        batch_api = client.BatchV1Api()
    except Exception as e:
        raise RuntimeError(f"Failed to load K8s config: {e}")

    job_id = f"kaniko-{uuid.uuid4().hex[:8]}"
    
    contexts_dir = "/var/lib/api/contexts"
    os.makedirs(contexts_dir, exist_ok=True)
    
    context_path = os.path.join(contexts_dir, f"{job_id}.tar.gz")
    
    if context_tar_gzip is None:
        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            df_info = tarfile.TarInfo("Dockerfile")
            df_bytes = dockerfile.encode("utf-8")
            df_info.size = len(df_bytes)
            tf.addfile(df_info, io.BytesIO(df_bytes))
        context_tar_gzip = buf.getvalue()
        
    with open(context_path, "wb") as f:
        f.write(context_tar_gzip)
        
    tag = image_tag or f"sandbox-{template_id}:latest"
    dest = f"{registry_host}/{tag}"
    kubelet_dest = f"{kubelet_registry_host}/{tag}"
    
    job_name = job_id
    
    b64_df = base64.b64encode(dockerfile.encode('utf-8')).decode('ascii')
    init_cmd = (
        f"apk add --no-cache curl tar && "
        f"curl -sSf http://{api_service_host}/internal/contexts/{job_id} -o /tmp/ctx.tar.gz && "
        f"tar -xzf /tmp/ctx.tar.gz -C /workspace && "
        f"echo '{b64_df}' | base64 -d > /workspace/Dockerfile"
    )
    
    job_spec = client.V1Job(
        metadata=client.V1ObjectMeta(name=job_name, namespace=namespace),
        spec=client.V1JobSpec(
            backoff_limit=0,
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"app": "kaniko-builder"}),
                spec=client.V1PodSpec(
                    restart_policy="Never",
                    init_containers=[
                        client.V1Container(
                            name="fetch-context",
                            image="alpine:3.19",
                            command=["sh", "-c", init_cmd],
                            volume_mounts=[
                                client.V1VolumeMount(name="workspace", mount_path="/workspace")
                            ]
                        )
                    ],
                    containers=[
                        client.V1Container(
                            name="kaniko",
                            image="gcr.io/kaniko-project/executor:latest",
                            args=[
                                "--dockerfile=/workspace/Dockerfile",
                                "--context=dir:///workspace",
                                f"--destination={dest}",
                                "--insecure",
                                "--insecure-pull",
                                "--skip-tls-verify",
                                "--cache=true"
                            ],
                            volume_mounts=[
                                client.V1VolumeMount(name="workspace", mount_path="/workspace")
                            ]
                        )
                    ],
                    volumes=[
                        client.V1Volume(name="workspace", empty_dir=client.V1EmptyDirVolumeSource())
                    ]
                )
            )
        )
    )
    
    try:
        batch_api.create_namespaced_job(namespace=namespace, body=job_spec)
    except ApiException as e:
        os.remove(context_path)
        raise RuntimeError(f"Failed to create Kaniko job: {e}")
        
    try:
        for _ in range(300):
            job = batch_api.read_namespaced_job_status(name=job_name, namespace=namespace)
            if job.status.succeeded:
                try:
                    batch_api.delete_namespaced_job(
                        name=job_name, 
                        namespace=namespace, 
                        body=client.V1DeleteOptions(propagation_policy="Background")
                    )
                except Exception as e:
                    logger.warning(f"Failed to delete job {job_name}: {e}")
                return kubelet_dest
            if job.status.failed:
                raise RuntimeError(f"Kaniko job failed: {job.status.conditions}")
            time.sleep(2)
        raise RuntimeError("Kaniko job timed out after 10 minutes")
    finally:
        if os.path.exists(context_path):
            os.remove(context_path)

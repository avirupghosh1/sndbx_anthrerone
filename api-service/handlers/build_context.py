"""Shared build-context upload helpers for SDK compatibility adapters."""

from __future__ import annotations

import base64
import copy
import io
import posixpath
import tarfile
from typing import Any, Iterable

from config import get_config

try:
    import boto3
except Exception:  # pragma: no cover - optional dependency import guard
    boto3 = None


E2B_UPLOAD_NAMESPACE = "e2b-template-build"
DAYTONA_UPLOAD_NAMESPACE = "daytona-object-storage"
DAYTONA_CONTEXT_BUCKET = "daytona-volume-builds"


def e2b_upload_key(template_id: str, files_hash: str) -> str:
    return f"{template_id.strip()}/{files_hash.strip()}.tar"


def daytona_context_key(organization_id: str, context_hash: str) -> str:
    return f"{organization_id.strip()}/{context_hash.strip()}/context.tar"


def image_building_s3_enabled() -> bool:
    return bool(get_config().IMAGE_BUILDING_AUTH_REQUIRED)


def _s3_client() -> Any:
    cfg = get_config()
    if boto3 is None:
        raise RuntimeError(
            "IMAGE_BUILDING_AUTH_REQUIRED=true requires boto3 in the api-service image"
        )
    kwargs: dict[str, Any] = {
        "region_name": cfg.IMAGE_BUILDING_S3_REGION,
        "aws_access_key_id": cfg.IMAGE_BUILDING_S3_ACCESS_KEY_ID,
        "aws_secret_access_key": cfg.IMAGE_BUILDING_S3_SECRET_ACCESS_KEY,
    }
    if cfg.IMAGE_BUILDING_S3_SESSION_TOKEN:
        kwargs["aws_session_token"] = cfg.IMAGE_BUILDING_S3_SESSION_TOKEN
    if cfg.IMAGE_BUILDING_S3_ENDPOINT_URL:
        kwargs["endpoint_url"] = cfg.IMAGE_BUILDING_S3_ENDPOINT_URL
    return boto3.client("s3", **kwargs)


def _s3_object_key(owner_client_id: str, namespace: str, object_key: str) -> str:
    cfg = get_config()
    owner = str(owner_client_id or "").strip().strip("/")
    ns = str(namespace or "").strip().strip("/")
    key = str(object_key or "").strip().lstrip("/")
    if not owner or not ns or not key:
        raise ValueError("template build upload S3 key is incomplete")
    parts = [cfg.IMAGE_BUILDING_S3_PREFIX, owner, ns, key]
    clean = [part for part in parts if part]
    return "/".join(clean)


def _s3_not_found(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return False
    err = response.get("Error") if isinstance(response.get("Error"), dict) else {}
    metadata = (
        response.get("ResponseMetadata")
        if isinstance(response.get("ResponseMetadata"), dict)
        else {}
    )
    code = str(err.get("Code") or "")
    status = metadata.get("HTTPStatusCode")
    return code in {"404", "NoSuchKey", "NotFound"} or status == 404


def _metadata_strings(metadata: dict[str, Any] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for key, value in dict(metadata or {}).items():
        safe_key = str(key or "").strip()
        if safe_key:
            out[safe_key] = str(value)
    return out


def put_template_build_upload(
    db: Any,
    owner_client_id: str,
    namespace: str,
    object_key: str,
    payload: bytes,
    *,
    content_type: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not image_building_s3_enabled():
        return db.put_template_build_upload(
            owner_client_id,
            namespace,
            object_key,
            payload,
            content_type=content_type,
            metadata=metadata,
        )

    data = bytes(payload or b"")
    cfg = get_config()
    key = _s3_object_key(owner_client_id, namespace, object_key)
    _s3_client().put_object(
        Bucket=cfg.IMAGE_BUILDING_S3_BUCKET,
        Key=key,
        Body=data,
        ContentType=content_type or "application/octet-stream",
        Metadata=_metadata_strings(metadata),
    )
    return {
        "owner_client_id": owner_client_id,
        "namespace": namespace,
        "object_key": object_key,
        "content_type": content_type or "",
        "payload": data,
        "metadata": dict(metadata or {}),
        "storage": "s3",
        "s3_bucket": cfg.IMAGE_BUILDING_S3_BUCKET,
        "s3_key": key,
    }


def get_template_build_upload(
    db: Any,
    owner_client_id: str,
    namespace: str,
    object_key: str,
) -> dict[str, Any] | None:
    if not image_building_s3_enabled():
        return db.get_template_build_upload(owner_client_id, namespace, object_key)

    cfg = get_config()
    key = _s3_object_key(owner_client_id, namespace, object_key)
    try:
        obj = _s3_client().get_object(Bucket=cfg.IMAGE_BUILDING_S3_BUCKET, Key=key)
    except Exception as exc:
        if _s3_not_found(exc):
            return None
        raise
    body = obj["Body"]
    try:
        payload = body.read()
    finally:
        close = getattr(body, "close", None)
        if callable(close):
            close()
    return {
        "owner_client_id": owner_client_id,
        "namespace": namespace,
        "object_key": object_key,
        "content_type": obj.get("ContentType") or "",
        "payload": bytes(payload or b""),
        "metadata": dict(obj.get("Metadata") or {}),
        "storage": "s3",
        "s3_bucket": cfg.IMAGE_BUILDING_S3_BUCKET,
        "s3_key": key,
    }


def template_build_upload_exists(
    db: Any,
    owner_client_id: str,
    namespace: str,
    object_key: str,
) -> bool:
    if not image_building_s3_enabled():
        return bool(db.template_build_upload_exists(owner_client_id, namespace, object_key))

    cfg = get_config()
    key = _s3_object_key(owner_client_id, namespace, object_key)
    try:
        _s3_client().head_object(Bucket=cfg.IMAGE_BUILDING_S3_BUCKET, Key=key)
        return True
    except Exception as exc:
        if _s3_not_found(exc):
            return False
        raise


def _safe_tar_name(name: str) -> str:
    normalized = posixpath.normpath(str(name or "").replace("\\", "/")).lstrip("/")
    if not normalized or normalized == ".":
        raise ValueError("build context archive contains an empty path")
    if normalized.startswith("../") or normalized == "..":
        raise ValueError(f"build context archive path escapes context: {name!r}")
    return normalized


def merge_tar_uploads_to_context_base64(payloads: Iterable[bytes]) -> str | None:
    """Merge uploaded tar/tar.gz archives into one gzip tar build context."""
    archive_payloads = [bytes(payload or b"") for payload in payloads if payload]
    if not archive_payloads:
        return None

    out = io.BytesIO()
    with tarfile.open(fileobj=out, mode="w:gz") as dst:
        for payload in archive_payloads:
            with tarfile.open(fileobj=io.BytesIO(payload), mode="r:*") as src:
                for member in src:
                    original = member
                    info = copy.copy(member)
                    info.name = _safe_tar_name(member.name)
                    if member.isfile():
                        src_file = src.extractfile(original)
                        if src_file is None:
                            continue
                        with src_file:
                            dst.addfile(info, src_file)
                    else:
                        dst.addfile(info)
    return base64.b64encode(out.getvalue()).decode("ascii")


def merged_context_from_uploads(
    db: Any,
    *,
    owner_client_id: str,
    namespace: str,
    object_keys: Iterable[str],
) -> str | None:
    payloads: list[bytes] = []
    missing: list[str] = []
    seen: set[str] = set()
    for object_key in object_keys:
        key = str(object_key or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        row = get_template_build_upload(db, owner_client_id, namespace, key)
        if not row:
            missing.append(key)
            continue
        payload = row.get("payload")
        if not isinstance(payload, (bytes, bytearray)):
            missing.append(key)
            continue
        payloads.append(bytes(payload))
    if missing:
        raise KeyError(", ".join(missing))
    return merge_tar_uploads_to_context_base64(payloads)

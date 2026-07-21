"""Template image availability and recovery for runtime-gateway backed templates."""

from __future__ import annotations

import logging
import re
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

from .envd_template_bake import should_embed_envd_at_template_build
from .gateway_targets import GatewayTarget, target_for_instance
from .runtime_gateway_templates import build_dockerfile_template_via_gateway
from .template_image import resolve_sandbox_image

logger = logging.getLogger(__name__)


def _sanitize_template_image_part(template_id: str) -> str:
    return (re.sub(r"[^a-z0-9._-]+", "-", (template_id or "").strip().lower()).strip("-") or "tpl")[:48]


def _looks_like_generated_template_ref(image_ref: str) -> bool:
    ref = (image_ref or "").strip()
    repo = ref.rsplit(":", 1)[0] if ":" in ref.rsplit("/", 1)[-1] else ref
    name = repo.rsplit("/", 1)[-1]
    return name.startswith(("mysandbox-df-", "mysandbox-snap", "tpl-"))


def _looks_like_explicit_image_ref(image_ref: Optional[str]) -> bool:
    raw = (image_ref or "").strip()
    if not raw:
        return False
    resolved = resolve_sandbox_image(raw)
    if resolved != raw:
        return True
    last = resolved.rsplit("/", 1)[-1]
    return "/" in resolved or ":" in last


class TemplateImageLifecycle:
    """Keeps template image DB refs aligned with live runtime-gateway state."""

    def __init__(self, manager: Any) -> None:
        self.manager = manager

    @property
    def db(self):
        return self.manager.db

    @property
    def config(self):
        return self.manager._config

    def find_gateway_with_image(
        self,
        image_ref: str,
        *,
        preferred_instance_id: Optional[str] = None,
        force_refresh: bool = True,
    ) -> Optional[GatewayTarget]:
        ref = (image_ref or "").strip()
        if not ref:
            return None
        targets = self.manager._gateway_targets()
        if not targets:
            return None
        ordered = list(targets)
        preferred = target_for_instance(ordered, preferred_instance_id or "")
        if preferred is not None:
            ordered = [preferred] + [t for t in ordered if t.instance_id != preferred.instance_id]
        if len(ordered) == 1:
            target = ordered[0]
            return target if self.manager._gateway_has_image(target, ref, force_refresh=force_refresh) else None
        with ThreadPoolExecutor(max_workers=min(8, len(ordered))) as pool:
            future_by_target = {
                pool.submit(self.manager._gateway_has_image, target, ref, force_refresh=force_refresh): target
                for target in ordered
            }
            for future in as_completed(future_by_target):
                target = future_by_target[future]
                try:
                    if bool(future.result()):
                        return target
                except Exception:
                    continue
        return None

    def registry_image_exists(self, target: Optional[GatewayTarget], image_ref: str) -> bool:
        ref = (image_ref or "").strip()
        if not ref:
            return False
        targets = self.manager._gateway_targets()
        ordered: List[GatewayTarget] = []
        if target is not None:
            ordered.append(target)
        ordered.extend(t for t in targets if target is None or t.instance_id != target.instance_id)
        for candidate in ordered:
            try:
                execution = self.manager._execution_for_gateway_target(candidate)
                fn = getattr(execution, "registry_image_exists", None)
                if callable(fn) and bool(fn(ref)):
                    return True
            except Exception:
                continue
        return False

    def push_gateway_image_to_registry(
        self,
        *,
        template_id: str,
        image_ref: str,
        source_target: GatewayTarget,
    ) -> Optional[str]:
        ref = (image_ref or "").strip()
        if not ref:
            return None
        try:
            execution = self.manager._execution_for_gateway_target(source_target)
            push = getattr(execution, "push_image_to_registry", None)
            if not callable(push):
                return None
            registry_ref = str(push(ref, template_id, 600) or "").strip()
            if registry_ref:
                logger.info(
                    "Template image restored to registry template=%s source_gateway=%s local_ref=%s registry_ref=%s",
                    template_id,
                    source_target.instance_id,
                    ref,
                    registry_ref,
                )
                self.manager._record_observability_event(
                    severity="info",
                    category="image",
                    action="registry_push_succeeded",
                    entity_type="template",
                    entity_id=template_id,
                    gateway_instance_id=source_target.instance_id,
                    template_id=template_id,
                    message="Restored template image to registry",
                    metadata={"local_ref": ref, "registry_ref": registry_ref},
                )
                return registry_ref
        except Exception as ex:  # noqa: BLE001
            logger.warning(
                "Template image registry restore failed template=%s source_gateway=%s image=%s: %s",
                template_id,
                source_target.instance_id,
                ref,
                ex,
            )
            self.manager._record_observability_event(
                severity="error",
                category="image",
                action="registry_push_failed",
                entity_type="template",
                entity_id=template_id,
                gateway_instance_id=source_target.instance_id,
                template_id=template_id,
                message="Template image registry restore failed",
                metadata={"local_ref": ref, "error": str(ex)},
            )
        return None

    def image_exists_for_row(self, row: Optional[Dict[str, Any]], image_ref: str) -> bool:
        ref = (image_ref or "").strip()
        if not ref:
            return False
        registry_ref = str((row or {}).get("registry_image_ref") or "").strip()
        if registry_ref and ref == registry_ref:
            return self.registry_image_exists(self.manager._gateway_target_for_template_row(row), ref)
        if self.manager.execution.get_backend_kind() in ("docker", "gvisor"):
            owner = str((row or {}).get("materialized_gateway_instance_id") or "").strip()
            return self.find_gateway_with_image(ref, preferred_instance_id=owner or None, force_refresh=True) is not None
        execution = self.manager.execution
        owner_target = self.manager._gateway_target_for_template_row(row)
        if owner_target is not None:
            execution = self.manager._execution_for_gateway_target(owner_target)
        fn = getattr(execution, "image_exists", None)
        if callable(fn):
            try:
                return bool(fn(ref))
            except Exception:
                return False
        return True

    def _build_target_for_row(self, row: Dict[str, Any]) -> Optional[GatewayTarget]:
        target = self.manager._gateway_target_for_template_row(row)
        if target is not None:
            return target
        return self.manager._best_gateway_by_load(
            self.manager._gateway_targets(),
            force_refresh=True,
            preferred_image_ref=str(row.get("warm_snapshot_image") or row.get("registry_image_ref") or ""),
        )

    def _record_rebuild_unavailable(
        self,
        template_id: str,
        *,
        warm_ref: str,
        base_image: str,
        message: str,
    ) -> None:
        self.db.set_template_build_error(template_id, message)
        self.manager._record_observability_event(
            severity="error",
            category="image",
            action="rebuild_unavailable",
            entity_type="template",
            entity_id=template_id,
            template_id=template_id,
            message="Template image missing and rebuild source is unavailable",
            metadata={"warm_ref": warm_ref, "base_image": base_image},
        )

    def _repair_missing_base_image_snapshot(
        self,
        template_id: str,
        row: Dict[str, Any],
        *,
        warm_ref: str,
        base_image: str,
    ) -> Optional[str]:
        if not base_image or not _looks_like_explicit_image_ref(base_image):
            self._record_rebuild_unavailable(
                template_id,
                warm_ref=warm_ref,
                base_image=base_image,
                message=(
                    "Template image missing from runtime and base image rebuild source "
                    f"is unavailable for {warm_ref or base_image or template_id}. Rebuild the template."
                ),
            )
            return None
        build = getattr(self.manager, "_build_registered_template_snapshot", None)
        if not callable(build):
            self._record_rebuild_unavailable(
                template_id,
                warm_ref=warm_ref,
                base_image=base_image,
                message="Template image missing from runtime and snapshot rebuild is unavailable.",
            )
            return None
        registry_ref = str(row.get("registry_image_ref") or "").strip()
        if warm_ref or registry_ref:
            missing_ref = warm_ref or registry_ref
            self.db.set_template_image_refs(
                template_id,
                warm_snapshot_image=None,
                registry_image_ref=None,
                materialized_gateway_instance_id=None,
                build_error=f"template image unavailable: {missing_ref}; rebuild required",
            )
            row = self.db.get_sandbox_template(template_id) or row
        target = self._build_target_for_row(row)
        try:
            ok = bool(build(template_id))
        except Exception as ex:  # noqa: BLE001
            self.db.set_template_build_error(template_id, str(ex))
            self.manager._record_observability_event(
                severity="error",
                category="image",
                action="rebuild_failed",
                entity_type="template",
                entity_id=template_id,
                gateway_instance_id=target.instance_id if target else None,
                template_id=template_id,
                message="Template image snapshot rebuild failed",
                metadata={"error": str(ex), "base_image": base_image, "build_mode": "snapshot"},
            )
            return None
        refreshed = self.db.get_sandbox_template(template_id) or row
        rebuilt = str(refreshed.get("warm_snapshot_image") or refreshed.get("registry_image_ref") or "").strip()
        if not ok or not rebuilt:
            error = str(refreshed.get("build_error") or "template snapshot rebuild failed").strip()
            self.db.set_template_build_error(template_id, error)
            self.manager._record_observability_event(
                severity="error",
                category="image",
                action="rebuild_failed",
                entity_type="template",
                entity_id=template_id,
                gateway_instance_id=target.instance_id if target else None,
                template_id=template_id,
                message="Template image snapshot rebuild failed",
                metadata={"error": error, "base_image": base_image, "build_mode": "snapshot"},
            )
            return None
        registry_ref = str(refreshed.get("registry_image_ref") or "").strip()
        gateway_instance_id = str(refreshed.get("materialized_gateway_instance_id") or "").strip()
        logger.info(
            "Template %s rebuilt missing base-image snapshot: %s registry=%s",
            template_id,
            rebuilt,
            registry_ref or "-",
        )
        self.manager._record_observability_event(
            severity="info",
            category="image",
            action="rebuild_succeeded",
            entity_type="template",
            entity_id=template_id,
            gateway_instance_id=gateway_instance_id or (target.instance_id if target else None),
            template_id=template_id,
            message="Rebuilt missing template image from base image snapshot",
            metadata={"image_ref": rebuilt, "registry_ref": registry_ref, "base_image": base_image, "build_mode": "snapshot"},
        )
        return rebuilt

    def repair_missing_image(self, template_id: str, row: Dict[str, Any]) -> Optional[str]:
        source_kind = (row.get("source_kind") or "").strip().lower()
        warm_ref = (row.get("warm_snapshot_image") or "").strip()
        base_image = (row.get("base_image") or "").strip()
        if source_kind != "dockerfile":
            return self._repair_missing_base_image_snapshot(
                template_id,
                row,
                warm_ref=warm_ref,
                base_image=base_image,
            )
        dockerfile = str(row.get("dockerfile_text") or "")
        if not dockerfile:
            self._record_rebuild_unavailable(
                template_id,
                warm_ref=warm_ref,
                base_image=base_image,
                message=(
                    "Template image missing from runtime and rebuild source is unavailable "
                    f"for {warm_ref or base_image}. Rebuild the template."
                ),
            )
            return None
        build_mode = (row.get("source_build_mode") or "docker_cli").strip() or "docker_cli"
        image_tag = warm_ref or None
        if not image_tag and base_image and _looks_like_generated_template_ref(base_image):
            image_tag = base_image
        if not image_tag:
            image_tag = f"mysandbox-df-{_sanitize_template_image_part(template_id)}:{uuid.uuid4().hex[:12]}"
        target = self._build_target_for_row(row)
        try:
            result = build_dockerfile_template_via_gateway(
                self.config,
                template_id=template_id,
                dockerfile=dockerfile,
                image_tag=image_tag,
                build_args=dict(row.get("build_args") or {}),
                context_tar_gzip_base64=(row.get("context_tar_gzip_base64") or None),
                build_mode=build_mode,
                embed_envd=bool(getattr(self.config, "ENVD_EMBED_AT_TEMPLATE_BUILD", True)),
                gateway_api_base=(target.api_base if target else None),
            )
        except RuntimeError as ex:
            self.db.set_template_build_error(template_id, str(ex))
            self.manager._record_observability_event(
                severity="error",
                category="image",
                action="rebuild_failed",
                entity_type="template",
                entity_id=template_id,
                gateway_instance_id=target.instance_id if target else None,
                template_id=template_id,
                message="Template image rebuild failed",
                metadata={"error": str(ex), "image_tag": image_tag or "", "build_mode": build_mode},
            )
            return None
        rebuilt = str(result.get("image_tag") or image_tag or "").strip()
        registry_ref = str(result.get("registry_image_ref") or "").strip()
        gateway_instance_id = str(result.get("gateway_instance_id") or "").strip()
        if not rebuilt:
            self.db.set_template_build_error(template_id, "runtime-gateway rebuild produced no image tag")
            self.manager._record_observability_event(
                severity="error",
                category="image",
                action="rebuild_failed",
                entity_type="template",
                entity_id=template_id,
                gateway_instance_id=target.instance_id if target else None,
                template_id=template_id,
                message="Runtime-gateway rebuild produced no image tag",
                metadata={"image_tag": image_tag or "", "build_mode": build_mode},
            )
            return None
        self.db.set_template_warm_snapshot(
            template_id,
            rebuilt,
            None,
            registry_image_ref=registry_ref or None,
            materialized_gateway_instance_id=gateway_instance_id or None,
        )
        if should_embed_envd_at_template_build(self.config):
            self.manager._mark_template_envd_baked(template_id)
        logger.info("Template %s rebuilt missing runtime image: %s registry=%s", template_id, rebuilt, registry_ref or "-")
        self.manager._record_observability_event(
            severity="info",
            category="image",
            action="rebuild_succeeded",
            entity_type="template",
            entity_id=template_id,
            gateway_instance_id=gateway_instance_id or (target.instance_id if target else None),
            template_id=template_id,
            message="Rebuilt missing template image",
            metadata={"image_ref": rebuilt, "registry_ref": registry_ref, "build_mode": build_mode},
        )
        self.manager.sync_warm_pool_default_segment(template_id, rebuilt)
        return rebuilt

    def ensure(self, template_id: str, row: Dict[str, Any], *, verify_live: bool = True) -> Dict[str, Any]:
        warm_ref = (row.get("warm_snapshot_image") or "").strip()
        registry_ref = (row.get("registry_image_ref") or "").strip()
        owner_instance = (row.get("materialized_gateway_instance_id") or "").strip()
        source_kind = (row.get("source_kind") or "").strip().lower()

        if registry_ref and not verify_live:
            return row

        owner_target = target_for_instance(self.manager._gateway_targets(), owner_instance)
        local_target = (
            self.find_gateway_with_image(
                warm_ref,
                preferred_instance_id=owner_instance or None,
                force_refresh=True,
            )
            if warm_ref
            else None
        )

        if registry_ref and self.registry_image_exists(owner_target or local_target, registry_ref):
            if local_target is None and warm_ref:
                self.db.set_template_image_refs(
                    template_id,
                    warm_snapshot_image=None,
                    registry_image_ref=registry_ref,
                    materialized_gateway_instance_id=None,
                    build_error=None,
                )
                logger.warning(
                    "Template %s warm image %s missing from live gateways; registry image still available: %s",
                    template_id,
                    warm_ref,
                    registry_ref,
                )
                self.manager._record_observability_event(
                    severity="warning",
                    category="image",
                    action="warm_missing_registry_available",
                    entity_type="template",
                    entity_id=template_id,
                    template_id=template_id,
                    message="Warm image is missing from live gateways but registry image is available",
                    metadata={"warm_ref": warm_ref, "registry_ref": registry_ref},
                )
                return self.db.get_sandbox_template(template_id) or row
            if local_target is not None and local_target.instance_id != owner_instance:
                self.db.set_template_image_refs(
                    template_id,
                    warm_snapshot_image=warm_ref,
                    registry_image_ref=registry_ref,
                    materialized_gateway_instance_id=local_target.instance_id,
                    build_error=None,
                )
                return self.db.get_sandbox_template(template_id) or row
            return self.db.get_sandbox_template(template_id) or row

        if local_target is not None:
            restored_ref = self.push_gateway_image_to_registry(
                template_id=template_id,
                image_ref=warm_ref,
                source_target=local_target,
            )
            self.db.set_template_image_refs(
                template_id,
                warm_snapshot_image=warm_ref,
                registry_image_ref=restored_ref,
                materialized_gateway_instance_id=local_target.instance_id,
                build_error=None if restored_ref else "registry image unavailable; warm image exists only on one runtime shard",
            )
            return self.db.get_sandbox_template(template_id) or row

        repair_attempted = False
        missing_ref = warm_ref or registry_ref
        if missing_ref:
            logger.warning(
                "Template %s image unavailable warm=%s registry=%s; attempting rebuild",
                template_id,
                warm_ref or "-",
                registry_ref or "-",
            )
            self.db.set_template_image_refs(
                template_id,
                warm_snapshot_image=None,
                registry_image_ref=None,
                materialized_gateway_instance_id=None,
                build_error=f"template image unavailable: {missing_ref}; rebuild required",
            )
            self.manager._record_observability_event(
                severity="error",
                category="image",
                action="image_missing",
                entity_type="template",
                entity_id=template_id,
                template_id=template_id,
                message="Template image is unavailable from live gateways and registry",
                metadata={"warm_ref": warm_ref, "registry_ref": registry_ref, "missing_ref": missing_ref},
            )
            row = self.db.get_sandbox_template(template_id) or row
            repair_attempted = True
            rebuilt = self.repair_missing_image(template_id, row)
            if rebuilt:
                return self.db.get_sandbox_template(template_id) or row
        if (
            not repair_attempted
            and source_kind == "dockerfile"
            and str(row.get("dockerfile_text") or "").strip()
        ):
            logger.warning(
                "Template %s has no live image refs but has stored Dockerfile source; attempting rebuild",
                template_id,
            )
            rebuilt = self.repair_missing_image(template_id, row)
            if rebuilt:
                return self.db.get_sandbox_template(template_id) or row
        if (
            not repair_attempted
            and source_kind != "dockerfile"
            and str(row.get("build_error") or "").strip()
            and str(row.get("base_image") or "").strip()
        ):
            logger.warning(
                "Template %s has no live image refs and a build error; attempting base-image snapshot rebuild",
                template_id,
            )
            rebuilt = self.repair_missing_image(template_id, row)
            if rebuilt:
                return self.db.get_sandbox_template(template_id) or row
        return self.db.get_sandbox_template(template_id) or row

    def image_for_target(
        self,
        *,
        template_id: str,
        row: Optional[Dict[str, Any]],
        requested_image: str,
        target: Optional[GatewayTarget],
    ) -> str:
        image = (requested_image or "").strip()
        if not image or target is None or not row:
            return image
        warm_ref = str(row.get("warm_snapshot_image") or "").strip()
        registry_ref = str(row.get("registry_image_ref") or "").strip()
        owner_instance = str(row.get("materialized_gateway_instance_id") or "").strip()
        if warm_ref and owner_instance and target.instance_id == owner_instance:
            if self.manager._gateway_has_image(
                target,
                warm_ref,
                force_refresh=True,
            ):
                logger.info(
                    "Template %s using local warm image on owner gateway=%s for request-time create: image=%s registry=%s",
                    template_id,
                    target.instance_id,
                    warm_ref,
                    registry_ref or "-",
                )
                return warm_ref
        if image == registry_ref:
            return image
        if registry_ref:
            logger.info(
                "Template %s using registry image on gateway=%s for request-time create: requested=%s registry=%s",
                template_id,
                target.instance_id,
                image or "-",
                registry_ref,
            )
            return registry_ref
        if image and self.manager._gateway_has_image(target, image, force_refresh=True):
            return image
        if registry_ref and registry_ref != image and self.registry_image_exists(target, registry_ref):
            logger.info(
                "Template %s using registry image on gateway=%s because local warm image is absent: warm=%s registry=%s",
                template_id,
                target.instance_id,
                warm_ref or image,
                registry_ref,
            )
            return registry_ref
        if warm_ref:
            local_target = self.find_gateway_with_image(
                warm_ref,
                preferred_instance_id=str(row.get("materialized_gateway_instance_id") or "").strip() or None,
                force_refresh=True,
            )
            if local_target is not None:
                restored_ref = self.push_gateway_image_to_registry(
                    template_id=template_id,
                    image_ref=warm_ref,
                    source_target=local_target,
                )
                if restored_ref:
                    self.db.set_template_image_refs(
                        template_id,
                        warm_snapshot_image=warm_ref,
                        registry_image_ref=restored_ref,
                        materialized_gateway_instance_id=local_target.instance_id,
                        build_error=None,
                    )
                    return restored_ref
        return image

    def reconcile(self, limit: int = 200) -> Dict[str, int]:
        stats = {"checked": 0, "changed": 0, "errors": 0}
        if self.manager.execution.get_backend_kind() not in ("docker", "gvisor"):
            return stats
        list_templates = getattr(self.db, "list_all_sandbox_templates", None)
        rows = list_templates(limit=max(1, int(limit))) if callable(list_templates) else self.db.list_sandbox_templates()
        for row in rows:
            template_id = str(row.get("template_id") or "").strip()
            if not template_id:
                continue
            has_refs = bool((str(row.get("warm_snapshot_image") or "").strip() or str(row.get("registry_image_ref") or "").strip()))
            has_rebuild_source = (
                (row.get("source_kind") or "").strip().lower() == "dockerfile"
                and bool(str(row.get("dockerfile_text") or "").strip())
            )
            if not (has_refs or has_rebuild_source):
                continue
            before = (
                str(row.get("warm_snapshot_image") or "").strip(),
                str(row.get("registry_image_ref") or "").strip(),
                str(row.get("materialized_gateway_instance_id") or "").strip(),
                str(row.get("build_error") or "").strip(),
            )
            try:
                stats["checked"] += 1
                updated = self.ensure(template_id, row)
                after = (
                    str(updated.get("warm_snapshot_image") or "").strip(),
                    str(updated.get("registry_image_ref") or "").strip(),
                    str(updated.get("materialized_gateway_instance_id") or "").strip(),
                    str(updated.get("build_error") or "").strip(),
                )
                if after != before:
                    stats["changed"] += 1
                    self.manager._record_observability_event(
                        severity="warning" if str(after[3] or "").strip() else "info",
                        category="reconcile",
                        action="template_image_changed",
                        entity_type="template",
                        entity_id=template_id,
                        template_id=template_id,
                        message="Template image availability reconciliation changed template image state",
                        metadata={"before": list(before), "after": list(after)},
                    )
            except Exception as ex:  # noqa: BLE001
                stats["errors"] += 1
                logger.warning("Template image availability reconcile failed template=%s: %s", template_id, ex)
                self.manager._record_observability_event(
                    severity="error",
                    category="reconcile",
                    action="template_image_reconcile_error",
                    entity_type="template",
                    entity_id=template_id,
                    template_id=template_id,
                    message="Template image availability reconcile failed",
                    metadata={"error": str(ex)},
                )
        return stats

"""Sandbox creation path and cold-create placement flow."""

from __future__ import annotations

import logging
import secrets
import time
import uuid
from typing import Any, Dict, Optional

from .container_manager import ContainerConfig
from .gateway_targets import GatewayTarget, target_for_instance
from .guest_ports import resolve_guest_ports
from .runtime_utils import is_container_like_execution
from .envd_template_bake import should_embed_envd_at_template_build
from .template_image import resolve_sandbox_image

logger = logging.getLogger(__name__)

def _create_env_from_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, str]:
    """Merge SDK ``metadata.e2b_shim_envs`` / ``metadata.env`` into pod environment."""
    md = metadata or {}
    out: Dict[str, str] = {}
    for key in ("e2b_shim_envs", "env"):
        block = md.get(key)
        if not isinstance(block, dict):
            continue
        for k, v in block.items():
            if k and v is not None:
                out[str(k)] = str(v)
    return out

def _resolve_sandbox_image(template_id: Optional[str]) -> str:
    return resolve_sandbox_image(template_id)

def _looks_like_explicit_image_ref(template_id: Optional[str]) -> bool:
    """True for raw image refs; false for friendly aliases that must be registered."""
    raw = (template_id or "").strip()
    if not raw:
        return True
    resolved = _resolve_sandbox_image(raw)
    if resolved != raw:
        return True
    last = resolved.rsplit("/", 1)[-1]
    return "/" in resolved or ":" in last


class SandboxCreationOpsMixin:
    def _apply_requested_warm_pool_size(
        self,
        pool: Any,
        *,
        template_id: str,
        cpu_limit: str,
        memory_limit: str,
        timeout: int,
        from_snapshot_image: Optional[str],
        desired_size: int,
        preferred_gateway_instance_id: Optional[str] = None,
    ) -> None:
        set_desired_size = getattr(pool, "set_desired_size", None)
        if callable(set_desired_size):
            set_desired_size(
                template_id,
                cpu_limit,
                memory_limit,
                int(timeout),
                from_snapshot_image,
                int(desired_size),
                preferred_gateway_instance_id=preferred_gateway_instance_id,
            )
            return
        self.note_warm_pool_segment(
            template_id=template_id,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            timeout=int(timeout),
            desired_size=int(desired_size),
            preferred_gateway_instance_id=preferred_gateway_instance_id,
        )

    def create_sandbox(
        self,
        template_id: str = "python:3.11",
        metadata: Optional[Dict[str, Any]] = None,
        cpu_limit: str = "1",
        memory_limit: str = "512m",
        timeout: int = 3600,
        from_snapshot_image: Optional[str] = None,
        owner_client_id: Optional[str] = None,
        owner_api_key_id: Optional[str] = None,
        warmpool_size: Optional[int] = None,
    ) -> Optional[str]:
        """Create new sandbox, optionally from a prior ``docker commit`` image or warm pool."""
        self._last_create_error = ""
        requested_warm_pool_size = (
            None if warmpool_size is None else max(0, int(warmpool_size))
        )
        desired_warm_pool_size = (
            requested_warm_pool_size
            if requested_warm_pool_size is not None
            else max(0, int(getattr(self._config, "SANDBOX_WARM_POOL_SIZE", 0) or 0))
        )
        snap = (from_snapshot_image or "").strip()
        if snap:
            return self._create_sandbox_fresh(
                template_id=template_id,
                metadata=metadata,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                timeout=timeout,
                from_snapshot_image=snap,
                owner_client_id=owner_client_id,
                owner_api_key_id=owner_api_key_id,
            )

        tid = (template_id or "").strip()
        tpl = self.db.get_sandbox_template(tid) if tid else None
        tid, tpl = self._resolve_template_alias_for_create(tid, tpl, owner_client_id)

        pool = getattr(self, "warm_pool", None)

        # With warm pool enabled, bare Docker image refs (no prior POST /templates row) are
        # auto-registered so the first create can build warm_snapshot_image + ensure_pool_for.
        # Skip the configured default pool template_id so MultiWarm.start()'s base-image segment
        # is not replaced by a conflicting registered-template path for the same key.
        warm_pool_default_tid = (
            self._config.SANDBOX_WARM_POOL_TEMPLATE_ID or self._config.DEFAULT_TEMPLATE
        ).strip()
        if (
            tid
            and tpl is None
            and desired_warm_pool_size > 0
            and tid != warm_pool_default_tid
        ):
            if not _looks_like_explicit_image_ref(tid):
                self._last_create_error = (
                    f"Unknown template alias {tid!r}. Build/register this template first, "
                    "or pass an explicit Docker image ref such as 'python:3.11' or 'registry/repo:tag'."
                )
                logger.warning(self._last_create_error)
                return None
            base_image = _resolve_sandbox_image(tid)
            self.db.upsert_sandbox_template(
                tid,
                base_image,
                {},
                "",
                20,
                "",
            )
            tpl = self.db.get_sandbox_template(tid)
            logger.info(
                "Auto-registered logical template template_id=%r base_image=%r (warm pool)",
                tid,
                base_image,
            )

        # First use of the configured default template (e.g. python:3.11): register + warm snapshot so
        # ``ENVD_EMBED_AT_TEMPLATE_BUILD`` can bake envd into the committed image without a prior POST /templates.
        if (
            tid
            and tpl is None
            and should_embed_envd_at_template_build(self._config)
            and is_container_like_execution(self.execution)
            and self.execution.get_backend_kind() in ("docker", "gvisor")
            and tid == (self._config.DEFAULT_TEMPLATE or "").strip()
        ):
            base_image = _resolve_sandbox_image(tid)
            self.db.upsert_sandbox_template(
                tid,
                base_image,
                {},
                "",
                20,
                "",
            )
            tpl = self.db.get_sandbox_template(tid)
            logger.info(
                "Auto-registered default template_id=%r base_image=%r (envd template embed)",
                tid,
                base_image,
            )

        if tpl:
            warm_ref_existing = (tpl.get("warm_snapshot_image") or tpl.get("registry_image_ref") or "").strip()
            base_image_existing = (tpl.get("base_image") or "").strip()
            if (
                not warm_ref_existing
                and base_image_existing
                and base_image_existing == tid
                and not _looks_like_explicit_image_ref(base_image_existing)
            ):
                self._last_create_error = (
                    f"Template alias {tid!r} is registered without a materialized image and has invalid "
                    f"base_image {base_image_existing!r}. Rebuild the template with a valid base image, "
                    "or create using the correct materialized template alias."
                )
                self.db.set_template_build_error(tid, self._last_create_error)
                logger.warning(self._last_create_error)
                return None
            if (
                pool is not None
                and requested_warm_pool_size is None
                and desired_warm_pool_size > 0
                and warm_ref_existing
            ):
                warm_key_existing = self.warm_pool_key(tid, cpu_limit, memory_limit, int(timeout))
                if self.warm_pool_ready_count(warm_key_existing) > 0:
                    sid = pool.try_acquire(
                        tid,
                        metadata,
                        cpu_limit,
                        memory_limit,
                        int(timeout),
                        owner_client_id=owner_client_id,
                        owner_api_key_id=owner_api_key_id,
                        wait_for_ready=False,
                    )
                    if sid:
                        return sid
            tpl = self._ensure_template_runtime_image(tid, tpl)
            if not (tpl.get("warm_snapshot_image") or tpl.get("registry_image_ref")):
                if (tpl.get("source_kind") or "").strip().lower() == "dockerfile":
                    self._last_create_error = str(
                        tpl.get("build_error")
                        or f"Template {tid} has stored Dockerfile source but could not be rebuilt"
                    )
                    return None
                if not self._build_registered_template_snapshot(tid):
                    self._last_create_error = str(
                        (self.db.get_sandbox_template(tid) or {}).get("build_error")
                        or f"Template {tid} could not be materialized"
                    )
                    return None
                tpl = self.db.get_sandbox_template(tid) or tpl
            warm_img = (tpl.get("warm_snapshot_image") or tpl.get("registry_image_ref") or "").strip()
            # Parsed Dockerfile builds store the committed OCI ref as ``base_image`` and should mirror
            # ``warm_snapshot_image``. If the snapshot column is still empty (e.g. failed UPDATE),
            # using ``_resolve_sandbox_image(template_id)`` would start ``newp1`` as a literal image
            # name while **env** still comes from the template row — looks like "right env, no /app".
            if not warm_img:
                bi = (tpl.get("base_image") or "").strip()
                if bi and bi != tid and (":" in bi or "/" in bi):
                    warm_img = bi
                    logger.warning(
                        "Template %r: warm_snapshot_image missing; using base_image %r for sandbox image",
                        tid,
                        bi,
                    )
            if warm_img and should_embed_envd_at_template_build(self._config) and not self._template_declares_envd_baked(tid):
                self._mark_template_envd_baked(tid)
                tpl = self.db.get_sandbox_template(tid) or tpl
            cfg = self._config
            first_pool_request = False
            seed_target: Optional[GatewayTarget] = None
            pool_managed_template = bool(pool is not None and desired_warm_pool_size > 0 and warm_img)
            warm_key = ""
            if pool is not None and warm_img and desired_warm_pool_size <= 0 and requested_warm_pool_size is not None:
                self._apply_requested_warm_pool_size(
                    pool,
                    template_id=tid,
                    cpu_limit=cpu_limit,
                    memory_limit=memory_limit,
                    timeout=int(timeout),
                    from_snapshot_image=warm_img,
                    desired_size=0,
                )
            if pool_managed_template:
                warm_key = self.warm_pool_key(tid, cpu_limit, memory_limit, int(timeout))
                valid_ready_count = self.warm_pool_ready_count(warm_key)
                first_pool_request = valid_ready_count <= 0
                if first_pool_request and self.execution.get_backend_kind() in ("docker", "gvisor"):
                    trust_gateway_targets = self._runtime_gateway_targets_authoritative()
                    seed_target = self._select_gateway_target_for_pool(
                        template_id=tid,
                        cpu_limit=cpu_limit,
                        memory_limit=memory_limit,
                        timeout=int(timeout),
                        template_row=tpl,
                        force_refresh=not trust_gateway_targets,
                        require_reachable=not trust_gateway_targets,
                    )
                    if seed_target is not None:
                        self.note_warm_pool_segment(
                            template_id=tid,
                            cpu_limit=cpu_limit,
                            memory_limit=memory_limit,
                            timeout=int(timeout),
                            desired_size=int(desired_warm_pool_size),
                            preferred_gateway_instance_id=seed_target.instance_id,
                        )
                if not first_pool_request:
                    if requested_warm_pool_size is not None:
                        self._apply_requested_warm_pool_size(
                            pool,
                            template_id=tid,
                            cpu_limit=cpu_limit,
                            memory_limit=memory_limit,
                            timeout=int(timeout),
                            from_snapshot_image=warm_img,
                            desired_size=int(desired_warm_pool_size),
                        )
                    else:
                        pool.ensure_pool_for(
                            tid,
                            cpu_limit,
                            memory_limit,
                            int(timeout),
                            warm_img,
                            desired_size=int(desired_warm_pool_size),
                        )
            if pool_managed_template and not first_pool_request:
                wait_for_warm_pool = requested_warm_pool_size is None
                sid = pool.try_acquire(
                    tid,
                    metadata,
                    cpu_limit,
                    memory_limit,
                    int(timeout),
                    owner_client_id=owner_client_id,
                    owner_api_key_id=owner_api_key_id,
                    wait_for_ready=wait_for_warm_pool,
                )
                if sid:
                    return sid
                if not wait_for_warm_pool:
                    logger.info(
                        "Warm pool empty for template %r after desired_size=%s update; falling back to cold create",
                        tid,
                        desired_warm_pool_size,
                    )
                else:
                    wait_sec = float(getattr(cfg, "SANDBOX_WARM_POOL_ACQUIRE_WAIT_SEC", 0.0) or 0.0)
                    segment_after = self.db.get_warm_pool_segment(warm_key) if warm_key else None
                    err = str((segment_after or {}).get("last_error") or "").strip()
                    logger.info(
                        "Warm pool unavailable for template %r after %.1fs; falling back to cold create%s",
                        tid,
                        wait_sec,
                        f": {err}" if err else "",
                    )
            sid = self._create_sandbox_fresh(
                template_id=tid,
                metadata=metadata,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                timeout=timeout,
                from_snapshot_image=warm_img or None,
                owner_client_id=owner_client_id,
                owner_api_key_id=owner_api_key_id,
                forced_gateway_instance_id=seed_target.instance_id if seed_target is not None else None,
            )
            if sid and pool_managed_template and first_pool_request:
                if requested_warm_pool_size is not None:
                    self._apply_requested_warm_pool_size(
                        pool,
                        template_id=tid,
                        cpu_limit=cpu_limit,
                        memory_limit=memory_limit,
                        timeout=int(timeout),
                        from_snapshot_image=warm_img,
                        desired_size=int(desired_warm_pool_size),
                        preferred_gateway_instance_id=(
                            seed_target.instance_id if seed_target is not None else None
                        ),
                    )
                else:
                    pool.ensure_pool_for(
                        tid,
                        cpu_limit,
                        memory_limit,
                        int(timeout),
                        warm_img,
                        desired_size=int(desired_warm_pool_size),
                    )
            return sid

        if pool is not None and requested_warm_pool_size is not None and desired_warm_pool_size <= 0:
            self._apply_requested_warm_pool_size(
                pool,
                template_id=tid,
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                timeout=int(timeout),
                from_snapshot_image=None,
                desired_size=0,
            )

        if pool is not None and desired_warm_pool_size > 0:
            sid = pool.try_acquire(
                tid,
                metadata,
                cpu_limit,
                memory_limit,
                int(timeout),
                owner_client_id=owner_client_id,
                owner_api_key_id=owner_api_key_id,
                wait_for_ready=requested_warm_pool_size is None,
            )
            if sid:
                return sid
        return self._create_sandbox_fresh(
            template_id=tid,
            metadata=metadata,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            timeout=timeout,
            from_snapshot_image=None,
            owner_client_id=owner_client_id,
            owner_api_key_id=owner_api_key_id,
        )

    def _create_sandbox_fresh(
        self,
        template_id: str = "python:3.11",
        metadata: Optional[Dict[str, Any]] = None,
        cpu_limit: str = "1",
        memory_limit: str = "512m",
        timeout: int = 3600,
        from_snapshot_image: Optional[str] = None,
        owner_client_id: Optional[str] = None,
        owner_api_key_id: Optional[str] = None,
        is_warm_pool: bool = False,
        warm_pool_key: Optional[str] = None,
        forced_gateway_instance_id: Optional[str] = None,
    ) -> Optional[str]:
        """Create a brand-new sandbox (never taken from the warm pool)."""
        create_started = time.monotonic()
        sandbox_id = f"sb-{uuid.uuid4().hex[:22]}"
        container_name = f"sandbox-{sandbox_id}"

        meta = dict(metadata or {})
        allow_pt = meta.pop("allow_public_traffic", None)
        network = meta.pop("network", None)
        if allow_pt is None and isinstance(network, dict) and "allow_public_traffic" in network:
            allow_pt = network.get("allow_public_traffic")
        if allow_pt is None:
            allow_pt = getattr(self._config, "SANDBOX_DEFAULT_ALLOW_PUBLIC_TRAFFIC", False)
        meta["allow_public_traffic"] = bool(allow_pt)
        meta["sandbox_allocation_source"] = "warm_pool_provision" if is_warm_pool else "cold_create"
        metadata = meta

        snap = (from_snapshot_image or "").strip()
        if snap:
            image = snap
        else:
            image = _resolve_sandbox_image(template_id)
        tpl = self.db.get_sandbox_template((template_id or "").strip()) if template_id else None
        runtime = self.execution.get_backend_kind()
        forced_target = None
        if runtime in ("docker", "gvisor") and forced_gateway_instance_id:
            forced_target = target_for_instance(self._gateway_targets(), forced_gateway_instance_id)
        if runtime in ("docker", "gvisor") and forced_target is not None:
            chosen_target = forced_target
        elif runtime in ("docker", "gvisor") and is_warm_pool:
            chosen_target = self._select_gateway_target_for_pool(
                template_id=(template_id or "").strip(),
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                timeout=int(timeout),
                template_row=tpl,
            )
        elif runtime in ("docker", "gvisor") and int(getattr(self._config, "SANDBOX_WARM_POOL_SIZE", 0) or 0) > 0:
            trust_gateway_targets = self._runtime_gateway_targets_authoritative()
            chosen_target = self._select_gateway_target_for_pool(
                template_id=(template_id or "").strip(),
                cpu_limit=cpu_limit,
                memory_limit=memory_limit,
                timeout=int(timeout),
                template_row=tpl,
                force_refresh=not trust_gateway_targets,
                require_reachable=not trust_gateway_targets,
            )
        elif runtime in ("docker", "gvisor") and tpl:
            chosen_target = self._gateway_target_for_template_row(tpl)
        elif runtime in ("docker", "gvisor"):
            chosen_target = self._best_gateway_by_load(
                self._gateway_targets(),
                force_refresh=True,
                preferred_image_ref=image,
            )
        else:
            chosen_target = None
        if runtime in ("docker", "gvisor") and chosen_target is None:
            self._last_create_error = "No reachable runtime-gateway shard is available for sandbox placement"
            logger.error(self._last_create_error)
            self._record_observability_event(
                severity="error",
                category="sandbox",
                action="create_failed",
                entity_type="sandbox",
                entity_id=sandbox_id,
                sandbox_id=sandbox_id,
                template_id=template_id,
                message=self._last_create_error,
                metadata={
                    "runtime": runtime,
                    "image": image,
                    "allocation_source": metadata.get("sandbox_allocation_source"),
                },
            )
            return None
        if runtime in ("docker", "gvisor") and tpl and chosen_target is not None:
            image = self._image_for_gateway_target(
                template_id=(template_id or "").strip(),
                row=tpl,
                requested_image=image,
                target=chosen_target,
            )
        execution = self._execution_for_gateway_target(chosen_target) if chosen_target else self.execution
        logger.info(
            "Creating sandbox %s runtime=%s template_id=%r image=%r (from_snapshot=%s gateway=%s)",
            sandbox_id,
            runtime,
            template_id,
            image,
            bool(snap),
            chosen_target.instance_id if chosen_target else "-",
        )

        env_for_create: Optional[Dict[str, str]] = None
        if tpl:
            ev = dict(tpl.get("env") or {})
            if ev:
                env_for_create = ev

        meta_env = _create_env_from_metadata(metadata)
        if meta_env:
            env_for_create = {**(env_for_create or {}), **meta_env}

        envd_port_cfg = max(1, min(65535, int(getattr(self._config, "ENVD_PORT", 49983))))
        is_container_backend = is_container_like_execution(self.execution)
        envd_always = is_container_backend and bool(getattr(self._config, "ENVD_ALWAYS_ON", True))
        publish_envd_legacy = (
            is_container_backend
            and bool(getattr(self._config, "ENVD_PUBLISH_PORT", False))
        )
        publish_envd = publish_envd_legacy
        start_envd = envd_always or publish_envd_legacy
        auto_start_envd = start_envd and bool(getattr(self._config, "ENVD_AUTO_START", True))

        guest_ports = resolve_guest_ports(
            metadata=metadata,
            template_row=tpl,
            config=self._config,
            include_envd=start_envd,
        )
        metadata = {**(metadata or {}), "guest_ports": guest_ports}

        envd_token: Optional[str] = None
        traffic_token: Optional[str] = None
        if start_envd:
            envd_token = secrets.token_urlsafe(32)
            if env_for_create is None:
                env_for_create = {}
            env_for_create = {**env_for_create, "ENVD_ACCESS_TOKEN": envd_token}
        from orchestrator.sandbox_connections import data_plane_enabled_for_config

        if is_container_backend and data_plane_enabled_for_config(self._config):
            traffic_token = secrets.token_urlsafe(32)

        startup_boot = None
        if is_container_like_execution(self.execution):
            startup_boot = self._startup_managed_bootstrap_spec(
                template_id,
                start_envd=auto_start_envd,
                envd_port=envd_port_cfg,
                execution=execution,
            )

        config = ContainerConfig(
            image=image,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            timeout=timeout,
            environment=env_for_create,
            guest_ports=guest_ports,
            publish_envd_port=publish_envd,
            envd_port=envd_port_cfg,
            startup_command=list(startup_boot.get("startup_command") or []) if startup_boot else None,
        )

        container_id = execution.create_container(container_name, config)
        if not container_id:
            logger.error("Failed to create workload for sandbox %s", sandbox_id)
            self._record_observability_event(
                severity="error",
                category="sandbox",
                action="create_failed",
                entity_type="sandbox",
                entity_id=sandbox_id,
                sandbox_id=sandbox_id,
                gateway_instance_id=chosen_target.instance_id if chosen_target else None,
                template_id=template_id,
                message="Failed to create sandbox workload",
                metadata={
                    "runtime": runtime,
                    "image": image,
                    "allocation_source": metadata.get("sandbox_allocation_source"),
                },
            )
            return None

        self.db.create_sandbox(
            sandbox_id=sandbox_id,
            container_id=container_id,
            template_id=template_id,
            metadata=metadata,
            cpu_limit=cpu_limit,
            memory_limit=memory_limit,
            timeout=timeout,
            runtime=runtime,
            owner_client_id=owner_client_id,
            owner_api_key_id=owner_api_key_id,
            is_warm_pool=bool(is_warm_pool),
            warm_pool_key=warm_pool_key,
            gateway_instance_id=chosen_target.instance_id if chosen_target else None,
            gateway_route_base=chosen_target.route_base if chosen_target else None,
            gateway_api_base=chosen_target.api_base if chosen_target else None,
            gateway_docker_host=None,
            state="starting",
        )

        if start_envd and is_container_backend and envd_token:
            md_envd: Dict[str, Any] = {"envd_access_token": envd_token}
            if traffic_token:
                md_envd["traffic_access_token"] = traffic_token
            if publish_envd:
                ehp = execution.get_container_tcp_host_port(container_id, envd_port_cfg)
                if ehp:
                    md_envd["envd_host_tcp_port"] = int(ehp)
                    hhost = (getattr(self._config, "ENVD_UPSTREAM_HTTP_HOST", None) or "127.0.0.1").strip()
                    logger.info(
                        "Sandbox %s envd legacy publish http://%s:%s/ (host → container :%s)",
                        sandbox_id,
                        hhost or "127.0.0.1",
                        ehp,
                        envd_port_cfg,
                    )
                else:
                    logger.warning(
                        "Sandbox %s: envd port publish requested but no host binding for tcp/%s",
                        sandbox_id,
                        envd_port_cfg,
                    )
            self.db.merge_sandbox_metadata(sandbox_id, md_envd)
        elif traffic_token:
            self.db.merge_sandbox_metadata(sandbox_id, {"traffic_access_token": traffic_token})

        bounded_client_bootstrap = (
            is_container_backend
            and not bool(is_warm_pool)
            and runtime in ("docker", "gvisor")
            and startup_boot is not None
        )
        if bounded_client_bootstrap:
            probes = self._startup_managed_readiness_probes(
                startup_boot,
                auto_start_envd=bool(auto_start_envd),
                envd_port_cfg=envd_port_cfg,
            )
            self.refresh_guest_routing_metadata(sandbox_id)
            wait_budget = float(getattr(self._config, "SANDBOX_COLD_CREATE_READY_WAIT_SEC", 1.0) or 0.0)
            ready = False
            started = time.monotonic()
            if probes and wait_budget > 0.0:
                ready = bool(
                    self._wait_for_gateway_guest_readiness(
                        sandbox_id,
                        container_id,
                        probes,
                        timeout_seconds=wait_budget,
                        warn_on_failure=False,
                    )
                )
            elapsed = round(max(0.0, time.monotonic() - started), 3)
            self.db.merge_sandbox_metadata(
                sandbox_id,
                {
                    "sandbox_bootstrap_pending": not ready,
                    "sandbox_bootstrap_error": "" if ready else "guest bootstrap still starting",
                    "sandbox_bootstrap_seconds": elapsed if ready else None,
                    "sandbox_create_seconds": round(max(0.0, time.monotonic() - create_started), 3),
                },
            )
            self.db.update_sandbox_state(sandbox_id, "running")
            logger.info(
                "Create latency: sandbox=%s source=%s gateway=%s create_seconds=%.3f bootstrap_ready=%s",
                sandbox_id,
                metadata.get("sandbox_allocation_source") or "-",
                chosen_target.instance_id if chosen_target else "-",
                max(0.0, time.monotonic() - create_started),
                ready,
            )
            self._record_observability_event(
                severity="info" if ready else "warning",
                category="sandbox",
                action="create_succeeded",
                entity_type="sandbox",
                entity_id=sandbox_id,
                sandbox_id=sandbox_id,
                gateway_instance_id=chosen_target.instance_id if chosen_target else None,
                template_id=template_id,
                message="Created sandbox" + ("" if ready else " with guest bootstrap still starting"),
                metadata={
                    "runtime": runtime,
                    "image": image,
                    "allocation_source": metadata.get("sandbox_allocation_source"),
                    "create_seconds": round(max(0.0, time.monotonic() - create_started), 3),
                    "bootstrap_ready": bool(ready),
                    "is_warm_pool": bool(is_warm_pool),
                    "warm_pool_key": warm_pool_key or "",
                },
            )
            if not ready:
                self._start_guest_bootstrap_background(sandbox_id, container_id, template_id)
                logger.info(
                    "Sandbox created: %s (guest bootstrap continuing, readiness_budget_seconds=%.3f)",
                    sandbox_id,
                    wait_budget,
                )
            else:
                logger.info("Sandbox created: %s (guest ready within cold-create budget)", sandbox_id)
            return sandbox_id

        if is_container_backend:
            if not self._bootstrap_guest_services(sandbox_id, container_id, template_id):
                logger.error("Sandbox %s bootstrap failed; tearing down workload", sandbox_id)
                self._record_observability_event(
                    severity="error",
                    category="sandbox",
                    action="create_failed",
                    entity_type="sandbox",
                    entity_id=sandbox_id,
                    sandbox_id=sandbox_id,
                    gateway_instance_id=chosen_target.instance_id if chosen_target else None,
                    template_id=template_id,
                    message="Sandbox guest bootstrap failed",
                    metadata={
                        "runtime": runtime,
                        "image": image,
                        "allocation_source": metadata.get("sandbox_allocation_source"),
                        "is_warm_pool": bool(is_warm_pool),
                        "warm_pool_key": warm_pool_key or "",
                    },
                )
                self.kill_sandbox(sandbox_id, force=True)
                return None
            if startup_boot is None:
                self.refresh_guest_routing_metadata(sandbox_id)
        else:
            self.refresh_guest_routing_metadata(sandbox_id)

        create_seconds = round(max(0.0, time.monotonic() - create_started), 3)
        self.db.merge_sandbox_metadata(sandbox_id, {"sandbox_create_seconds": create_seconds})
        self.db.update_sandbox_state(sandbox_id, "running")
        logger.info(
            "Create latency: sandbox=%s source=%s gateway=%s create_seconds=%.3f",
            sandbox_id,
            metadata.get("sandbox_allocation_source") or "-",
            chosen_target.instance_id if chosen_target else "-",
            create_seconds,
        )
        self._record_observability_event(
            severity="info",
            category="sandbox",
            action="create_succeeded",
            entity_type="sandbox",
            entity_id=sandbox_id,
            sandbox_id=sandbox_id,
            gateway_instance_id=chosen_target.instance_id if chosen_target else None,
            template_id=template_id,
            message="Created sandbox",
            metadata={
                "runtime": runtime,
                "image": image,
                "allocation_source": metadata.get("sandbox_allocation_source"),
                "create_seconds": create_seconds,
                "is_warm_pool": bool(is_warm_pool),
                "warm_pool_key": warm_pool_key or "",
            },
        )
        logger.info("Sandbox created: %s", sandbox_id)
        return sandbox_id

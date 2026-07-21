from types import SimpleNamespace

from orchestrator.gateway_targets import GatewayTarget
from orchestrator.template_image_lifecycle import TemplateImageLifecycle


class FakeDB:
    def __init__(self, row):
        self.row = dict(row)

    def get_sandbox_template(self, template_id):
        if self.row.get("template_id") == template_id:
            return dict(self.row)
        return None

    def set_template_build_error(self, template_id, message):
        if self.row.get("template_id") == template_id:
            self.row["build_error"] = message
            return True
        return False

    def set_template_warm_snapshot(
        self,
        template_id,
        image_ref,
        build_error=None,
        *,
        registry_image_ref=None,
        materialized_gateway_instance_id=None,
    ):
        if self.row.get("template_id") != template_id:
            return False
        self.row["warm_snapshot_image"] = image_ref
        self.row["registry_image_ref"] = registry_image_ref
        self.row["materialized_gateway_instance_id"] = materialized_gateway_instance_id
        self.row["build_error"] = build_error
        return True

    def set_template_image_refs(
        self,
        template_id,
        *,
        warm_snapshot_image,
        registry_image_ref,
        materialized_gateway_instance_id,
        build_error,
    ):
        if self.row.get("template_id") != template_id:
            return False
        self.row["warm_snapshot_image"] = warm_snapshot_image
        self.row["registry_image_ref"] = registry_image_ref
        self.row["materialized_gateway_instance_id"] = materialized_gateway_instance_id
        self.row["build_error"] = build_error
        return True


class FakeManager:
    def __init__(self, row, *, build_ok=True):
        self.db = FakeDB(row)
        self._config = SimpleNamespace(ENVD_EMBED_AT_TEMPLATE_BUILD=False)
        self.build_ok = build_ok
        self.build_calls = []
        self.events = []
        self.gateway_images = {}

    def _build_registered_template_snapshot(self, template_id):
        self.build_calls.append(template_id)
        if not self.build_ok:
            self.db.set_template_build_error(template_id, "snapshot build failed")
            return False
        self.db.set_template_warm_snapshot(
            template_id,
            "mysandbox-snap:tpl-python-3.11-new",
            registry_image_ref="registry/templates/python-3.11:new",
            materialized_gateway_instance_id="runtime-gateway-1",
        )
        return True

    def _gateway_target_for_template_row(self, row):
        return None

    def _best_gateway_by_load(self, targets, *, force_refresh=False, preferred_image_ref=""):
        return None

    def _gateway_targets(self):
        return []

    def _gateway_has_image(self, target, image_ref, *, force_refresh=False):
        return bool(self.gateway_images.get((target.instance_id, image_ref)))

    def _record_observability_event(self, **kwargs):
        self.events.append(kwargs)


def test_repair_missing_base_image_snapshot_rebuilds_and_updates_refs():
    manager = FakeManager(
        {
            "template_id": "python:3.11",
            "base_image": "python:3.11",
            "warm_snapshot_image": "mysandbox-snap:tpl-python-3.11-old",
            "registry_image_ref": "",
            "build_error": "template image unavailable: mysandbox-snap:tpl-python-3.11-old; rebuild required",
            "source_kind": "",
        }
    )

    rebuilt = TemplateImageLifecycle(manager).repair_missing_image("python:3.11", manager.db.get_sandbox_template("python:3.11"))

    assert rebuilt == "mysandbox-snap:tpl-python-3.11-new"
    assert manager.build_calls == ["python:3.11"]
    assert manager.db.row["warm_snapshot_image"] == "mysandbox-snap:tpl-python-3.11-new"
    assert manager.db.row["registry_image_ref"] == "registry/templates/python-3.11:new"
    assert manager.db.row["materialized_gateway_instance_id"] == "runtime-gateway-1"
    assert manager.db.row["build_error"] is None
    assert manager.events[-1]["action"] == "rebuild_succeeded"
    assert manager.events[-1]["metadata"]["build_mode"] == "snapshot"


def test_ensure_retries_base_image_snapshot_when_rebuild_error_has_no_refs():
    manager = FakeManager(
        {
            "template_id": "python:3.11",
            "base_image": "python:3.11",
            "warm_snapshot_image": None,
            "registry_image_ref": None,
            "materialized_gateway_instance_id": None,
            "build_error": "template image unavailable: mysandbox-snap:tpl-python-3.11-old; rebuild required",
            "source_kind": "",
        }
    )

    updated = TemplateImageLifecycle(manager).ensure("python:3.11", manager.db.get_sandbox_template("python:3.11"))

    assert manager.build_calls == ["python:3.11"]
    assert updated["warm_snapshot_image"] == "mysandbox-snap:tpl-python-3.11-new"
    assert updated["build_error"] is None


def test_repair_missing_base_image_snapshot_reports_unavailable_source():
    manager = FakeManager(
        {
            "template_id": "friendly-template",
            "base_image": "friendly-template",
            "warm_snapshot_image": None,
            "registry_image_ref": None,
            "build_error": "template image unavailable: old; rebuild required",
            "source_kind": "",
        }
    )

    rebuilt = TemplateImageLifecycle(manager).repair_missing_image(
        "friendly-template",
        manager.db.get_sandbox_template("friendly-template"),
    )

    assert rebuilt is None
    assert manager.build_calls == []
    assert "base image rebuild source is unavailable" in manager.db.row["build_error"]
    assert manager.events[-1]["action"] == "rebuild_unavailable"


def test_image_for_target_prefers_owner_local_warm_image_over_registry_ref():
    row = {
        "template_id": "tpl-fast",
        "base_image": "local-template:tag",
        "warm_snapshot_image": "local-template:tag",
        "registry_image_ref": "registry.local/templates/tpl-fast:tag",
        "materialized_gateway_instance_id": "runtime-gateway-1",
    }
    manager = FakeManager(row)
    manager.gateway_images[("runtime-gateway-1", "local-template:tag")] = True
    target = GatewayTarget("runtime-gateway-1", "http://gateway-1", "http://gateway-1")

    image = TemplateImageLifecycle(manager).image_for_target(
        template_id="tpl-fast",
        row=row,
        requested_image="registry.local/templates/tpl-fast:tag",
        target=target,
    )

    assert image == "local-template:tag"


def test_image_for_target_falls_back_to_registry_when_owner_local_image_is_missing():
    row = {
        "template_id": "tpl-fast",
        "base_image": "local-template:tag",
        "warm_snapshot_image": "local-template:tag",
        "registry_image_ref": "registry.local/templates/tpl-fast:tag",
        "materialized_gateway_instance_id": "runtime-gateway-1",
    }
    manager = FakeManager(row)
    target = GatewayTarget("runtime-gateway-1", "http://gateway-1", "http://gateway-1")

    image = TemplateImageLifecycle(manager).image_for_target(
        template_id="tpl-fast",
        row=row,
        requested_image="registry.local/templates/tpl-fast:tag",
        target=target,
    )

    assert image == "registry.local/templates/tpl-fast:tag"


def test_image_for_target_falls_back_to_registry_from_warm_ref_when_owner_image_is_missing():
    row = {
        "template_id": "tpl-fast",
        "base_image": "local-template:tag",
        "warm_snapshot_image": "local-template:tag",
        "registry_image_ref": "registry.local/templates/tpl-fast:tag",
        "materialized_gateway_instance_id": "runtime-gateway-1",
    }
    manager = FakeManager(row)
    target = GatewayTarget("runtime-gateway-1", "http://gateway-1", "http://gateway-1")

    image = TemplateImageLifecycle(manager).image_for_target(
        template_id="tpl-fast",
        row=row,
        requested_image="local-template:tag",
        target=target,
    )

    assert image == "registry.local/templates/tpl-fast:tag"

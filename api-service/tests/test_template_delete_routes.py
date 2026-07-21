import asyncio
import os

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/sandboxes")
os.environ.setdefault("DATABASE_TYPE", "postgres")
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("API_KEY", "client-key")
os.environ.setdefault("INTERNAL_API_KEY", "internal-key")
os.environ.setdefault("ADMIN_API_KEY", "admin-key")
os.environ["IMAGE_BUILDING_AUTH_REQUIRED"] = "false"

from handlers import daytona_compat, e2b_compat  # noqa: E402
from middleware import ApiKeyPrincipal  # noqa: E402


def _principal() -> ApiKeyPrincipal:
    return ApiKeyPrincipal(
        client_id="client-a",
        key_id="key-a",
        key_name="test",
        key_prefix="test",
        email="user@example.com",
        display_name="User",
        is_active=True,
    )


class FakeTemplateDeleteDB:
    def __init__(self):
        self.row = {
            "template_id": "tpl-client-a-smoke",
            "template_alias": "smoke",
            "owner_client_id": "client-a",
        }
        self.deleted_templates = []
        self.disabled_segments = []
        self.deleted_snapshots = []

    def get_sandbox_template_by_alias(self, client_id, template_alias):
        if client_id == "client-a" and self.row and template_alias == self.row["template_alias"]:
            return dict(self.row)
        return None

    def get_sandbox_template(self, template_id):
        if self.row and template_id == self.row["template_id"]:
            return dict(self.row)
        return None

    def delete_sandbox_template(self, template_id, owner_client_id=None):
        if self.row and template_id == self.row["template_id"] and owner_client_id == "client-a":
            self.deleted_templates.append((template_id, owner_client_id))
            self.row = None
            return True
        return False

    def list_warm_pool_segments(self):
        return [
            {
                "warm_pool_key": "tpl-client-a-smoke|1|512m|600",
                "template_id": "tpl-client-a-smoke",
                "cpu_limit": "1",
                "memory_limit": "512m",
                "timeout": 600,
                "desired_size": 1,
            }
        ]

    def disable_warm_pool_segment(self, warm_pool_key, message=None):
        self.disabled_segments.append((warm_pool_key, message))
        return True

    def get_sandbox_snapshot(self, snapshot_id, owner_client_id=None):
        return None

    def delete_sandbox_snapshot(self, snapshot_id, owner_client_id=None):
        self.deleted_snapshots.append((snapshot_id, owner_client_id))
        return True

    def list_all_sandbox_snapshots(self, limit=100, owner_client_id=None):
        return []


class FakeSnapshotDeleteDB(FakeTemplateDeleteDB):
    def __init__(self):
        super().__init__()
        self.row = None
        self.snapshot = {
            "snapshot_id": "snap-1",
            "label": "legacy-snapshot",
            "owner_client_id": "client-a",
        }

    def get_sandbox_snapshot(self, snapshot_id, owner_client_id=None):
        if snapshot_id == self.snapshot["snapshot_id"] and owner_client_id == "client-a":
            return dict(self.snapshot)
        return None

    def list_all_sandbox_snapshots(self, limit=100, owner_client_id=None):
        if owner_client_id == "client-a":
            return [dict(self.snapshot)]
        return []


class FakeStaleTemplateSegmentDB(FakeTemplateDeleteDB):
    def __init__(self):
        super().__init__()
        self.row = None


class FakeTaggedTemplateDeleteDB(FakeTemplateDeleteDB):
    def __init__(self):
        super().__init__()
        self.row = {
            "template_id": "tpl-client-a-smoke-tag",
            "template_alias": "smoke:tag",
            "owner_client_id": "client-a",
        }

    def list_warm_pool_segments(self):
        return [
            {
                "warm_pool_key": "tpl-client-a-smoke-tag|1|512m|600",
                "template_id": "tpl-client-a-smoke-tag",
                "cpu_limit": "1",
                "memory_limit": "512m",
                "timeout": 600,
                "desired_size": 1,
            }
        ]


class FakeManager:
    def __init__(self, db):
        self.db = db
        self.trimmed_segments = []
        self.warm_pool = FakeWarmPool()

    def trim_warm_pool_to_size(self, warm_pool_key, desired_size):
        self.trimmed_segments.append((warm_pool_key, desired_size))
        return 1


class FakeWarmPool:
    def __init__(self):
        self.drains = []

    def ensure_pool_for(
        self,
        logical_template_id,
        cpu_limit,
        memory_limit,
        timeout,
        from_snapshot_image,
        desired_size=None,
    ):
        self.drains.append(
            {
                "logical_template_id": logical_template_id,
                "cpu_limit": cpu_limit,
                "memory_limit": memory_limit,
                "timeout": timeout,
                "from_snapshot_image": from_snapshot_image,
                "desired_size": desired_size,
            }
        )


class FakeStorageDB:
    def __init__(self):
        self.uploads = []

    def put_template_build_upload(
        self,
        owner_client_id,
        namespace,
        object_key,
        payload,
        *,
        content_type="",
        metadata=None,
    ):
        self.uploads.append(
            {
                "owner_client_id": owner_client_id,
                "namespace": namespace,
                "object_key": object_key,
                "payload": payload,
                "content_type": content_type,
                "metadata": metadata or {},
            }
        )
        return self.uploads[-1]

    def template_build_upload_exists(self, owner_client_id, namespace, object_key):
        return any(
            item["owner_client_id"] == owner_client_id
            and item["namespace"] == namespace
            and item["object_key"] == object_key
            for item in self.uploads
        )


class FakeStorageManager:
    def __init__(self, db):
        self.db = db


class FakeStorageRequest:
    def __init__(self, payload=b"context", headers=None, query_params=None, method="PUT"):
        self._payload = payload
        self.headers = headers or {}
        self.query_params = query_params or {}
        self.method = method

    async def body(self):
        return self._payload


def test_e2b_template_delete_removes_owned_template_and_disables_warm_pool():
    db = FakeTemplateDeleteDB()
    manager = FakeManager(db)

    response = asyncio.run(e2b_compat.delete_template_or_snapshot("smoke", _principal(), manager))

    assert response.status_code == 204
    assert db.deleted_templates == [("tpl-client-a-smoke", "client-a")]
    assert db.disabled_segments[0][0] == "tpl-client-a-smoke|1|512m|600"
    assert manager.warm_pool.drains == [
        {
            "logical_template_id": "tpl-client-a-smoke",
            "cpu_limit": "1",
            "memory_limit": "512m",
            "timeout": 600,
            "from_snapshot_image": None,
            "desired_size": 0,
        }
    ]
    assert manager.trimmed_segments == [("tpl-client-a-smoke|1|512m|600", 0)]
    assert db.deleted_snapshots == []


def test_e2b_template_tag_delete_drains_tagged_warm_pool_segment():
    db = FakeTaggedTemplateDeleteDB()
    manager = FakeManager(db)

    class _Body:
        async def json(self):
            return {"name": "smoke", "tags": ["tag"]}

    response = asyncio.run(e2b_compat.remove_template_tags(_Body(), _principal(), manager))

    assert response.status_code == 204
    assert db.deleted_templates == [("tpl-client-a-smoke-tag", "client-a")]
    assert db.disabled_segments[0][0] == "tpl-client-a-smoke-tag|1|512m|600"
    assert manager.warm_pool.drains[0]["desired_size"] == 0


def test_daytona_snapshot_delete_removes_backing_template():
    db = FakeTemplateDeleteDB()
    manager = FakeManager(db)

    response = asyncio.run(daytona_compat.delete_snapshot("smoke", _principal(), manager))

    assert response.status_code == 204
    assert db.deleted_templates == [("tpl-client-a-smoke", "client-a")]
    assert db.disabled_segments[0][0] == "tpl-client-a-smoke|1|512m|600"


def test_delete_drains_stale_warm_pool_segment_after_template_row_is_gone():
    db = FakeStaleTemplateSegmentDB()
    manager = FakeManager(db)

    response = asyncio.run(daytona_compat.delete_snapshot("smoke", _principal(), manager))

    assert response.status_code == 204
    assert db.deleted_templates == []
    assert db.disabled_segments[0][0] == "tpl-client-a-smoke|1|512m|600"
    assert manager.warm_pool.drains[0]["desired_size"] == 0


def test_daytona_snapshot_delete_falls_back_to_legacy_snapshot_rows():
    db = FakeSnapshotDeleteDB()
    manager = FakeManager(db)

    response = asyncio.run(daytona_compat.delete_snapshot("legacy-snapshot", _principal(), manager))

    assert response.status_code == 204
    assert db.deleted_snapshots == [("snap-1", "client-a")]


def test_daytona_object_storage_context_rejects_missing_session_token():
    db = FakeStorageDB()
    manager = FakeStorageManager(db)

    response = asyncio.run(
        daytona_compat.daytona_object_storage_context(
            daytona_compat.DAYTONA_CONTEXT_BUCKET,
            "client-a",
            "hash-a",
            FakeStorageRequest(headers={}),
            manager,
        )
    )

    assert response.status_code == 403
    assert db.uploads == []


def test_daytona_object_storage_context_accepts_valid_session_token():
    db = FakeStorageDB()
    manager = FakeStorageManager(db)
    token = daytona_compat._storage_token("client-a")

    response = asyncio.run(
        daytona_compat.daytona_object_storage_context(
            daytona_compat.DAYTONA_CONTEXT_BUCKET,
            "client-a",
            "hash-a",
            FakeStorageRequest(headers={"x-amz-security-token": token}),
            manager,
        )
    )

    assert response.status_code == 200
    assert len(db.uploads) == 1
    assert db.uploads[0]["owner_client_id"] == "client-a"

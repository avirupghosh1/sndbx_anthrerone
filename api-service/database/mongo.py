"""MongoDB database backend."""

from __future__ import annotations

from .common import *

try:
    import pymongo
    from pymongo import ReturnDocument
    from pymongo.errors import DuplicateKeyError
except Exception:  # noqa: BLE001
    pymongo = None
    ReturnDocument = None
    DuplicateKeyError = Exception


class _MongoDatabase:
    """Persistent metadata store backed by MongoDB."""

    def __init__(
        self,
        database_url: str,
        mongodb_password: Optional[str] = None,
        database_username: Optional[str] = None,
        database_password: Optional[str] = None,
    ):
        if pymongo is None:
            raise RuntimeError("pymongo is required for MongoDB DATABASE_URL")
        self.database_url = _resolve_mongodb_url(
            database_url,
            mongodb_password=mongodb_password,
            database_username=database_username,
            database_password=database_password,
        )
        self.database_name = _mongodb_database_name(self.database_url)
        self._lock = Lock()
        self._lock_owner_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex}"
        timeout_ms = int(os.getenv("MONGODB_SERVER_SELECTION_TIMEOUT_MS", "10000"))
        self.client = pymongo.MongoClient(self.database_url, serverSelectionTimeoutMS=timeout_ms)
        self.db = self.client[self.database_name]
        self._init_db()

    def _init_db(self) -> None:
        attempts = 4
        for attempt in range(attempts):
            try:
                self.client.admin.command("ping")
                break
            except Exception as ex:  # noqa: BLE001
                if attempt >= attempts - 1:
                    raise
                time.sleep(0.25 * (attempt + 1))

        self.db.clients.create_index("email", unique=True)
        self.db.api_keys.create_index("key_hash", unique=True)
        self.db.api_keys.create_index([("client_id", 1), ("created_at", -1)])
        self.db.sandboxes.create_index("container_id", unique=True, sparse=True)
        self.db.sandboxes.create_index([("owner_client_id", 1), ("created_at", -1)])
        self.db.sandboxes.create_index([("gateway_instance_id", 1), ("created_at", 1)])
        self.db.sandboxes.create_index([("state", 1), ("lease_expires_at", 1)])
        self.db.sandboxes.create_index([("is_warm_pool", 1), ("warm_pool_key", 1), ("created_at", 1)])
        self.db.commands_history.create_index([("sandbox_id", 1), ("created_at", -1)])
        self.db.sandbox_snapshots.create_index([("source_sandbox_id", 1), ("created_at", -1)])
        self.db.sandbox_templates.create_index([("owner_client_id", 1), ("template_alias", 1)])
        self.db.sandbox_templates.create_index("template_alias")
        self.db.template_builds.create_index([("owner_client_id", 1), ("created_at", -1)])
        self.db.template_build_uploads.create_index(
            [("owner_client_id", 1), ("namespace", 1), ("object_key", 1)],
            unique=True,
        )
        self.db.template_build_upload_chunks.create_index([("upload_id", 1), ("idx", 1)])
        self.db.warm_pool_segments.create_index([("desired_size", 1), ("updated_at", -1)])
        self.db.warm_pool_segments.update_many(
            {"ready_image_ref": {"$exists": True}},
            {"$unset": {"ready_image_ref": ""}},
        )
        self.db.distributed_locks.create_index("expires_at")
        self.db.observability_events.create_index([("timestamp", -1)])
        self.db.observability_events.create_index([("sandbox_id", 1), ("timestamp", -1)])
        self.db.observability_events.create_index([("template_id", 1), ("timestamp", -1)])
        self.db.observability_events.create_index([("gateway_instance_id", 1), ("timestamp", -1)])
        self.db.observability_events.create_index([("category", 1), ("action", 1), ("timestamp", -1)])
        self.db.observability_events.create_index([("severity", 1), ("timestamp", -1)])
        self.db.observability_metric_samples.create_index([("timestamp", -1)])
        self.db.observability_metric_samples.create_index([("sample_type", 1), ("timestamp", -1)])
        self.db.observability_metric_samples.create_index([("gateway_instance_id", 1), ("timestamp", -1)])
        self.db.observability_metric_samples.create_index([("warm_pool_key", 1), ("timestamp", -1)])

    @staticmethod
    def _without_id(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not doc:
            return None
        out = dict(doc)
        out.pop("_id", None)
        return out

    @staticmethod
    def _metadata_value(value: Any) -> Dict[str, Any]:
        if isinstance(value, dict):
            return dict(value)
        if isinstance(value, str) and value.strip():
            try:
                loaded = json.loads(value)
                return loaded if isinstance(loaded, dict) else {}
            except Exception:  # noqa: BLE001
                return {}
        return {}

    @staticmethod
    def _sandbox_dict_from_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
        d = _MongoDatabase._without_id(doc) or {}
        d["metadata"] = _MongoDatabase._metadata_value(d.get("metadata"))
        d.setdefault("runtime", "docker")
        d.setdefault("disk_limit", "")
        d.setdefault("gateway_instance_id", "")
        d.setdefault("gateway_route_base", "")
        d.setdefault("gateway_api_base", "")
        d.setdefault("gateway_docker_host", "")
        d["is_warm_pool"] = bool(d.get("is_warm_pool"))
        d.setdefault("warm_pool_key", "")
        return d

    @staticmethod
    def _client_dict_from_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
        d = _MongoDatabase._without_id(doc) or {}
        return {
            "client_id": d.get("client_id"),
            "email": d.get("email"),
            "password_hash": d.get("password_hash"),
            "display_name": d.get("display_name") or "",
            "is_active": bool(d.get("is_active")),
            "created_at": d.get("created_at"),
            "updated_at": d.get("updated_at"),
        }

    @staticmethod
    def _api_key_dict_from_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
        d = _MongoDatabase._without_id(doc) or {}
        return {
            "key_id": d.get("key_id"),
            "client_id": d.get("client_id"),
            "name": d.get("name"),
            "key_prefix": d.get("key_prefix"),
            "key_hash": d.get("key_hash"),
            "created_at": d.get("created_at"),
            "updated_at": d.get("updated_at"),
            "last_used_at": d.get("last_used_at"),
            "revoked_at": d.get("revoked_at"),
        }

    @staticmethod
    def _warm_pool_segment_dict_from_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
        d = _MongoDatabase._without_id(doc) or {}
        return {
            "warm_pool_key": d.get("warm_pool_key"),
            "template_id": d.get("template_id"),
            "cpu_limit": d.get("cpu_limit"),
            "memory_limit": d.get("memory_limit"),
            "timeout": int(d.get("timeout") or 0),
            "desired_size": int(d.get("desired_size") or 0),
            "inflight_count": int(d.get("inflight_count") or 0),
            "inflight_updated_at": d.get("inflight_updated_at"),
            "handoff_count": int(d.get("handoff_count") or 0),
            "failed_count": int(d.get("failed_count") or 0),
            "last_handoff_at": d.get("last_handoff_at"),
            "last_refill_at": d.get("last_refill_at"),
            "ready_image_ref": None,
            "preferred_gateway_instance_id": d.get("preferred_gateway_instance_id"),
            "last_error": d.get("last_error"),
            "created_at": d.get("created_at"),
            "updated_at": d.get("updated_at"),
        }

    @staticmethod
    def _template_dict_from_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
        d = _MongoDatabase._without_id(doc) or {}
        env = d.get("env")
        if not isinstance(env, dict):
            env = _MongoDatabase._metadata_value(d.get("env_json"))
        build_args = d.get("build_args")
        if not isinstance(build_args, dict):
            build_args = _MongoDatabase._metadata_value(d.get("build_args_json"))
        return {
            "template_id": d.get("template_id"),
            "base_image": d.get("base_image"),
            "env": dict(env or {}),
            "start_cmd": d.get("start_cmd") or "",
            "settle_seconds": int(d.get("settle_seconds") or 20),
            "warm_snapshot_image": d.get("warm_snapshot_image"),
            "registry_image_ref": d.get("registry_image_ref"),
            "materialized_gateway_instance_id": d.get("materialized_gateway_instance_id"),
            "build_error": d.get("build_error"),
            "created_at": d.get("created_at"),
            "updated_at": d.get("updated_at"),
            "ready_cmd": d.get("ready_cmd") or "",
            "owner_client_id": d.get("owner_client_id"),
            "owner_api_key_id": d.get("owner_api_key_id"),
            "template_alias": d.get("template_alias") or d.get("template_id") or "",
            "source_kind": d.get("source_kind") or "",
            "source_build_mode": d.get("source_build_mode") or "",
            "dockerfile_text": d.get("dockerfile_text"),
            "build_args": dict(build_args or {}),
            "context_tar_gzip_base64": d.get("context_tar_gzip_base64"),
        }

    @staticmethod
    def _template_build_dict_from_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
        d = _MongoDatabase._without_id(doc) or {}
        return {
            "build_id": d.get("build_id"),
            "template_id": d.get("template_id"),
            "template_alias": d.get("template_alias") or d.get("template_id"),
            "owner_client_id": d.get("owner_client_id"),
            "owner_api_key_id": d.get("owner_api_key_id"),
            "requested_mode": d.get("requested_mode") or "",
            "effective_mode": d.get("effective_mode") or "",
            "status": d.get("status") or "",
            "image_tag": d.get("image_tag"),
            "registry_image_ref": d.get("registry_image_ref"),
            "gateway_instance_id": d.get("gateway_instance_id"),
            "build_log": d.get("build_log") or "",
            "error_text": d.get("error_text"),
            "created_at": d.get("created_at"),
            "updated_at": d.get("updated_at"),
            "completed_at": d.get("completed_at"),
        }

    @staticmethod
    def _observability_event_dict_from_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
        d = _MongoDatabase._without_id(doc) or {}
        metadata = d.get("metadata")
        if not isinstance(metadata, dict):
            metadata = _MongoDatabase._metadata_value(metadata)
        return {
            "event_id": d.get("event_id"),
            "timestamp": d.get("timestamp"),
            "severity": d.get("severity") or "info",
            "category": d.get("category") or "",
            "action": d.get("action") or "",
            "entity_type": d.get("entity_type") or "",
            "entity_id": d.get("entity_id") or "",
            "gateway_instance_id": d.get("gateway_instance_id") or "",
            "template_id": d.get("template_id") or "",
            "sandbox_id": d.get("sandbox_id") or "",
            "message": d.get("message") or "",
            "metadata": dict(metadata or {}),
        }

    @staticmethod
    def _observability_metric_sample_dict_from_doc(doc: Dict[str, Any]) -> Dict[str, Any]:
        d = _MongoDatabase._without_id(doc) or {}
        metrics = d.get("metrics")
        if not isinstance(metrics, dict):
            metrics = _MongoDatabase._metadata_value(metrics)
        return {
            "sample_id": d.get("sample_id"),
            "timestamp": d.get("timestamp"),
            "sample_type": d.get("sample_type") or "",
            "gateway_instance_id": d.get("gateway_instance_id") or "",
            "warm_pool_key": d.get("warm_pool_key") or "",
            "metrics": dict(metrics or {}),
        }

    def acquire_advisory_lock(self, lock_name: str) -> bool:
        name = (lock_name or "").strip()
        if not name:
            return False
        ttl = max(5, int(os.getenv("MONGODB_ADVISORY_LOCK_TTL_SEC", "30")))
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat().replace("+00:00", "Z")
        expires = (now_dt + timedelta(seconds=ttl)).isoformat().replace("+00:00", "Z")
        collection = self.db.distributed_locks
        lock_doc = {
            "_id": name,
            "lock_name": name,
            "owner_id": self._lock_owner_id,
            "expires_at": expires,
            "created_at": now,
            "updated_at": now,
        }
        eligible = {
            "_id": name,
            "$or": [
                {"owner_id": self._lock_owner_id},
                {"expires_at": {"$lte": now}},
                {"expires_at": {"$exists": False}},
            ],
        }
        result = collection.update_one(
            eligible,
            {
                "$set": {
                    "lock_name": name,
                    "owner_id": self._lock_owner_id,
                    "expires_at": expires,
                    "updated_at": now,
                }
            },
        )
        if result.matched_count > 0:
            return True
        if collection.find_one({"_id": name}, {"_id": 1}) is not None:
            return False
        try:
            collection.insert_one(lock_doc)
        except DuplicateKeyError:
            return False
        return True

    def create_client(
        self,
        client_id: str,
        email: str,
        password_hash: str,
        display_name: str = "",
        *,
        is_active: bool = True,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        doc = {
            "_id": client_id,
            "client_id": client_id,
            "email": email.strip().lower(),
            "password_hash": password_hash,
            "display_name": display_name.strip(),
            "is_active": bool(is_active),
            "created_at": now,
            "updated_at": now,
        }
        with self._lock:
            self.db.clients.insert_one(doc)
        return self.get_client(client_id) or {}

    def get_client(self, client_id: str) -> Optional[Dict[str, Any]]:
        doc = self.db.clients.find_one({"_id": client_id})
        return self._client_dict_from_doc(doc) if doc else None

    def get_client_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        doc = self.db.clients.find_one({"email": email.strip().lower()})
        return self._client_dict_from_doc(doc) if doc else None

    def create_api_key(
        self,
        *,
        key_id: str,
        client_id: str,
        name: str,
        key_prefix: str,
        key_hash: str,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        doc = {
            "_id": key_id,
            "key_id": key_id,
            "client_id": client_id,
            "name": name.strip(),
            "key_prefix": key_prefix.strip(),
            "key_hash": key_hash,
            "created_at": now,
            "updated_at": now,
            "last_used_at": None,
            "revoked_at": None,
        }
        with self._lock:
            self.db.api_keys.insert_one(doc)
        return self._get_api_key_record(key_id) or {}

    def _get_api_key_record(self, key_id: str) -> Optional[Dict[str, Any]]:
        doc = self.db.api_keys.find_one({"_id": key_id})
        return self._api_key_dict_from_doc(doc) if doc else None

    def list_api_keys_for_client(self, client_id: str, *, include_revoked: bool = False) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {"client_id": client_id}
        if not include_revoked:
            query["revoked_at"] = None
        return [
            self._api_key_dict_from_doc(doc)
            for doc in self.db.api_keys.find(query).sort("created_at", -1)
        ]

    def get_api_key_principal(self, key_hash: str) -> Optional[Dict[str, Any]]:
        key = self.db.api_keys.find_one({"key_hash": key_hash})
        if not key:
            return None
        client = self.db.clients.find_one({"_id": key.get("client_id")})
        if not client:
            return None
        return {
            "key_id": key.get("key_id"),
            "client_id": key.get("client_id"),
            "name": key.get("name"),
            "key_prefix": key.get("key_prefix"),
            "key_hash": key.get("key_hash"),
            "created_at": key.get("created_at"),
            "updated_at": key.get("updated_at"),
            "last_used_at": key.get("last_used_at"),
            "revoked_at": key.get("revoked_at"),
            "email": client.get("email"),
            "display_name": client.get("display_name") or "",
            "is_active": bool(client.get("is_active")),
        }

    def get_api_key_principal_by_id(self, key_id: str) -> Optional[Dict[str, Any]]:
        key = self.db.api_keys.find_one({"_id": key_id})
        if not key:
            return None
        client = self.db.clients.find_one({"_id": key.get("client_id")})
        if not client:
            return None
        return {
            "key_id": key.get("key_id"),
            "client_id": key.get("client_id"),
            "name": key.get("name"),
            "key_prefix": key.get("key_prefix"),
            "key_hash": key.get("key_hash"),
            "created_at": key.get("created_at"),
            "updated_at": key.get("updated_at"),
            "last_used_at": key.get("last_used_at"),
            "revoked_at": key.get("revoked_at"),
            "email": client.get("email"),
            "display_name": client.get("display_name") or "",
            "is_active": bool(client.get("is_active")),
        }

    def touch_api_key_used(self, key_id: str) -> bool:
        now = _utc_now_iso()
        res = self.db.api_keys.update_one(
            {"_id": key_id},
            {"$set": {"last_used_at": now, "updated_at": now}},
        )
        return res.matched_count > 0

    def revoke_api_key(self, key_id: str, client_id: str) -> bool:
        now = _utc_now_iso()
        res = self.db.api_keys.update_one(
            {"_id": key_id, "client_id": client_id, "revoked_at": None},
            {"$set": {"revoked_at": now, "updated_at": now}},
        )
        return res.modified_count > 0

    def create_sandbox(
        self,
        sandbox_id: str,
        container_id: str,
        template_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        cpu_limit: str = "1",
        memory_limit: str = "512m",
        timeout: int = 3600,
        runtime: str = "docker",
        disk_limit: str = "",
        owner_client_id: Optional[str] = None,
        owner_api_key_id: Optional[str] = None,
        is_warm_pool: bool = False,
        warm_pool_key: Optional[str] = None,
        gateway_instance_id: Optional[str] = None,
        gateway_route_base: Optional[str] = None,
        gateway_api_base: Optional[str] = None,
        gateway_docker_host: Optional[str] = None,
        state: str = "running",
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        lease_seconds = max(3600, int(timeout)) if is_warm_pool else max(60, int(timeout))
        lease_expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
        ).isoformat().replace("+00:00", "Z")
        doc = {
            "_id": sandbox_id,
            "sandbox_id": sandbox_id,
            "container_id": container_id,
            "state": (state or "running").strip() or "running",
            "template_id": template_id,
            "created_at": now,
            "updated_at": now,
            "metadata": dict(metadata or {}),
            "cpu_limit": cpu_limit,
            "memory_limit": memory_limit,
            "disk_limit": disk_limit,
            "timeout": int(timeout),
            "lease_expires_at": lease_expires_at,
            "runtime": runtime,
            "owner_client_id": owner_client_id,
            "owner_api_key_id": owner_api_key_id,
            "is_warm_pool": bool(is_warm_pool),
            "warm_pool_key": (warm_pool_key or "").strip() or None,
            "gateway_instance_id": gateway_instance_id,
            "gateway_route_base": gateway_route_base,
            "gateway_api_base": gateway_api_base,
            "gateway_docker_host": gateway_docker_host,
        }
        with self._lock:
            self.db.sandboxes.insert_one(doc)
        return self._sandbox_dict_from_doc(doc)

    def get_sandbox(self, sandbox_id: str) -> Optional[Dict[str, Any]]:
        doc = self.db.sandboxes.find_one({"_id": sandbox_id})
        return self._sandbox_dict_from_doc(doc) if doc else None

    def get_sandbox_by_container(self, container_id: str) -> Optional[Dict[str, Any]]:
        doc = self.db.sandboxes.find_one({"container_id": container_id})
        return self._sandbox_dict_from_doc(doc) if doc else None

    def update_sandbox_state(self, sandbox_id: str, state: str) -> bool:
        now = _utc_now_iso()
        res = self.db.sandboxes.update_one(
            {"_id": sandbox_id},
            {"$set": {"state": state, "updated_at": now}},
        )
        return res.matched_count > 0

    def merge_sandbox_metadata(self, sandbox_id: str, updates: Optional[Dict[str, Any]]) -> bool:
        updates = dict(updates or {})
        updates.pop("_warm_pool", None)
        now = _utc_now_iso()
        with self._lock:
            doc = self.db.sandboxes.find_one({"_id": sandbox_id}, {"metadata": 1})
            if not doc:
                return False
            merged = {**self._metadata_value(doc.get("metadata")), **updates}
            merged.pop("_warm_pool", None)
            res = self.db.sandboxes.update_one(
                {"_id": sandbox_id},
                {"$set": {"metadata": merged, "updated_at": now}},
            )
        return res.matched_count > 0

    def update_sandbox_timeout(self, sandbox_id: str, timeout_seconds: int) -> bool:
        now = _utc_now_iso()
        lease_expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=max(60, int(timeout_seconds)))
        ).isoformat().replace("+00:00", "Z")
        res = self.db.sandboxes.update_one(
            {"_id": sandbox_id},
            {"$set": {"timeout": int(timeout_seconds), "lease_expires_at": lease_expires_at, "updated_at": now}},
        )
        return res.matched_count > 0

    def claim_warm_pool_sandbox(
        self,
        *,
        warm_pool_key: str,
        gateway_instance_id: Optional[str],
        owner_client_id: Optional[str],
        owner_api_key_id: Optional[str],
        metadata_updates: Optional[Dict[str, Any]] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        key = (warm_pool_key or "").strip()
        gateway = (gateway_instance_id or "").strip()
        if not key:
            return None
        updates = dict(metadata_updates or {})
        updates.pop("_warm_pool", None)
        claim_started = time.monotonic()
        query: Dict[str, Any] = {
            "state": "running",
            "is_warm_pool": True,
            "warm_pool_key": key,
        }
        if gateway:
            query["gateway_instance_id"] = gateway
        for _ in range(8):
            picked = self.db.sandboxes.find_one(query, sort=[("created_at", 1)])
            if not picked:
                return None
            prev = self._metadata_value(picked.get("metadata"))
            prev.pop("_warm_pool", None)
            merged = {**prev, **updates}
            base_wait = float(merged.get("sandbox_allocation_acquire_wait_seconds") or 0.0)
            merged["sandbox_allocation_acquire_wait_seconds"] = round(
                base_wait + max(0.0, time.monotonic() - claim_started),
                3,
            )
            timeout_value = int(timeout_seconds) if timeout_seconds is not None else int(picked.get("timeout") or 3600)
            lease_expires_at = (
                datetime.now(timezone.utc) + timedelta(seconds=max(60, timeout_value))
            ).isoformat().replace("+00:00", "Z")
            now = _utc_now_iso()
            result = self.db.sandboxes.find_one_and_update(
                {
                    "_id": picked.get("_id"),
                    "state": "running",
                    "is_warm_pool": True,
                    "warm_pool_key": key,
                },
                {
                    "$set": {
                        "owner_client_id": owner_client_id,
                        "owner_api_key_id": owner_api_key_id,
                        "is_warm_pool": False,
                        "warm_pool_key": None,
                        "metadata": merged,
                        "timeout": timeout_value,
                        "lease_expires_at": lease_expires_at,
                        "updated_at": now,
                    }
                },
                return_document=ReturnDocument.AFTER,
            )
            if result:
                self.db.warm_pool_segments.update_one(
                    {"_id": key},
                    {"$inc": {"handoff_count": 1}, "$set": {"last_handoff_at": now, "updated_at": now}},
                )
                return self._sandbox_dict_from_doc(result)
        return None

    def list_warm_pool_sandboxes(self, *, warm_pool_key: Optional[str] = None) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {"state": "running", "is_warm_pool": True}
        if (warm_pool_key or "").strip():
            query["warm_pool_key"] = (warm_pool_key or "").strip()
        return [
            self._sandbox_dict_from_doc(doc)
            for doc in self.db.sandboxes.find(query).sort("created_at", 1)
        ]

    def upsert_warm_pool_segment(
        self,
        *,
        warm_pool_key: str,
        template_id: str,
        cpu_limit: str,
        memory_limit: str,
        timeout: int,
        desired_size: int,
        preferred_gateway_instance_id: Optional[str] = None,
        last_error: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        key = (warm_pool_key or "").strip()
        if not key:
            raise ValueError("warm_pool_key is required")
        set_values: Dict[str, Any] = {
            "warm_pool_key": key,
            "template_id": template_id,
            "cpu_limit": str(cpu_limit),
            "memory_limit": str(memory_limit),
            "timeout": int(timeout),
            "desired_size": max(0, int(desired_size)),
            "last_error": last_error,
            "updated_at": now,
            "preferred_gateway_instance_id": (preferred_gateway_instance_id or "").strip() or None,
        }
        set_on_insert: Dict[str, Any] = {
            "created_at": now,
            "inflight_count": 0,
            "inflight_updated_at": None,
            "handoff_count": 0,
            "failed_count": 0,
            "last_handoff_at": None,
            "last_refill_at": None,
        }
        self.db.warm_pool_segments.update_one(
            {"_id": key},
            {
                "$set": set_values,
                "$setOnInsert": set_on_insert,
                "$unset": {"ready_image_ref": ""},
            },
            upsert=True,
        )
        return self.get_warm_pool_segment(key) or {}

    def get_warm_pool_segment(self, warm_pool_key: str) -> Optional[Dict[str, Any]]:
        key = (warm_pool_key or "").strip()
        if not key:
            return None
        doc = self.db.warm_pool_segments.find_one({"_id": key})
        return self._warm_pool_segment_dict_from_doc(doc) if doc else None

    def list_warm_pool_segments(self) -> List[Dict[str, Any]]:
        return [
            self._warm_pool_segment_dict_from_doc(doc)
            for doc in self.db.warm_pool_segments.find({"desired_size": {"$gt": 0}}).sort("updated_at", -1)
        ]

    def reserve_warm_pool_slots(
        self,
        *,
        warm_pool_key: str,
        ready_count: int,
        batch_max: int,
    ) -> int:
        now = _utc_now_iso()
        key = (warm_pool_key or "").strip()
        want = max(0, int(batch_max))
        if not key or want <= 0:
            return 0
        for _ in range(8):
            seg = self.db.warm_pool_segments.find_one({"_id": key})
            if not seg:
                return 0
            desired = max(0, int(seg.get("desired_size") or 0))
            inflight = max(0, int(seg.get("inflight_count") or 0))
            ready = max(0, int(ready_count))
            max_useful_inflight = max(0, desired - ready)
            if inflight > max_useful_inflight:
                res = self.db.warm_pool_segments.update_one(
                    {"_id": key, "inflight_count": inflight},
                    {"$set": {"inflight_count": max_useful_inflight, "inflight_updated_at": now, "updated_at": now}},
                )
                if res.modified_count <= 0:
                    continue
                inflight = max_useful_inflight
            reserve = max(0, min(want, desired - ready - inflight))
            if reserve <= 0:
                return 0
            res = self.db.warm_pool_segments.update_one(
                {"_id": key, "inflight_count": inflight},
                {
                    "$set": {
                        "inflight_count": inflight + reserve,
                        "inflight_updated_at": now,
                        "last_refill_at": now,
                        "updated_at": now,
                    }
                },
            )
            if res.modified_count > 0:
                return reserve
        return 0

    def reset_warm_pool_inflight(self, *, warm_pool_key: str, stale_after_seconds: float) -> bool:
        now = _utc_now_iso()
        key = (warm_pool_key or "").strip()
        if not key:
            return False
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=max(0.0, float(stale_after_seconds)))
        ).isoformat().replace("+00:00", "Z")
        res = self.db.warm_pool_segments.update_one(
            {
                "_id": key,
                "inflight_count": {"$ne": 0},
                "$or": [
                    {"inflight_updated_at": {"$lte": cutoff}},
                    {"inflight_updated_at": None, "created_at": {"$lte": cutoff}},
                    {"inflight_updated_at": {"$exists": False}, "created_at": {"$lte": cutoff}},
                ],
            },
            {"$set": {"inflight_count": 0, "inflight_updated_at": None, "updated_at": now}},
        )
        return res.modified_count > 0

    def release_warm_pool_slots(self, *, warm_pool_key: str, count: int) -> bool:
        return self.complete_warm_pool_slots(warm_pool_key=warm_pool_key, count=count, success=True)

    def complete_warm_pool_slots(self, *, warm_pool_key: str, count: int, success: bool) -> bool:
        now = _utc_now_iso()
        key = (warm_pool_key or "").strip()
        release = max(0, int(count))
        if not key or release <= 0:
            return False
        for _ in range(8):
            seg = self.db.warm_pool_segments.find_one({"_id": key})
            if not seg:
                return False
            inflight = max(0, int(seg.get("inflight_count") or 0))
            next_inflight = max(0, inflight - release)
            res = self.db.warm_pool_segments.update_one(
                {"_id": key, "inflight_count": inflight},
                {
                    "$set": {
                        "inflight_count": next_inflight,
                        "inflight_updated_at": now if next_inflight > 0 else None,
                        "updated_at": now,
                    },
                    "$inc": {"failed_count": 0 if success else release},
                },
            )
            if res.modified_count > 0:
                return True
        return False

    def count_running_sandboxes(
        self,
        *,
        gateway_instance_id: Optional[str] = None,
        template_id: Optional[str] = None,
    ) -> int:
        query: Dict[str, Any] = {"state": "running"}
        if (gateway_instance_id or "").strip():
            query["gateway_instance_id"] = (gateway_instance_id or "").strip()
        if (template_id or "").strip():
            query["template_id"] = (template_id or "").strip()
        return int(self.db.sandboxes.count_documents(query))

    def set_warm_pool_segment_preferred_gateway(
        self,
        warm_pool_key: str,
        preferred_gateway_instance_id: Optional[str],
        *,
        clear_error: bool = False,
    ) -> bool:
        now = _utc_now_iso()
        key = (warm_pool_key or "").strip()
        if not key:
            return False
        values: Dict[str, Any] = {
            "preferred_gateway_instance_id": (preferred_gateway_instance_id or "").strip() or None,
            "updated_at": now,
        }
        if clear_error:
            values["last_error"] = None
        res = self.db.warm_pool_segments.update_one({"_id": key}, {"$set": values})
        return res.matched_count > 0

    def set_warm_pool_segment_error(self, warm_pool_key: str, message: Optional[str]) -> bool:
        now = _utc_now_iso()
        key = (warm_pool_key or "").strip()
        if not key:
            return False
        res = self.db.warm_pool_segments.update_one(
            {"_id": key},
            {"$set": {"last_error": message, "updated_at": now}},
        )
        return res.matched_count > 0

    def disable_warm_pool_segment(self, warm_pool_key: str, message: Optional[str] = None) -> bool:
        now = _utc_now_iso()
        key = (warm_pool_key or "").strip()
        if not key:
            return False
        res = self.db.warm_pool_segments.update_one(
            {"_id": key},
            {
                "$set": {
                    "desired_size": 0,
                    "inflight_count": 0,
                    "inflight_updated_at": None,
                    "preferred_gateway_instance_id": None,
                    "last_error": message,
                    "updated_at": now,
                }
            },
        )
        return res.matched_count > 0

    def list_expired_sandboxes(self, now_iso: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        cutoff = now_iso or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        query = {"state": "running", "lease_expires_at": {"$ne": None, "$lte": cutoff}}
        return [
            self._sandbox_dict_from_doc(doc)
            for doc in self.db.sandboxes.find(query).sort("lease_expires_at", 1).limit(int(limit))
        ]

    def purge_lost_sandboxes(self, older_than_seconds: int, limit: int = 100) -> int:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=max(0, int(older_than_seconds)))
        ).isoformat().replace("+00:00", "Z")
        docs = list(
            self.db.sandboxes.find(
                {"state": "lost", "updated_at": {"$lte": cutoff}},
                {"sandbox_id": 1},
            ).sort("updated_at", 1).limit(int(limit))
        )
        purged = 0
        for doc in docs:
            sandbox_id = str(doc.get("sandbox_id") or doc.get("_id"))
            self.db.commands_history.delete_many({"sandbox_id": sandbox_id})
            self.db.sandbox_snapshots.delete_many({"source_sandbox_id": sandbox_id})
            res = self.db.sandboxes.delete_one({"_id": sandbox_id})
            purged += int(res.deleted_count)
        return purged

    def delete_sandbox(self, sandbox_id: str) -> bool:
        self.db.commands_history.delete_many({"sandbox_id": sandbox_id})
        self.db.sandbox_snapshots.delete_many({"source_sandbox_id": sandbox_id})
        res = self.db.sandboxes.delete_one({"_id": sandbox_id})
        return res.deleted_count > 0

    def list_sandboxes(
        self,
        limit: int = 100,
        offset: int = 0,
        owner_client_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {}
        if owner_client_id:
            query["owner_client_id"] = owner_client_id
        return [
            self._sandbox_dict_from_doc(doc)
            for doc in self.db.sandboxes.find(query).sort("created_at", -1).skip(int(offset)).limit(int(limit))
        ]

    def list_sandboxes_for_gateway(
        self,
        gateway_instance_id: str,
        *,
        limit: int = 1000,
    ) -> List[Dict[str, Any]]:
        gateway = (gateway_instance_id or "").strip()
        if not gateway:
            return []
        query: Dict[str, Any] = {
            "state": "running",
            "gateway_instance_id": gateway,
        }
        return [
            self._sandbox_dict_from_doc(doc)
            for doc in self.db.sandboxes.find(query)
            .sort([("is_warm_pool", -1), ("created_at", -1)])
            .limit(int(limit))
        ]

    def insert_sandbox_snapshot(
        self,
        snapshot_id: str,
        source_sandbox_id: str,
        image_ref: str,
        label: Optional[str],
        owner_client_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        doc = {
            "_id": snapshot_id,
            "snapshot_id": snapshot_id,
            "source_sandbox_id": source_sandbox_id,
            "image_ref": image_ref,
            "label": label or "",
            "created_at": now,
            "owner_client_id": owner_client_id,
        }
        self.db.sandbox_snapshots.insert_one(doc)
        return {
            "snapshot_id": snapshot_id,
            "source_sandbox_id": source_sandbox_id,
            "image_ref": image_ref,
            "label": label or "",
            "created_at": now,
        }

    def list_sandbox_snapshots(
        self,
        sandbox_id: str,
        limit: int = 50,
        owner_client_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {"source_sandbox_id": sandbox_id}
        if owner_client_id:
            query["owner_client_id"] = owner_client_id
        return [
            {
                "snapshot_id": doc.get("snapshot_id"),
                "source_sandbox_id": doc.get("source_sandbox_id"),
                "image_ref": doc.get("image_ref"),
                "label": doc.get("label"),
                "created_at": doc.get("created_at"),
            }
            for doc in self.db.sandbox_snapshots.find(query).sort("created_at", -1).limit(int(limit))
        ]

    def get_sandbox_snapshot(
        self,
        snapshot_id: str,
        owner_client_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        query: Dict[str, Any] = {"_id": snapshot_id}
        if owner_client_id:
            query["owner_client_id"] = owner_client_id
        doc = self.db.sandbox_snapshots.find_one(query)
        if not doc:
            return None
        return {
            "snapshot_id": doc.get("snapshot_id"),
            "source_sandbox_id": doc.get("source_sandbox_id"),
            "image_ref": doc.get("image_ref"),
            "label": doc.get("label"),
            "created_at": doc.get("created_at"),
        }

    def delete_sandbox_snapshot(
        self,
        snapshot_id: str,
        owner_client_id: Optional[str] = None,
    ) -> bool:
        query: Dict[str, Any] = {"_id": snapshot_id}
        if owner_client_id:
            query["owner_client_id"] = owner_client_id
        res = self.db.sandbox_snapshots.delete_one(query)
        return int(getattr(res, "deleted_count", 0) or 0) > 0

    def list_all_sandbox_snapshots(
        self,
        limit: int = 100,
        owner_client_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {}
        if owner_client_id:
            query["owner_client_id"] = owner_client_id
        return [
            {
                "snapshot_id": doc.get("snapshot_id"),
                "source_sandbox_id": doc.get("source_sandbox_id"),
                "image_ref": doc.get("image_ref"),
                "label": doc.get("label"),
                "created_at": doc.get("created_at"),
            }
            for doc in self.db.sandbox_snapshots.find(query).sort("created_at", -1).limit(int(limit))
        ]

    def upsert_sandbox_template(
        self,
        template_id: str,
        base_image: str,
        env: Optional[Dict[str, Any]] = None,
        start_cmd: str = "",
        settle_seconds: int = 20,
        ready_cmd: str = "",
        owner_client_id: Optional[str] = None,
        owner_api_key_id: Optional[str] = None,
        template_alias: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        settle_seconds = max(0, min(int(settle_seconds), 600))
        ready_cmd = (ready_cmd or "").strip()
        alias = (template_alias or template_id).strip() or template_id
        self.db.sandbox_templates.update_one(
            {"_id": template_id},
            {
                "$set": {
                    "template_id": template_id,
                    "base_image": base_image,
                    "env": dict(env or {}),
                    "start_cmd": start_cmd,
                    "settle_seconds": settle_seconds,
                    "ready_cmd": ready_cmd,
                    "warm_snapshot_image": None,
                    "registry_image_ref": None,
                    "materialized_gateway_instance_id": None,
                    "build_error": None,
                    "updated_at": now,
                    "owner_client_id": owner_client_id,
                    "owner_api_key_id": owner_api_key_id,
                    "template_alias": alias,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
        return self.get_sandbox_template(template_id) or {}

    def get_sandbox_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        doc = self.db.sandbox_templates.find_one({"_id": template_id})
        return self._template_dict_from_doc(doc) if doc else None

    def merge_template_env(self, template_id: str, env_updates: Dict[str, Any]) -> bool:
        tid = (template_id or "").strip()
        updates = {str(k): v for k, v in dict(env_updates or {}).items() if str(k)}
        if not tid or not updates:
            return False
        set_values = {f"env.{key}": value for key, value in updates.items()}
        set_values["updated_at"] = _utc_now_iso()
        res = self.db.sandbox_templates.update_one({"_id": tid}, {"$set": set_values})
        return int(getattr(res, "matched_count", 0) or 0) > 0

    def get_sandbox_template_by_alias(
        self,
        client_id: str,
        template_alias: str,
    ) -> Optional[Dict[str, Any]]:
        alias = (template_alias or "").strip()
        doc = self.db.sandbox_templates.find_one({"owner_client_id": client_id, "template_alias": alias})
        return self._template_dict_from_doc(doc) if doc else None

    def delete_sandbox_template(
        self,
        template_id: str,
        owner_client_id: Optional[str] = None,
    ) -> bool:
        query: Dict[str, Any] = {"_id": template_id}
        if owner_client_id:
            query["owner_client_id"] = owner_client_id
        res = self.db.sandbox_templates.delete_one(query)
        return int(getattr(res, "deleted_count", 0) or 0) > 0

    def get_best_sandbox_template_by_alias(
        self,
        template_alias: str,
        *,
        owner_client_id: Optional[str] = None,
        exclude_template_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        alias = (template_alias or "").strip()
        if not alias:
            return None
        query: Dict[str, Any] = {"template_alias": alias}
        if exclude_template_id:
            query["template_id"] = {"$ne": exclude_template_id}
        if owner_client_id:
            query["$or"] = [
                {"owner_client_id": owner_client_id},
                {"owner_client_id": None},
                {"owner_client_id": ""},
                {"owner_client_id": {"$exists": False}},
            ]
        candidates = list(self.db.sandbox_templates.find(query).sort("updated_at", -1).limit(100))
        if not candidates:
            return None

        def rank(doc: Dict[str, Any]) -> tuple[int, int]:
            has_image = 0 if (doc.get("warm_snapshot_image") or doc.get("registry_image_ref")) else 1
            owner_rank = 0 if owner_client_id and doc.get("owner_client_id") == owner_client_id else 1
            return (has_image, owner_rank)

        selected = sorted(candidates, key=rank)[0]
        return self._template_dict_from_doc(selected)

    def set_template_warm_snapshot(
        self,
        template_id: str,
        image_ref: str,
        build_error: Optional[str] = None,
        *,
        registry_image_ref: Optional[str] = None,
        materialized_gateway_instance_id: Optional[str] = None,
    ) -> bool:
        now = _utc_now_iso()
        res = self.db.sandbox_templates.update_one(
            {"_id": template_id},
            {
                "$set": {
                    "warm_snapshot_image": image_ref,
                    "registry_image_ref": registry_image_ref,
                    "materialized_gateway_instance_id": materialized_gateway_instance_id,
                    "build_error": build_error,
                    "updated_at": now,
                }
            },
        )
        return res.matched_count > 0

    def set_template_image_refs(
        self,
        template_id: str,
        *,
        warm_snapshot_image: Optional[str],
        registry_image_ref: Optional[str],
        materialized_gateway_instance_id: Optional[str],
        build_error: Optional[str] = None,
    ) -> bool:
        now = _utc_now_iso()
        res = self.db.sandbox_templates.update_one(
            {"_id": template_id},
            {
                "$set": {
                    "warm_snapshot_image": (warm_snapshot_image or "").strip() or None,
                    "registry_image_ref": (registry_image_ref or "").strip() or None,
                    "materialized_gateway_instance_id": (materialized_gateway_instance_id or "").strip() or None,
                    "build_error": build_error,
                    "updated_at": now,
                }
            },
        )
        return int(getattr(res, "matched_count", 0) or 0) > 0

    def set_template_build_error(self, template_id: str, message: str) -> bool:
        now = _utc_now_iso()
        res = self.db.sandbox_templates.update_one(
            {"_id": template_id},
            {"$set": {"warm_snapshot_image": None, "build_error": message, "updated_at": now}},
        )
        return res.matched_count > 0

    def set_template_build_source(
        self,
        template_id: str,
        *,
        source_kind: str,
        source_build_mode: str,
        dockerfile_text: Optional[str],
        build_args: Optional[Dict[str, str]],
        context_tar_gzip_base64: Optional[str],
    ) -> bool:
        now = _utc_now_iso()
        res = self.db.sandbox_templates.update_one(
            {"_id": template_id},
            {
                "$set": {
                    "source_kind": (source_kind or "").strip(),
                    "source_build_mode": (source_build_mode or "").strip(),
                    "dockerfile_text": dockerfile_text,
                    "build_args": dict(build_args or {}),
                    "context_tar_gzip_base64": context_tar_gzip_base64,
                    "updated_at": now,
                }
            },
        )
        return res.matched_count > 0

    def list_all_snapshot_image_refs(self) -> List[str]:
        refs = [
            str(doc.get("image_ref") or "").strip()
            for doc in self.db.sandbox_snapshots.find({"image_ref": {"$nin": [None, ""]}}, {"image_ref": 1})
        ]
        return [ref for ref in refs if ref]

    def list_sandbox_templates(self, owner_client_id: Optional[str] = None) -> List[Dict[str, Any]]:
        if owner_client_id:
            query: Dict[str, Any] = {"owner_client_id": owner_client_id}
            sort_key = "template_alias"
        else:
            query = {"owner_client_id": None}
            sort_key = "template_id"
        return [
            self._template_dict_from_doc(doc)
            for doc in self.db.sandbox_templates.find(query).sort(sort_key, 1)
        ]

    def list_all_sandbox_templates(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        cursor = self.db.sandbox_templates.find({}).sort("updated_at", -1)
        if limit is not None:
            cursor = cursor.limit(max(1, int(limit)))
        return [self._template_dict_from_doc(doc) for doc in cursor]

    def record_observability_event(
        self,
        *,
        severity: str,
        category: str,
        action: str,
        entity_type: str = "",
        entity_id: str = "",
        gateway_instance_id: Optional[str] = None,
        template_id: Optional[str] = None,
        sandbox_id: Optional[str] = None,
        message: str = "",
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = timestamp or _utc_now_iso()
        event_id = f"evt-{uuid.uuid4().hex[:20]}"
        doc = {
            "_id": event_id,
            "event_id": event_id,
            "timestamp": now,
            "severity": (severity or "info").strip().lower(),
            "category": (category or "").strip().lower(),
            "action": (action or "").strip().lower(),
            "entity_type": (entity_type or "").strip().lower(),
            "entity_id": (entity_id or "").strip(),
            "gateway_instance_id": (gateway_instance_id or "").strip(),
            "template_id": (template_id or "").strip(),
            "sandbox_id": (sandbox_id or "").strip(),
            "message": (message or "").strip(),
            "metadata": dict(metadata or {}),
        }
        self.db.observability_events.insert_one(doc)
        return self._observability_event_dict_from_doc(doc)

    def list_observability_events(
        self,
        *,
        limit: int = 100,
        offset: int = 0,
        severity: Optional[str] = None,
        category: Optional[str] = None,
        action: Optional[str] = None,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        gateway_instance_id: Optional[str] = None,
        template_id: Optional[str] = None,
        sandbox_id: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {}
        for key, value in (
            ("severity", severity),
            ("category", category),
            ("action", action),
            ("entity_type", entity_type),
            ("entity_id", entity_id),
            ("gateway_instance_id", gateway_instance_id),
            ("template_id", template_id),
            ("sandbox_id", sandbox_id),
        ):
            raw = (value or "").strip()
            if raw:
                query[key] = raw.lower() if key in {"severity", "category", "action", "entity_type"} else raw
        ts_filter: Dict[str, Any] = {}
        if (since or "").strip():
            ts_filter["$gte"] = since.strip()
        if (until or "").strip():
            ts_filter["$lte"] = until.strip()
        if ts_filter:
            query["timestamp"] = ts_filter
        return [
            self._observability_event_dict_from_doc(doc)
            for doc in self.db.observability_events.find(query)
            .sort("timestamp", -1)
            .skip(max(0, int(offset)))
            .limit(max(1, min(int(limit), 1000)))
        ]

    def record_observability_metric_sample(
        self,
        *,
        sample_type: str,
        metrics: Dict[str, Any],
        gateway_instance_id: Optional[str] = None,
        warm_pool_key: Optional[str] = None,
        timestamp: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = timestamp or _utc_now_iso()
        sample_id = f"obsmet-{uuid.uuid4().hex[:20]}"
        doc = {
            "_id": sample_id,
            "sample_id": sample_id,
            "timestamp": now,
            "sample_type": (sample_type or "").strip().lower(),
            "gateway_instance_id": (gateway_instance_id or "").strip(),
            "warm_pool_key": (warm_pool_key or "").strip(),
            "metrics": dict(metrics or {}),
        }
        self.db.observability_metric_samples.insert_one(doc)
        return self._observability_metric_sample_dict_from_doc(doc)

    def list_observability_metric_samples(
        self,
        *,
        sample_type: Optional[str] = None,
        gateway_instance_id: Optional[str] = None,
        warm_pool_key: Optional[str] = None,
        since: Optional[str] = None,
        until: Optional[str] = None,
        limit: int = 500,
        ascending: bool = True,
    ) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {}
        if (sample_type or "").strip():
            query["sample_type"] = sample_type.strip().lower()
        if (gateway_instance_id or "").strip():
            query["gateway_instance_id"] = gateway_instance_id.strip()
        if (warm_pool_key or "").strip():
            query["warm_pool_key"] = warm_pool_key.strip()
        ts_filter: Dict[str, Any] = {}
        if (since or "").strip():
            ts_filter["$gte"] = since.strip()
        if (until or "").strip():
            ts_filter["$lte"] = until.strip()
        if ts_filter:
            query["timestamp"] = ts_filter
        direction = 1 if ascending else -1
        return [
            self._observability_metric_sample_dict_from_doc(doc)
            for doc in self.db.observability_metric_samples.find(query)
            .sort("timestamp", direction)
            .limit(max(1, min(int(limit), 5000)))
        ]

    def purge_observability_before(self, cutoff_iso: str) -> Dict[str, int]:
        cutoff = (cutoff_iso or "").strip()
        if not cutoff:
            return {"events": 0, "metric_samples": 0}
        events = self.db.observability_events.delete_many({"timestamp": {"$lt": cutoff}})
        samples = self.db.observability_metric_samples.delete_many({"timestamp": {"$lt": cutoff}})
        return {
            "events": int(getattr(events, "deleted_count", 0) or 0),
            "metric_samples": int(getattr(samples, "deleted_count", 0) or 0),
        }

    def create_template_build(
        self,
        *,
        build_id: str,
        template_id: str,
        template_alias: str,
        owner_client_id: Optional[str],
        owner_api_key_id: Optional[str],
        requested_mode: str,
        effective_mode: str,
        status: str,
        image_tag: Optional[str] = None,
        registry_image_ref: Optional[str] = None,
        gateway_instance_id: Optional[str] = None,
        build_log: str = "",
        error_text: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        doc = {
            "_id": build_id,
            "build_id": build_id,
            "template_id": template_id,
            "template_alias": template_alias,
            "owner_client_id": owner_client_id,
            "owner_api_key_id": owner_api_key_id,
            "requested_mode": requested_mode,
            "effective_mode": effective_mode,
            "status": status,
            "image_tag": image_tag,
            "registry_image_ref": registry_image_ref,
            "gateway_instance_id": gateway_instance_id,
            "build_log": build_log,
            "error_text": error_text,
            "created_at": now,
            "updated_at": now,
            "completed_at": now if status in ("success", "failed") else None,
        }
        self.db.template_builds.insert_one(doc)
        return self.get_template_build(build_id) or {}

    def update_template_build(
        self,
        build_id: str,
        *,
        status: Optional[str] = None,
        effective_mode: Optional[str] = None,
        image_tag: Optional[str] = None,
        registry_image_ref: Optional[str] = None,
        gateway_instance_id: Optional[str] = None,
        build_log: Optional[str] = None,
        error_text: Optional[str] = None,
    ) -> bool:
        now = _utc_now_iso()
        current = self.get_template_build(build_id)
        if not current:
            return False
        next_status = status or current["status"]
        values = {
            "status": next_status,
            "effective_mode": effective_mode if effective_mode is not None else current["effective_mode"],
            "image_tag": image_tag if image_tag is not None else current["image_tag"],
            "registry_image_ref": registry_image_ref if registry_image_ref is not None else current.get("registry_image_ref"),
            "gateway_instance_id": gateway_instance_id if gateway_instance_id is not None else current.get("gateway_instance_id"),
            "build_log": build_log if build_log is not None else current["build_log"],
            "error_text": error_text if error_text is not None else current["error_text"],
            "updated_at": now,
            "completed_at": now if next_status in ("success", "failed") else current["completed_at"],
        }
        res = self.db.template_builds.update_one({"_id": build_id}, {"$set": values})
        return res.matched_count > 0

    def get_template_build(self, build_id: str) -> Optional[Dict[str, Any]]:
        doc = self.db.template_builds.find_one({"_id": build_id})
        return self._template_build_dict_from_doc(doc) if doc else None

    def list_template_builds_for_client(
        self,
        client_id: str,
        *,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        return [
            self._template_build_dict_from_doc(doc)
            for doc in self.db.template_builds.find({"owner_client_id": client_id}).sort("created_at", -1).limit(int(limit))
        ]

    def put_template_build_upload(
        self,
        owner_client_id: str,
        namespace: str,
        object_key: str,
        payload: bytes,
        *,
        content_type: str = "",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        owner = str(owner_client_id or "")
        ns = str(namespace or "")
        key = str(object_key or "")
        upload_id = _template_build_upload_id(owner, ns, key)
        data = bytes(payload or b"")
        chunks = [
            data[i : i + _UPLOAD_CHUNK_BYTES]
            for i in range(0, len(data), _UPLOAD_CHUNK_BYTES)
        ] or [b""]
        self.db.template_build_upload_chunks.delete_many({"upload_id": upload_id})
        if chunks:
            self.db.template_build_upload_chunks.insert_many(
                [
                    {
                        "_id": f"{upload_id}:{idx}",
                        "upload_id": upload_id,
                        "idx": idx,
                        "data": chunk,
                    }
                    for idx, chunk in enumerate(chunks)
                ]
            )
        self.db.template_build_uploads.update_one(
            {"owner_client_id": owner, "namespace": ns, "object_key": key},
            {
                "$set": {
                    "upload_id": upload_id,
                    "owner_client_id": owner,
                    "namespace": ns,
                    "object_key": key,
                    "content_type": content_type or "",
                    "metadata": dict(metadata or {}),
                    "size": len(data),
                    "chunk_count": len(chunks),
                    "updated_at": now,
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
        return self.get_template_build_upload(owner, ns, key) or {}

    def get_template_build_upload(
        self,
        owner_client_id: str,
        namespace: str,
        object_key: str,
    ) -> Optional[Dict[str, Any]]:
        owner = str(owner_client_id or "")
        ns = str(namespace or "")
        key = str(object_key or "")
        doc = self.db.template_build_uploads.find_one(
            {"owner_client_id": owner, "namespace": ns, "object_key": key}
        )
        if not doc:
            return None
        upload_id = str(doc.get("upload_id") or _template_build_upload_id(owner, ns, key))
        chunks = self.db.template_build_upload_chunks.find({"upload_id": upload_id}).sort("idx", 1)
        payload = b"".join(bytes(chunk.get("data") or b"") for chunk in chunks)
        return {
            "upload_id": upload_id,
            "owner_client_id": owner,
            "namespace": ns,
            "object_key": key,
            "content_type": str(doc.get("content_type") or ""),
            "payload": payload,
            "metadata": dict(doc.get("metadata") or {}),
            "created_at": doc.get("created_at"),
            "updated_at": doc.get("updated_at"),
        }

    def template_build_upload_exists(
        self,
        owner_client_id: str,
        namespace: str,
        object_key: str,
    ) -> bool:
        return bool(
            self.db.template_build_uploads.find_one(
                {
                    "owner_client_id": str(owner_client_id or ""),
                    "namespace": str(namespace or ""),
                    "object_key": str(object_key or ""),
                },
                {"_id": 1},
            )
        )

    def add_command_history(
        self,
        command_id: str,
        sandbox_id: str,
        command: str,
        exit_code: int,
        stdout: str,
        stderr: str,
        pid: int,
        execution_time: float,
    ) -> bool:
        now = _utc_now_iso()
        doc = {
            "_id": command_id,
            "command_id": command_id,
            "sandbox_id": sandbox_id,
            "command": command,
            "exit_code": exit_code,
            "stdout": stdout,
            "stderr": stderr,
            "pid": pid,
            "execution_time": execution_time,
            "created_at": now,
        }
        self.db.commands_history.insert_one(doc)
        return True

    def get_command_history(self, sandbox_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        return [
            {
                "command_id": doc.get("command_id"),
                "sandbox_id": doc.get("sandbox_id"),
                "command": doc.get("command"),
                "exit_code": doc.get("exit_code"),
                "stdout": doc.get("stdout"),
                "stderr": doc.get("stderr"),
                "pid": doc.get("pid"),
                "execution_time": doc.get("execution_time"),
                "created_at": doc.get("created_at"),
            }
            for doc in self.db.commands_history.find({"sandbox_id": sandbox_id}).sort("created_at", -1).limit(int(limit))
        ]

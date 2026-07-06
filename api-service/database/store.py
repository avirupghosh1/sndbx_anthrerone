"""Database layer for storing sandbox, template, and tenant state."""

import json
import time
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Dict, List, Optional

try:
    import psycopg
except Exception:  # noqa: BLE001
    psycopg = None


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class Database:
    """Persistent metadata store backed by PostgreSQL."""

    class _CursorProxy:
        def __init__(self, raw_cursor):
            self._raw = raw_cursor

        def execute(self, sql: str, params=None):
            rendered = self._render(sql)
            if params is None:
                return self._raw.execute(rendered)
            return self._raw.execute(rendered, params)

        def executemany(self, sql: str, params_seq):
            return self._raw.executemany(self._render(sql), params_seq)

        def _render(self, sql: str) -> str:
            return sql.replace("?", "%s")

        def __iter__(self):
            return iter(self._raw)

        def __getattr__(self, name: str):
            return getattr(self._raw, name)

    class _ConnectionProxy:
        def __init__(self, raw_conn):
            self._raw = raw_conn

        def cursor(self):
            return Database._CursorProxy(self._raw.cursor())

        def __getattr__(self, name: str):
            return getattr(self._raw, name)

    def _table_columns(self, cursor, table_name: str) -> List[str]:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = ?
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [str(r[0]) for r in cursor.fetchall()]

    def _migrate_add_runtime_column(self, cursor) -> None:
        cols = self._table_columns(cursor, "sandboxes")
        if "runtime" not in cols:
            cursor.execute(
                "ALTER TABLE sandboxes ADD COLUMN runtime TEXT NOT NULL DEFAULT 'docker'"
            )
        if "lease_expires_at" not in cols:
            cursor.execute(
                "ALTER TABLE sandboxes ADD COLUMN lease_expires_at TEXT"
            )
        if "gateway_instance_id" not in cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN gateway_instance_id TEXT")
        if "gateway_route_base" not in cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN gateway_route_base TEXT")
        if "gateway_api_base" not in cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN gateway_api_base TEXT")
        if "gateway_docker_host" not in cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN gateway_docker_host TEXT")
        if "is_warm_pool" not in cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN is_warm_pool INTEGER NOT NULL DEFAULT 0")
        if "warm_pool_key" not in cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN warm_pool_key TEXT")

    @staticmethod
    def _sandbox_dict_from_row(cursor, row) -> Dict[str, Any]:
        names = [d[0] for d in cursor.description]
        d = dict(zip(names, row))
        d["metadata"] = json.loads(d["metadata"]) if d.get("metadata") else {}
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
    def _client_dict_from_row(row: tuple) -> Dict[str, Any]:
        return {
            "client_id": row[0],
            "email": row[1],
            "password_hash": row[2],
            "display_name": row[3] or "",
            "is_active": bool(row[4]),
            "created_at": row[5],
            "updated_at": row[6],
        }

    @staticmethod
    def _api_key_dict_from_row(row: tuple) -> Dict[str, Any]:
        return {
            "key_id": row[0],
            "client_id": row[1],
            "name": row[2],
            "key_prefix": row[3],
            "key_hash": row[4],
            "created_at": row[5],
            "updated_at": row[6],
            "last_used_at": row[7],
            "revoked_at": row[8],
        }

    def __init__(self, database_url: str):
        self.database_url = (database_url or "").strip()
        if not self.database_url.startswith(("postgres://", "postgresql://")):
            raise ValueError("DATABASE_URL must be a non-empty PostgreSQL DSN")
        self._lock = Lock()
        self._advisory_lock_conns: Dict[str, Any] = {}
        self._init_db()

    def _connect(self):
        if psycopg is None:
            raise RuntimeError("psycopg is required for PostgreSQL DATABASE_URL")
        return self._ConnectionProxy(self._connect_postgres())

    def _connect_postgres(self, *, autocommit: bool = False):
        attempts = 4
        last_error = None
        for attempt in range(attempts):
            try:
                return psycopg.connect(self.database_url, autocommit=autocommit)
            except Exception as ex:  # noqa: BLE001
                last_error = ex
                text = f"{type(ex).__name__}: {ex}".lower()
                transient = any(
                    needle in text
                    for needle in (
                        "failed to resolve host",
                        "temporary failure in name resolution",
                        "connection refused",
                        "could not connect",
                        "timeout expired",
                    )
                )
                if not transient or attempt >= attempts - 1:
                    raise
                time.sleep(0.25 * (attempt + 1))
        raise last_error  # type: ignore[misc]

    def acquire_postgres_advisory_lock(self, lock_name: str) -> bool:
        name = (lock_name or "").strip()
        if not name:
            return False
        with self._lock:
            existing = self._advisory_lock_conns.get(name)
            if existing is not None:
                return True
            if psycopg is None:
                return False
            conn = self._connect_postgres(autocommit=True)
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))",
                        (name,),
                    )
                    row = cursor.fetchone()
                ok = bool(row and row[0])
                if ok:
                    self._advisory_lock_conns[name] = conn
                    return True
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                raise
            try:
                conn.close()
            except Exception:
                pass
            return False

    def _init_db(self):
        """Initialize database schema."""
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                ("sndbx_schema_migration",),
            )

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS clients (
                    client_id TEXT PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    display_name TEXT NOT NULL DEFAULT '',
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS api_keys (
                    key_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    key_prefix TEXT NOT NULL,
                    key_hash TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_used_at TEXT,
                    revoked_at TEXT,
                    FOREIGN KEY (client_id) REFERENCES clients(client_id)
                )
            """)

            # Sandboxes table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sandboxes (
                    sandbox_id TEXT PRIMARY KEY,
                    container_id TEXT UNIQUE,
                    state TEXT NOT NULL,
                    template_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    metadata TEXT,
                    cpu_limit TEXT,
                    memory_limit TEXT,
                    disk_limit TEXT,
                    timeout INTEGER,
                    lease_expires_at TEXT,
                    runtime TEXT NOT NULL DEFAULT 'docker',
                    owner_client_id TEXT,
                    owner_api_key_id TEXT,
                    is_warm_pool INTEGER NOT NULL DEFAULT 0,
                    warm_pool_key TEXT,
                    gateway_instance_id TEXT,
                    gateway_route_base TEXT,
                    gateway_api_base TEXT,
                    gateway_docker_host TEXT
                )
            """)

            self._migrate_add_runtime_column(cursor)

            # Agents table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS agents (
                    agent_id TEXT PRIMARY KEY,
                    sandbox_id TEXT NOT NULL,
                    agent_name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    config TEXT,
                    last_heartbeat TEXT,
                    pid INTEGER,
                    FOREIGN KEY (sandbox_id) REFERENCES sandboxes(sandbox_id)
                )
            """)

            # Agent messages table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS agent_messages (
                    message_id TEXT PRIMARY KEY,
                    agent_id TEXT NOT NULL,
                    sandbox_id TEXT NOT NULL,
                    message_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    processed BOOLEAN DEFAULT FALSE,
                    FOREIGN KEY (agent_id) REFERENCES agents(agent_id),
                    FOREIGN KEY (sandbox_id) REFERENCES sandboxes(sandbox_id)
                )
            """)

            # Commands history table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS commands_history (
                    command_id TEXT PRIMARY KEY,
                    sandbox_id TEXT NOT NULL,
                    command TEXT NOT NULL,
                    exit_code INTEGER,
                    stdout TEXT,
                    stderr TEXT,
                    pid INTEGER,
                    execution_time REAL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (sandbox_id) REFERENCES sandboxes(sandbox_id)
                )
            """)

            # Filesystem snapshots (Docker ``docker commit`` image refs; see docs/E2B_COMPARISON.md)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sandbox_snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    source_sandbox_id TEXT NOT NULL,
                    image_ref TEXT NOT NULL,
                    label TEXT,
                    created_at TEXT NOT NULL,
                    owner_client_id TEXT
                )
            """)

            # Logical template_id -> base image + env + start_cmd; warm_snapshot_image after one-time Docker build
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sandbox_templates (
                    template_id TEXT PRIMARY KEY,
                    template_alias TEXT NOT NULL DEFAULT '',
                    base_image TEXT NOT NULL,
                    env_json TEXT NOT NULL,
                    start_cmd TEXT NOT NULL,
                    settle_seconds INTEGER NOT NULL DEFAULT 20,
                    warm_snapshot_image TEXT,
                    registry_image_ref TEXT,
                    materialized_gateway_instance_id TEXT,
                    build_error TEXT,
                    owner_client_id TEXT,
                    owner_api_key_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS template_builds (
                    build_id TEXT PRIMARY KEY,
                    template_id TEXT NOT NULL,
                    template_alias TEXT NOT NULL DEFAULT '',
                    owner_client_id TEXT,
                    owner_api_key_id TEXT,
                    requested_mode TEXT NOT NULL DEFAULT '',
                    effective_mode TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    image_tag TEXT,
                    registry_image_ref TEXT,
                    gateway_instance_id TEXT,
                    build_log TEXT,
                    error_text TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS service_leases (
                    lease_name TEXT PRIMARY KEY,
                    owner_id TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS warm_pool_segments (
                    warm_pool_key TEXT PRIMARY KEY,
                    template_id TEXT NOT NULL,
                    cpu_limit TEXT NOT NULL,
                    memory_limit TEXT NOT NULL,
                    timeout INTEGER NOT NULL,
                    desired_size INTEGER NOT NULL DEFAULT 0,
                    inflight_count INTEGER NOT NULL DEFAULT 0,
                    inflight_updated_at TEXT,
                    handoff_count INTEGER NOT NULL DEFAULT 0,
                    failed_count INTEGER NOT NULL DEFAULT 0,
                    last_handoff_at TEXT,
                    last_refill_at TEXT,
                    ready_image_ref TEXT,
                    preferred_gateway_instance_id TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            self._migrate_templates_ready_cmd(cursor)
            self._migrate_tenant_columns(cursor)
            self._migrate_template_source_columns(cursor)
            self._migrate_warm_pool_segment_columns(cursor)
            cursor.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_sandbox_templates_owner_alias
                ON sandbox_templates(owner_client_id, template_alias)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_sandboxes_owner_client
                ON sandboxes(owner_client_id, created_at)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_api_keys_client
                ON api_keys(client_id, created_at)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_template_builds_owner_created
                ON template_builds(owner_client_id, created_at DESC)
                """
            )
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_sandboxes_gateway_instance
                ON sandboxes(gateway_instance_id, created_at)
                """
            )

            conn.commit()
            conn.close()

    def _migrate_templates_ready_cmd(self, cursor) -> None:
        cols = self._table_columns(cursor, "sandbox_templates")
        if cols and "ready_cmd" not in cols:
            cursor.execute(
                "ALTER TABLE sandbox_templates ADD COLUMN ready_cmd TEXT NOT NULL DEFAULT ''"
            )
        if cols and "registry_image_ref" not in cols:
            cursor.execute(
                "ALTER TABLE sandbox_templates ADD COLUMN registry_image_ref TEXT"
            )
        if cols and "materialized_gateway_instance_id" not in cols:
            cursor.execute(
                "ALTER TABLE sandbox_templates ADD COLUMN materialized_gateway_instance_id TEXT"
            )

    def _migrate_tenant_columns(self, cursor) -> None:
        sandbox_cols = self._table_columns(cursor, "sandboxes")
        if sandbox_cols and "owner_client_id" not in sandbox_cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN owner_client_id TEXT")
        if sandbox_cols and "owner_api_key_id" not in sandbox_cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN owner_api_key_id TEXT")
        if sandbox_cols and "disk_limit" not in sandbox_cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN disk_limit TEXT")
        if sandbox_cols and "gateway_instance_id" not in sandbox_cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN gateway_instance_id TEXT")
        if sandbox_cols and "gateway_route_base" not in sandbox_cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN gateway_route_base TEXT")
        if sandbox_cols and "gateway_api_base" not in sandbox_cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN gateway_api_base TEXT")
        if sandbox_cols and "gateway_docker_host" not in sandbox_cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN gateway_docker_host TEXT")
        if sandbox_cols and "is_warm_pool" not in sandbox_cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN is_warm_pool INTEGER NOT NULL DEFAULT 0")
        if sandbox_cols and "warm_pool_key" not in sandbox_cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN warm_pool_key TEXT")
        warm_cols = self._table_columns(cursor, "warm_pool_segments")
        if warm_cols and "inflight_count" not in warm_cols:
            cursor.execute(
                "ALTER TABLE warm_pool_segments ADD COLUMN inflight_count INTEGER NOT NULL DEFAULT 0"
            )

    def _migrate_template_source_columns(self, cursor) -> None:
        cols = self._table_columns(cursor, "sandbox_templates")
        if cols and "source_kind" not in cols:
            cursor.execute("ALTER TABLE sandbox_templates ADD COLUMN source_kind TEXT NOT NULL DEFAULT ''")
        if cols and "source_build_mode" not in cols:
            cursor.execute("ALTER TABLE sandbox_templates ADD COLUMN source_build_mode TEXT NOT NULL DEFAULT ''")
        if cols and "dockerfile_text" not in cols:
            cursor.execute("ALTER TABLE sandbox_templates ADD COLUMN dockerfile_text TEXT")
        if cols and "build_args_json" not in cols:
            cursor.execute("ALTER TABLE sandbox_templates ADD COLUMN build_args_json TEXT")
        if cols and "context_tar_gzip_base64" not in cols:
            cursor.execute("ALTER TABLE sandbox_templates ADD COLUMN context_tar_gzip_base64 TEXT")

        template_cols = self._table_columns(cursor, "sandbox_templates")
        if template_cols and "template_alias" not in template_cols:
            cursor.execute("ALTER TABLE sandbox_templates ADD COLUMN template_alias TEXT NOT NULL DEFAULT ''")
            cursor.execute("UPDATE sandbox_templates SET template_alias = template_id WHERE template_alias = ''")
        if template_cols and "owner_client_id" not in template_cols:
            cursor.execute("ALTER TABLE sandbox_templates ADD COLUMN owner_client_id TEXT")
        if template_cols and "owner_api_key_id" not in template_cols:
            cursor.execute("ALTER TABLE sandbox_templates ADD COLUMN owner_api_key_id TEXT")
        if template_cols and "registry_image_ref" not in template_cols:
            cursor.execute("ALTER TABLE sandbox_templates ADD COLUMN registry_image_ref TEXT")
        if template_cols and "materialized_gateway_instance_id" not in template_cols:
            cursor.execute("ALTER TABLE sandbox_templates ADD COLUMN materialized_gateway_instance_id TEXT")

        snap_cols = self._table_columns(cursor, "sandbox_snapshots")
        if snap_cols and "owner_client_id" not in snap_cols:
            cursor.execute("ALTER TABLE sandbox_snapshots ADD COLUMN owner_client_id TEXT")
        build_cols = self._table_columns(cursor, "template_builds")
        if build_cols and "registry_image_ref" not in build_cols:
            cursor.execute("ALTER TABLE template_builds ADD COLUMN registry_image_ref TEXT")
        if build_cols and "gateway_instance_id" not in build_cols:
            cursor.execute("ALTER TABLE template_builds ADD COLUMN gateway_instance_id TEXT")

    def _migrate_warm_pool_segment_columns(self, cursor) -> None:
        cols = self._table_columns(cursor, "warm_pool_segments")
        if cols and "inflight_count" not in cols:
            cursor.execute(
                "ALTER TABLE warm_pool_segments ADD COLUMN inflight_count INTEGER NOT NULL DEFAULT 0"
            )
        if cols and "inflight_updated_at" not in cols:
            cursor.execute(
                "ALTER TABLE warm_pool_segments ADD COLUMN inflight_updated_at TEXT"
            )
        if cols and "handoff_count" not in cols:
            cursor.execute(
                "ALTER TABLE warm_pool_segments ADD COLUMN handoff_count INTEGER NOT NULL DEFAULT 0"
            )
        if cols and "failed_count" not in cols:
            cursor.execute(
                "ALTER TABLE warm_pool_segments ADD COLUMN failed_count INTEGER NOT NULL DEFAULT 0"
            )
        if cols and "last_handoff_at" not in cols:
            cursor.execute("ALTER TABLE warm_pool_segments ADD COLUMN last_handoff_at TEXT")
        if cols and "last_refill_at" not in cols:
            cursor.execute("ALTER TABLE warm_pool_segments ADD COLUMN last_refill_at TEXT")

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
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO clients
                (client_id, email, password_hash, display_name, is_active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    client_id,
                    email.strip().lower(),
                    password_hash,
                    display_name.strip(),
                    1 if is_active else 0,
                    now,
                    now,
                ),
            )
            conn.commit()
            conn.close()
        return self.get_client(client_id) or {}

    def get_client(self, client_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM clients WHERE client_id = ?", (client_id,))
            row = cursor.fetchone()
            conn.close()
        return self._client_dict_from_row(row) if row else None

    def get_client_by_email(self, email: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM clients WHERE email = ?", (email.strip().lower(),))
            row = cursor.fetchone()
            conn.close()
        return self._client_dict_from_row(row) if row else None

    def list_clients(self) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM clients ORDER BY created_at ASC")
            rows = cursor.fetchall()
            conn.close()
        return [self._client_dict_from_row(row) for row in rows]

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
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO api_keys
                (key_id, client_id, name, key_prefix, key_hash, created_at, updated_at, last_used_at, revoked_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL)
                """,
                (key_id, client_id, name.strip(), key_prefix.strip(), key_hash, now, now),
            )
            conn.commit()
            conn.close()
        return self.get_api_key_record(key_id) or {}

    def get_api_key_record(self, key_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM api_keys WHERE key_id = ?", (key_id,))
            row = cursor.fetchone()
            conn.close()
        return self._api_key_dict_from_row(row) if row else None

    def list_api_keys_for_client(self, client_id: str, *, include_revoked: bool = False) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM api_keys WHERE client_id = ?"
        if not include_revoked:
            sql += " AND revoked_at IS NULL"
        sql += " ORDER BY created_at DESC"
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(sql, (client_id,))
            rows = cursor.fetchall()
            conn.close()
        return [self._api_key_dict_from_row(row) for row in rows]

    def get_api_key_principal(self, key_hash: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    k.key_id,
                    k.client_id,
                    k.name,
                    k.key_prefix,
                    k.key_hash,
                    k.created_at,
                    k.updated_at,
                    k.last_used_at,
                    k.revoked_at,
                    c.email,
                    c.display_name,
                    c.is_active
                FROM api_keys k
                JOIN clients c ON c.client_id = k.client_id
                WHERE k.key_hash = ?
                """,
                (key_hash,),
            )
            row = cursor.fetchone()
            conn.close()
        if not row:
            return None
        return {
            "key_id": row[0],
            "client_id": row[1],
            "name": row[2],
            "key_prefix": row[3],
            "key_hash": row[4],
            "created_at": row[5],
            "updated_at": row[6],
            "last_used_at": row[7],
            "revoked_at": row[8],
            "email": row[9],
            "display_name": row[10] or "",
            "is_active": bool(row[11]),
        }

    def touch_api_key_used(self, key_id: str) -> bool:
        now = _utc_now_iso()
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE api_keys SET last_used_at = ?, updated_at = ? WHERE key_id = ?",
                (now, now, key_id),
            )
            n = cursor.rowcount
            conn.commit()
            conn.close()
        return n > 0

    def revoke_api_key(self, key_id: str, client_id: str) -> bool:
        now = _utc_now_iso()
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE api_keys
                SET revoked_at = ?, updated_at = ?
                WHERE key_id = ? AND client_id = ? AND revoked_at IS NULL
                """,
                (now, now, key_id, client_id),
            )
            n = cursor.rowcount
            conn.commit()
            conn.close()
        return n > 0

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
        """Create sandbox record."""
        now = _utc_now_iso()
        lease_seconds = max(3600, int(timeout)) if is_warm_pool else max(60, int(timeout))
        lease_expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=lease_seconds)
        ).isoformat().replace("+00:00", "Z")
        metadata_json = json.dumps(metadata or {})

        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO sandboxes
                (sandbox_id, container_id, state, template_id, created_at, updated_at, metadata,
                 cpu_limit, memory_limit, disk_limit, timeout, lease_expires_at, runtime,
                 owner_client_id, owner_api_key_id, is_warm_pool, warm_pool_key, gateway_instance_id, gateway_route_base,
                 gateway_api_base, gateway_docker_host)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sandbox_id,
                container_id,
                (state or "running").strip() or "running",
                template_id,
                now,
                now,
                metadata_json,
                cpu_limit,
                memory_limit,
                disk_limit,
                timeout,
                lease_expires_at,
                runtime,
                owner_client_id,
                owner_api_key_id,
                1 if is_warm_pool else 0,
                (warm_pool_key or "").strip() or None,
                gateway_instance_id,
                gateway_route_base,
                gateway_api_base,
                gateway_docker_host,
            ))

            conn.commit()
            conn.close()

        return {
            "sandbox_id": sandbox_id,
            "container_id": container_id,
            "state": (state or "running").strip() or "running",
            "created_at": now,
            "updated_at": now,
            "metadata": metadata or {},
            "runtime": runtime,
            "disk_limit": disk_limit,
            "owner_client_id": owner_client_id,
            "owner_api_key_id": owner_api_key_id,
            "is_warm_pool": bool(is_warm_pool),
            "warm_pool_key": (warm_pool_key or "").strip(),
            "gateway_instance_id": gateway_instance_id or "",
            "gateway_route_base": gateway_route_base or "",
            "gateway_api_base": gateway_api_base or "",
            "gateway_docker_host": gateway_docker_host or "",
        }

    def get_sandbox(self, sandbox_id: str) -> Optional[Dict[str, Any]]:
        """Get sandbox by ID."""
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM sandboxes WHERE sandbox_id = ?", (sandbox_id,))
            row = cursor.fetchone()
            if not row:
                conn.close()
                return None
            result = self._sandbox_dict_from_row(cursor, row)
            conn.close()

        return result

    def get_sandbox_by_container(self, container_id: str) -> Optional[Dict[str, Any]]:
        """Get sandbox by container ID."""
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM sandboxes WHERE container_id = ?", (container_id,))
            row = cursor.fetchone()
            if not row:
                conn.close()
                return None
            result = self._sandbox_dict_from_row(cursor, row)
            conn.close()

        return result

    def update_sandbox_state(self, sandbox_id: str, state: str) -> bool:
        """Update sandbox state."""
        now = _utc_now_iso()

        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute(
                "UPDATE sandboxes SET state = ?, updated_at = ? WHERE sandbox_id = ?",
                (state, now, sandbox_id),
            )

            conn.commit()
            conn.close()

        return cursor.rowcount > 0

    def merge_sandbox_metadata(self, sandbox_id: str, updates: Optional[Dict[str, Any]]) -> bool:
        """Merge ``updates`` into existing JSON metadata (and strip internal ``_warm_pool`` flag)."""
        if updates is None:
            updates = {}
        now = _utc_now_iso()
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT metadata FROM sandboxes WHERE sandbox_id = ?", (sandbox_id,))
            row = cursor.fetchone()
            if not row:
                conn.close()
                return False
            cur = json.loads(row[0] or "{}")
            merged = {**cur, **updates}
            merged.pop("_warm_pool", None)
            cursor.execute(
                "UPDATE sandboxes SET metadata = ?, updated_at = ? WHERE sandbox_id = ?",
                (json.dumps(merged), now, sandbox_id),
            )
            rowcount = cursor.rowcount
            conn.commit()
            conn.close()
        return rowcount > 0

    def update_sandbox_timeout(self, sandbox_id: str, timeout_seconds: int) -> bool:
        """Update recorded sandbox lease timeout (seconds) and extend the lease from now."""
        now = _utc_now_iso()
        lease_expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=max(60, int(timeout_seconds)))
        ).isoformat().replace("+00:00", "Z")
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE sandboxes SET timeout = ?, lease_expires_at = ?, updated_at = ? WHERE sandbox_id = ?",
                (int(timeout_seconds), lease_expires_at, now, sandbox_id),
            )
            n = cursor.rowcount
            conn.commit()
            conn.close()
        return n > 0

    def assign_sandbox_owner(
        self,
        sandbox_id: str,
        *,
        owner_client_id: Optional[str],
        owner_api_key_id: Optional[str],
    ) -> bool:
        now = _utc_now_iso()
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE sandboxes
                SET owner_client_id = ?, owner_api_key_id = ?, is_warm_pool = 0, warm_pool_key = NULL, updated_at = ?
                WHERE sandbox_id = ?
                """,
                (owner_client_id, owner_api_key_id, now, sandbox_id),
            )
            n = cursor.rowcount
            conn.commit()
            conn.close()
        return n > 0

    def assign_sandbox_gateway(
        self,
        sandbox_id: str,
        *,
        gateway_instance_id: Optional[str],
        gateway_route_base: Optional[str],
        gateway_api_base: Optional[str],
        gateway_docker_host: Optional[str],
    ) -> bool:
        now = _utc_now_iso()
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE sandboxes
                SET gateway_instance_id = ?, gateway_route_base = ?, gateway_api_base = ?,
                    gateway_docker_host = ?, updated_at = ?
                WHERE sandbox_id = ?
                """,
                (
                    (gateway_instance_id or "").strip() or None,
                    (gateway_route_base or "").strip() or None,
                    (gateway_api_base or "").strip() or None,
                    (gateway_docker_host or "").strip() or None,
                    now,
                    sandbox_id,
                ),
            )
            n = cursor.rowcount
            conn.commit()
            conn.close()
        return n > 0

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
        now = _utc_now_iso()
        key = (warm_pool_key or "").strip()
        gateway = (gateway_instance_id or "").strip()
        if not key:
            return None
        updates = dict(metadata_updates or {})
        updates.pop("_warm_pool", None)
        claim_started = time.monotonic()
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT warm_pool_key
            FROM warm_pool_segments
            WHERE warm_pool_key = ?
            FOR UPDATE
            """,
            (key,),
        )
        sql = """
                SELECT *
                FROM sandboxes
                WHERE state = 'running'
                  AND is_warm_pool = 1
                  AND warm_pool_key = ?
                  {gateway_clause}
                ORDER BY created_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
        """
        if gateway:
            rendered = sql.format(gateway_clause="AND gateway_instance_id = ?")
            params = (key, gateway)
        else:
            rendered = sql.format(gateway_clause="")
            params = (key,)
        cursor.execute(rendered, params)
        row = cursor.fetchone()
        if not row:
            conn.commit()
            conn.close()
            return None
        picked = self._sandbox_dict_from_row(cursor, row)
        prev = dict(picked.get("metadata") or {})
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
        cursor.execute(
            """
            UPDATE sandboxes
            SET owner_client_id = ?, owner_api_key_id = ?, is_warm_pool = 0,
                warm_pool_key = NULL, metadata = ?, timeout = ?,
                lease_expires_at = ?, updated_at = ?
            WHERE sandbox_id = ?
            RETURNING *
            """,
            (
                owner_client_id,
                owner_api_key_id,
                json.dumps(merged),
                timeout_value,
                lease_expires_at,
                now,
                picked["sandbox_id"],
            ),
        )
        updated = cursor.fetchone()
        result = self._sandbox_dict_from_row(cursor, updated) if updated else None
        if result:
            cursor.execute(
                """
                UPDATE warm_pool_segments
                SET handoff_count = handoff_count + 1,
                    last_handoff_at = ?,
                    updated_at = ?
                WHERE warm_pool_key = ?
                """,
                (now, now, key),
            )
        conn.commit()
        conn.close()
        return result

    def list_warm_pool_sandboxes(self, *, warm_pool_key: Optional[str] = None) -> List[Dict[str, Any]]:
        conn = self._connect()
        cursor = conn.cursor()
        if (warm_pool_key or "").strip():
            cursor.execute(
                """
                SELECT * FROM sandboxes
                WHERE state = 'running'
                  AND is_warm_pool = 1
                  AND warm_pool_key = ?
                ORDER BY created_at ASC
                """,
                ((warm_pool_key or "").strip(),),
            )
        else:
            cursor.execute(
                """
                SELECT * FROM sandboxes
                WHERE state = 'running'
                  AND is_warm_pool = 1
                ORDER BY created_at ASC
                """
            )
        rows = cursor.fetchall()
        out = [self._sandbox_dict_from_row(cursor, row) for row in rows]
        conn.close()
        return out

    def upsert_warm_pool_segment(
        self,
        *,
        warm_pool_key: str,
        template_id: str,
        cpu_limit: str,
        memory_limit: str,
        timeout: int,
        desired_size: int,
        ready_image_ref: Optional[str] = None,
        preferred_gateway_instance_id: Optional[str] = None,
        last_error: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        key = (warm_pool_key or "").strip()
        if not key:
            raise ValueError("warm_pool_key is required")
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT warm_pool_key FROM warm_pool_segments WHERE warm_pool_key = ?",
                (key,),
            )
            exists = cursor.fetchone() is not None
            if exists:
                cursor.execute(
                    """
                    UPDATE warm_pool_segments
                    SET template_id = ?, cpu_limit = ?, memory_limit = ?, timeout = ?,
                        desired_size = ?, ready_image_ref = ?,
                        preferred_gateway_instance_id = COALESCE(?, preferred_gateway_instance_id),
                        last_error = ?, updated_at = ?
                    WHERE warm_pool_key = ?
                    """,
                    (
                        template_id,
                        str(cpu_limit),
                        str(memory_limit),
                        int(timeout),
                        max(0, int(desired_size)),
                        (ready_image_ref or "").strip() or None,
                        (preferred_gateway_instance_id or "").strip() or None,
                        last_error,
                        now,
                        key,
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO warm_pool_segments
                    (warm_pool_key, template_id, cpu_limit, memory_limit, timeout, desired_size, inflight_count,
                     inflight_updated_at, handoff_count, failed_count, last_handoff_at, last_refill_at,
                     ready_image_ref, preferred_gateway_instance_id, last_error, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        key,
                        template_id,
                        str(cpu_limit),
                        str(memory_limit),
                        int(timeout),
                        max(0, int(desired_size)),
                        0,
                        None,
                        0,
                        0,
                        None,
                        None,
                        (ready_image_ref or "").strip() or None,
                        (preferred_gateway_instance_id or "").strip() or None,
                        last_error,
                        now,
                        now,
                    ),
                )
            conn.commit()
            conn.close()
        return self.get_warm_pool_segment(key) or {}

    def get_warm_pool_segment(self, warm_pool_key: str) -> Optional[Dict[str, Any]]:
        key = (warm_pool_key or "").strip()
        if not key:
            return None
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM warm_pool_segments WHERE warm_pool_key = ?", (key,))
            row = cursor.fetchone()
            if not row:
                conn.close()
                return None
            names = [d[0] for d in cursor.description]
            src = dict(zip(names, row))
            conn.close()
        return {
            "warm_pool_key": src.get("warm_pool_key"),
            "template_id": src.get("template_id"),
            "cpu_limit": src.get("cpu_limit"),
            "memory_limit": src.get("memory_limit"),
            "timeout": int(src.get("timeout") or 0),
            "desired_size": int(src.get("desired_size") or 0),
            "inflight_count": int(src.get("inflight_count") or 0),
            "inflight_updated_at": src.get("inflight_updated_at"),
            "handoff_count": int(src.get("handoff_count") or 0),
            "failed_count": int(src.get("failed_count") or 0),
            "last_handoff_at": src.get("last_handoff_at"),
            "last_refill_at": src.get("last_refill_at"),
            "ready_image_ref": src.get("ready_image_ref"),
            "preferred_gateway_instance_id": src.get("preferred_gateway_instance_id"),
            "last_error": src.get("last_error"),
            "created_at": src.get("created_at"),
            "updated_at": src.get("updated_at"),
        }

    def list_warm_pool_segments(self) -> List[Dict[str, Any]]:
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM warm_pool_segments
            WHERE desired_size > 0
            ORDER BY updated_at DESC
            """
        )
        rows = cursor.fetchall()
        names = [d[0] for d in cursor.description]
        conn.close()
        out: List[Dict[str, Any]] = []
        for row in rows:
            src = dict(zip(names, row))
            out.append(
                {
                    "warm_pool_key": src.get("warm_pool_key"),
                    "template_id": src.get("template_id"),
                    "cpu_limit": src.get("cpu_limit"),
                    "memory_limit": src.get("memory_limit"),
                    "timeout": int(src.get("timeout") or 0),
                    "desired_size": int(src.get("desired_size") or 0),
                    "inflight_count": int(src.get("inflight_count") or 0),
                    "inflight_updated_at": src.get("inflight_updated_at"),
                    "handoff_count": int(src.get("handoff_count") or 0),
                    "failed_count": int(src.get("failed_count") or 0),
                    "last_handoff_at": src.get("last_handoff_at"),
                    "last_refill_at": src.get("last_refill_at"),
                    "ready_image_ref": src.get("ready_image_ref"),
                    "preferred_gateway_instance_id": src.get("preferred_gateway_instance_id"),
                    "last_error": src.get("last_error"),
                    "created_at": src.get("created_at"),
                    "updated_at": src.get("updated_at"),
                }
            )
        return out

    def reserve_warm_pool_slots(
        self,
        *,
        warm_pool_key: str,
        ready_count: int,
        batch_max: int,
    ) -> int:
        now = _utc_now_iso()
        key = (warm_pool_key or "").strip()
        if not key:
            return 0
        want = max(0, int(batch_max))
        if want <= 0:
            return 0
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT desired_size, inflight_count
            FROM warm_pool_segments
            WHERE warm_pool_key = ?
            FOR UPDATE
            """,
            (key,),
        )
        row = cursor.fetchone()
        if not row:
            conn.commit()
            conn.close()
            return 0
        desired = max(0, int(row[0] or 0))
        inflight = max(0, int(row[1] or 0))
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM sandboxes
            WHERE state = 'running'
              AND is_warm_pool = 1
              AND warm_pool_key = ?
            """,
            (key,),
        )
        ready_row = cursor.fetchone()
        ready = max(0, int((ready_row or [0])[0] or 0))
        max_useful_inflight = max(0, desired - ready)
        if inflight > max_useful_inflight:
            inflight = max_useful_inflight
            cursor.execute(
                """
                UPDATE warm_pool_segments
                SET inflight_count = ?, inflight_updated_at = ?, updated_at = ?
                WHERE warm_pool_key = ?
                """,
                (inflight, now, now, key),
            )
        reserve = max(0, min(want, desired - ready - inflight))
        if reserve > 0:
            cursor.execute(
                """
                UPDATE warm_pool_segments
                SET inflight_count = ?, inflight_updated_at = ?, last_refill_at = ?, updated_at = ?
                WHERE warm_pool_key = ?
                """,
                (inflight + reserve, now, now, now, key),
            )
        conn.commit()
        conn.close()
        return reserve

    def reset_warm_pool_inflight(self, *, warm_pool_key: str, stale_after_seconds: float) -> bool:
        """Clear stale warm-pool reservations for a segment.

        ``inflight_count`` represents create work owned by a live API process. If
        that process restarts after reserving slots, no worker remains that can
        release them. The warm-pool leader only clears reservations that have
        aged beyond ``stale_after_seconds`` so live long-running pulls do not
        get double-counted.
        """
        now = _utc_now_iso()
        key = (warm_pool_key or "").strip()
        stale_after = max(0.0, float(stale_after_seconds))
        if not key:
            return False
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=stale_after)
        ).isoformat().replace("+00:00", "Z")
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE warm_pool_segments
            SET inflight_count = 0, inflight_updated_at = NULL, updated_at = ?
            WHERE warm_pool_key = ?
              AND inflight_count <> 0
              AND COALESCE(inflight_updated_at, created_at) <= ?
            """,
            (now, key, cutoff),
        )
        n = cursor.rowcount
        conn.commit()
        conn.close()
        return n > 0

    def release_warm_pool_slots(self, *, warm_pool_key: str, count: int) -> bool:
        return self.complete_warm_pool_slots(warm_pool_key=warm_pool_key, count=count, success=True)

    def complete_warm_pool_slots(self, *, warm_pool_key: str, count: int, success: bool) -> bool:
        now = _utc_now_iso()
        key = (warm_pool_key or "").strip()
        release = max(0, int(count))
        if not key or release <= 0:
            return False
        conn = self._connect()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT inflight_count
            FROM warm_pool_segments
            WHERE warm_pool_key = ?
            FOR UPDATE
            """,
            (key,),
        )
        row = cursor.fetchone()
        if not row:
            conn.commit()
            conn.close()
            return False
        inflight = max(0, int(row[0] or 0))
        cursor.execute(
            """
            UPDATE warm_pool_segments
            SET inflight_count = ?,
                inflight_updated_at = CASE WHEN ? > 0 THEN ? ELSE NULL END,
                failed_count = failed_count + ?,
                updated_at = ?
            WHERE warm_pool_key = ?
            """,
            (
                max(0, inflight - release),
                max(0, inflight - release),
                now,
                0 if success else release,
                now,
                key,
            ),
        )
        n = cursor.rowcount
        conn.commit()
        conn.close()
        return n > 0

    def count_running_sandboxes(
        self,
        *,
        gateway_instance_id: Optional[str] = None,
        template_id: Optional[str] = None,
    ) -> int:
        gateway = (gateway_instance_id or "").strip()
        template = (template_id or "").strip()
        where = ["state = 'running'"]
        params: list[Any] = []
        if gateway:
            where.append("gateway_instance_id = ?")
            params.append(gateway)
        if template:
            where.append("template_id = ?")
            params.append(template)
        sql = f"SELECT COUNT(*) FROM sandboxes WHERE {' AND '.join(where)}"
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(sql, tuple(params))
            row = cursor.fetchone()
            conn.close()
        return int((row or [0])[0] or 0)

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
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            if clear_error:
                cursor.execute(
                    """
                    UPDATE warm_pool_segments
                    SET preferred_gateway_instance_id = ?, last_error = NULL, updated_at = ?
                    WHERE warm_pool_key = ?
                    """,
                    (((preferred_gateway_instance_id or "").strip() or None), now, key),
                )
            else:
                cursor.execute(
                    """
                    UPDATE warm_pool_segments
                    SET preferred_gateway_instance_id = ?, updated_at = ?
                    WHERE warm_pool_key = ?
                    """,
                    (((preferred_gateway_instance_id or "").strip() or None), now, key),
                )
            n = cursor.rowcount
            conn.commit()
            conn.close()
        return n > 0

    def set_warm_pool_segment_error(self, warm_pool_key: str, message: Optional[str]) -> bool:
        now = _utc_now_iso()
        key = (warm_pool_key or "").strip()
        if not key:
            return False
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE warm_pool_segments
                SET last_error = ?, updated_at = ?
                WHERE warm_pool_key = ?
                """,
                (message, now, key),
            )
            n = cursor.rowcount
            conn.commit()
            conn.close()
        return n > 0

    def acquire_service_lease(
        self,
        *,
        lease_name: str,
        owner_id: str,
        ttl_seconds: int,
    ) -> bool:
        now_dt = datetime.now(timezone.utc)
        now = now_dt.isoformat().replace("+00:00", "Z")
        expires = (now_dt + timedelta(seconds=max(5, int(ttl_seconds)))).isoformat().replace("+00:00", "Z")
        name = (lease_name or "").strip()
        owner = (owner_id or "").strip()
        if not name or not owner:
            return False
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO service_leases (lease_name, owner_id, expires_at, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (lease_name) DO UPDATE
                SET owner_id = EXCLUDED.owner_id,
                    expires_at = EXCLUDED.expires_at,
                    updated_at = EXCLUDED.updated_at
                WHERE service_leases.owner_id = EXCLUDED.owner_id
                   OR service_leases.expires_at <= ?
                RETURNING owner_id
                """,
                (name, owner, expires, now, now),
            )
            row = cursor.fetchone()
            conn.commit()
            conn.close()
            return bool(row and str(row[0] or "").strip() == owner)

    def list_expired_sandboxes(self, now_iso: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Return running sandboxes whose lease has elapsed."""
        cutoff = now_iso or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM sandboxes
                WHERE state = 'running'
                  AND lease_expires_at IS NOT NULL
                  AND lease_expires_at <= ?
                ORDER BY lease_expires_at ASC
                LIMIT ?
                """,
                (cutoff, int(limit)),
            )
            rows = cursor.fetchall()
            out = [self._sandbox_dict_from_row(cursor, row) for row in rows]
            conn.close()
        return out

    def purge_lost_sandboxes(self, older_than_seconds: int, limit: int = 100) -> int:
        """Delete sandbox rows stuck in ``state='lost'`` beyond the retention window."""
        cutoff = (
            datetime.now(timezone.utc) - timedelta(seconds=max(0, int(older_than_seconds)))
        ).isoformat().replace("+00:00", "Z")
        purged = 0
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT sandbox_id
                FROM sandboxes
                WHERE state = 'lost'
                  AND updated_at <= ?
                ORDER BY updated_at ASC
                LIMIT ?
                """,
                (cutoff, int(limit)),
            )
            sandbox_ids = [str(row[0]) for row in cursor.fetchall()]
            for sandbox_id in sandbox_ids:
                cursor.execute("DELETE FROM agent_messages WHERE sandbox_id = ?", (sandbox_id,))
                cursor.execute("DELETE FROM commands_history WHERE sandbox_id = ?", (sandbox_id,))
                cursor.execute("DELETE FROM sandbox_snapshots WHERE source_sandbox_id = ?", (sandbox_id,))
                cursor.execute("SELECT agent_id FROM agents WHERE sandbox_id = ?", (sandbox_id,))
                agent_ids = [str(row[0]) for row in cursor.fetchall()]
                for agent_id in agent_ids:
                    cursor.execute("DELETE FROM agent_messages WHERE agent_id = ?", (agent_id,))
                cursor.execute("DELETE FROM agents WHERE sandbox_id = ?", (sandbox_id,))
                cursor.execute("DELETE FROM sandboxes WHERE sandbox_id = ?", (sandbox_id,))
                purged += 1
            conn.commit()
            conn.close()
        return purged

    def delete_sandbox(self, sandbox_id: str) -> bool:
        """Delete sandbox."""
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("DELETE FROM agent_messages WHERE sandbox_id = ?", (sandbox_id,))
            cursor.execute("DELETE FROM commands_history WHERE sandbox_id = ?", (sandbox_id,))
            cursor.execute("DELETE FROM sandbox_snapshots WHERE source_sandbox_id = ?", (sandbox_id,))
            cursor.execute("SELECT agent_id FROM agents WHERE sandbox_id = ?", (sandbox_id,))
            agent_ids = [str(row[0]) for row in cursor.fetchall()]
            for agent_id in agent_ids:
                cursor.execute("DELETE FROM agent_messages WHERE agent_id = ?", (agent_id,))
            cursor.execute("DELETE FROM agents WHERE sandbox_id = ?", (sandbox_id,))
            cursor.execute("DELETE FROM sandboxes WHERE sandbox_id = ?", (sandbox_id,))

            conn.commit()
            conn.close()

        return cursor.rowcount > 0

    def list_sandboxes(
        self,
        limit: int = 100,
        offset: int = 0,
        owner_client_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List all sandboxes."""
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            if owner_client_id:
                cursor.execute(
                    """
                    SELECT * FROM sandboxes
                    WHERE owner_client_id = ?
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                    """,
                    (owner_client_id, limit, offset),
                )
            else:
                cursor.execute(
                    """
                    SELECT * FROM sandboxes
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                    """,
                    (limit, offset),
                )
            rows = cursor.fetchall()
            out = [self._sandbox_dict_from_row(cursor, row) for row in rows]
            conn.close()

        return out

    def insert_sandbox_snapshot(
        self,
        snapshot_id: str,
        source_sandbox_id: str,
        image_ref: str,
        label: Optional[str],
        owner_client_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO sandbox_snapshots (snapshot_id, source_sandbox_id, image_ref, label, created_at, owner_client_id)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (snapshot_id, source_sandbox_id, image_ref, label or "", now, owner_client_id),
            )
            conn.commit()
            conn.close()
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
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            if owner_client_id:
                cursor.execute(
                    """
                    SELECT snapshot_id, source_sandbox_id, image_ref, label, created_at
                    FROM sandbox_snapshots
                    WHERE source_sandbox_id = ? AND owner_client_id = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (sandbox_id, owner_client_id, limit),
                )
            else:
                cursor.execute(
                    """
                    SELECT snapshot_id, source_sandbox_id, image_ref, label, created_at
                    FROM sandbox_snapshots
                    WHERE source_sandbox_id = ?
                    ORDER BY created_at DESC
                    LIMIT ?
                    """,
                    (sandbox_id, limit),
                )
            rows = cursor.fetchall()
            conn.close()
        return [
            {
                "snapshot_id": r[0],
                "source_sandbox_id": r[1],
                "image_ref": r[2],
                "label": r[3],
                "created_at": r[4],
            }
            for r in rows
        ]

    @staticmethod
    def _template_dict_from_row(cursor, row: tuple) -> Dict[str, Any]:
        names = [d[0] for d in cursor.description]
        src = dict(zip(names, row))
        build_args_json = src.get("build_args_json")
        return {
            "template_id": src.get("template_id"),
            "base_image": src.get("base_image"),
            "env": json.loads(src.get("env_json") or "{}"),
            "start_cmd": src.get("start_cmd") or "",
            "settle_seconds": int(src.get("settle_seconds") or 20),
            "warm_snapshot_image": src.get("warm_snapshot_image"),
            "registry_image_ref": src.get("registry_image_ref"),
            "materialized_gateway_instance_id": src.get("materialized_gateway_instance_id"),
            "build_error": src.get("build_error"),
            "created_at": src.get("created_at"),
            "updated_at": src.get("updated_at"),
            "ready_cmd": src.get("ready_cmd") or "",
            "owner_client_id": src.get("owner_client_id"),
            "owner_api_key_id": src.get("owner_api_key_id"),
            "template_alias": (src.get("template_alias") or src.get("template_id") or ""),
            "source_kind": src.get("source_kind") or "",
            "source_build_mode": src.get("source_build_mode") or "",
            "dockerfile_text": src.get("dockerfile_text"),
            "build_args": json.loads(build_args_json) if build_args_json else {},
            "context_tar_gzip_base64": src.get("context_tar_gzip_base64"),
        }

    @staticmethod
    def _template_build_dict_from_row(cursor, row: tuple) -> Dict[str, Any]:
        names = [d[0] for d in cursor.description]
        src = dict(zip(names, row))
        return {
            "build_id": src.get("build_id"),
            "template_id": src.get("template_id"),
            "template_alias": src.get("template_alias") or src.get("template_id"),
            "owner_client_id": src.get("owner_client_id"),
            "owner_api_key_id": src.get("owner_api_key_id"),
            "requested_mode": src.get("requested_mode") or "",
            "effective_mode": src.get("effective_mode") or "",
            "status": src.get("status") or "",
            "image_tag": src.get("image_tag"),
            "registry_image_ref": src.get("registry_image_ref"),
            "gateway_instance_id": src.get("gateway_instance_id"),
            "build_log": src.get("build_log") or "",
            "error_text": src.get("error_text"),
            "created_at": src.get("created_at"),
            "updated_at": src.get("updated_at"),
            "completed_at": src.get("completed_at"),
        }

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
        """Register or replace a logical template (Docker: used for one-time warm snapshot build)."""
        now = _utc_now_iso()
        env_json = json.dumps(env or {})
        settle_seconds = max(0, min(int(settle_seconds), 600))
        ready_cmd = (ready_cmd or "").strip()
        alias = (template_alias or template_id).strip() or template_id

        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT template_id FROM sandbox_templates WHERE template_id = ?", (template_id,))
            exists = cursor.fetchone() is not None
            if exists:
                cursor.execute(
                    """
                    UPDATE sandbox_templates
                    SET base_image = ?, env_json = ?, start_cmd = ?, settle_seconds = ?, ready_cmd = ?,
                        warm_snapshot_image = NULL, registry_image_ref = NULL,
                        materialized_gateway_instance_id = NULL, build_error = NULL, updated_at = ?,
                        owner_client_id = ?, owner_api_key_id = ?, template_alias = ?
                    WHERE template_id = ?
                    """,
                    (
                        base_image,
                        env_json,
                        start_cmd,
                        settle_seconds,
                        ready_cmd,
                        now,
                        owner_client_id,
                        owner_api_key_id,
                        alias,
                        template_id,
                    ),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO sandbox_templates
                    (template_id, base_image, env_json, start_cmd, settle_seconds, ready_cmd,
                     warm_snapshot_image, registry_image_ref, materialized_gateway_instance_id,
                     build_error, created_at, updated_at, owner_client_id, owner_api_key_id, template_alias)
                    VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, ?, ?, ?, ?)
                    """,
                    (
                        template_id,
                        base_image,
                        env_json,
                        start_cmd,
                        settle_seconds,
                        ready_cmd,
                        now,
                        now,
                        owner_client_id,
                        owner_api_key_id,
                        alias,
                    ),
                )
            conn.commit()
            conn.close()

        return self.get_sandbox_template(template_id) or {}

    def get_sandbox_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM sandbox_templates WHERE template_id = ?", (template_id,))
            row = cursor.fetchone()
            result = self._template_dict_from_row(cursor, row) if row else None
            conn.close()
        return result

    def get_sandbox_template_by_alias(
        self,
        client_id: str,
        template_alias: str,
    ) -> Optional[Dict[str, Any]]:
        alias = (template_alias or "").strip()
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM sandbox_templates
                WHERE owner_client_id = ? AND template_alias = ?
                """,
                (client_id, alias),
            )
            row = cursor.fetchone()
            result = self._template_dict_from_row(cursor, row) if row else None
            conn.close()
        return result

    def get_best_sandbox_template_by_alias(
        self,
        template_alias: str,
        *,
        owner_client_id: Optional[str] = None,
        exclude_template_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Resolve a client-facing alias to the most usable template row.

        Template creates are tenant-owned, but sandbox creates may arrive with the
        friendly alias. Prefer the caller's materialized row over global
        auto-registration stubs so aliases never get treated as raw Docker image names.
        """
        alias = (template_alias or "").strip()
        if not alias:
            return None

        where = ["template_alias = ?"]
        params: List[Any] = [alias]
        if exclude_template_id:
            where.append("template_id <> ?")
            params.append(exclude_template_id)
        if owner_client_id:
            where.append("(owner_client_id = ? OR owner_client_id IS NULL OR owner_client_id = '')")
            params.append(owner_client_id)

        owner_rank = "CASE WHEN 1 = 1 THEN 0 ELSE 1 END"
        if owner_client_id:
            owner_rank = "CASE WHEN owner_client_id = ? THEN 0 ELSE 1 END"
            params.append(owner_client_id)

        sql = f"""
            SELECT * FROM sandbox_templates
            WHERE {' AND '.join(where)}
            ORDER BY
                CASE
                    WHEN COALESCE(NULLIF(warm_snapshot_image, ''), NULLIF(registry_image_ref, '')) IS NOT NULL
                    THEN 0 ELSE 1
                END,
                {owner_rank},
                updated_at DESC
            LIMIT 1
        """
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(sql, tuple(params))
            row = cursor.fetchone()
            result = self._template_dict_from_row(cursor, row) if row else None
            conn.close()
        return result

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
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE sandbox_templates
                SET warm_snapshot_image = ?, registry_image_ref = ?, materialized_gateway_instance_id = ?,
                    build_error = ?, updated_at = ?
                WHERE template_id = ?
                """,
                (
                    image_ref,
                    registry_image_ref,
                    materialized_gateway_instance_id,
                    build_error,
                    now,
                    template_id,
                ),
            )
            n = cursor.rowcount
            conn.commit()
            conn.close()
        return n > 0

    def set_template_build_error(self, template_id: str, message: str) -> bool:
        now = _utc_now_iso()
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE sandbox_templates
                SET warm_snapshot_image = NULL, build_error = ?, updated_at = ?
                WHERE template_id = ?
                """,
                (message, now, template_id),
            )
            n = cursor.rowcount
            conn.commit()
            conn.close()
        return n > 0

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
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE sandbox_templates
                SET source_kind = ?, source_build_mode = ?, dockerfile_text = ?,
                    build_args_json = ?, context_tar_gzip_base64 = ?, updated_at = ?
                WHERE template_id = ?
                """,
                (
                    (source_kind or "").strip(),
                    (source_build_mode or "").strip(),
                    dockerfile_text,
                    json.dumps(build_args or {}),
                    context_tar_gzip_base64,
                    now,
                    template_id,
                ),
            )
            n = cursor.rowcount
            conn.commit()
            conn.close()
        return n > 0

    def list_all_snapshot_image_refs(self) -> List[str]:
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT image_ref FROM sandbox_snapshots WHERE image_ref IS NOT NULL AND image_ref != ''")
            rows = cursor.fetchall()
            conn.close()
        return [str(row[0]).strip() for row in rows if row and str(row[0]).strip()]

    def list_sandbox_templates(self, owner_client_id: Optional[str] = None) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            if owner_client_id:
                cursor.execute(
                    """
                    SELECT * FROM sandbox_templates
                    WHERE owner_client_id = ?
                    ORDER BY COALESCE(NULLIF(template_alias, ''), template_id)
                    """,
                    (owner_client_id,),
                )
            else:
                cursor.execute(
                    """
                    SELECT * FROM sandbox_templates
                    WHERE owner_client_id IS NULL
                    ORDER BY template_id
                    """
                )
            rows = cursor.fetchall()
            out = [self._template_dict_from_row(cursor, r) for r in rows]
            conn.close()
        return out

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
        completed_at = now if status in ("success", "failed") else None
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO template_builds
                (build_id, template_id, template_alias, owner_client_id, owner_api_key_id,
                 requested_mode, effective_mode, status, image_tag, registry_image_ref,
                 gateway_instance_id, build_log, error_text, created_at, updated_at, completed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    build_id,
                    template_id,
                    template_alias,
                    owner_client_id,
                    owner_api_key_id,
                    requested_mode,
                    effective_mode,
                    status,
                    image_tag,
                    registry_image_ref,
                    gateway_instance_id,
                    build_log,
                    error_text,
                    now,
                    now,
                    completed_at,
                ),
            )
            conn.commit()
            conn.close()
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
        next_mode = effective_mode if effective_mode is not None else current["effective_mode"]
        next_image = image_tag if image_tag is not None else current["image_tag"]
        next_registry = registry_image_ref if registry_image_ref is not None else current.get("registry_image_ref")
        next_gateway_instance = (
            gateway_instance_id
            if gateway_instance_id is not None
            else current.get("gateway_instance_id")
        )
        next_log = build_log if build_log is not None else current["build_log"]
        next_error = error_text if error_text is not None else current["error_text"]
        completed_at = now if next_status in ("success", "failed") else current["completed_at"]
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE template_builds
                SET status = ?, effective_mode = ?, image_tag = ?, registry_image_ref = ?,
                    gateway_instance_id = ?, build_log = ?, error_text = ?, updated_at = ?, completed_at = ?
                WHERE build_id = ?
                """,
                (
                    next_status,
                    next_mode,
                    next_image,
                    next_registry,
                    next_gateway_instance,
                    next_log,
                    next_error,
                    now,
                    completed_at,
                    build_id,
                ),
            )
            n = cursor.rowcount
            conn.commit()
            conn.close()
        return n > 0

    def get_template_build(self, build_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM template_builds WHERE build_id = ?", (build_id,))
            row = cursor.fetchone()
            result = self._template_build_dict_from_row(cursor, row) if row else None
            conn.close()
        return result

    def list_template_builds_for_client(
        self,
        client_id: str,
        *,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT * FROM template_builds
                WHERE owner_client_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (client_id, int(limit)),
            )
            rows = cursor.fetchall()
            out = [self._template_build_dict_from_row(cursor, row) for row in rows]
            conn.close()
        return out

    def create_agent(
        self,
        agent_id: str,
        sandbox_id: str,
        agent_name: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create agent record."""
        now = _utc_now_iso()
        config_json = json.dumps(config or {})

        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO agents
                (agent_id, sandbox_id, agent_name, state, created_at, updated_at, config, last_heartbeat)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (agent_id, sandbox_id, agent_name, "running", now, now, config_json, now))

            conn.commit()
            conn.close()

        return {
            "agent_id": agent_id,
            "sandbox_id": sandbox_id,
            "agent_name": agent_name,
            "state": "running",
            "created_at": now,
            "updated_at": now,
            "config": config or {},
            "last_heartbeat": now,
        }

    def get_agent(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Get agent by ID."""
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,))
            row = cursor.fetchone()
            conn.close()

        if not row:
            return None

        return {
            "agent_id": row[0],
            "sandbox_id": row[1],
            "agent_name": row[2],
            "state": row[3],
            "created_at": row[4],
            "updated_at": row[5],
            "config": json.loads(row[6]) if row[6] else {},
            "last_heartbeat": row[7],
            "pid": row[8],
        }

    def list_sandbox_agents(self, sandbox_id: str) -> List[Dict[str, Any]]:
        """List agents in sandbox."""
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM agents WHERE sandbox_id = ? ORDER BY created_at DESC", (sandbox_id,))
            rows = cursor.fetchall()
            conn.close()

        return [
            {
                "agent_id": row[0],
                "sandbox_id": row[1],
                "agent_name": row[2],
                "state": row[3],
                "created_at": row[4],
                "updated_at": row[5],
                "config": json.loads(row[6]) if row[6] else {},
                "last_heartbeat": row[7],
                "pid": row[8],
            }
            for row in rows
        ]

    def update_agent_state(self, agent_id: str, state: str) -> bool:
        """Update agent state."""
        now = _utc_now_iso()

        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute(
                "UPDATE agents SET state = ?, updated_at = ? WHERE agent_id = ?",
                (state, now, agent_id),
            )

            conn.commit()
            conn.close()

        return cursor.rowcount > 0

    def update_agent_heartbeat(self, agent_id: str) -> bool:
        """Update agent heartbeat."""
        now = _utc_now_iso()

        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute(
                "UPDATE agents SET last_heartbeat = ?, updated_at = ? WHERE agent_id = ?",
                (now, now, agent_id),
            )

            conn.commit()
            conn.close()

        return cursor.rowcount > 0

    def delete_agent(self, agent_id: str) -> bool:
        """Delete agent."""
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute("DELETE FROM agents WHERE agent_id = ?", (agent_id,))

            conn.commit()
            conn.close()

        return cursor.rowcount > 0

    def add_agent_message(
        self,
        message_id: str,
        agent_id: str,
        sandbox_id: str,
        message_type: str,
        content: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Add agent message."""
        now = _utc_now_iso()
        content_json = json.dumps(content)

        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO agent_messages
                (message_id, agent_id, sandbox_id, message_type, content, timestamp, processed)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (message_id, agent_id, sandbox_id, message_type, content_json, now, False))

            conn.commit()
            conn.close()

        return {
            "message_id": message_id,
            "agent_id": agent_id,
            "message_type": message_type,
            "content": content,
            "timestamp": now,
            "processed": False,
        }

    def get_agent_messages(self, agent_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get agent messages."""
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute(
                "SELECT * FROM agent_messages WHERE agent_id = ? ORDER BY timestamp DESC LIMIT ?",
                (agent_id, limit),
            )
            rows = cursor.fetchall()
            conn.close()

        return [
            {
                "message_id": row[0],
                "agent_id": row[1],
                "sandbox_id": row[2],
                "message_type": row[3],
                "content": json.loads(row[4]),
                "timestamp": row[5],
                "processed": bool(row[6]),
            }
            for row in rows
        ]

    def mark_message_processed(self, message_id: str) -> bool:
        """Mark message as processed."""
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute(
                "UPDATE agent_messages SET processed = TRUE WHERE message_id = ?",
                (message_id,),
            )

            conn.commit()
            conn.close()

        return cursor.rowcount > 0

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
        """Add command to history."""
        now = _utc_now_iso()

        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO commands_history
                (command_id, sandbox_id, command, exit_code, stdout, stderr, pid, execution_time, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (command_id, sandbox_id, command, exit_code, stdout, stderr, pid, execution_time, now))

            conn.commit()
            conn.close()

        return cursor.rowcount > 0

    def get_command_history(self, sandbox_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get command history for sandbox."""
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute(
                "SELECT * FROM commands_history WHERE sandbox_id = ? ORDER BY created_at DESC LIMIT ?",
                (sandbox_id, limit),
            )
            rows = cursor.fetchall()
            conn.close()

        return [
            {
                "command_id": row[0],
                "sandbox_id": row[1],
                "command": row[2],
                "exit_code": row[3],
                "stdout": row[4],
                "stderr": row[5],
                "pid": row[6],
                "execution_time": row[7],
                "created_at": row[8],
            }
            for row in rows
        ]

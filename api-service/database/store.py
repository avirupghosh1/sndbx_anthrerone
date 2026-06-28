"""Database layer for storing sandbox, template, and tenant state."""

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Any, Dict, List, Optional


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class Database:
    """SQLite database for persistent storage."""

    SQLITE_BUSY_TIMEOUT_MS = 30_000

    @staticmethod
    def _migrate_add_runtime_column(cursor) -> None:
        cursor.execute("PRAGMA table_info(sandboxes)")
        cols = [r[1] for r in cursor.fetchall()]
        if "runtime" not in cols:
            cursor.execute(
                "ALTER TABLE sandboxes ADD COLUMN runtime TEXT NOT NULL DEFAULT 'docker'"
            )
        if "lease_expires_at" not in cols:
            cursor.execute(
                "ALTER TABLE sandboxes ADD COLUMN lease_expires_at TEXT"
            )

    @staticmethod
    def _sandbox_dict_from_row(cursor: sqlite3.Cursor, row) -> Dict[str, Any]:
        names = [d[0] for d in cursor.description]
        d = dict(zip(names, row))
        d["metadata"] = json.loads(d["metadata"]) if d.get("metadata") else {}
        d.setdefault("runtime", "docker")
        d.setdefault("disk_limit", "")
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

    def __init__(self, db_path: str = "sandboxes.db"):
        self.db_path = db_path
        self._lock = Lock()
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=self.SQLITE_BUSY_TIMEOUT_MS / 1000.0)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute(f"PRAGMA busy_timeout = {self.SQLITE_BUSY_TIMEOUT_MS}")
        return conn

    def _init_db(self):
        """Initialize database schema."""
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")

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
                    owner_client_id TEXT,
                    owner_api_key_id TEXT
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
                    processed BOOLEAN DEFAULT 0,
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
                    build_log TEXT,
                    error_text TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    completed_at TEXT
                )
            """)

            self._migrate_templates_ready_cmd(cursor)
            self._migrate_tenant_columns(cursor)
            self._migrate_template_source_columns(cursor)
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

            conn.commit()
            conn.close()

    @staticmethod
    def _migrate_templates_ready_cmd(cursor) -> None:
        cursor.execute("PRAGMA table_info(sandbox_templates)")
        cols = [r[1] for r in cursor.fetchall()]
        if cols and "ready_cmd" not in cols:
            cursor.execute(
                "ALTER TABLE sandbox_templates ADD COLUMN ready_cmd TEXT NOT NULL DEFAULT ''"
            )

    @staticmethod
    def _migrate_tenant_columns(cursor) -> None:
        cursor.execute("PRAGMA table_info(sandboxes)")
        sandbox_cols = [r[1] for r in cursor.fetchall()]
        if sandbox_cols and "owner_client_id" not in sandbox_cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN owner_client_id TEXT")
        if sandbox_cols and "owner_api_key_id" not in sandbox_cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN owner_api_key_id TEXT")
        if sandbox_cols and "disk_limit" not in sandbox_cols:
            cursor.execute("ALTER TABLE sandboxes ADD COLUMN disk_limit TEXT")

    @staticmethod
    def _migrate_template_source_columns(cursor) -> None:
        cursor.execute("PRAGMA table_info(sandbox_templates)")
        cols = [r[1] for r in cursor.fetchall()]
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

        cursor.execute("PRAGMA table_info(sandbox_templates)")
        template_cols = [r[1] for r in cursor.fetchall()]
        if template_cols and "template_alias" not in template_cols:
            cursor.execute("ALTER TABLE sandbox_templates ADD COLUMN template_alias TEXT NOT NULL DEFAULT ''")
            cursor.execute("UPDATE sandbox_templates SET template_alias = template_id WHERE template_alias = ''")
        if template_cols and "owner_client_id" not in template_cols:
            cursor.execute("ALTER TABLE sandbox_templates ADD COLUMN owner_client_id TEXT")
        if template_cols and "owner_api_key_id" not in template_cols:
            cursor.execute("ALTER TABLE sandbox_templates ADD COLUMN owner_api_key_id TEXT")

        cursor.execute("PRAGMA table_info(sandbox_snapshots)")
        snap_cols = [r[1] for r in cursor.fetchall()]
        if snap_cols and "owner_client_id" not in snap_cols:
            cursor.execute("ALTER TABLE sandbox_snapshots ADD COLUMN owner_client_id TEXT")

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
    ) -> Dict[str, Any]:
        """Create sandbox record."""
        now = _utc_now_iso()
        lease_expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=max(60, int(timeout)))
        ).isoformat().replace("+00:00", "Z")
        metadata_json = json.dumps(metadata or {})

        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO sandboxes
                (sandbox_id, container_id, state, template_id, created_at, updated_at, metadata,
                 cpu_limit, memory_limit, disk_limit, timeout, lease_expires_at, runtime,
                 owner_client_id, owner_api_key_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sandbox_id,
                container_id,
                "running",
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
            ))

            conn.commit()
            conn.close()

        return {
            "sandbox_id": sandbox_id,
            "container_id": container_id,
            "state": "running",
            "created_at": now,
            "updated_at": now,
            "metadata": metadata or {},
            "runtime": runtime,
            "disk_limit": disk_limit,
            "owner_client_id": owner_client_id,
            "owner_api_key_id": owner_api_key_id,
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
                SET owner_client_id = ?, owner_api_key_id = ?, updated_at = ?
                WHERE sandbox_id = ?
                """,
                (owner_client_id, owner_api_key_id, now, sandbox_id),
            )
            n = cursor.rowcount
            conn.commit()
            conn.close()
        return n > 0

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
    def _template_dict_from_row(row: tuple) -> Dict[str, Any]:
        n = len(row)
        ready_cmd = (row[9] if n > 9 else "") or ""
        owner_client_id = row[10] if n > 10 else None
        owner_api_key_id = row[11] if n > 11 else None
        template_alias = row[12] if n > 12 else row[0]
        source_kind = (row[13] if n > 13 else "") or ""
        source_build_mode = (row[14] if n > 14 else "") or ""
        dockerfile_text = row[15] if n > 15 else None
        build_args_json = row[16] if n > 16 else None
        context_tar_gzip_base64 = row[17] if n > 17 else None
        return {
            "template_id": row[0],
            "base_image": row[1],
            "env": json.loads(row[2] or "{}"),
            "start_cmd": row[3] or "",
            "settle_seconds": int(row[4] or 20),
            "warm_snapshot_image": row[5],
            "build_error": row[6],
            "created_at": row[7],
            "updated_at": row[8],
            "ready_cmd": ready_cmd,
            "owner_client_id": owner_client_id,
            "owner_api_key_id": owner_api_key_id,
            "template_alias": template_alias or row[0],
            "source_kind": source_kind,
            "source_build_mode": source_build_mode,
            "dockerfile_text": dockerfile_text,
            "build_args": json.loads(build_args_json) if build_args_json else {},
            "context_tar_gzip_base64": context_tar_gzip_base64,
        }

    @staticmethod
    def _template_build_dict_from_row(row: tuple) -> Dict[str, Any]:
        return {
            "build_id": row[0],
            "template_id": row[1],
            "template_alias": row[2] or row[1],
            "owner_client_id": row[3],
            "owner_api_key_id": row[4],
            "requested_mode": row[5] or "",
            "effective_mode": row[6] or "",
            "status": row[7] or "",
            "image_tag": row[8],
            "build_log": row[9] or "",
            "error_text": row[10],
            "created_at": row[11],
            "updated_at": row[12],
            "completed_at": row[13],
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
                        warm_snapshot_image = NULL, build_error = NULL, updated_at = ?,
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
                     warm_snapshot_image, build_error, created_at, updated_at, owner_client_id,
                     owner_api_key_id, template_alias)
                    VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?)
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
            conn.close()
        if not row:
            return None
        return self._template_dict_from_row(row)

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
            conn.close()
        if row:
            return self._template_dict_from_row(row)
        return None

    def set_template_warm_snapshot(
        self,
        template_id: str,
        image_ref: str,
        build_error: Optional[str] = None,
    ) -> bool:
        now = _utc_now_iso()
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE sandbox_templates
                SET warm_snapshot_image = ?, build_error = ?, updated_at = ?
                WHERE template_id = ?
                """,
                (image_ref, build_error, now, template_id),
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
            conn.close()
        return [self._template_dict_from_row(r) for r in rows]

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
                 requested_mode, effective_mode, status, image_tag, build_log, error_text,
                 created_at, updated_at, completed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
        next_log = build_log if build_log is not None else current["build_log"]
        next_error = error_text if error_text is not None else current["error_text"]
        completed_at = now if next_status in ("success", "failed") else current["completed_at"]
        with self._lock:
            conn = self._connect()
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE template_builds
                SET status = ?, effective_mode = ?, image_tag = ?, build_log = ?,
                    error_text = ?, updated_at = ?, completed_at = ?
                WHERE build_id = ?
                """,
                (
                    next_status,
                    next_mode,
                    next_image,
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
            conn.close()
        return self._template_build_dict_from_row(row) if row else None

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
            conn.close()
        return [self._template_build_dict_from_row(row) for row in rows]

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
            conn = sqlite3.connect(self.db_path)
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
            conn = sqlite3.connect(self.db_path)
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
            conn = sqlite3.connect(self.db_path)
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
            conn = sqlite3.connect(self.db_path)
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
            conn = sqlite3.connect(self.db_path)
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
            conn = sqlite3.connect(self.db_path)
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
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO agent_messages
                (message_id, agent_id, sandbox_id, message_type, content, timestamp, processed)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (message_id, agent_id, sandbox_id, message_type, content_json, now, 0))

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
            conn = sqlite3.connect(self.db_path)
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
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute(
                "UPDATE agent_messages SET processed = 1 WHERE message_id = ?",
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
            conn = sqlite3.connect(self.db_path)
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
            conn = sqlite3.connect(self.db_path)
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

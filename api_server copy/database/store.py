"""Database layer for storing sandbox and agent state."""

import sqlite3
import json
from datetime import datetime
from typing import Optional, Dict, Any, List
from threading import Lock


class Database:
    """SQLite database for persistent storage."""

    @staticmethod
    def _migrate_add_runtime_column(cursor) -> None:
        cursor.execute("PRAGMA table_info(sandboxes)")
        cols = [r[1] for r in cursor.fetchall()]
        if "runtime" not in cols:
            cursor.execute(
                "ALTER TABLE sandboxes ADD COLUMN runtime TEXT NOT NULL DEFAULT 'docker'"
            )

    @staticmethod
    def _sandbox_dict_from_row(cursor: sqlite3.Cursor, row) -> Dict[str, Any]:
        names = [d[0] for d in cursor.description]
        d = dict(zip(names, row))
        d["metadata"] = json.loads(d["metadata"]) if d.get("metadata") else {}
        d.setdefault("runtime", "docker")
        return d

    def __init__(self, db_path: str = "sandboxes.db"):
        self.db_path = db_path
        self._lock = Lock()
        self._init_db()

    def _init_db(self):
        """Initialize database schema."""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

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
                    timeout INTEGER
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
                    created_at TEXT NOT NULL
                )
            """)

            # Logical template_id -> base image + env + start_cmd; warm_snapshot_image after one-time Docker build
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sandbox_templates (
                    template_id TEXT PRIMARY KEY,
                    base_image TEXT NOT NULL,
                    env_json TEXT NOT NULL,
                    start_cmd TEXT NOT NULL,
                    settle_seconds INTEGER NOT NULL DEFAULT 20,
                    warm_snapshot_image TEXT,
                    build_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)

            self._migrate_templates_ready_cmd(cursor)

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
    ) -> Dict[str, Any]:
        """Create sandbox record."""
        now = datetime.utcnow().isoformat() + "Z"
        metadata_json = json.dumps(metadata or {})

        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO sandboxes
                (sandbox_id, container_id, state, template_id, created_at, updated_at, metadata, cpu_limit, memory_limit, timeout, runtime)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                timeout,
                runtime,
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
        }

    def get_sandbox(self, sandbox_id: str) -> Optional[Dict[str, Any]]:
        """Get sandbox by ID."""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
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
            conn = sqlite3.connect(self.db_path)
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
        now = datetime.utcnow().isoformat() + "Z"

        with self._lock:
            conn = sqlite3.connect(self.db_path)
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
        now = datetime.utcnow().isoformat() + "Z"
        with self._lock:
            conn = sqlite3.connect(self.db_path)
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
        """Update recorded sandbox lease timeout (seconds)."""
        now = datetime.utcnow().isoformat() + "Z"
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                "UPDATE sandboxes SET timeout = ?, updated_at = ? WHERE sandbox_id = ?",
                (int(timeout_seconds), now, sandbox_id),
            )
            n = cursor.rowcount
            conn.commit()
            conn.close()
        return n > 0

    def delete_sandbox(self, sandbox_id: str) -> bool:
        """Delete sandbox."""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("DELETE FROM sandboxes WHERE sandbox_id = ?", (sandbox_id,))

            conn.commit()
            conn.close()

        return cursor.rowcount > 0

    def list_sandboxes(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        """List all sandboxes."""
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("SELECT * FROM sandboxes LIMIT ? OFFSET ?", (limit, offset))
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
    ) -> Dict[str, Any]:
        now = datetime.utcnow().isoformat() + "Z"
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO sandbox_snapshots (snapshot_id, source_sandbox_id, image_ref, label, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (snapshot_id, source_sandbox_id, image_ref, label or "", now),
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

    def list_sandbox_snapshots(self, sandbox_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
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
        }

    def upsert_sandbox_template(
        self,
        template_id: str,
        base_image: str,
        env: Optional[Dict[str, Any]] = None,
        start_cmd: str = "",
        settle_seconds: int = 20,
        ready_cmd: str = "",
    ) -> Dict[str, Any]:
        """Register or replace a logical template (Docker: used for one-time warm snapshot build)."""
        now = datetime.utcnow().isoformat() + "Z"
        env_json = json.dumps(env or {})
        settle_seconds = max(0, min(int(settle_seconds), 600))
        ready_cmd = (ready_cmd or "").strip()

        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT template_id FROM sandbox_templates WHERE template_id = ?", (template_id,))
            exists = cursor.fetchone() is not None
            if exists:
                cursor.execute(
                    """
                    UPDATE sandbox_templates
                    SET base_image = ?, env_json = ?, start_cmd = ?, settle_seconds = ?, ready_cmd = ?,
                        warm_snapshot_image = NULL, build_error = NULL, updated_at = ?
                    WHERE template_id = ?
                    """,
                    (base_image, env_json, start_cmd, settle_seconds, ready_cmd, now, template_id),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO sandbox_templates
                    (template_id, base_image, env_json, start_cmd, settle_seconds, ready_cmd,
                     warm_snapshot_image, build_error, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?)
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
                    ),
                )
            conn.commit()
            conn.close()

        return self.get_sandbox_template(template_id) or {}

    def get_sandbox_template(self, template_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM sandbox_templates WHERE template_id = ?", (template_id,))
            row = cursor.fetchone()
            conn.close()
        if not row:
            return None
        return self._template_dict_from_row(row)

    def set_template_warm_snapshot(
        self,
        template_id: str,
        image_ref: str,
        build_error: Optional[str] = None,
    ) -> bool:
        now = datetime.utcnow().isoformat() + "Z"
        with self._lock:
            conn = sqlite3.connect(self.db_path)
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
        now = datetime.utcnow().isoformat() + "Z"
        with self._lock:
            conn = sqlite3.connect(self.db_path)
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

    def list_sandbox_templates(self) -> List[Dict[str, Any]]:
        with self._lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM sandbox_templates ORDER BY template_id")
            rows = cursor.fetchall()
            conn.close()
        return [self._template_dict_from_row(r) for r in rows]

    def create_agent(
        self,
        agent_id: str,
        sandbox_id: str,
        agent_name: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create agent record."""
        now = datetime.utcnow().isoformat() + "Z"
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
        now = datetime.utcnow().isoformat() + "Z"

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
        now = datetime.utcnow().isoformat() + "Z"

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
        now = datetime.utcnow().isoformat() + "Z"
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
        now = datetime.utcnow().isoformat() + "Z"

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

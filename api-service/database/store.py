"""Database facade selecting the configured backend."""

from __future__ import annotations

import os
from typing import Optional

from .common import _normalize_database_type
from .mongo import _MongoDatabase
from .postgres import _PostgresDatabase


class Database:
    """Persistent metadata store facade for PostgreSQL or MongoDB."""

    def __new__(
        cls,
        database_url: str,
        mongodb_password: Optional[str] = None,
        database_type: Optional[str] = None,
        database_username: Optional[str] = None,
        database_password: Optional[str] = None,
    ):
        url = (database_url or "").strip()
        backend = _normalize_database_type(database_type or os.getenv("DATABASE_TYPE"), url)
        if backend == "postgres":
            return _PostgresDatabase(
                url,
                database_username=database_username,
                database_password=database_password,
            )
        return _MongoDatabase(
            url,
            mongodb_password=mongodb_password,
            database_username=database_username,
            database_password=database_password,
        )

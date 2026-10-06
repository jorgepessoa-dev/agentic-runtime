from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Any

MIGRATION_ROOT = Path(__file__).resolve().parents[3] / "migrations"


class DatabaseUnavailable(RuntimeError):
    pass


def connect(dsn: str | None = None) -> Any:
    """Open a psycopg connection without retaining provider-specific state."""
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError as exc:
        raise DatabaseUnavailable("install project dependencies to use PostgreSQL") from exc
    database_url = dsn or os.environ.get("DATABASE_URL")
    if not database_url:
        raise DatabaseUnavailable("DATABASE_URL is not configured")
    return psycopg.connect(database_url, row_factory=dict_row)


def apply_migrations(connection: Any, root: Path = MIGRATION_ROOT) -> list[str]:
    """Apply numbered SQL files once, rejecting modified applied migrations."""
    applied: list[str] = []
    with connection.transaction():
        # An established deployment's migrator owns CREATE on the runtime
        # schema, but deliberately has no CREATE privilege on the database.
        # PostgreSQL checks database CREATE even for IF NOT EXISTS.
        if not connection.execute("SELECT 1 FROM pg_namespace WHERE nspname='runtime'").fetchone():
            connection.execute("CREATE SCHEMA runtime")
        connection.execute("""CREATE TABLE IF NOT EXISTS runtime.schema_migrations (
            version text PRIMARY KEY, checksum text NOT NULL,
            applied_at timestamptz NOT NULL DEFAULT now())""")
        for schema in ("runtime", "evolution"):
            for path in sorted((root / schema).glob("[0-9][0-9][0-9][0-9]_*.sql")):
                version = f"{schema}/{path.name}"
                sql = path.read_text(encoding="utf-8")
                checksum = hashlib.sha256(sql.encode()).hexdigest()
                row = connection.execute(
                    "SELECT checksum FROM runtime.schema_migrations WHERE version=%s", (version,)
                ).fetchone()
                if row:
                    if row["checksum"] != checksum:
                        raise RuntimeError(f"applied migration checksum changed: {version}")
                    continue
                connection.execute(sql)
                connection.execute(
                    "INSERT INTO runtime.schema_migrations(version,checksum) VALUES (%s,%s)",
                    (version, checksum),
                )
                applied.append(version)
    return applied

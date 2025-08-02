from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import aiosqlite  # type: ignore
import msgpack  # type: ignore

from ..stream import Stream

__all__ = ["SqliteStream"]


class SqliteStream(Stream):
    def __init__(self, filename: str, polling_interval: float = 0.1):
        super().__init__(polling_interval)
        self.filename = filename

    async def init(self, tables: list[str]) -> None:
        async with aiosqlite.connect(self.filename) as db:
            for table in tables:
                await db.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        task_id BLOB PRIMARY KEY,
                        payload BLOB NOT NULL,
                        created_at TEXT NOT NULL
                    )
                    """
                )
            await db.commit()

    async def store(self, table: str, task_id: UUID, result: Any) -> None:
        payload = msgpack.packb(result)
        created_at = datetime.now(timezone.utc).isoformat()

        async with aiosqlite.connect(self.filename) as db:
            await db.execute(
                f"INSERT INTO {table} (task_id, payload, created_at) VALUES (?, ?, ?)",
                (task_id.bytes, payload, created_at),
            )
            await db.commit()

    async def clean(self, table: str, ttl: int) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=ttl)
        cutoff_str = cutoff.isoformat()

        async with aiosqlite.connect(self.filename) as db:
            await db.execute(
                f"DELETE FROM {table} WHERE created_at < ?",
                (cutoff_str,),
            )
            await db.commit()

    async def retrieve(self, table: str, task_ids: list[UUID]) -> dict[UUID, Any]:
        if not task_ids:
            return {}

        task_id_bytes = [task_id.bytes for task_id in task_ids]
        placeholders = ", ".join("?" for _ in task_id_bytes)

        async with aiosqlite.connect(self.filename) as db:
            async with db.execute(
                f"SELECT task_id, payload FROM {table} WHERE task_id IN ({placeholders})",
                task_id_bytes,
            ) as cursor:
                rows = await cursor.fetchall()
                return {
                    UUID(bytes=task_id_bytes): msgpack.unpackb(payload)
                    for task_id_bytes, payload in rows
                }

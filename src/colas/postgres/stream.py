from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg  # type: ignore[import-untyped]
import msgpack  # type: ignore[import-untyped]

from ..stream import Stream

__all__ = ["PostgresStream"]


class PostgresStream(Stream):
    def __init__(self, pool: asyncpg.Pool, polling_interval: float = 0.1):
        super().__init__(polling_interval)
        self._pool = pool

    async def init(self, tables: list[str]) -> None:
        async with self._pool.acquire() as connection:
            for table in tables:
                await connection.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table} (
                        task_id UUID PRIMARY KEY,
                        payload BYTEA NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL
                    )
                    """
                )

    async def store(self, table: str, task_id: UUID, result: Any) -> None:
        payload = msgpack.packb(result)
        created_at = datetime.now(timezone.utc)

        async with self._pool.acquire() as connection:
            await connection.execute(
                f"INSERT INTO {table} (task_id, payload, created_at) VALUES ($1, $2, $3)",
                task_id,
                payload,
                created_at,
            )

    async def clean(self, table: str, ttl: int) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=ttl)

        async with self._pool.acquire() as connection:
            await connection.execute(
                f"DELETE FROM {table} WHERE created_at < $1",
                cutoff,
            )

    async def retrieve(self, table: str, task_ids: list[UUID]) -> dict[UUID, Any]:
        if not task_ids:
            return {}

        async with self._pool.acquire() as connection:
            rows = await connection.fetch(
                f"SELECT task_id, payload FROM {table} WHERE task_id = ANY($1)",
                task_ids,
            )
            return {row["task_id"]: msgpack.unpackb(row["payload"]) for row in rows}

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg  # type: ignore[import-untyped]
import msgpack  # type: ignore[import-untyped]

from ..stream import Stream

__all__ = ["PostgresStream"]


class PostgresStream(Stream):
    def __init__(self, dsn: str, table_name: str, polling_interval: float = 0.1):
        super().__init__(polling_interval)
        self.dsn = dsn
        self.table_name = table_name
        self._pool: asyncpg.Pool | None = None

    async def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self.dsn)
        return self._pool

    async def init(self) -> None:
        pool = await self.pool()
        async with pool.acquire() as connection:
            await connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    task_id UUID PRIMARY KEY,
                    payload BYTEA NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL
                )
                """
            )

    async def store(self, task_id: UUID, result: Any) -> None:
        payload = msgpack.packb(result)
        created_at = datetime.now(timezone.utc)

        pool = await self.pool()
        async with pool.acquire() as connection:
            await connection.execute(
                f"INSERT INTO {self.table_name} (task_id, payload, created_at) VALUES ($1, $2, $3)",
                task_id,
                payload,
                created_at,
            )

    async def clean(self, ttl: int) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=ttl)

        pool = await self.pool()
        async with pool.acquire() as connection:
            await connection.execute(
                f"DELETE FROM {self.table_name} WHERE created_at < $1",
                cutoff,
            )

    async def retrieve(self, task_ids: list[UUID]) -> dict[UUID, Any]:
        if not task_ids:
            return {}

        pool = await self.pool()
        async with pool.acquire() as connection:
            rows = await connection.fetch(
                f"SELECT task_id, payload FROM {self.table_name} WHERE task_id = ANY($1)",
                task_ids,
            )
            return {row["task_id"]: msgpack.unpackb(row["payload"]) for row in rows}

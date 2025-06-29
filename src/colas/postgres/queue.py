from __future__ import annotations

import asyncpg  # type: ignore
import msgpack  # type: ignore

from ..queue import Queue
from ..task import Task

__all__ = ["PostgresQueue"]


class PostgresQueue(Queue):
    def __init__(self, dsn: str, queue_name: str, polling_interval: float = 0.1):
        super().__init__(polling_interval)
        self.dsn = dsn
        self.queue_name = queue_name
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
                CREATE TABLE IF NOT EXISTS {self.queue_name} (
                    position BIGSERIAL PRIMARY KEY,
                    task_id UUID NOT NULL,
                    payload BYTEA NOT NULL
                )
            """
            )

    async def push(self, task: Task) -> None:
        payload = msgpack.packb((task.name, task.args, task.kwargs))
        pool = await self.pool()
        async with pool.acquire() as connection:
            await connection.execute(
                f"INSERT INTO {self.queue_name} (task_id, payload) VALUES ($1, $2)",
                task.task_id,
                payload,
            )

    async def pop(self) -> Task | None:
        pool = await self.pool()
        async with pool.acquire() as connection:
            row = await connection.fetchrow(
                f"""
                WITH oldest AS (
                    SELECT position, task_id, payload
                    FROM {self.queue_name}
                    ORDER BY position ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM {self.queue_name}
                WHERE position IN (SELECT position FROM oldest)
                RETURNING task_id, payload
                """
            )
            if row is None:
                return None

            task_id, payload = row["task_id"], row["payload"]
            name, args, kwargs = msgpack.unpackb(payload)
            return Task(
                task_id=task_id,
                name=name,
                args=tuple(args),
                kwargs=kwargs,
            )

from __future__ import annotations

import asyncpg  # type: ignore
import msgpack  # type: ignore

from ..queue import Queue
from ..task import Task

__all__ = ["PostgresQueue"]


class PostgresQueue(Queue):
    def __init__(self, pool: asyncpg.Pool, polling_interval: float = 0.1):
        super().__init__(polling_interval)
        self._pool = pool

    async def init(self, queues: list[str]) -> None:
        async with self._pool.acquire() as connection:
            for queue in queues:
                await connection.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {queue} (
                        position BIGSERIAL PRIMARY KEY,
                        task_id UUID NOT NULL,
                        payload BYTEA NOT NULL
                    )
                """
                )

    async def push(self, queue: str, task: Task) -> None:
        payload = msgpack.packb((task.name, task.args, task.kwargs))
        async with self._pool.acquire() as connection:
            await connection.execute(
                f"INSERT INTO {queue} (task_id, payload) VALUES ($1, $2)",
                task.task_id,
                payload,
            )

    async def pop(self, queue: str) -> Task | None:
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(
                f"""
                WITH oldest AS (
                    SELECT position, task_id, payload
                    FROM {queue}
                    ORDER BY position ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                )
                DELETE FROM {queue}
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

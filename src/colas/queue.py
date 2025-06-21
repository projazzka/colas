import asyncio
from typing import AsyncGenerator
from uuid import UUID

import aiosqlite
import msgpack  # type: ignore

from .task import Task


class Queue:
    def __init__(self, filename: str, queue_name: str, polling_interval: float = 0.1):
        self.filename = filename
        self.queue_name = queue_name
        self.polling_interval = polling_interval

    async def init(self):
        async with aiosqlite.connect(self.filename) as db:
            await db.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.queue_name} (
                    position INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id BLOB NOT NULL,
                    payload BLOB NOT NULL
                )
            """
            )
            await db.commit()

    async def push(self, task: Task) -> None:
        task_id_bytes = task.task_id.bytes
        payload = msgpack.packb((task.name, task.args, task.kwargs))

        async with aiosqlite.connect(self.filename) as db:
            await db.execute(
                f"INSERT INTO {self.queue_name} (task_id, payload) VALUES (?, ?)",
                (task_id_bytes, payload),
            )
            await db.commit()

    async def pop(self) -> Task | None:
        async with aiosqlite.connect(self.filename) as db:
            async with db.execute(
                f"""
                WITH oldest AS (
                    SELECT position, task_id, payload
                    FROM {self.queue_name}
                    ORDER BY position ASC
                    LIMIT 1
                )
                DELETE FROM {self.queue_name}
                WHERE position IN (SELECT position FROM oldest)
                RETURNING task_id, payload
                """
            ) as cursor:
                row = await cursor.fetchone()
                if row is None:
                    return None

                task_id_bytes, payload = row
                task_id = UUID(bytes=task_id_bytes)
                name, args, kwargs = msgpack.unpackb(payload)
                return Task(
                    task_id=task_id,
                    name=name,
                    args=tuple(args),
                    kwargs=kwargs,
                )

    async def tasks(self) -> AsyncGenerator[Task, None]:
        while True:
            task = await self.pop()
            if task:
                yield task
            else:
                await asyncio.sleep(self.polling_interval)

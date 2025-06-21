import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator, Callable
from uuid import UUID

import aiosqlite
import msgpack  # type: ignore


@dataclass
class Task:
    task_id: UUID
    name: str
    args: tuple
    kwargs: dict


class Quincy:
    def __init__(self, filename: str):
        self.filename = filename
        self._tasks: dict[str, Callable] = {}

    async def init(self):
        self.queue = Queue(self.filename, "tasks")

    def task(self, func):
        self._tasks[func.__name__] = func

        async def wrapper(*args, **kwargs):
            return await self._execute_handler(func.__name__, *args, **kwargs)

        return wrapper

    async def _execute_handler(self, name: str, *args, **kwargs):
        return await self._tasks[name](*args, **kwargs)

    def run(self):
        pass  # For now, just a placeholder


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


class Results:
    def __init__(self, filename: str, table_name: str, polling_interval: float = 0.1):
        self.filename = filename
        self.table_name = table_name
        self.polling_interval = polling_interval

    async def init(self):
        async with aiosqlite.connect(self.filename) as db:
            await db.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    task_id BLOB PRIMARY KEY,
                    payload BLOB NOT NULL,
                    created_at TEXT NOT NULL
                )
                """
            )
            await db.commit()

    async def store(self, task_id: UUID, result: Any) -> None:
        payload = msgpack.packb(result)
        created_at = datetime.now(timezone.utc).isoformat()

        async with aiosqlite.connect(self.filename) as db:
            await db.execute(
                f"INSERT INTO {self.table_name} (task_id, payload, created_at) VALUES (?, ?, ?)",
                (task_id.bytes, payload, created_at),
            )
            await db.commit()

    async def clean(self, ttl: int) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=ttl)
        cutoff_str = cutoff.isoformat()

        async with aiosqlite.connect(self.filename) as db:
            await db.execute(
                f"DELETE FROM {self.table_name} WHERE created_at < ?",
                (cutoff_str,),
            )
            await db.commit()

    async def wait(self, task_id: UUID) -> Any:
        while True:
            results = await self.retrieve([task_id])
            if task_id in results:
                return results[task_id]
            await asyncio.sleep(self.polling_interval)

    async def retrieve(self, task_ids: list[UUID]) -> dict[UUID, Any]:
        if not task_ids:
            return {}

        task_id_bytes = [task_id.bytes for task_id in task_ids]
        placeholders = ", ".join("?" for _ in task_id_bytes)

        async with aiosqlite.connect(self.filename) as db:
            async with db.execute(
                f"SELECT task_id, payload FROM {self.table_name} WHERE task_id IN ({placeholders})",
                task_id_bytes,
            ) as cursor:
                rows = await cursor.fetchall()
                return {
                    UUID(bytes=task_id_bytes): msgpack.unpackb(payload)
                    for task_id_bytes, payload in rows
                }

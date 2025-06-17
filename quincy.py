import uuid
from typing import Callable
from uuid import UUID

import aiosqlite
import msgpack  # type: ignore


class Quincy:
    def __init__(self, filename: str):
        self.filename = filename
        self._tasks: dict[str, Callable] = {}

    async def init(self):
        self.queue = Queue(self.filename, "tasks")

    def task(self, func):
        """Decorator to register an async function as a task."""
        self._tasks[func.__name__] = func

        async def wrapper(*args, **kwargs):
            return await self._execute_handler(func.__name__, *args, **kwargs)

        return wrapper

    async def _execute_handler(self, name: str, *args, **kwargs):
        return await self._tasks[name](*args, **kwargs)

    def run(self):
        """Run the Quincy application."""
        pass  # For now, just a placeholder


class Queue:
    def __init__(self, filename: str, queue_name: str):
        self.filename = filename
        self.queue_name = queue_name

    async def init(self):
        """Initialize the queue by ensuring the table exists."""
        await self._ensure_table()

    async def _ensure_table(self):
        """Ensure the queue table exists with the correct schema."""
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

    async def push(self, task_id: UUID, name: str, *args, **kwargs) -> None:
        """Push a task to the queue.

        Args:
            task_id: UUID to identify this task
            name: Name of the task
            *args: Positional arguments for the task
            **kwargs: Keyword arguments for the task
        """
        task_id_bytes = task_id.bytes
        payload = msgpack.packb((name, args, kwargs))

        async with aiosqlite.connect(self.filename) as db:
            await db.execute(
                f"INSERT INTO {self.queue_name} (task_id, payload) VALUES (?, ?)",
                (task_id_bytes, payload),
            )
            await db.commit()

    async def pop(self) -> tuple[UUID, tuple[str, tuple, dict]] | None:
        """Pop the oldest task from the queue and delete it atomically.
        Returns a tuple of (task_id, (name, args, kwargs)) or None if queue is empty."""
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
                task_id = uuid.UUID(bytes=task_id_bytes)
                return task_id, msgpack.unpackb(payload)

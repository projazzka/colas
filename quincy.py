from typing import Callable

import aiosqlite
import msgpack  # type: ignore


class Quincy:
    def __init__(self, filename: str):
        self.filename = filename
        self._tasks: dict[str, Callable] = {}

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
                    payload BLOB NOT NULL
                )
            """
            )
            await db.commit()

    async def push(self, name: str, *args, **kwargs):
        """Push a task to the queue."""
        payload = msgpack.packb((name, args, kwargs))

        async with aiosqlite.connect(self.filename) as db:
            await db.execute(
                f"INSERT INTO {self.queue_name} (payload) VALUES (?)", (payload,)
            )
            await db.commit()

    async def pop(self) -> tuple[str, tuple, dict] | None:
        """Pop the oldest task from the queue and delete it atomically."""
        async with aiosqlite.connect(self.filename) as db:
            async with db.execute(
                f"""
                WITH oldest AS (
                    SELECT position, payload 
                    FROM {self.queue_name}
                    ORDER BY position ASC
                    LIMIT 1
                )
                DELETE FROM {self.queue_name}
                WHERE position IN (SELECT position FROM oldest)
                RETURNING payload
                """
            ) as cursor:
                row = await cursor.fetchone()
                if row is None:
                    return None

                (payload,) = row
                return msgpack.unpackb(payload)

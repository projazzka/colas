import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from typing import Any, AsyncGenerator
from uuid import UUID

import aiosqlite
import asyncpg  # type: ignore
import msgpack  # type: ignore


class Results(ABC):
    def __init__(self, polling_interval: float = 0.1):
        self.polling_interval = polling_interval

    @abstractmethod
    async def init(self) -> None: ...

    @abstractmethod
    async def store(self, task_id: UUID, result: Any) -> None: ...

    @abstractmethod
    async def clean(self, ttl: int) -> None: ...

    @abstractmethod
    async def wait(self, task_id: UUID) -> Any: ...

    @abstractmethod
    async def retrieve(self, task_ids: list[UUID]) -> dict[UUID, Any]: ...


class SqliteResults(Results):
    def __init__(self, filename: str, table_name: str, polling_interval: float = 0.1):
        super().__init__(polling_interval)
        self.filename = filename
        self.table_name = table_name

    async def init(self) -> None:
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


class PostgresResults(Results):
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

    async def wait(self, task_id: UUID) -> Any:
        while True:
            results = await self.retrieve([task_id])
            if task_id in results:
                return results[task_id]
            await asyncio.sleep(self.polling_interval)

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

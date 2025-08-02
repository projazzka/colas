from __future__ import annotations

import asyncpg  # type: ignore

__all__ = ["create_connection_pool"]


async def create_connection_pool(dsn: str, **kwargs) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn, **kwargs)
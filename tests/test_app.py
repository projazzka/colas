import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from colas import Colas
from colas.postgres.queue import PostgresQueue
from colas.postgres.stream import PostgresStream
from colas.sqlite.queue import SqliteQueue
from colas.sqlite.stream import SqliteStream


@pytest.mark.asyncio
async def test_happy_path(temp_db_file):
    # Use sqlite:// URL instead of bare file path
    app = Colas()

    @app.task
    async def mul(a: int, b: int) -> int:
        return a * b

    await app.connect(f"sqlite://{temp_db_file}")
    await app.init()
    worker_task = asyncio.create_task(app.run())

    result = await mul(2, 3)
    assert result == 6

    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_dsn_backend_selection_sqlite(temp_db_file):
    # Test sqlite:// URL with absolute path
    app_url = Colas()
    await app_url.connect(f"sqlite://{temp_db_file}")
    assert isinstance(app_url.queue, SqliteQueue)
    assert isinstance(app_url.results, SqliteStream)

    # Test sqlite:// URL with relative path
    app_rel = Colas()
    await app_rel.connect("sqlite://./test.db")
    assert isinstance(app_rel.queue, SqliteQueue)
    assert isinstance(app_rel.results, SqliteStream)


class MockConnection:
    async def execute(self, *args, **kwargs):
        pass

    async def fetch(self, *args, **kwargs):
        return []

    async def fetchrow(self, *args, **kwargs):
        return None


class MockAcquire:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class MockPool:
    def __init__(self):
        self.connection = MockConnection()

    def acquire(self):
        return MockAcquire(self.connection)


@pytest.mark.asyncio
async def test_dsn_backend_selection_postgres():
    mock_pool = MockPool()

    with patch(
        "colas.postgres.connection.create_connection_pool", return_value=mock_pool
    ):
        # Test postgresql:// URL
        app_pg = Colas()
        await app_pg.connect("postgresql://user:pass@localhost/db")
        assert isinstance(app_pg.queue, PostgresQueue)
        assert isinstance(app_pg.results, PostgresStream)

        # Test postgres:// URL (alternative format)
        app_pg2 = Colas()
        await app_pg2.connect("postgres://user:pass@localhost/db")
        assert isinstance(app_pg2.queue, PostgresQueue)
        assert isinstance(app_pg2.results, PostgresStream)

        # Test postgresql:// with port
        app_pg3 = Colas()
        await app_pg3.connect("postgresql://user:pass@localhost:5432/db")
        assert isinstance(app_pg3.queue, PostgresQueue)
        assert isinstance(app_pg3.results, PostgresStream)


@pytest.mark.asyncio
async def test_dsn_validation_errors():
    # Test unknown scheme raises ValueError
    app = Colas()
    with pytest.raises(ValueError, match="Unsupported DSN"):
        await app.connect("unknown://some/path")

    # Test missing scheme raises ValueError
    app2 = Colas()
    with pytest.raises(ValueError, match="Unsupported DSN"):
        await app2.connect("relative/path/to/file.db")

    # Test another unknown scheme
    app3 = Colas()
    with pytest.raises(ValueError, match="Unsupported DSN"):
        await app3.connect("mysql://user:pass@localhost/db")

import asyncio

import pytest

from colas import Colas
from colas.queue import PostgresQueue, SqliteQueue
from colas.results import PostgresResults, SqliteResults


@pytest.mark.asyncio
async def test_happy_path(temp_db_file):
    app = Colas(str(temp_db_file))

    @app.task
    async def mul(a: int, b: int) -> int:
        return a * b

    await app.init()
    worker_task = asyncio.create_task(app.run())

    result = await mul(2, 3)
    assert result == 6

    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass


def test_dsn_backend_selection_sqlite(temp_db_file):
    # Test file path (no scheme)
    app = Colas(str(temp_db_file))
    assert isinstance(app.queue, SqliteQueue)
    assert isinstance(app.results, SqliteResults)

    # Test sqlite:// URL with absolute path
    app_url = Colas(f"sqlite://{temp_db_file}")
    assert isinstance(app_url.queue, SqliteQueue)
    assert isinstance(app_url.results, SqliteResults)

    # Test sqlite:// URL with relative path
    app_rel = Colas("sqlite://./test.db")
    assert isinstance(app_rel.queue, SqliteQueue)
    assert isinstance(app_rel.results, SqliteResults)


def test_dsn_backend_selection_postgres():
    # Test postgresql:// URL
    app_pg = Colas("postgresql://user:pass@localhost/db")
    assert isinstance(app_pg.queue, PostgresQueue)
    assert isinstance(app_pg.results, PostgresResults)

    # Test postgres:// URL (alternative format)
    app_pg2 = Colas("postgres://user:pass@localhost/db")
    assert isinstance(app_pg2.queue, PostgresQueue)
    assert isinstance(app_pg2.results, PostgresResults)

    # Test postgresql:// with port
    app_pg3 = Colas("postgresql://user:pass@localhost:5432/db")
    assert isinstance(app_pg3.queue, PostgresQueue)
    assert isinstance(app_pg3.results, PostgresResults)


def test_dsn_parsing_edge_cases():
    # Test unknown scheme defaults to SQLite
    app_unknown = Colas("unknown://some/path")
    assert isinstance(app_unknown.queue, SqliteQueue)
    assert isinstance(app_unknown.results, SqliteResults)

    # Test empty scheme defaults to SQLite
    app_empty = Colas("relative/path/to/file.db")
    assert isinstance(app_empty.queue, SqliteQueue)
    assert isinstance(app_empty.results, SqliteResults)

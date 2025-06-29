import asyncio

import pytest

from colas import Colas
from colas.postgres.queue import PostgresQueue
from colas.postgres.results import PostgresResults
from colas.sqlite.queue import SqliteQueue
from colas.sqlite.results import SqliteResults


@pytest.mark.asyncio
async def test_happy_path(temp_db_file):
    # Use sqlite:// URL instead of bare file path
    app = Colas(f"sqlite://{temp_db_file}")

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


def test_dsn_validation_errors():
    # Test unknown scheme raises ValueError
    with pytest.raises(ValueError, match="Unsupported DSN"):
        Colas("unknown://some/path")

    # Test missing scheme raises ValueError
    with pytest.raises(ValueError, match="Unsupported DSN"):
        Colas("relative/path/to/file.db")

    # Test another unknown scheme
    with pytest.raises(ValueError, match="Unsupported DSN"):
        Colas("mysql://user:pass@localhost/db")

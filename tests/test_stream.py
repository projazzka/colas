import asyncio
import uuid
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from freezegun import freeze_time
from testcontainers.postgres import PostgresContainer  # type: ignore

from colas.postgres.stream import PostgresStream
from colas.sqlite.stream import SqliteStream
from colas.stream import Stream


@pytest.fixture
def sqlite_stream(sqlite_stream_factory) -> SqliteStream:
    return sqlite_stream_factory()


@pytest.fixture
def postgres_stream(postgres_stream_factory) -> PostgresStream:
    return postgres_stream_factory()


@pytest.fixture
def sqlite_stream_factory(temp_db_file: Path):
    def factory(
        polling_interval: float = 0.1, table_name: str = "test_stream"
    ) -> SqliteStream:
        return SqliteStream(
            str(temp_db_file), table_name, polling_interval=polling_interval
        )

    return factory


@pytest.fixture
def postgres_stream_factory(postgres_container: PostgresContainer):
    dsn = postgres_container.get_connection_url(driver=None)

    def factory(
        polling_interval: float = 0.1, table_name: str = "test_stream"
    ) -> PostgresStream:
        return PostgresStream(dsn, table_name, polling_interval=polling_interval)

    return factory


@pytest.fixture(params=["sqlite_stream", "postgres_stream"])
def implementation(request) -> Stream:
    return request.getfixturevalue(request.param)


@pytest.fixture(params=["sqlite_stream_factory", "postgres_stream_factory"])
def implementation_factory(request):
    return request.getfixturevalue(request.param)


@pytest.mark.asyncio
async def test_store_and_poll(implementation: Stream):
    stream = implementation
    await stream.init()

    task_id_1 = uuid.uuid4()
    result_1 = {"result": "success", "data": [1, 2, 3]}
    await stream.store(task_id_1, result_1)

    task_id_2 = uuid.uuid4()
    result_2 = "a simple string result"
    await stream.store(task_id_2, result_2)

    polled_stream = await stream.retrieve([task_id_1, task_id_2])
    assert len(polled_stream) == 2
    assert polled_stream[task_id_1] == result_1
    assert polled_stream[task_id_2] == result_2


@pytest.mark.asyncio
async def test_poll_non_existent(implementation: Stream):
    stream = implementation
    await stream.init()

    task_id = uuid.uuid4()
    polled_stream = await stream.retrieve([task_id])
    assert len(polled_stream) == 0


@pytest.mark.asyncio
async def test_poll_empty_list(implementation: Stream):
    stream = implementation
    await stream.init()

    polled_stream = await stream.retrieve([])
    assert len(polled_stream) == 0


@pytest.mark.asyncio
async def test_store_and_poll_mixed(implementation: Stream):
    stream = implementation
    await stream.init()

    task_id_1 = uuid.uuid4()
    result_1 = {"result": "success"}
    await stream.store(task_id_1, result_1)

    task_id_2 = uuid.uuid4()  # This one is not stored

    task_id_3 = uuid.uuid4()
    result_3 = "another result"
    await stream.store(task_id_3, result_3)

    polled_stream = await stream.retrieve([task_id_1, task_id_2, task_id_3])
    assert len(polled_stream) == 2
    assert polled_stream[task_id_1] == result_1
    assert task_id_2 not in polled_stream
    assert polled_stream[task_id_3] == result_3


@pytest.mark.asyncio
async def test_stream_isolation(implementation_factory):
    stream1 = implementation_factory(table_name="stream_1")
    stream2 = implementation_factory(table_name="stream_2")

    await stream1.init()
    await stream2.init()

    # Store a result in the first table
    task_id = uuid.uuid4()
    result = "some data"
    await stream1.store(task_id, result)

    # The second table should have no result for this task_id
    polled_stream2 = await stream2.retrieve([task_id])
    assert len(polled_stream2) == 0

    # The first table should have the result
    polled_stream1 = await stream1.retrieve([task_id])
    assert len(polled_stream1) == 1
    assert polled_stream1[task_id] == result


@pytest.mark.asyncio
async def test_clean(implementation: Stream):
    with freeze_time("2023-01-01 12:00:00") as freezer:
        stream = implementation
        await stream.init()

        # Store a result that should be cleaned
        task_id_1 = uuid.uuid4()
        await stream.store(task_id_1, "old_result")

        # Simulate time passing
        freezer.tick(timedelta(hours=2))

        # Store a result that should NOT be cleaned
        task_id_2 = uuid.uuid4()
        await stream.store(task_id_2, "new_result")

        # Clean results older than 1 hour
        await stream.clean(ttl=3600)

        # Check that the old result is gone and the new one is still there
        polled_stream = await stream.retrieve([task_id_1, task_id_2])
        assert len(polled_stream) == 1
        assert task_id_1 not in polled_stream
        assert polled_stream[task_id_2] == "new_result"


@pytest.mark.asyncio
async def test_wait_for_result_immediate(implementation: Stream):
    stream = implementation
    await stream.init()

    task_id = uuid.uuid4()
    expected_result = "the result"
    await stream.store(task_id, expected_result)

    retrieved_result = await stream.wait(task_id)
    assert retrieved_result == expected_result


@pytest.mark.asyncio
async def test_wait_for_result_with_polling(implementation_factory):
    stream = implementation_factory(polling_interval=10)
    await stream.init()
    task_id = uuid.uuid4()

    with patch("colas.stream.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        mock_sleep.side_effect = asyncio.TimeoutError("Stop waiting")

        with pytest.raises(asyncio.TimeoutError):
            await stream.wait(task_id)

        mock_sleep.assert_awaited_once_with(10)

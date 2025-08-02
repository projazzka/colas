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
    def factory(polling_interval: float = 0.1) -> SqliteStream:
        return SqliteStream(str(temp_db_file), polling_interval=polling_interval)

    return factory


@pytest.fixture
def postgres_stream_factory(postgres_container: PostgresContainer):
    dsn = postgres_container.get_connection_url(driver=None)

    def factory(polling_interval: float = 0.1) -> PostgresStream:
        return PostgresStream(dsn, polling_interval=polling_interval)

    return factory


@pytest.fixture(params=["sqlite_stream", "postgres_stream"])
def implementation(request) -> Stream:
    return request.getfixturevalue(request.param)


@pytest.fixture(params=["sqlite_stream_factory", "postgres_stream_factory"])
def implementation_factory(request):
    return request.getfixturevalue(request.param)


@pytest.mark.asyncio
async def test_store_and_poll(implementation: Stream):
    stream_impl = implementation
    await stream_impl.init(["test_stream"])

    task_id_1 = uuid.uuid4()
    result_1 = {"result": "success", "data": [1, 2, 3]}
    await stream_impl.store("test_stream", task_id_1, result_1)

    task_id_2 = uuid.uuid4()
    result_2 = "a simple string result"
    await stream_impl.store("test_stream", task_id_2, result_2)

    polled_stream = await stream_impl.retrieve("test_stream", [task_id_1, task_id_2])
    assert len(polled_stream) == 2
    assert polled_stream[task_id_1] == result_1
    assert polled_stream[task_id_2] == result_2


@pytest.mark.asyncio
async def test_poll_non_existent(implementation: Stream):
    stream_impl = implementation
    await stream_impl.init(["test_stream"])

    task_id = uuid.uuid4()
    polled_stream = await stream_impl.retrieve("test_stream", [task_id])
    assert len(polled_stream) == 0


@pytest.mark.asyncio
async def test_poll_empty_list(implementation: Stream):
    stream_impl = implementation
    await stream_impl.init(["test_stream"])

    polled_stream = await stream_impl.retrieve("test_stream", [])
    assert len(polled_stream) == 0


@pytest.mark.asyncio
async def test_store_and_poll_mixed(implementation: Stream):
    stream_impl = implementation
    await stream_impl.init(["test_stream"])

    task_id_1 = uuid.uuid4()
    result_1 = {"result": "success"}
    await stream_impl.store("test_stream", task_id_1, result_1)

    task_id_2 = uuid.uuid4()  # This one is not stored

    task_id_3 = uuid.uuid4()
    result_3 = "another result"
    await stream_impl.store("test_stream", task_id_3, result_3)

    polled_stream = await stream_impl.retrieve(
        "test_stream", [task_id_1, task_id_2, task_id_3]
    )
    assert len(polled_stream) == 2
    assert polled_stream[task_id_1] == result_1
    assert task_id_2 not in polled_stream
    assert polled_stream[task_id_3] == result_3


@pytest.mark.asyncio
async def test_stream_isolation(implementation_factory):
    stream_impl = implementation_factory()

    await stream_impl.init(["stream_1", "stream_2"])

    # Store a result in the first table
    task_id = uuid.uuid4()
    result = "some data"
    await stream_impl.store("stream_1", task_id, result)

    # The second table should have no result for this task_id
    polled_stream2 = await stream_impl.retrieve("stream_2", [task_id])
    assert len(polled_stream2) == 0

    # The first table should have the result
    polled_stream1 = await stream_impl.retrieve("stream_1", [task_id])
    assert len(polled_stream1) == 1
    assert polled_stream1[task_id] == result


@pytest.mark.asyncio
async def test_clean(implementation: Stream):
    with freeze_time("2023-01-01 12:00:00") as freezer:
        stream_impl = implementation
        await stream_impl.init(["test_stream"])

        # Store a result that should be cleaned
        task_id_1 = uuid.uuid4()
        await stream_impl.store("test_stream", task_id_1, "old_result")

        # Simulate time passing
        freezer.tick(timedelta(hours=2))

        # Store a result that should NOT be cleaned
        task_id_2 = uuid.uuid4()
        await stream_impl.store("test_stream", task_id_2, "new_result")

        # Clean results older than 1 hour
        await stream_impl.clean("test_stream", ttl=3600)

        # Check that the old result is gone and the new one is still there
        polled_stream = await stream_impl.retrieve(
            "test_stream", [task_id_1, task_id_2]
        )
        assert len(polled_stream) == 1
        assert task_id_1 not in polled_stream
        assert polled_stream[task_id_2] == "new_result"


@pytest.mark.asyncio
async def test_wait_for_result_immediate(implementation: Stream):
    stream_impl = implementation
    await stream_impl.init(["test_stream"])

    task_id = uuid.uuid4()
    expected_result = "the result"
    await stream_impl.store("test_stream", task_id, expected_result)

    retrieved_result = await stream_impl.wait("test_stream", task_id)
    assert retrieved_result == expected_result


@pytest.mark.asyncio
async def test_wait_for_result_with_polling(implementation_factory):
    stream_impl = implementation_factory(polling_interval=10)
    await stream_impl.init(["test_stream"])
    task_id = uuid.uuid4()

    with patch("colas.stream.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        mock_sleep.side_effect = asyncio.TimeoutError("Stop waiting")

        with pytest.raises(asyncio.TimeoutError):
            await stream_impl.wait("test_stream", task_id)

        mock_sleep.assert_awaited_once_with(10)

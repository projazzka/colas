import asyncio
import uuid
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from freezegun import freeze_time
from testcontainers.postgres import PostgresContainer  # type: ignore

from colas import PostgresResults, Results, SqliteResults


@pytest.fixture
def sqlite_results(sqlite_results_factory) -> SqliteResults:
    return sqlite_results_factory()


@pytest.fixture
def postgres_results(postgres_results_factory) -> PostgresResults:
    return postgres_results_factory()


@pytest.fixture
def sqlite_results_factory(temp_db_file: Path):
    def factory(
        polling_interval: float = 0.1, table_name: str = "test_results"
    ) -> SqliteResults:
        return SqliteResults(
            str(temp_db_file), table_name, polling_interval=polling_interval
        )

    return factory


@pytest.fixture
def postgres_results_factory(postgres_container: PostgresContainer):
    dsn = postgres_container.get_connection_url(driver=None)

    def factory(
        polling_interval: float = 0.1, table_name: str = "test_results"
    ) -> PostgresResults:
        return PostgresResults(dsn, table_name, polling_interval=polling_interval)

    return factory


@pytest.fixture(params=["sqlite_results", "postgres_results"])
def implementation(request) -> Results:
    return request.getfixturevalue(request.param)


@pytest.fixture(params=["sqlite_results_factory", "postgres_results_factory"])
def implementation_factory(request):
    return request.getfixturevalue(request.param)


@pytest.mark.asyncio
async def test_store_and_poll(implementation: Results):
    results = implementation
    await results.init()

    task_id_1 = uuid.uuid4()
    result_1 = {"result": "success", "data": [1, 2, 3]}
    await results.store(task_id_1, result_1)

    task_id_2 = uuid.uuid4()
    result_2 = "a simple string result"
    await results.store(task_id_2, result_2)

    polled_results = await results.retrieve([task_id_1, task_id_2])
    assert len(polled_results) == 2
    assert polled_results[task_id_1] == result_1
    assert polled_results[task_id_2] == result_2


@pytest.mark.asyncio
async def test_poll_non_existent(implementation: Results):
    results = implementation
    await results.init()

    task_id = uuid.uuid4()
    polled_results = await results.retrieve([task_id])
    assert len(polled_results) == 0


@pytest.mark.asyncio
async def test_poll_empty_list(implementation: Results):
    results = implementation
    await results.init()

    polled_results = await results.retrieve([])
    assert len(polled_results) == 0


@pytest.mark.asyncio
async def test_store_and_poll_mixed(implementation: Results):
    results = implementation
    await results.init()

    task_id_1 = uuid.uuid4()
    result_1 = {"result": "success"}
    await results.store(task_id_1, result_1)

    task_id_2 = uuid.uuid4()  # This one is not stored

    task_id_3 = uuid.uuid4()
    result_3 = "another result"
    await results.store(task_id_3, result_3)

    polled_results = await results.retrieve([task_id_1, task_id_2, task_id_3])
    assert len(polled_results) == 2
    assert polled_results[task_id_1] == result_1
    assert task_id_2 not in polled_results
    assert polled_results[task_id_3] == result_3


@pytest.mark.asyncio
async def test_results_isolation(implementation_factory):
    results1 = implementation_factory(table_name="results_1")
    results2 = implementation_factory(table_name="results_2")

    await results1.init()
    await results2.init()

    # Store a result in the first table
    task_id = uuid.uuid4()
    result = "some data"
    await results1.store(task_id, result)

    # The second table should have no result for this task_id
    polled_results2 = await results2.retrieve([task_id])
    assert len(polled_results2) == 0

    # The first table should have the result
    polled_results1 = await results1.retrieve([task_id])
    assert len(polled_results1) == 1
    assert polled_results1[task_id] == result


@pytest.mark.asyncio
async def test_clean(implementation: Results):
    with freeze_time("2023-01-01 12:00:00") as freezer:
        results = implementation
        await results.init()

        # Store a result that should be cleaned
        task_id_1 = uuid.uuid4()
        await results.store(task_id_1, "old_result")

        # Simulate time passing
        freezer.tick(timedelta(hours=2))

        # Store a result that should NOT be cleaned
        task_id_2 = uuid.uuid4()
        await results.store(task_id_2, "new_result")

        # Clean results older than 1 hour
        await results.clean(ttl=3600)

        # Check that the old result is gone and the new one is still there
        polled_results = await results.retrieve([task_id_1, task_id_2])
        assert len(polled_results) == 1
        assert task_id_1 not in polled_results
        assert polled_results[task_id_2] == "new_result"


@pytest.mark.asyncio
async def test_wait_for_result_immediate(implementation: Results):
    results = implementation
    await results.init()

    task_id = uuid.uuid4()
    expected_result = "the result"
    await results.store(task_id, expected_result)

    retrieved_result = await results.wait(task_id)
    assert retrieved_result == expected_result


@pytest.mark.asyncio
async def test_wait_for_result_with_polling(implementation_factory):
    results = implementation_factory(polling_interval=10)
    await results.init()
    task_id = uuid.uuid4()

    with patch("colas.results.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        mock_sleep.side_effect = asyncio.TimeoutError("Stop waiting")

        with pytest.raises(asyncio.TimeoutError):
            await results.wait(task_id)

        mock_sleep.assert_awaited_once_with(10)

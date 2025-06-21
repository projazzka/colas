import uuid
from datetime import timedelta

import pytest
from freezegun import freeze_time

from quincy import Results


@pytest.mark.asyncio
async def test_store_and_poll(temp_db_file):
    results = Results(str(temp_db_file), "test_results")
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
async def test_poll_non_existent(temp_db_file):
    results = Results(str(temp_db_file), "test_results")
    await results.init()

    task_id = uuid.uuid4()
    polled_results = await results.retrieve([task_id])
    assert len(polled_results) == 0


@pytest.mark.asyncio
async def test_poll_empty_list(temp_db_file):
    results = Results(str(temp_db_file), "test_results")
    await results.init()

    polled_results = await results.retrieve([])
    assert len(polled_results) == 0


@pytest.mark.asyncio
async def test_store_and_poll_mixed(temp_db_file):
    results = Results(str(temp_db_file), "test_results")
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
async def test_results_isolation(temp_db_file):
    db_file = str(temp_db_file)
    results1 = Results(db_file, "results_1")
    results2 = Results(db_file, "results_2")

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
async def test_clean(temp_db_file):
    with freeze_time("2023-01-01 12:00:00") as freezer:
        results = Results(str(temp_db_file), "test_results")
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

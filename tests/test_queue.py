import asyncio
import threading
import uuid
from pathlib import Path
from typing import Iterator
from unittest.mock import AsyncMock, patch

import pytest
from testcontainers.postgres import PostgresContainer  # type: ignore

from colas.postgres.queue import PostgresQueue
from colas.queue import Queue
from colas.sqlite.queue import SqliteQueue
from colas.task import Task


@pytest.fixture
def sqlite_queue(sqlite_queue_factory) -> SqliteQueue:
    return sqlite_queue_factory()


@pytest.fixture
def postgres_queue(postgres_queue_factory) -> PostgresQueue:
    return postgres_queue_factory()


@pytest.fixture
def sqlite_queue_factory(temp_db_file: Path):
    def factory() -> SqliteQueue:
        return SqliteQueue(str(temp_db_file), "test_queue")

    return factory


@pytest.fixture
def postgres_queue_factory(postgres_container: PostgresContainer):
    dsn = postgres_container.get_connection_url(driver=None)

    def factory() -> PostgresQueue:
        return PostgresQueue(dsn, "test_queue")

    return factory


@pytest.fixture(params=["sqlite_queue", "postgres_queue"])
def implementation(request) -> Queue:
    return request.getfixturevalue(request.param)


@pytest.fixture(params=["sqlite_queue_factory", "postgres_queue_factory"])
def implementation_factory(request):
    return request.getfixturevalue(request.param)


@pytest.mark.asyncio
async def test_push_and_pop(implementation: Queue):
    queue = implementation
    await queue.init()

    task_1 = Task(
        task_id=uuid.uuid4(), name="test_task_1", args=(1, 2), kwargs={"a": 3}
    )
    await queue.push(task_1)

    task_2 = Task(
        task_id=uuid.uuid4(), name="test_task_2", args=(4, 5), kwargs={"b": 6}
    )
    await queue.push(task_2)

    popped_task_1 = await queue.pop()
    assert popped_task_1 is not None
    assert popped_task_1.task_id == task_1.task_id
    assert popped_task_1.name == "test_task_1"
    assert popped_task_1.args == (1, 2)
    assert popped_task_1.kwargs == {"a": 3}

    popped_task_2 = await queue.pop()
    assert popped_task_2 is not None
    assert popped_task_2.task_id == task_2.task_id
    assert popped_task_2.name == "test_task_2"
    assert popped_task_2.args == (4, 5)
    assert popped_task_2.kwargs == {"b": 6}

    assert await queue.pop() is None


@pytest.mark.asyncio
async def test_pop_from_empty_queue(implementation: Queue):
    queue = implementation
    await queue.init()

    assert await queue.pop() is None


@pytest.mark.asyncio
async def test_queue_isolation(temp_db_file):
    db_file = str(temp_db_file)
    queue1 = SqliteQueue(db_file, "queue_1")
    queue2 = SqliteQueue(db_file, "queue_2")

    await queue1.init()
    await queue2.init()

    # Push to the first queue
    task = Task(task_id=uuid.uuid4(), name="test_task", args=(), kwargs={})
    await queue1.push(task)

    # The second queue should be empty
    assert await queue2.pop() is None

    # The first queue should have the task
    popped_task = await queue1.pop()
    assert popped_task is not None
    assert popped_task.task_id == task.task_id


def worker(queue_factory, results_list):
    async def pop_tasks():
        queue = queue_factory()
        while True:
            task = await queue.pop()
            if task is None:
                break
            results_list.append(task)

    asyncio.run(pop_tasks())


@pytest.mark.asyncio
async def test_threaded_concurrent_pop(implementation_factory):
    num_tasks = 100
    num_workers = 10
    queue = implementation_factory()
    await queue.init()

    # Populate the queue
    task_ids = []
    for i in range(num_tasks):
        task = Task(task_id=uuid.uuid4(), name=str(i), args=(), kwargs={})
        task_ids.append(task.task_id)
        await queue.push(task)

    results: list[Task] = []
    threads = []
    for _ in range(num_workers):
        thread = threading.Thread(target=worker, args=(implementation_factory, results))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    # Verify that all tasks were popped exactly once
    assert len(results) == num_tasks
    # Check for uniqueness of the task "name"
    popped_task_ids = {task.task_id for task in results}
    assert popped_task_ids == set(task_ids)


@pytest.mark.asyncio
async def test_tasks_generator(implementation: Queue):
    queue = implementation
    await queue.init()

    task_1 = Task(
        task_id=uuid.uuid4(), name="test_task_1", args=(1, 2), kwargs={"a": 3}
    )
    await queue.push(task_1)

    task_2 = Task(
        task_id=uuid.uuid4(), name="test_task_2", args=(4, 5), kwargs={"b": 6}
    )
    await queue.push(task_2)

    received_tasks = []
    async for task in queue.tasks():
        received_tasks.append(task)
        if len(received_tasks) == 2:
            break

    assert len(received_tasks) == 2
    assert received_tasks[0].task_id == task_1.task_id
    assert received_tasks[1].task_id == task_2.task_id


@pytest.mark.asyncio
async def test_tasks_generator_sleeps(implementation: Queue):
    polling_interval = 10.0
    implementation.polling_interval = polling_interval
    queue = implementation
    await queue.init()
    tasks_gen = queue.tasks()

    class StopLoop(Exception):
        pass

    with patch("colas.queue.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        mock_sleep.side_effect = StopLoop

        with pytest.raises(StopLoop):
            await anext(tasks_gen)

        mock_sleep.assert_awaited_once_with(polling_interval)

import asyncio
import threading
import uuid
from unittest.mock import AsyncMock, patch

import pytest

from quincy import Queue, Task


@pytest.mark.asyncio
async def test_push_and_pop(temp_db_file):
    queue = Queue(str(temp_db_file), "test_queue")
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
async def test_pop_from_empty_queue(temp_db_file):
    queue = Queue(str(temp_db_file), "test_queue")
    await queue.init()

    assert await queue.pop() is None


@pytest.mark.asyncio
async def test_queue_isolation(temp_db_file):
    db_file = str(temp_db_file)
    queue1 = Queue(db_file, "queue_1")
    queue2 = Queue(db_file, "queue_2")

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


def worker(queue, results_list):
    async def pop_tasks():
        while True:
            task = await queue.pop()
            if task is None:
                break
            results_list.append(task)

    asyncio.run(pop_tasks())


@pytest.mark.asyncio
async def test_threaded_concurrent_pop(temp_db_file):
    num_tasks = 100
    num_workers = 10
    db_file = str(temp_db_file)
    queue = Queue(db_file, "concurrent_queue")
    await queue.init()

    # Populate the queue
    task_ids = []
    for i in range(num_tasks):
        task = Task(task_id=uuid.uuid4(), name=str(i), args=(), kwargs={})
        task_ids.append(task.task_id)
        await queue.push(task)

    results = []
    threads = []
    for _ in range(num_workers):
        thread = threading.Thread(target=worker, args=(queue, results))
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
async def test_tasks_generator(temp_db_file):
    queue = Queue(str(temp_db_file), "test_queue")
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
async def test_tasks_generator_sleeps(temp_db_file):
    polling_interval = 10.0
    queue = Queue(str(temp_db_file), "test_queue", polling_interval=polling_interval)
    await queue.init()
    tasks_gen = queue.tasks()

    class StopLoop(Exception):
        pass

    with patch("quincy.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        mock_sleep.side_effect = StopLoop

        with pytest.raises(StopLoop):
            await anext(tasks_gen)

        mock_sleep.assert_awaited_once_with(polling_interval)

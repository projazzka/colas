import uuid

import pytest

from quincy import Queue


@pytest.mark.asyncio
async def test_push_and_pop(temp_db_file):
    queue = Queue(str(temp_db_file), "test_queue")
    await queue.init()

    task_id_1 = uuid.uuid4()
    await queue.push(task_id_1, "test_task_1", *[1, 2], **{"a": 3})

    task_id_2 = uuid.uuid4()
    await queue.push(task_id_2, "test_task_2", *[4, 5], **{"b": 6})

    popped_task_1 = await queue.pop()
    assert popped_task_1 is not None
    popped_task_id_1, (popped_name_1, popped_args_1, popped_kwargs_1) = popped_task_1
    assert popped_task_id_1 == task_id_1
    assert popped_name_1 == "test_task_1"
    assert popped_args_1 == [1, 2]
    assert popped_kwargs_1 == {"a": 3}

    popped_task_2 = await queue.pop()
    assert popped_task_2 is not None
    popped_task_id_2, (popped_name_2, popped_args_2, popped_kwargs_2) = popped_task_2
    assert popped_task_id_2 == task_id_2
    assert popped_name_2 == "test_task_2"
    assert popped_args_2 == [4, 5]
    assert popped_kwargs_2 == {"b": 6}

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
    task_id = uuid.uuid4()
    await queue1.push(task_id, "test_task", *[], **{})

    # The second queue should be empty
    assert await queue2.pop() is None

    # The first queue should have the task
    popped_task = await queue1.pop()
    assert popped_task is not None
    popped_task_id, _ = popped_task
    assert popped_task_id == task_id

import uuid

import pytest

from quincy import Queue


@pytest.mark.asyncio
async def test_push_and_pop(temp_db_file):
    queue = Queue(str(temp_db_file), "test_queue")
    await queue.init()

    task_id = uuid.uuid4()
    name = "test_task"
    args = [1, 2]
    kwargs = {"c": 3}

    await queue.push(task_id, name, *args, **kwargs)

    popped_task = await queue.pop()

    assert popped_task is not None
    popped_task_id, (popped_name, popped_args, popped_kwargs) = popped_task

    assert popped_task_id == task_id
    assert popped_name == name
    assert popped_args == args
    assert popped_kwargs == kwargs

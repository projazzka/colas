import asyncio

import pytest

from colas import Colas


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

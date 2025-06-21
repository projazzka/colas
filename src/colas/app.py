from typing import Any, Callable, Coroutine
from uuid import uuid4

from .queue import Queue
from .results import Results
from .task import Task


class Colas:
    def __init__(self, filename: str):
        self.filename = filename
        self._tasks: dict[str, Callable[..., Coroutine[Any, Any, Any]]] = {}

    async def init(self) -> None:
        self.queue = Queue(self.filename, "tasks")
        self.results = Results(self.filename, "results")
        await self.queue.init()
        await self.results.init()

    def task(
        self, func: Callable[..., Coroutine[Any, Any, Any]]
    ) -> Callable[..., Coroutine[Any, Any, Any]]:
        self._tasks[func.__name__] = func

        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await self._execute_handler(func.__name__, *args, **kwargs)

        return wrapper

    async def _execute_handler(self, name: str, *args: Any, **kwargs: Any) -> Any:
        task = Task(
            task_id=uuid4(),
            name=name,
            args=args,
            kwargs=kwargs,
        )
        await self.queue.push(task)
        return await self.results.wait(task.task_id)

    async def run(self) -> None:
        async for task in self.queue.tasks():
            func = self._tasks[task.name]
            result = await func(*task.args, **task.kwargs)
            await self.results.store(task.task_id, result)

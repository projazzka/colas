from typing import Any, Callable, Coroutine
from urllib.parse import urlparse
from uuid import uuid4

from .queue import PostgresQueue, Queue, SqliteQueue
from .results import PostgresResults, Results, SqliteResults
from .task import Task


class Colas:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self._tasks: dict[str, Callable[..., Coroutine[Any, Any, Any]]] = {}
        self.queue: Queue
        self.results: Results

        # Parse DSN and choose backend based on scheme
        parsed = urlparse(dsn)

        if parsed.scheme in ("postgresql", "postgres"):
            self.queue = PostgresQueue(dsn, "tasks")
            self.results = PostgresResults(dsn, "results")
        elif parsed.scheme == "sqlite":
            # Extract filename from sqlite:// URL
            filename = parsed.path
            self.queue = SqliteQueue(filename, "tasks")
            self.results = SqliteResults(filename, "results")
        else:
            # Assume file path for SQLite (no scheme or unknown scheme)
            self.queue = SqliteQueue(dsn, "tasks")
            self.results = SqliteResults(dsn, "results")

    async def init(self) -> None:
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

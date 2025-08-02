from typing import Any, Callable, Coroutine
from urllib.parse import urlparse
from uuid import uuid4

from .queue import Queue
from .stream import Stream
from .task import Task


class Colas:
    def __init__(self) -> None:
        self._tasks: dict[str, Callable[..., Coroutine[Any, Any, Any]]] = {}
        self.queue: Queue | None = None
        self.stream: Stream | None = None

    async def connect(self, dsn: str) -> None:
        parsed = urlparse(dsn)

        match parsed.scheme:
            case "postgresql" | "postgres":
                from .postgres.connection import create_connection_pool  # noqa: WPS433
                from .postgres.queue import PostgresQueue  # noqa: WPS433
                from .postgres.stream import PostgresStream  # noqa: WPS433

                pool = await create_connection_pool(dsn)
                self.queue = PostgresQueue(pool)
                self.stream = PostgresStream(pool)
            case "sqlite":
                from .sqlite.queue import SqliteQueue  # noqa: WPS433
                from .sqlite.stream import SqliteStream  # noqa: WPS433

                filename = parsed.path
                self.queue = SqliteQueue(filename)
                self.stream = SqliteStream(filename)
            case _:
                raise ValueError(f"Unsupported DSN: {dsn}")

    async def init(self) -> None:
        if self.queue is None or self.stream is None:
            raise RuntimeError("Must call connect() before init()")

        await self.queue.init(["tasks"])
        await self.stream.init(["results"])

    def task(
        self, func: Callable[..., Coroutine[Any, Any, Any]]
    ) -> Callable[..., Coroutine[Any, Any, Any]]:
        self._tasks[func.__name__] = func

        async def wrapper(*args: Any, **kwargs: Any) -> Any:  # noqa: D401
            return await self._execute_handler(func.__name__, *args, **kwargs)

        return wrapper

    async def _execute_handler(self, name: str, *args: Any, **kwargs: Any) -> Any:
        if self.queue is None or self.stream is None:
            raise RuntimeError("Must call connect() before using tasks")

        task = Task(
            task_id=uuid4(),
            name=name,
            args=args,
            kwargs=kwargs,
        )
        await self.queue.push("tasks", task)
        return await self.stream.wait("results", task.task_id)

    async def run(self) -> None:
        if self.queue is None or self.stream is None:
            raise RuntimeError("Must call connect() before running")

        async for task in self.queue.tasks("tasks"):
            func = self._tasks[task.name]
            result = await func(*task.args, **task.kwargs)
            await self.stream.store("results", task.task_id, result)

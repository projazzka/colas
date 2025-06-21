from typing import Any, Callable, Coroutine

from .queue import Queue


class Colas:
    def __init__(self, filename: str):
        self.filename = filename
        self._tasks: dict[str, Callable[..., Coroutine[Any, Any, Any]]] = {}

    async def init(self) -> None:
        self.queue = Queue(self.filename, "tasks")
        await self.queue.init()

    def task(
        self, func: Callable[..., Coroutine[Any, Any, Any]]
    ) -> Callable[..., Coroutine[Any, Any, Any]]:
        self._tasks[func.__name__] = func

        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await self._execute_handler(func.__name__, *args, **kwargs)

        return wrapper

    async def _execute_handler(self, name: str, *args: Any, **kwargs: Any) -> Any:
        return await self._tasks[name](*args, **kwargs)

    def run(self) -> None:
        pass  # For now, just a placeholder

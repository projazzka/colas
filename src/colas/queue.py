import asyncio
from abc import ABC, abstractmethod
from typing import AsyncGenerator

from colas.task import Task


class Queue(ABC):
    def __init__(self, polling_interval: float = 0.1):
        self.polling_interval = polling_interval

    @abstractmethod
    async def init(self, queues: list[str]) -> None: ...

    @abstractmethod
    async def push(self, queue: str, task: Task) -> None: ...

    @abstractmethod
    async def pop(self, queue: str) -> Task | None: ...

    async def tasks(self, queue: str) -> AsyncGenerator[Task, None]:
        while True:
            task = await self.pop(queue)
            if task:
                yield task
            else:
                await asyncio.sleep(self.polling_interval)


__all__: list[str] = ["Queue"]

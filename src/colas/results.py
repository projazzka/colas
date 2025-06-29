import asyncio
from abc import ABC, abstractmethod
from typing import Any
from uuid import UUID


class Results(ABC):
    def __init__(self, polling_interval: float = 0.1):
        self.polling_interval = polling_interval

    @abstractmethod
    async def init(self) -> None: ...

    @abstractmethod
    async def store(self, task_id: UUID, result: Any) -> None: ...

    @abstractmethod
    async def clean(self, ttl: int) -> None: ...

    async def wait(self, task_id: UUID) -> Any:
        while True:
            results = await self.retrieve([task_id])
            if task_id in results:
                return results[task_id]
            await asyncio.sleep(self.polling_interval)

    @abstractmethod
    async def retrieve(self, task_ids: list[UUID]) -> dict[UUID, Any]: ...


__all__: list[str] = ["Results"]

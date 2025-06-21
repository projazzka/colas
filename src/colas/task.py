from dataclasses import dataclass
from uuid import UUID


@dataclass
class Task:
    task_id: UUID
    name: str
    args: tuple
    kwargs: dict

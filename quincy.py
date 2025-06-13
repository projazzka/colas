from typing import Callable


class Quincy:
    def __init__(self, filename: str):
        self.filename = filename
        self._tasks: dict[str, Callable] = {}

    def task(self, func):
        """Decorator to register an async function as a task."""
        self._tasks[func.__name__] = func

        async def wrapper(*args, **kwargs):
            return await self._tasks[func.__name__](*args, **kwargs)

        return wrapper

    def run(self):
        """Run the Quincy application."""
        pass  # For now, just a placeholder

# Colas

A modern, minimalist task queue framework.

  - [x] fully async
  - [x] supports database backends for queues
  - [x] no crazy inventions for task discovery, logging or configuration

Supported backends:
  - Sqlite
  - Postgres

Author: Igor Prochazka (@projazzka)

## Installation

```
pip install colas[sqlite]  # For Sqlite backend
pip install colas[postgresql]  # For Postgresql backend
```

## Usage

```
# tasks.py

from colas import Colas

app = Colas("sqlite://./colas.db")

@app.task
async def multiply(a: int, b: int) -> int:
    return a * b


if __name__ == "__main__":
    app.run()  # starts the worker
```

On the client side simply call the registered tasks.
```
# main.py

from tasks import multiply

result = await multiply(2, 3)  # enqueues the tasks and waits for the response
```

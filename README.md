# Colas

A modern task queue framework.

  - [X] fully async
  - [X] supports database backends for queues

Supported backends:
  - Sqlite
  - Postgres

Author: Igor Prochazka (@projazzka)

## Installation

```
pip install colas
```

## Usage

```
from colas import Colas

app = Colas(filename="quincy.db")

@app.task
async def multiply(a: int, b: int) -> int:
    return a * b


if __name__ == "__main__":
    app.run()
```

On the client side simply do
```
from tasks import multiply

result = await multiply(2, 3)
```



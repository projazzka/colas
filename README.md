# Quincy

A fresh new task queue framework.

[X] fully async
[X] supports database backends for queues (including Sqlite!)

Author: Igor Prochazka (@projazzka)

## Installation

TBD

## Usage

````
import quincy

app = quincy.Quincy(filename="quincy.db")

@app.task
async def multiply(a: int, b: int) -> int:
    return a * b


if __name__ == "__main__":
    app.run()
```

On the client side simply do
```
from tasks import app

result = await multiply(2, 3)
````



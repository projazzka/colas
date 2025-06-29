import colas

app = colas.Colas("sqlite://quincy.db")


@app.task
async def hello_world():
    print("Hello, world!")


@app.task
async def multiply(a: int, b: int) -> int:
    return a * b


if __name__ == "__main__":
    import asyncio

    asyncio.run(app.run())

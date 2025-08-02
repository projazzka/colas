import colas

app = colas.Colas()


@app.task
async def hello_world():
    print("Hello, world!")


@app.task
async def multiply(a: int, b: int) -> int:
    return a * b


async def init_app():
    """Initialize the app - must be called before using tasks"""
    await app.connect("sqlite://quincy.db")
    await app.init()


if __name__ == "__main__":
    import asyncio

    async def main():
        await init_app()
        await app.run()

    asyncio.run(main())

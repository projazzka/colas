import asyncio

from worker import hello_world, init_app, multiply


async def main():
    await init_app()  # Initialize the shared app
    await hello_world()
    result = await multiply(2, 3)
    assert result == 6


if __name__ == "__main__":
    asyncio.run(main())

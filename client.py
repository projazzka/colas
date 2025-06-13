import asyncio

from worker import hello_world, multiply


async def main():
    await hello_world()
    result = await multiply(2, 3)
    assert result == 6


if __name__ == "__main__":
    asyncio.run(main())

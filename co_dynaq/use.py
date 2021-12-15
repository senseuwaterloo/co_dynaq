import trio

from contextlib import asynccontextmanager

@asynccontextmanager
async def use(queue):

    raise NotImplementedError

    async def reprioritise_task():
        while True:
            pass

    async with trio.open_nursery as nursery:
        nursery.start_soon()

import anyio
from collections import deque
from datetime import datetime
from dataclasses import dataclass

class FifoQueue():
    def __init__(self, items=None):
        self.lock = anyio.Lock()
        self.queue = deque(items)
        self.closed = False
        self.condition = anyio.Condition(self.lock)
        self.condition

    async def put(self, item):
        async with self.condition:
            self.queue.append(item)
            self.condition.notify()


    async def pop(self):
        async with self.condition:
            await self.condition.wait()
            return self.queue.popleft()

    async def empty(self):
        async with self.lock:
            return len(self.queue) == 0 and self.closed

    async def close(self):
        async with self.lock:
            self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        if await self.empty():
            raise StopAsyncIteration
        else:
            return await self.pop()

    async def __aenter__(self):
        return self

    async def __aexit__(self):
        await self.close()

import trio
from collections import deque

class FlexiQueue(trio.abc.AsyncResource):
    def __init__(self, capacity=None, items=None):
        self._hasitem = trio.Condition()
        self._hasspace = trio.Condition()
        self._capacity = capacity

        if items is None:
            self._queue = deque(maxlen=capacity)
        else:
            self._queue = deque(items)

        self._closed = False

    async def put(self, item):
        async with self._hasspace:
            if self._closed:
                raise trio.ClosedResourceError
            if self._capacity is not None and len(self._queue) == self._capacity:
                await self._hasspace.wait()
                if self._closed:
                    raise trio.ClosedResourceError

            self._queue.append(item)

        async with self._hasitem:
            self._hasitem.notify()

    async def pop(self):
        async with self._hasitem:
            if self._closed:
                raise trio.ClosedResourceError
            if len(self._queue) == 0:
                await self._hasitem.wait()
                if self._closed:
                    raise trio.ClosedResourceError

            async with self._hasspace:
                self._hasspace.notify()

            return self._queue.popleft()

    @property
    def closed(self):
        return self._closed

    async def close(self):
        async with self._hasitem, self._hasspace:
            self._hasitem.notify_all()
            self._hasspace.notify_all()
            self._queue.clear()
            self._closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return await self.pop()
        except trio.ClosedResourceError as e:
            raise StopAsyncIteration from e

    async def aclose(self, *_):
        await self.close()

from collections import deque
import math

from .sync import synchronised, Queue

@synchronised
class FifoQueue(Queue):
    def __init__(self, capacity=None, items=None):
        if items is None:
            self._queue = deque(maxlen=capacity)
        else:
            self._queue = deque(items)

    def __len__(self):
        return len(self._queue)

    @property
    def maxlen(self):
        return self._queue.maxlen or math.inf

    def put(self, item):
        return self._queue.append(item)

    def pop(self):
        return self._queue.popleft()

    def replace(self, item):
        self._queue.append(item)
        return self._queue.popleft()

    def clear(self):
        return self._queue.clear()

    def isempty(self):
        return len(self._queue) == 0

    def isfull(self):
        return len(self._queue) == self._queue.maxlen

    def peek(self):
        return self._queue[0]


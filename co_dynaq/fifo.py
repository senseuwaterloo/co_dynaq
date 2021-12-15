from collections import deque
import math
from typing import Deque, Dict, Generator, Generic, Tuple, TypeVar

from .abc import Queue
from .entry import Entry
from .channel import channel

__all__ = ['FifoQueue']

T = TypeVar('T')

@channel
class FifoQueue(Queue, Generic[T]):
    _maxlen: int
    size: int
    entries: Dict[T, Entry[T]]
    queue = Deque[Entry[T]]
    def __init__(self, capacity: int=math.inf, items: Generator[Tuple[T, int],None,None]=None):
        self._maxlen=capacity
        if items is None:
            self.queue = deque()
            self.size = 0
            self.entries = dict()
        else:
            if len(items) > capacity:
                raise IndexError("Capacity reached.")
            self.entries = {item: Entry(priority=priority, ref=item) for item, priority in items}
            self.queue = deque(self.entries.values())
            self.size = len(self.queue)

    def __len__(self):
        return self.size

    def reprioritise(self, _):
        pass

    def clone(self):
        return self

    def clean(self):
        self.queue = deque(entry for entry in self.queue if not entry.removed)

    def remove(self, item):
        e = self.entries.pop(item)
        e.removed = True
        self.size -= 1
        return e.ref, e.priority

    def update(self, item, amount=0):
        pass

    @property
    def maxlen(self):
        return self._maxlen

    def put(self, item, priority=0):
        entry = Entry(priority=priority, ref=item)
        self.entries[item] = entry
        self.size += 1
        return self.queue.append(entry)

    def pop(self):
        while not self.isempty():
            entry = self.queue.popleft()
            if not entry.removed:
                del self.entries[+entry]
                self.size -= 1
                return +entry
        raise IndexError

    def replace(self, item, priority=0):
        out = +self.pop()
        self.put(item, priority=priority)
        return out

    def pushpop(self, item, priority=0):
        self.put(item, priority=priority)
        return self.pop()

    def clear(self):
        self.size = 0
        return self.queue.clear()

    def isempty(self):
        return self.size <= 0

    def isfull(self):
        return self.size >= self._maxlen

    def peek(self):
        if self.isempty():
            raise IndexError
        return self.queue[0]

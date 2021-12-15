import math
import heapq
from typing import Dict, Generator, Generic, List, Tuple, TypeVar

from .abc import Queue
from .channel import channel
from .entry import Entry

__all__ = ['SingleQueue']

T = TypeVar('T')

@channel
class SingleQueue(Queue[T], Generic[T]):
    entries: Dict[T, Entry[T]]
    queue: List[Entry[T]]
    _maxlen: int
    size: int

    def __init__(self, capacity: float=math.inf, items: Generator[Tuple[T, int],None,None]=None):
        self._maxlen = capacity
        if items is None:
            self.entries = dict()
            self.queue = list()
            self.size = 0
        else:
            self.entries = {item: Entry(priority=priority, ref=item) for item, priority in items}
            self.queue = list(self.entries.values())
            self.size = 0

    def clean(self):
        self.queue = list(entry for entry in self.queue if not entry.removed)
        heapq.heapify(self.queue)

    def remove(self, item:T):
        e = self.entries.pop(item)
        e.removed = True
        self.size -= 1
        return e.ref, e.priority

    def update(self, item:T, amount:int):
        ref, priority = self.remove(item)
        self.put(ref, priority=priority + amount)
        return ref, priority

    def clone(self):
        return self

    def __len__(self):
        return self.size

    @property
    def maxlen(self):
        return self._maxlen

    def put(self, item:T, priority:int=0):
        entry = Entry[T](priority, item)
        self.entries[item] = entry
        self.size += 1
        return heapq.heappush(self.queue, entry)

    def pop(self):
        while not self.isempty():
            entry = heapq.heappop(self.queue)
            if not entry.removed:
                del self.entries[+entry]
                self.size -= 1
                return +entry
        raise IndexError

    def replace(self, item, priority=0):
        out = self.pop()
        self.push(item, priority)
        return out

    def pushpop(self, item, priority=0):
        self.push(item, priority)
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
            raise IndexError('Queue is empty.')
        return +self.queue[0]

    def __iter__(self):
        return self

    def __next__(self):
        return self.pop()

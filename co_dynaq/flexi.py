from ast import Index
import math
import heapq
from typing import Dict, Generator, Generic, Tuple, TypeVar
import trio
from .single import SingleQueue
from .channel import channel
from .abc.queue import Queue
from .fifo import FifoQueue
from .entry import Entry
from .abc import Queue

__all__ = ['FlexiQueue']

T = TypeVar('T')

@channel
class FlexiQueue(Queue, Generic[T]):
    waiting_queue: Queue[T]
    dispatch_queue: Queue[T]

    def __init__(self, p:float, dispatch:int=60, capacity:int=math.inf, items:Generator[Tuple[T, int], None, None]=None, _waiting_queue: FifoQueue[T]=None):
        if dispatch >= math.inf:
            raise ValueError("A co_dynaq_flexi can not have infinite dispatch queue capacity. Use co_dynaq_single instead.")
        self.p = p
        if _waiting_queue is None:
            self.waiting_queue = FifoQueue(capacity=capacity - dispatch, items=items)
        else:
            self.waiting_queue = _waiting_queue

        self.dispatch_queue = SingleQueue(capacity=dispatch, items=None)

    def clean(self):
        self.waiting_queue.clean()
        self.dispatch_queue.clean()

    def remove(self, item:T):
        try:
            return self.dispatch_queue.remove(item)
        except KeyError:
            return self.waiting_queue.remove(item)

    def update(self, item:T, amount:int):
        return self.dispatch_queue.update(item, amount=amount)

    def pop(self):
        # fill the queue if necessary
        if len(self.dispatch_queue) <= self.p * self.dispatch_queue.maxlen:
            self.fill()

        try:
            return self.dispatch_queue.pop()
        except IndexError as e:
            raise IndexError from e

    def fill(self):
        while not self.dispatch_queue.isfull() and not self.waiting_queue.isempty():
            item, priority = self.waiting_queue.pop()
            self.dispatch_queue.put(item, priority=priority)

    
    def pushpop(self, item, priority=0):
        self.put(item, priority=priority)
        return self.pop()

    def replace(self, item, priority=0):
        out = self.pop()
        self.put(item, priority=priority)
        return out

    @property
    def dispatch(self):
        return self.dispatch_queue.maxlen
    
    def clone(self, dispatch=None):
        return FlexiQueue(self.p, dispatch=dispatch or self.dispatch, capacity=self.maxlen, items=None, _waiting_queue=self.waiting_queue)

    def __len__(self):
        return len(self.dispatch_queue) + len(self.waiting_queue)

    @property
    def maxlen(self):
        return self.dispatch_queue.maxlen + self.waiting_queue.maxlen

    def put(self, item, priority=0):
        self.waiting_queue.put((item, priority))

    def clear(self):
        self.dispatch_queue.clear()
        self.waiting_queue.clear()

    def isempty(self):
        return self.dispatch_queue.isempty() and self.waiting_queue.isempty()

    def isfull(self):
        return self.waiting_queue.isfull()

    def peek(self):
        # fill the queue if necessary
        if len(self.dispatch_queue) <= self.p * self.dispatch_queue.maxlen:
            self.fill()

        return self.dispatch_queue.peek()

    def __iter__(self):
        return self

    def __next__(self):
        return self.pop()

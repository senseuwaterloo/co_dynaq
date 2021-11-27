import heapq
from collections import deque
import math

from .sync import synchronised, Queue

@synchronised
class PriorityQueue(Queue):
    def __init__(self, capacity=math.inf, items=None):
        self.__maxlen = capacity
        if items is None:
            self.__queue = []
        else:
            self.__queue = heapq.heapify(list(items))

    def __len__(self):
        return self.__queue.__len__()

    @property
    def maxlen(self):
        return self.__maxlen

    def put(self, item):
        return heapq.heappush(self.__queue, item)

    def pop(self):
        out =  heapq.heappop(self.__queue)
        return out

    def replace(self, item):
        return heapq.heapreplace(self.__queue, item)

    def pushpop(self, item):
        return heapq.heappushpop(self.__queue, item)

    def clear(self):
        return self.__queue.clear()

    def isempty(self):
        return self.__queue.__len__() == 0

    def isfull(self):
        return self.__queue.__len__() == self.__maxlen

    def peek(self):
        return self.__queue[0]

    def __iter__(self):
        return self

    def __next__(self):
        return self.pop()

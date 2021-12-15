import math
from typing import Generator, Generic, Tuple, TypeVar

from co_dynaq.fifo import FifoQueue

from .flexi import FlexiQueue

__all__ = ['DoubleQueue']

T = TypeVar('T')

class DoubleQueue(FlexiQueue[T], Generic[T]):

    def __init__(self, dispatch:int=60, capacity:int=math.inf, items:Generator[Tuple[T, int], None, None]=None, _waiting_queue: FifoQueue[T]=None):
        super().__init__(0, dispatch=dispatch, capacity=capacity, items=items, _waiting_queue=_waiting_queue)
    
    def clone(self, dispatch=None):
        return type(self)(dispatch=dispatch or self.dispatch, capacity=self.maxlen, items=None, _waiting_queue=self.waiting_queue)

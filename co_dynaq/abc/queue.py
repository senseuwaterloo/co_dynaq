from collections import OrderedDict
import itertools
from functools import partial, wraps
import typing
from typing import Generic, List, Set, Tuple, Type, TypeVar, Union
import trio
import abc
from outcome import Error, Value
from dataclasses import dataclass, field

from co_dynaq.entry import Entry

__all__ = ['Queue']

T = TypeVar('T')

class Queue(Generic[T], abc.ABC):
    @abc.abstractmethod
    def __init__(self, capacity:int=0, items:List[T]=None):
        ...
    @abc.abstractmethod
    def put(self, item:T, priority:int=0) -> None:
        ...
    @abc.abstractmethod
    def pop(self) -> T:
        ...
    @abc.abstractmethod
    def replace(self, item:T, priority:int=0) -> T:
        ...
    @abc.abstractmethod
    def pushpop(self, item:T, priority:int=0) -> T:
        ...
    @abc.abstractmethod
    def clear(self, item:T):
        ...
    @abc.abstractmethod
    def __len__(self) -> int:
        ...
    @abc.abstractmethod
    def isempty(self) -> bool:
        ...
    @abc.abstractmethod
    def isfull(self) -> bool:
        ...
    @abc.abstractmethod
    def peek(self) -> T:
        ...
    @abc.abstractmethod
    def clone(self) -> 'Queue[T]':
        ...
    @abc.abstractmethod
    def remove(self, item:T) -> Tuple[T, int]:
        ...
    @abc.abstractmethod
    def update(self, item:T, amount:int) -> Tuple[T, int]:
        ...
    @property
    @abc.abstractmethod
    def maxlen(self) -> int:
        ...
    @abc.abstractmethod
    def clean(self):
        ...
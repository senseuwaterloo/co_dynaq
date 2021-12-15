from dataclasses import dataclass, field
from typing import Generic, TypeVar
from itertools import count

__all__ = ['Entry']

sequencer = count()

T = TypeVar('T')

@dataclass(order=True)
class Entry(Generic[T]):
    priority: int
    sequence: int = field(init=False, default_factory=sequencer.__next__)
    ref: T = field(compare=False)
    removed: bool = field(init=False, compare=False, default=False)

    def __pos__(self):
        return self.ref
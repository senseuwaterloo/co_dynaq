from collections import OrderedDict
from dataclasses import dataclass, field
import typing
from typing import Generic, Set, Type, TypeVar, Union
import trio
from outcome import Error, Value

from .abc.queue import Queue

T = TypeVar('T')

def channel(qclass:Queue[T]):
    qclass.synchronise = lambda self: Channel(self, ChannelState())
    return qclass


class EndOfQueue(Exception):
    ...


@dataclass
class ChannelState(Generic[T]):
    channels: Set = field(default_factory=set)
    put_tasks: typing.OrderedDict[trio.lowlevel.Task, T] = field(default_factory=OrderedDict)
    pop_tasks: typing.OrderedDict[trio.lowlevel.Task, None] = field(default_factory=OrderedDict)


class Channel(trio.abc.AsyncResource, Queue[T], Generic[T]):
        closed: bool
        state: ChannelState[T]

        def __init__(self, queue: Queue[T], state: ChannelState[T]):
            self.queue = queue
            self.closed = False
            self.state = state

            state.channels.add(self)


        def __repr__(self):
            return f"<channel {self.queue.__repr__()}>"


        @trio.lowlevel.enable_ki_protection
        def put_nowait(self, item, priority=0):
            if self.closed:
                raise trio.ClosedResourceError
            elif self.state.pop_tasks:
                assert self.queue.isempty()
                task, _ = self.state.pop_tasks.popitem(last=False)
                trio.lowlevel.reschedule(task, Value(item))
            elif not self.queue.isfull():
                self.queue.put(item, priority=priority)
            else:
                raise trio.WouldBlock


        @trio.lowlevel.enable_ki_protection
        async def put(self, item, priority=0):
            await trio.lowlevel.checkpoint_if_cancelled()
            try:
                self.put_nowait(item, priority=priority)
            except trio.WouldBlock:
                pass
            else:
                await trio.lowlevel.cancel_shielded_checkpoint()
                return

            task = trio.lowlevel.current_task()
            self.state.put_tasks[task] = (item, priority)
            
            def abort(_):
                del self.state.put_tasks[task]
                return trio.lowlevel.Abort.SUCCEEDED

            return await trio.lowlevel.wait_task_rescheduled(abort)


        @trio.lowlevel.enable_ki_protection
        def pop_nowait(self):
            if self.closed and self.queue.isempty():
                raise EndOfQueue
            elif self.state.put_tasks:
                assert self.queue.isfull()
                task, (item, priority) = self.state.put_tasks.popitem(last=False)
                trio.lowlevel.reschedule(task, Value(None))
                return self.queue.replace(item, priority=priority)
            elif not self.queue.isempty():
                return self.queue.pop()
            else:
                raise trio.WouldBlock


        @trio.lowlevel.enable_ki_protection
        async def pop(self):
            await trio.lowlevel.checkpoint_if_cancelled()
            try:
                item = self.pop_nowait()
            except trio.WouldBlock:
                pass
            else:
                await trio.lowlevel.cancel_shielded_checkpoint()
                return item

            task = trio.lowlevel.current_task()
            self.state.pop_tasks[task] = None
            
            def abort(_):
                del self.state.pop_tasks[task]
                return trio.lowlevel.Abort.SUCCEEDED

            return await trio.lowlevel.wait_task_rescheduled(abort)


        def clone(self):
            if self.closed:
                raise trio.ClosedResourceError
            return Channel(self.queue.clone(), self.state)
        

        def __enter__(self):
            return self


        def __exit__(self, exc_type, exc_val, exc_tb):
            self.close()


        @trio.lowlevel.enable_ki_protection
        def close(self):
            if len(self.state.channels) == 0:
                return

            for channel in self.state.channels:
                channel.closed = True

            for task in self.state.put_tasks.keys():
                trio.lowlevel.reschedule(task, Error(trio.ClosedResourceError()))
            self.state.put_tasks.clear()

            for task in self.state.pop_tasks.keys():
                if not self.queue.isempty():
                    item = self.queue.pop()
                    task, _ = self.state.pop_tasks.popitem(last=True)
                    trio.lowlevel.reschedule(task, Value(item))
                else:
                    trio.lowlevel.reschedule(task, Error(EndOfQueue()))
            self.state.pop_tasks.clear()

            self.state.channels.clear()



        @trio.lowlevel.enable_ki_protection
        async def aclose(self):
            self.close()
            await trio.lowlevel.checkpoint()


        def __aiter__(self):
            return self


        async def __anext__(self):
            try:
                return await self.pop()
            except EndOfQueue as e:
                raise StopAsyncIteration from e



        def clean(self):
            self.queue.clean()

        def update(self, item:T, amount:int):
            return self.queue.update(item, amount)

        def __len__(self):
            return self.queue.__len__()

        def clear(self):
            return self.queue.clear()

        def isempty(self):
            return self.queue.isempty()

        def isfull(self):
            return self.queue.isfull()

        def maxlen(self):
            return self.queue.maxlen()

        def peek(self):
            return self.queue.peek()

        def pushpop(self, item, priority=0):
            return self.queue.pushpop(item, priority)

        def replace(self, item, priority=0):
            return self.queue.replace(item, priority)

        def remove(self, item):
            return self.queue.remove(item)

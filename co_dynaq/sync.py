from collections import OrderedDict
import trio
import abc
from outcome import Error, Value

class Queue(abc.ABC):
    @abc.abstractmethod
    async def put(self, item):
        pass
    @abc.abstractmethod
    async def pop(self):
        pass
    @abc.abstractmethod
    async def replace(self):
        pass
    @abc.abstractmethod
    async def clear(self, item):
        pass
    @abc.abstractmethod
    async def __len__(self, item):
        pass
    @abc.abstractmethod
    def isempty(self):
        pass
    @abc.abstractmethod
    def isfull(self):
        pass
    @abc.abstractmethod
    def peek(self):
        pass
    @property
    @abc.abstractmethod
    def maxlen(self):
        pass

class EndOfQueue(Exception):
    pass

def synchronised(qclass):
    if not issubclass(qclass, Queue):
        raise TypeError(f'{qclass} is not a queue.')


    class SynchronisedQueue(qclass, trio.abc.AsyncResource):

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.__closed = False
            self.__put_tasks = OrderedDict()
            self.__pop_tasks = OrderedDict()

        @trio.lowlevel.enable_ki_protection
        def put_nowait(self, item):
            if self.closed:
                raise trio.ClosedResourceError
            elif self.__pop_tasks:
                assert len(self) == 0
                task, _ = self.__pop_tasks.popitem(last=False)
                trio.lowlevel.reschedule(task, Value(item))
            elif len(self) < self.maxlen:
                super().put(item)
            else:
                raise trio.WouldBlock

        @trio.lowlevel.enable_ki_protection
        async def put(self, item):
            await trio.lowlevel.checkpoint_if_cancelled()
            try:
                self.put_nowait(item)
            except trio.WouldBlock:
                pass
            else:
                await trio.lowlevel.cancel_shielded_checkpoint()
                return

            task = trio.lowlevel.current_task()
            self.__put_tasks[task] = item
            
            def abort(_):
                del self.__put_tasks[task]
                return trio.lowlevel.Abort.SUCCEEDED

            return await trio.lowlevel.wait_task_rescheduled(abort)

        @trio.lowlevel.enable_ki_protection
        def pop_nowait(self):
            if self.closed and len(self) == 0:
                raise EndOfQueue
            elif self.__put_tasks:
                assert len(self) == self.maxlen
                task, item = self.__put_tasks.popitem(last=False)
                trio.lowlevel.reschedule(task, Value(None))
                return super().replace(item)
            elif len(self) > 0:
                return super().pop()
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
            self.__pop_tasks[task] = None
            
            def abort(_):
                del self.__pop_tasks[task]
                return trio.lowlevel.Abort.SUCCEEDED

            return await trio.lowlevel.wait_task_rescheduled(abort)


        @property
        def closed(self):
            return self.__closed

        @trio.lowlevel.enable_ki_protection
        def close(self):
            if self.__closed:
                return
            self.__closed = True

            for task in self.__put_tasks.keys():
                trio.lowlevel.reschedule(task, Error(trio.ClosedResourceError()))
            self.__put_tasks.clear()

            
            for task in self.__pop_tasks.keys():
                if len(self) > 0:
                    item = super().pop()
                    task, _ = self.__pop_tasks.popitem(last=True)
                    trio.lowlevel.reschedule(task, Value(item))
                else:
                    trio.lowlevel.reschedule(task, Error(EndOfQueue()))
            self.__pop_tasks.clear()

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

        def __iter__(self):
            return super().__iter__()

        def __next__(self):
            return super().__next__()

    return SynchronisedQueue

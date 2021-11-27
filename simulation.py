import pandas as pd
import trio
import math

from dataclasses import dataclass
from pathlib import Path
from co_dynaq import FifoQueue, PriorityQueue
from tqdm import tqdm

from co_dynaq.sync import EndOfQueue

@dataclass
class Test:
    change_request: int
    test_suite: str
    request_time: int
    status: str
    execution_time: int


class EVBus(trio.abc.AsyncResource):
    """
    Event bus for concurrent discrete event simulation.
    """

    def __init__(self):
        self._queue = PriorityQueue()
        self._time = 0
        self._seq = 0

        self._num_tasks = 0

    async def __aenter__(self):
        await self.register()
        return self

    async def __aexit__(self, *_):
        await self.unregister()
        return

    async def register(self):
        self._num_tasks += 1
        await trio.sleep(0)

    async def unregister(self):
        self._num_tasks -= 1
        if self._num_tasks == 0 and len(self._queue) > 0:
            self.consume()
        await trio.sleep(0)

    def consume(self): 
        time, _, ev = self._queue.pop_nowait()
        assert time >= self._time
        self._time = time
        ev.set()

    async def until(self, time):
        event = trio.Event()
        self._queue.put_nowait((time, self.seq(), event))
        await self.unregister()
        await event.wait()
        await self.register()

    async def wait(self, dt=None):
        if dt is None:
            if len(self._queue) == 0:
                await self.until(self._time)
            else:
                time, *_ = self._queue.peek()
                await self.until(time)
        else:
            await self.until(self._time + dt)

    async def aclose(self):
        await self._queue.aclose()

    def seq(self):
        out = self._seq
        self._seq += 1
        return out

    @property
    def time(self):
        return self._time

    # async def run(self):
    #     # while True:
    #     #     if self._queue.closed:
    #     #         break
    #     #     while len(self._queue) > 0:
    #     #         time, _, ev = self._queue.peek()
    #     #         if time < self._time:
    #     #             ev.set()
    #     #             self._queue.pop_nowait()
    #     #         else:
    #     #             break
    #     #         await trio.sleep(0)
    #     #     self._time += self._dt
    #     #     await trio.sleep(0)
    #     async for time, _, ev in self._queue:
    #         assert self.time <= time
    #         self._time = time
    #         ev.set()


async def simulate(requests, test_count, request_count, put, queues):
    pgen = tqdm(total=request_count, position=0)
    psta = tqdm(total=test_count, position=1)

    # start task group syncrhonised clock for multi machine setup
    async with trio.open_nursery() as nursery:
        bus = EVBus()

        started = trio.Event()

        # define load generator
        async def load_generator():
            async with bus:

                # set start signal
                started.set()

                for request_time, tests in requests:

                    # wait until event
                    await bus.until(request_time)

                    for test in tests:

                        # schedule the test
                        await put(test)

                    pgen.update(1)


        # define test executor
        async def executor(queue, stream):
            time = 0.0
            async with stream:
                async with bus:

                    # wait for start signal
                    await started.wait()

                    while True:
                        try:
                            test = queue.pop_nowait()
                        except trio.WouldBlock:

                            # reschedule to next event
                            await bus.wait()

                            # sync time
                            time = bus.time
                        except EndOfQueue:
                            break
                        else:

                            # wait for execution
                            time += test.execution_time
                            await bus.until(time)

                            # test finished, report results
                            result = (
                                test, # the test
                                test.change_request, # change_request
                                test.status, # status
                                time - test.request_time # response_time
                            )
                            await stream.send(result)

    # start simulation

        # test result report stream
        send_stream, recv_stream = trio.open_memory_channel(0)

        # start load_generator
        nursery.start_soon(load_generator)

        # start executors
        async with send_stream:
            for queue in queues:
                nursery.start_soon(executor, queue, send_stream.clone())

        # collect data
        with recv_stream:
            stats = {}
            async for test, change_request, status, response_time in recv_stream:

                # get stats for change request
                if change_request not in stats:
                    stat = [0, math.inf, math.inf, 0]
                else:
                    stat = stats[change_request]

                # update stats
                stat[0] = response_time
                if status == 'FAILED':
                    if stat[1] == math.inf:
                        stat[1] = response_time
                    stat[2] = response_time
                stats[change_request] = stat
                
                psta.update(1)

    # post process stats
    stats = pd.DataFrame.from_dict(stats, orient='index', columns=['total_time', 'time_to_first_fail', 'time_to_last_fail', 'delayed_fail_count'])
    stats.index.name = "change_request"
    stats.loc[stats['time_to_first_fail'] > stats['total_time'], 'time_to_first_fail'] = stats['total_time']
    stats.loc[stats['time_to_last_fail'] > stats['total_time'], 'time_to_last_fail'] = stats['total_time']

    return stats


def main():
    cwd = Path()

    filename = 'test.out'

    data = pd.read_csv(cwd / 'data' / filename, names=['test_suite', 'change_request', 'stage', 'status', 'launch_time', 'execution_time', 'size', 'shard_number', 'run_number', 'language'], parse_dates=['launch_time'])
    requests = data.groupby('change_request')

    queue = FifoQueue()

    def generate_test():
        for change_request, request in requests:
            request = request.sort_values(['change_request', 'launch_time'])
            request_time = request.launch_time.min().timestamp()
            yield request_time, (
                Test(
                    change_request=row.change_request,
                    test_suite=row.test_suite,
                    request_time=request_time,
                    status=row.status,
                    execution_time=row.execution_time
                )
                for _, row in request.iterrows()
            )
                        
        queue.close()

    async def put(test):
        queue.put_nowait(test)

    stats = trio.run(simulate, generate_test(), len(data), len(requests), put, [queue])
    stats.to_csv(cwd / 'out' / filename)
    



if __name__ == '__main__':
    main()

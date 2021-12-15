from collections import defaultdict, deque
from typing import Generator, List, Tuple
import pandas as pd
import trio
import math
import heapq
import pickle
import co_dynaq
import json
import argparse

from dataclasses import dataclass, field
from pathlib import Path
from tqdm import tqdm
from functools import partial

from co_dynaq.channel import Channel, EndOfQueue
from co_dynaq.utils.co_fail import co_fail
 

@dataclass
class Test:
    # __slots__ = ['change_request', 'test_suite', 'request_time', 'status', 'execution_time', 'co_fail', 'request']
    change_request: int = field()
    test_suite: str = field(repr=False)
    request_time: int = field()
    status: str = field()
    execution_time: int = field()
    co_fail: dict = field(repr=False)
    request: list = field(repr=False)

    def __hash__(self):
        return id(self)


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--co-fails", type=str, default=None, required=True)
    parser.add_argument("-t", "--train-file", type=str, default=None)
    parser.add_argument("-i", "--test-file", type=str, default=None, required=True)
    parser.add_argument("-o", "--out-file", type=str, default=None, required=True)

    parser.add_argument("-m", "--mode", type=str, choices=['fifo', 'single', 'double', 'flexi'], default="fifo")
    parser.add_argument("-c", "--num-executors", type=int, default=1)
    parser.add_argument("-n", "--no-clone", action='store_true')
    parser.add_argument("-d", "--dispatch", type=int, default=60)
    parser.add_argument("-p", "--fill-ratio", type=float, default=0.7)

    args = parser.parse_args()

    mode = args.mode
    if mode == 'fifo':
        queue = co_dynaq.FifoQueue().synchronise()
    elif mode == 'single':
        queue = co_dynaq.SingleQueue().synchronise()
    elif mode == 'double':
        queue = co_dynaq.DoubleQueue(dispatch=args.dispatch).synchronise()
    elif mode == 'flexi':
        queue = co_dynaq.FlexiQueue(p=args.fill_ratio, dispatch=args.dispatch).synchronise()

    cwd = Path()

    tests = load_tests(Path(args.test_file))
    
    try:
        cofails = load_co_fails(Path(args.co_fails))
    except:
        cofails = compute_co_fails(Path(args.train_file), Path(args.co_fails))

    requests = [request for _, request in tests.groupby('change_request')]
                    

    # run the simulation
    stats = trio.run(simulate, generate_requests(queue, requests, cofails), len(requests), len(tests), queue, args.num_executors, args.no_clone)
 
    # with open(cwd / 'out' / f"{testfile}.double.json", "w") as fh:
    #     print('writing', fh.name)
    #     json.dump(stats, fh)
    with open(Path(args.out_file), "wb") as fh:
        print('writing', fh.name)
        pickle.dump(stats, fh)


def generate_requests(queue, requests, cofails):
    for request in requests:
        request_time = request['request_time'].view('int64').iloc[0] // 10**6
        tests = []
        for _, row in request.iterrows():
            tests.append(
                Test(
                    change_request=row.change_request,
                    test_suite=row.test_suite,
                    request_time=request_time,
                    status=row.status,
                    execution_time=row.execution_time,
                    co_fail=cofails[(row.test_suite, row.status)],
                    request=tests
                )
            )

        yield request_time, tests    

    queue.close()


def load_tests(filename):

    # read data
    with open(filename, 'r') as fh:
        print('reading', filename)
        data = pd.read_csv(fh, names=['test_suite', 'change_request', 'stage', 'status', 'launch_time', 'execution_time', 'size', 'shard_number', 'run_number', 'language'], parse_dates=['launch_time'])

    # reshape data
    data = data[['test_suite', 'change_request', 'launch_time', 'execution_time', 'status']]
    request_times = data.loc[data.groupby('change_request')['launch_time'].idxmin(), 'launch_time'].rename('request_time')
    data = data.merge(request_times, how='outer', left_index=True, right_index=True).ffill()
    data = data[['test_suite', 'change_request', 'request_time', 'execution_time', 'status']]

    return data


def load_co_fails(filename):

    # compute co_failure
    co_fails = None
    with open(filename, 'rb') as fh:
        print('reading', fh.name)
        co_fails = pickle.load(fh)
    return co_fails

    
def compute_co_fails(in_file, out_file):
    with open(in_file, 'r') as fh:
        print('reading', fh.name)
        train = pd.read_csv(fh, names=['test_suite', 'change_request', 'stage', 'status', 'launch_time', 'execution_time', 'size', 'shard_number', 'run_number', 'language'], parse_dates=['launch_time'])


    # compute failed and passed sets
    failed_sets = train[train.status == 'FAILED'].groupby('test_suite').change_request.apply(set)
    passed_sets = train[train.status == 'PASSED'].groupby('test_suite').change_request.apply(set)
    sets = pd.concat([failed_sets.rename('failed'), passed_sets.rename('passed')], names=['test_suite'], axis=1)
    sets = sets.apply(lambda c: c.apply(lambda s: s if isinstance(s, set) else set()))

    co_fails = defaultdict(partial(defaultdict, partial(float, 0.5)))
    for t2, t1, ff, fp in tqdm(co_fail(list(sets.itertuples()), list(sets.itertuples())), desc="co_fail", total=len(sets)**2):
        co_fails[(t1, 'FAILED')][t2] = ff
        co_fails[(t1, 'PASSED')][t2] = fp

    with open(out_file, 'wb') as fh:
        print('writing', fh.name)
        pickle.dump(co_fails, fh)

    return co_fails


# Below are definitions of the simulation


class EVBus:
    """
    Event bus for concurrent discrete event simulation.
    """

    def __init__(self):
        self._queue = []
        self._time = 0
        self._seq = 0

        self._num_tasks = 0

    async def __aenter__(self):
        await self.register()
        return self

    async def __aexit__(self, *_):
        await self.unregister()
        return True

    async def register(self):
        self._num_tasks += 1
        await trio.sleep(0)

    async def unregister(self):
        await trio.sleep(0)
        assert self._num_tasks > 0
        self._num_tasks -= 1
        if self._num_tasks <= 0 and len(self._queue) > 0:
            self.consume()

    def consume(self):
        time, _, ev = heapq.heappop(self._queue)
        assert time >= self._time
        self._time = time
        ev.set()

    async def run(self, a, *args, **kwargs):
        await self.unregister()
        try:
            out = await a(*args, **kwargs)
        finally:
            await self.register()
        return out

    async def take(self, g):
        await self.unregister()
        async for i in g:
            self.register()
            yield i
            self.unregister()
        self.register()

    async def until(self, time):
        event = trio.Event()
        heapq.heappush(self._queue, (time, self.seq(), event))
        await self.run(event.wait)

    async def wait(self, delta=None, until=None):
        if until is not None:
            await self.until(until)
        elif delta is not None:
            await self.until(self._time + delta)
        else:
            self._queue.sort()
            await self.until(next((t for t, s, e in self._queue if t > self._time), self._time + 1))
        return self._time

    def seq(self):
        out = self._seq
        self._seq += 1
        return out

    @property
    def time(self):
        return self._time


async def simulate(requests: Generator[Tuple[int, List[Test]], None, None], request_count: int, test_count: int, queue: Channel, num_shards: int, no_clone: bool=False):
    pgen = tqdm(total=request_count, desc="load_gen")
    pres = tqdm(total=test_count, desc="response")

    ready_countdown = num_shards

    # start task group syncrhonised clock for multi machine setup
    async with trio.open_nursery() as nursery:
        bus = EVBus()

        # define load generator
        async def load_generator():
            async with bus, queue:
                # set start signal

                while ready_countdown != 0:
                    await trio.sleep(0)

                tqdm.write(f'Load generator {id(trio.lowlevel.current_task())} started.')

                for request_time, tests in requests:

                    # wait until event
                    await bus.wait(until=request_time)

                    assert bus._num_tasks > 0

                    for test in tests:

                        # schedule the test
                        await queue.put(test)

                    pgen.update(1)
            tqdm.write(f'Load generator {id(trio.lowlevel.current_task())} stopped.')



        # define test executor
        async def executor(queue, stream):

            async def garbage_collector():
                while not queue.closed or not queue.isempty():
                    await trio.sleep(10)
                    queue.clean()

            async with bus, queue, stream:
                tqdm.write(f'Executor {id(trio.lowlevel.current_task())} started on queue {queue}.')
                nonlocal ready_countdown
                ready_countdown -= 1

                out_set = deque()
                # nursery.start_soon(garbage_collector)

                while True:
                    try:
                        test = queue.pop_nowait()
                    except EndOfQueue:
                        break
                    except trio.WouldBlock:
                        await bus.wait()
                        continue


                    # wait for execution
                    time = await bus.wait(test.execution_time)

                    # out_set.append(test)
                    # if len(out_set) >= 20:
                        # for t1 in out_set:
                        # update priority
                    t1 = test
                    for t2 in t1.request:
                        try:
                            queue.update(t2, 0.5 - t1.co_fail[t2.test_suite])
                        except KeyError:
                            pass
                        # out_set.clear()

                    # test finished, report results
                    result = (
                        test, # the test
                        test.change_request, # change_request
                        test.status, # status
                        time - test.request_time # response_time
                    )

                    await stream.send(result)

            tqdm.write(f'Executor {id(trio.lowlevel.current_task())} stopped.')

    # start simulation

        # test result report stream
        send_stream, recv_stream = trio.open_memory_channel(0)

        # start load_generator
        nursery.start_soon(load_generator)

        # start executors
        async with send_stream:
            if no_clone:
                for _ in range(num_shards):
                    nursery.start_soon(executor, queue, send_stream.clone())
            else:
                for _ in range(num_shards):
                    nursery.start_soon(executor, queue.clone(), send_stream.clone())

        # collect data
        with recv_stream:
            stats = defaultdict(partial(dict, total_time=0, time_to_first_fail=math.inf, time_to_last_fail=math.inf, response_times={}))
            async for t1, change_request, status, response_time in recv_stream:

                # get stats for change request
                stat = stats[change_request]

                # update stats
                stat['total_time'] = response_time
                if status == 'FAILED':
                    if response_time < stat['time_to_first_fail']:
                        stat['time_to_first_fail'] = response_time
                    stat['time_to_last_fail'] = response_time
                    stat['response_times'][t1.test_suite] = response_time
                stats[change_request] = stat
                
                pres.update(1)

    pgen.close()
    pres.close()

    print('Postprocessing stats.')
                
    for stat in stats.values():
        if stat['total_time'] < stat['time_to_first_fail']:
            stat['time_to_first_fail'] = stat['total_time']
        if stat['total_time'] < stat['time_to_last_fail']:
            stat['time_to_last_fail'] = stat['total_time']

    return stats
    

if __name__ == '__main__':
    main()

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


async def simulate(requests, test_count, request_count, put, queues):
    pgen = tqdm(total=request_count, position=0)
    psta = tqdm(total=test_count, position=1)

    # start task group syncrhonised clock for multi machine setup
    async with trio.open_nursery() as nursery:

        # define load generator
        async def load_generator():
            for request_time, tests in requests:

                for test in tests:

                    # schedule the test
                    await put(test)

                pgen.update(1)


        # define test executor
        async def executor(queue, stream):
            async with stream:
                    async for test in queue:

                        # test finished, report results
                        await stream.send(test)

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
            time = 0
            async for result in recv_stream:

                # get stats for change request
                if result.change_request not in stats:
                    stat = [0, math.inf, math.inf, 0]
                else:
                    stat = stats[result.change_request]
                
                # response time
                if result.request_time > time:
                    time = result.request_time

                time += result.execution_time

                response_time = time - result.request_time


                # update stats
                stat[0] = response_time
                if result.status == 'FAILED':
                    if stat[1] == math.inf:
                        stat[1] = response_time
                    stat[2] = response_time
                stats[result.change_request] = stat
                
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
    stats.to_csv(cwd / 'out_single' / filename)
    



if __name__ == '__main__':
    main()

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import sys
import os


sys.path.append(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'src'))

import argparse
import asyncio
from typing import List
import pandas

from dse_sim.generator.workload_generator import LoadedWorkloadGenerator
from dse_sim.profiler.profiler import Profiler
from dse_sim.collection.dse_collection import DSECollectionConfig
import dse_sim.config as config


from dse_sim.collection.dse_collection import DSECollection
from dse_sim.requests.actions.scale_down import ScaleDownRequest
from dse_sim.requests.actions.scale_in import ScaleInRequest
from dse_sim.requests.actions.scale_out import ScaleOutRequest
from dse_sim.requests.actions.scale_up import ScaleUpRequest
from dse_sim.requests.actions.scale_to import ScaleToRequest

from dse_sim.agent.agent import Agent

ACTION_SPACE = {
    'ScaleUp': ScaleUpRequest,
    'ScaleDown': ScaleDownRequest,
    'ScaleIn': ScaleInRequest,
    'ScaleOut': ScaleOutRequest,
    'ScaleTo': ScaleToRequest
}

class StdioAgent(Agent):
    def __init__(self, collection: DSECollection):
        super().__init__(collection)
        self.profiler = None

    def act(self, history: pandas.DataFrame) -> float:
        latest = history['time'].iloc[-1]
        print(history[history['time'] == latest].to_csv(index=False))
        print('>')

        response = input()

        if self.profiler is None:
            self.profiler = list(self.collection.get_by_type(Profiler))[0]

        parts = response.rstrip(',').split(',')
        sleep, actions = parts[0], list(parts[1:])

        for idx, action in enumerate(actions):
            if '(' not in action:
                action = f'{action}(1)'


            args = action.split('(')[1].split(')')[0].replace(' ','').split(',')
            action = action.split('(')[0]

            if len(args) == 1 and '=' not in args[0]:
                args = [f'count={args[0]}']

            args = {a.split('=')[0].strip(): int(a.split('=')[1].strip()) for a in args}

            actions[idx] = [action, args]

        for action, kwargs in actions:
            self.send_action_request(ACTION_SPACE[action](self.collection, self, **kwargs))

        return float(sleep)


async def main(collection_name: str,
               num_workers: int,
               half_ocus_per_worker: int,
               index_names: List[str],
               shards_per_index: List[int],
               profiler_output_path: str,
               profiler_first_delay: float,
               workload_path: str,
               ):

    # create collection
    dse_config = DSECollectionConfig(num_workers=num_workers, half_ocus_per_worker=half_ocus_per_worker)
    c = DSECollection(collection_name, dse_config, log_file=None if config.DEBUG else False)

    main_loop = await c.run()

    # construct two indices for the cluster, skipping all the construction delays
    indices = [(await c.add_index(index, instant=True)) for index in index_names]

    # create and attach shards to the workers in a round robin fashion
    workers = (list(c.workers) * sum(shards_per_index))[:sum(shards_per_index)]
    for (index, num_shards) in zip(indices, shards_per_index):
        for shard_idx in range(num_shards):
            await index.create_shard(workers.pop(), instant=True)

    # attach a profiler to the collection, which saves metrics in a csv every minute
    p = Profiler(c, save_path=profiler_output_path, agents=[StdioAgent(c)])
    asyncio.create_task(p.run_periodically(profiler_first_delay), name="Profiler")

    # build a synthetic input generator which creates synthetic requests in a sinusoidal-type pattern
    generator = LoadedWorkloadGenerator(c, workload_path)

    asyncio.create_task(generator.run_until(1e10))

    # run the simulation for long enough
    await asyncio.sleep(0)
    await c.tear_down(1e10)

    return await main_loop


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run the simulator with stdio for agent communication')

    parser.add_argument('--collection_name', type=str, default='test', help='Collection Name')
    parser.add_argument('--num_workers', type=int, default=2, help='Number of workers')
    parser.add_argument('--half_ocus_per_worker', type=int, default=2, help='Number of half OCUS per worker')
    parser.add_argument('--indices', type=str, default='hello,world', help='Comma-separated list of indices')
    parser.add_argument('--shards_per_index', type=str, default='6,6', help='Number of shards in each index')
    parser.add_argument('--profiler_output_path', type=str, default='/dev/null', help='Output path for the profiler results')
    parser.add_argument('--profiler_first_delay', type=int, default=60, help='Timestamp at which the profiler first produces output')
    parser.add_argument('--workload_path', type=str, default='/tmp/workload.jsonl', help='Path to the jsonl workload file')


    p = parser.parse_args()
    index_names = p.indices.split(',')
    shards_per_index = [int(i) for i in p.shards_per_index.split(',')]
    assert len(index_names) == len(shards_per_index)

    asyncio.run(main(p.collection_name, p.num_workers, p.half_ocus_per_worker, index_names, shards_per_index,
                     p.profiler_output_path, p.profiler_first_delay, p.workload_path))

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import asyncio
import sys
from termcolor import colored

from src.dse_sim.agent.oblivious import ObliviousAgent
from src.dse_sim.requests.actions.scale_out import ScaleOutRequest
from src.dse_sim.requests.actions.scale_in import ScaleInRequest
from src.dse_sim.generator.workload_generator import SinusoidalRateGenerator
from src.dse_sim.profiler.profiler import Profiler
from src.dse_sim.collection.dse_collection import DSECollection, DSECollectionConfig
from datetime import datetime
from src.dse_sim.util import seeded_random
import src.dse_sim.config as config


# predefine some tasks for the agent to run in this test

agent_predefined_tasks = {
        200: [ScaleOutRequest],
        1500: [ScaleOutRequest],
        1900: [ScaleOutRequest],
        2200: [ScaleOutRequest],
        2500: [ScaleOutRequest],
        3100: [ScaleInRequest],
        3400: [ScaleInRequest],
        3700: [ScaleInRequest],
        4000: [ScaleInRequest],
        4300: [ScaleInRequest],
    }

async def main():
    start = datetime.now()

    # create collection
    dse_config = DSECollectionConfig(num_workers=2, half_ocus_per_worker=2)
    c = DSECollection("test", dse_config, log_file=None if config.DEBUG else False)

    main_loop = await c.run()

    # construct two indices for the cluster, skipping all the construction delays
    indices = [
        await c.add_index('hello', instant=True),
        await c.add_index('world', instant=True)
    ]

    # create and attach 6 shards from each index to each worker
    for worker in c.workers:
        for index in indices:
            await index.create_shards(worker, 6, instant=True)

    # attach a profiler to the collection, which saves metrics in a csv every minute
    p = Profiler(c, save_path='/tmp/metrics.csv', agents=[ObliviousAgent(c, agent_predefined_tasks)])
    asyncio.create_task(p.run_periodically(60), name="Profiler")

    # build a synthetic input generator which creates synthetic requests in a sinusoidal-type pattern
    generator = SinusoidalRateGenerator(c, 100, 1500,
                                        lambda: max(seeded_random.gauss(6.2e7, 4e7), 1),
                                        lambda: min(8e9, seeded_random.expovariate(1. / 1e8))
                                        )
    asyncio.create_task(generator.run_until(15000))

    # run the simulation for 15000 simulation seconds
    await asyncio.sleep(0)
    await c.tear_down(15000)

    seconds = (datetime.now() - start).total_seconds()
    print(colored(f"Executed {generator.count} queries in {seconds:.3f} seconds (rate = {generator.count / seconds:.3f} queries/sec).", 'green'), file=sys.stderr)
    return await main_loop

asyncio.run(main())

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import asyncio
import random
import argparse
from typing import List

from dse_sim.generator.workload_generator import SinusoidalRateGenerator


async def generate(start: float, until: float, indices: List[str],
                  rate_mean: float, rate_period: float,
                  mean_cpu: float, std_cpu: float,
                  mean_ram: float, std_ram: float,
                  save_path: str):
    class FakeIndex:
        def __init__(self, name, collection):
            self.name = name
            self.collection = collection
    class FakeCollection:
        def __init__(self):
            self.time = start
            self.indices = [FakeIndex(i, self) for i in indices]
        async def yield_for(self, t: float):
            self.time += t

    fc = FakeCollection()
    generator = SinusoidalRateGenerator(fc, rate_mean, rate_period,
                                    lambda: max(random.gauss(mean_cpu, std_cpu), 1),
                                    lambda: min(mean_ram, random.expovariate(1. / std_ram)),
                                    save_path=save_path
                                    )
    while fc.time < until:
        (await generator.next_request()).close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Generate workload with sinusoidal rate pattern')
    parser.add_argument('--start', type=float, default=0, help='Start time')
    parser.add_argument('--until', type=float, default=15000, help='End time')
    parser.add_argument('--indices', type=str, nargs='+', default=['hello', 'world'], help='List of indices')
    parser.add_argument('--rate-mean', type=float, default=100, help='Mean rate')
    parser.add_argument('--rate-period', type=float, default=1500, help='Rate period')
    parser.add_argument('--mean-cpu', type=float, default=6.2e7, help='Mean CPU')
    parser.add_argument('--std-cpu', type=float, default=4e7, help='Std dev of CPU')
    parser.add_argument('--mean-ram', type=float, default=8e9, help='Mean RAM')
    parser.add_argument('--std-ram', type=float, default=1e8, help='Parameter for RAM distribution')
    parser.add_argument('--save-path', type=str, default='/tmp/workload.jsonl', help='Path to save the workload')
    p = parser.parse_args()

    asyncio.run(generate(p.start, p.until, p.indices, p.rate_mean, p.rate_period,
                         p.mean_cpu, p.std_cpu, p.mean_ram, p.std_ram, p.save_path))

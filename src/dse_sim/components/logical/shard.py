# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

from collections import defaultdict
from typing import Dict, List, Union

from dse_sim.collection.actor import Actor, ProfilableActor
from dse_sim.collection.index import Index
from dse_sim.util import seeded_random as random


class Shard(ProfilableActor):
    def run_profiler(self) -> Union[float, Dict[str, float]]:
        out = {}

        for worker in sorted(set(self._cpu_cycles).union(self._num_requests).union(self._size), key=str):
            out[f'cpu_cycles({worker.id})'  ] = self._cpu_cycles.get(worker, 0.0)
            out[f'num_requests({worker.id})'] = self._num_requests.get(worker, 0.0)
            out[f'size({worker.id})'] = self._size.get(worker, 0.0)

        self._cpu_cycles = defaultdict(float)
        self._num_requests = defaultdict(float)

        return out

    def __init__(self, index: Index):
        super().__init__(index.collection)
        self.index = index

        self.workers = list()

        self._cpu_cycles = defaultdict(float)
        self._num_requests = defaultdict(float)
        self._size = defaultdict(float)

    async def reassign(self, old_worker: 'Worker', new_worker: 'Worker'):
        self.workers[self.workers.index(old_worker)] = new_worker

    def assign_worker(self, worker: 'Worker'):
        if worker not in self.workers:
            self.workers.append(worker)

    def any_worker(self) -> 'Worker':
        return random.choice(self.workers)

    @property
    def primary_worker(self) -> 'Worker':
        return self.workers[0]

    @property
    def replica_workers(self) -> List['Worker']:
        return self.workers[1:]

    def log_compute(self, worker: 'Worker', cpu_cycles: float):
        self._cpu_cycles[worker] += cpu_cycles

    def log_request(self, worker: 'Worker'):
        self._num_requests[worker] += 1

    def log_size(self, worker: 'Worker', size: float):
        self._size[worker] += size

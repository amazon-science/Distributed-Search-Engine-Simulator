# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

from __future__ import annotations

import asyncio
import itertools
import math
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, TYPE_CHECKING

from dse_sim.collection.actor import ProfilableActor
from dse_sim.collection.dse_collection import DSECollection
from dse_sim.components.logical.worker import Worker, WorkerStatus
from dse_sim.config import DEBUG
from dse_sim.util import seeded_random

if TYPE_CHECKING:
    from dse_sim.requests.http import HTTPRequest
    from dse_sim.components.logical.shard import Shard


class Index(ProfilableActor):
    repr = [(None, 'name')]

    def run_profiler(self) -> Dict[str, float]:
        out = {
            'total_requests': self._total_requests,
            'latency': self._total_latency / (sum(self._results.values()) or 1),
            'num_shards': len(self.shards)
        }

        self._total_requests = 0
        self._total_latency = 0

        out.update(sorted(self._results.items()))
        self._results = defaultdict(int)

        return out

    _hash_p: int = 1610612741

    def __init__(self, collection: DSECollection, name: str, shards: Iterable[Shard]):
        self.name = name
        super().__init__(collection)

        self.shards: List[Shard] = list(shards)

        self._hash_a = seeded_random.randrange(0, Index._hash_p)
        self._hash_b = seeded_random.randrange(0, Index._hash_p)

        self._total_requests = 0
        self._total_latency = 0
        self._results = defaultdict(int)
        self._num_replicas = 1

    def _add_shard(self, shard: 'Shard') -> None:
        self.shards.append(shard)

    def _remove_shard(self, shard: 'Shard') -> None:
        self.shards.remove(shard)

    async def create_shard(self, worker: Worker, instant: bool = False) -> 'Shard':
        from dse_sim.components.logical.shard import Shard

        if DEBUG:
            self.log(f"Spawning new shard.")
        await self.collection.clock.yield_for(self.collection.config.SHARD_CREATION_TIME if not instant else 0)
        shard = Shard(self)
        shard.workers.append(worker)

        if DEBUG:
            self.log(f"Done spawning shard.")

        self._add_shard(shard)
        return shard

    async def create_shards(self, worker: Worker, num_shards: int, instant: bool = False):
        tasks = [asyncio.create_task(self.create_shard(worker, instant)) for _ in range(num_shards)]
        out = await asyncio.gather(*tasks)
        _ = [t.done() for t in tasks]
        return out

    def get_shard_for(self, document_id: int) -> Shard:
        return self.shards[self._hash(document_id) % len(self.shards)]

    def get_shard_from_hash(self, hash_: int):
        return self.shards[hash_ % len(self.shards)]

    def _hash(self, document_id: int) -> int:
        return (self._hash_a * document_id + self._hash_b) % self._hash_p

    async def compute_shard(self, document_id: int, given_hash: Optional[int] = None) -> 'Shard':
        if given_hash:
            if given_hash < 0:
                shard_id = -given_hash
                return self.shards[shard_id-1]
            else:
                hash_ = self._hash(given_hash)
        else:
            # hash_timeout = self.collection.config.COMPUTE_HASH_TIMEOUT
            # await self.collection.yield_for(hash_timeout)
            hash_ = self._hash(document_id)

        return self.get_shard_from_hash(hash_)

    def log_request(self, request: HTTPRequest):
        self._total_requests += 1

    def log_result(self, request: HTTPRequest, result: str, latency: float):
        self._results[result] += 1
        self._total_latency += latency

    @property
    def num_replicas(self) -> int:
        return len(self.shards[0].replica_workers)

    async def transfer_shards_for_blue_green(self, new_workers: List[Worker]):
        assert all(worker.status == WorkerStatus.GREEN for worker in new_workers)
        new_worker_iter = itertools.cycle(new_workers)

        new_assignment = []
        for shard in self.shards:
            for old_worker in shard.workers:
                assert old_worker.status == WorkerStatus.BLUE

                new_worker = next(new_worker_iter)
                new_assignment.append((shard, old_worker, new_worker))

        reassign_shard_tasks = [
            asyncio.create_task(shard.reassign(old_worker, new_worker))
            for (shard, old_worker, new_worker) in new_assignment
        ]
        result = await asyncio.gather(*reassign_shard_tasks)
        _ = [t.done() for t in reassign_shard_tasks]

        return result

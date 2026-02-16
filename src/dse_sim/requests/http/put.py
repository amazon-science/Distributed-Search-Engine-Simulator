# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import asyncio
import json
from typing import List, Optional

from dse_sim.collection.dse_collection import DSECollection
from dse_sim.collection.index import Index
from dse_sim.components.logical.shard import Shard
from dse_sim.components.logical.worker import Worker
from dse_sim.requests.http import HTTPRequest


class PutRequest(HTTPRequest):
    @staticmethod
    def from_json(collection: DSECollection, s: str):
        d = json.loads(s)
        time = d['time']
        index = collection.get_index(d['index'])
        document_id = d['document_id']
        cpu_size = d['cpu_size']
        mem_size = d['mem_size']
        given_hash = None if (d['given_hash'] == []) else d['given_hash']
        return PutRequest(time, index, document_id, cpu_size, mem_size, given_hash)

    def to_json(self) -> str:
        return json.dumps({
            'type': 'PUT',
            'time': self.time,
            'index': self.index.name,
            'document_id': self.document_id,
            'cpu_size': self.cpu_size,
            'mem_size': self.mem_size,
            'given_hash': self.given_hash or []
        })

    def __init__(self,
                 time: float,
                 index: Index,
                 document_id: int,
                 cpu_size: float,
                 mem_size: float,
                 given_hash: Optional[int] = None):

        super().__init__(time, index)
        self.document_id = document_id

        self.cpu_size = cpu_size
        self.mem_size = mem_size
        self.given_hash = given_hash

    async def process(self):
        await self.collection.yield_until(self.time)

        shard: Shard = await self.index.compute_shard(self.document_id, self.given_hash)

        primary_worker: Worker = shard.primary_worker
        replica_workers: List[Worker] = shard.replica_workers

        # first set of actions -- write to primary worker
        primary_response = await self.write_for_worker(shard, primary_worker)

        if primary_response.startswith('5') or not replica_workers:
            return primary_response

        self.completion_time = self.collection.time

        # after that's done, second set of actions goes to ALL replicas
        replica_tasks = [self.write_for_worker(shard, worker) for worker in replica_workers]
        responses = await asyncio.gather(*replica_tasks)

        return max(responses)

    async def write_for_worker(self, shard: Shard, worker: Worker) -> str:
        worker.log_request(self)

        shard.log_request(worker)
        shard.log_compute(worker, self.cpu_size)
        shard.log_size(worker, self.mem_size)

        actions = [
            lambda w: w.reserve_compute(self.cpu_size, self.mem_size, label=repr(self)),
            lambda w: w.reserve_disk('write', self.mem_size, label=repr(self))
        ]

        response = '4xx'
        for idx, action in enumerate(actions):
            result = await action(worker)
            if result == '5xx':
                response = result

        worker.log_result(self, response)
        return response

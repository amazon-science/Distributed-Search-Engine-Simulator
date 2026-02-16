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
from dse_sim.util import seeded_random as random


class SearchRequest(HTTPRequest):
    @staticmethod
    def from_json(collection: DSECollection, s: str):
        d = json.loads(s)
        time = d['time']
        index = collection.get_index(d['index'])
        cpu_size_query = d['cpu_size_query']
        mem_size_query = d['mem_size_query']
        cpu_size_response = d['cpu_size_response']
        mem_size_response = d['mem_size_response']
        cpu_size_collate = d['cpu_size_collate']
        given_hashes = d['given_hashes'] or None

        return SearchRequest(time, index,
                             cpu_size_query, mem_size_query, cpu_size_response,
                             mem_size_response, cpu_size_collate, given_hashes)

    def to_json(self) -> str:
        return json.dumps({
            'type': 'SEARCH',
            'time': self.time,
            'index': self.index.name,
            'cpu_size_query': self.cpu_size_query,
            'mem_size_query': self.mem_size_query,
            'cpu_size_response': self.cpu_size_response,
            'mem_size_response': self.mem_size_response,
            'cpu_size_collate': self.cpu_size_collate,
            'given_hashes': self.given_hashes or []
        })

    def __init__(self,
                 time: float,
                 index: Index,
                 cpu_size_query: float,
                 mem_size_query: float,
                 cpu_size_response: float,
                 mem_size_response: float,
                 cpu_size_collate: float,
                 given_hashes: Optional[List[int]] = None):

        super().__init__(time, index)

        self.cpu_size_query = cpu_size_query
        self.mem_size_query = mem_size_query

        self.cpu_size_response = cpu_size_response
        self.mem_size_response = mem_size_response

        self.cpu_size_collate = cpu_size_collate

        self.given_hashes = given_hashes

    async def process(self):
        await self.collection.yield_until(self.time)

        # identify the relevant shards
        if self.given_hashes is None:
            shards: List[Shard] = self.index.shards
        else:
            shards: List[Shard] = list(set(self.index.get_shard_from_hash(hash_) for hash_ in self.given_hashes))

        # get a worker cover for the shards -- may contain duplicates!!
        worker_cover = {shard: shard.any_worker() for shard in shards}

        shard_worker_pairs = list(worker_cover.items())
        relevant_workers = list(worker_cover.values())

        # issue the per-shard search tasks to workers
        tasks = [asyncio.create_task(self.worker_process(shard, worker)) for shard,worker in shard_worker_pairs]
        responses = await asyncio.gather(*tasks)
        _ = [t for t in tasks if t.done()]

        worst_response = max(responses)
        if worst_response.startswith('5'):
            return worst_response

        # collate the responses on any of these workers
        # for now we use the one hosting the most relevant shards, ties broken at random
        any_worker = max(set(relevant_workers), key=lambda x: relevant_workers.count(x) + random.random())

        response = await any_worker.reserve_compute(
            self.cpu_size_collate,
            len(shards) * self.mem_size_response,
            label=repr(self)
        )

        return response

    async def worker_process(self, shard: Shard, worker: Worker) -> str:
        worker.log_request(self)

        shard.log_request(worker)
        shard.log_compute(worker, self.cpu_size_query + self.cpu_size_response)

        actions = [
            lambda w: w.reserve_compute(self.cpu_size_query, self.mem_size_query, label=repr(self)),
            lambda w: w.reserve_disk('read', self.mem_size_response, label=repr(self)),
            lambda w: w.reserve_compute(self.cpu_size_response, self.mem_size_response, label=repr(self)),
        ]

        response = '4xx'
        for idx, action in enumerate(actions):
            result = await action(worker)
            if result == '5xx':
                response = result

        worker.log_result(self, response)
        return response

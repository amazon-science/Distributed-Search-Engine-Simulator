# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

from __future__ import annotations

import asyncio
import sys
from collections import defaultdict
from enum import Enum
from typing import Dict, Optional, TYPE_CHECKING

from dse_sim.collection import DSEError
from dse_sim.collection.actor import ProfilableActor
from dse_sim.collection.dse_collection import DSECollection
from dse_sim.components.physical.disk import Disk
from dse_sim.components.physical.ram import RAM, RAMCapacityError
from dse_sim.config import DEBUG

if TYPE_CHECKING:
    from dse_sim.requests.http import HTTPRequest


class WorkerStatus(Enum):
    OBSOLETE = -1
    BLUE = 1
    GREEN = 2



class Worker(ProfilableActor):
    def __init__(self,
                 collection: DSECollection,
                 initial_cpus: Optional[int],
                 cpu_spec: 'CPUSpec',
                 memory_bytes: float,
                 status: WorkerStatus = WorkerStatus.BLUE):

        from dse_sim.components.physical.cpu import CPUPool

        super().__init__(collection)

        self.cpu_pool: CPUPool

        if initial_cpus is not None:
            self.cpu_pool = CPUPool(self, initial_cpus, cpu_spec,
                                    queue_capacity=self.collection.dse_config.queue_length)

        self.memory = RAM(self, memory_bytes)
        self.disk = Disk(self, self.collection.config.DISK_READ_SPEED, self.collection.config.DISK_WRITE_SPEED)

        self._total_requests = 0
        self._results = defaultdict(int)

        self.queue_length = 0
        self.request_count = 0

        self.status = status

    def run_profiler(self) -> Dict[str, float]:
        out = {'total_requests': self._total_requests}
        self._total_requests = 0

        out.update(sorted(self._results.items()))
        self._results = defaultdict(int)

        out['QueueLength'] = self.queue_length

        return out

    async def reserve_disk(self, method: str, bytes: float, *, label=None):
        assert method in ('read', 'write')

        if DEBUG:
            suffix = f" for {label}" if label is not None else ""

        if DEBUG:
            self.log(f"Sending {method} {bytes} byte request to disk{suffix}")

        try:
            if self.memory.available < bytes:
                return '5xx'
                raise RAMCapacityError()

            m = getattr(self.disk, {'read': 'reserve_read', 'write': 'reserve_write'}[method])
            disk_task = m(bytes)
            result = await self.memory.reserve_pending(bytes, disk_task)
            if result == '5xx':
                return '5xx'

            if DEBUG:
                self.log(f"Successfully {method} {bytes} bytes{suffix}")
            return '4xx'
        except DSEError as e:
            if DEBUG:
                self.log(f"5xx ERROR when issuing {method} for {bytes} bytes{suffix}: {e.__class__.__name__}({e})")
            return '5xx'

    async def reserve_compute(self, cpu_cycles: float, mem_bytes: float, *, label=None):
        await self.collection.yield_for(1e-9)

        request_id = self.request_count
        self.request_count += 1

        if DEBUG:
            suffix = f" for {label}" if label is not None else ""

        if DEBUG:
            self.log(f"Reserving {cpu_cycles} CPU cycles and {mem_bytes} bytes of RAM{suffix} for request{request_id}")

        if self.queue_length >= self.collection.dse_config.queue_length:
            if DEBUG:
                self.log(f"Reservation for request{request_id} failed due to queue length")
            return '5xx'
        elif self.memory.available < mem_bytes:
            # if DEBUG:
            #     self.log(f"request{request_id} is waiting for memory (available: {self.memory.available}, needed: {mem_bytes}).")
            # await self.memory.next
            return '5xx'

        queue_delta = 1
        try:
            self.queue_length += queue_delta

            cpu_task = self.cpu_pool.reserve(cpu_cycles)
            if '5xx' == await self.memory.reserve_pending(mem_bytes, cpu_task):
                return '5xx'

            if DEBUG:
                self.log(f"Successful cpu/mem reservation{suffix}")

            self.queue_length -= queue_delta
            queue_delta = 0
            return '4xx'
        except DSEError as e:
            self.queue_length -= queue_delta
            if DEBUG:
                self.log(
                    f"5xx ERROR when reserving {cpu_cycles} cycles and {mem_bytes} bytes: {e.__class__.__name__}({e})")
            return '5xx'
        except Exception as e:
            self.queue_length -= queue_delta
            raise e

    @staticmethod
    async def spin_up(collection: DSECollection, initial_cpus: int, cpu_spec: 'CPUSpec', memory_bytes: float,
                      queue_capacity: int, count: int = 1):
        from dse_sim.components.physical.cpu import CPUPool

        workers = [Worker(collection, None, cpu_spec, memory_bytes, status=WorkerStatus.GREEN) for _ in range(count)]

        if DEBUG:
            collection.log(f"Spinning up", workers)

        cpu_pool_tasks = [CPUPool.spin_up(worker, initial_cpus, cpu_spec, queue_capacity=queue_capacity) for worker in workers]
        cpu_pools = await asyncio.gather(*cpu_pool_tasks, return_exceptions=True)
        for worker, pool in zip(workers, cpu_pools):
            worker.cpu_pool = pool

        await collection.clock.yield_for(collection.config.WORKER_READY_TIME)

        return workers

    async def spin_down(self):
        assert self.status == WorkerStatus.OBSOLETE

        if DEBUG:
            self.log(f"Spinning down.")
        await self.cpu_pool.spin_down()
        self.delete()

    def log_request(self, request: HTTPRequest):
        self._total_requests += 1

    def log_result(self, request: HTTPRequest, result: str):
        self._results[result] += 1


class WorkerQueueError(DSEError):
    pass


class WorkerShutDownError(DSEError):
    pass

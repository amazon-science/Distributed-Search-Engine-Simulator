# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

from typing import Dict, Optional

from dse_sim.collection.actor import ProfilableActor
from dse_sim.config import DEBUG


class Disk(ProfilableActor):
    def __init__(self, worker: 'Worker', read_speed: float, write_speed: float):
        self.worker = worker

        self.read_speed = read_speed
        self.write_speed = write_speed

        self._last_read_start_time: Optional[float] = None
        self._last_write_start_time: Optional[float] = None

        self._total_read = 0
        self._total_write = 0

        super().__init__(worker.collection)

    def run_profiler(self) -> Dict[str, float]:
        current_time = self.collection.time
        total_time = current_time - self.last_profiled

        completed_reads = self._total_read
        completed_writes = self._total_write

        self._total_read = 0.0
        self._total_write = 0.0

        if self._last_read_start_time is not None:
            partial_reads = (current_time - self._last_read_start_time) * self.read_speed
            self._last_read_start_time = current_time
        else:
            partial_reads = 0.

        if self._last_write_start_time is not None:
            partial_writes = (current_time - self._last_write_start_time) * self.write_speed
            self._last_write_start_time = current_time
        else:
            partial_writes = 0.

        total_reads = completed_reads + partial_reads
        total_writes = completed_writes + partial_writes

        read_utilization_frac = total_reads / (total_time * self.read_speed)
        write_utilization_frac = total_writes / (total_time * self.write_speed)

        return {
            'read_utilization_frac': read_utilization_frac,
            'write_utilization_frac': write_utilization_frac,
            'read_bytes': total_reads,
            'write_bytes': total_writes,
            'read_rate_bytes': total_reads / total_time,
            'write_rate_bytes': total_writes / total_time,
        }

    async def reserve_write(self, bytes: float):
        duration = bytes / self.write_speed

        self._last_write_start_time = self.collection.time

        if DEBUG:
            self.log(f"Writing for {duration} until {self.collection.time + duration}")
        await self.collection.yield_for(duration)
        if DEBUG:
            self.log(f"Completed write of {bytes} bytes.")

        self._total_write += bytes
        self._last_write_start_time = None

    async def reserve_read(self, bytes: float):
        duration = bytes / self.read_speed

        self._last_read_start_time = self.collection.time

        if DEBUG:
            self.log(f"Reading for {duration} until {self.collection.time + duration}")

        await self.collection.yield_for(duration)

        if DEBUG:
            self.log(f"Completed read of {bytes} bytes.")

        self._total_read += bytes
        self._last_read_start_time = None

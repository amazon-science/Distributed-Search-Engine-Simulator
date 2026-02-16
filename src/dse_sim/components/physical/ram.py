# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

from asyncio import Future
from typing import Dict

from dse_sim.collection import DSEError
from dse_sim.collection.actor import ProfilableActor
from dse_sim.config import DEBUG


class RAM(ProfilableActor):
    def __init__(self, worker: "Worker", capacity: float):
        self.worker: "Worker" = worker
        self.capacity: float = capacity
        self.utilized: float = 0

        self._integrated_utilization = 0.0
        self._ongoing_reservations = set()

        super().__init__(worker.collection)

    def run_profiler(self) -> Dict[str, float]:
        current_time = self.collection.time
        last_profiled = self.last_profiled

        total_time = current_time - last_profiled

        partial_utilization = sum(
            (current_time - max(last_profiled, start)) * num_bytes
            for (start, num_bytes, _) in self._ongoing_reservations
        )
        utilization_total = self._integrated_utilization + partial_utilization

        utilization_avg = utilization_total / total_time
        self._integrated_utilization = 0.0
        return {
            f"utilization_avg({self.worker.id})": utilization_avg,
            f"utilization_ratio({self.worker.id})": utilization_avg / self.capacity,
        }

    @property
    def available(self):
        return self.capacity - self.utilized

    async def reserve_pending(self, num_bytes: float, future: Future):
        if self.available < num_bytes:
            return '5xx'
            raise RAMCapacityError(
                f"Tried to allocate {num_bytes} bytes but only {self.available} out of {self.capacity} bytes are free."
            )

        start = self.collection.time
        reservation = (start, num_bytes, object())

        if DEBUG:
            self.log(f"Allocating {num_bytes} bytes of memory")

        self._ongoing_reservations.add(reservation)
        self.utilized += num_bytes

        result = await future

        end = self.collection.time
        self._integrated_utilization += num_bytes * (
                end - max(start, self.last_profiled)
        )

        self._ongoing_reservations.remove(reservation)
        self.utilized -= num_bytes

        if DEBUG:
            self.log(f"Freed {num_bytes} bytes of memory")

        return result


class RAMCapacityError(DSEError):
    pass

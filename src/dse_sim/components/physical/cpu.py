# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import asyncio
from asyncio import Future
from dataclasses import dataclass
from typing import Dict, List, Optional

from dse_sim.collection import DSEError
from dse_sim.collection.actor import Actor, ProfilableActor
from dse_sim.components.logical.worker import Worker, WorkerShutDownError
from dse_sim.config import Config, DEBUG
from dse_sim.util.bounded_queue import BoundedQueue


class CPU(ProfilableActor):
    def __init__(self, pool: 'CPUPool', freq: float):
        self.pool: CPUPool = pool
        self.freq = freq

        self._last_reserved_time: Optional[float] = None
        self._busy_time = 0
        self._current_task = None
        self._enabled = True
        self._empty_profile_count = 0

        self.blue_green = False
        self.baseline_utilization = 0.0

        super().__init__(pool.collection)

    def start_blue_green(self):
        assert self.blue_green is False
        self.blue_green = self.collection.time

        self.freq *= (1 - Config.BLUE_GREEN_CPU_UTILIZATION)
        self.baseline_utilization = Config.BLUE_GREEN_CPU_UTILIZATION

    def run_profiler(self) -> Dict[str, float]:
        if self._busy_time == 0 and not self.busy:
            self._empty_profile_count += 1
            if self._empty_profile_count >= 3:
                return {}

        current_time = self.collection.time
        total_time = current_time - self.last_profiled
        completed_duration = self._busy_time
        self._busy_time = 0.0

        if self.busy:
            partial_duration = (current_time - self._last_reserved_time) * (1 - self.baseline_utilization)
            self._last_reserved_time = current_time
        else:
            partial_duration = 0.0

        utilization_frac = (completed_duration + partial_duration) / total_time

        baseline_utilization = self.baseline_utilization * (current_time - max(self.last_profiled, self.blue_green)) / total_time

        return {f'utilization_frac({self.pool.worker.id})': baseline_utilization + utilization_frac}

    async def reserve(self, cycles: float):
        if self not in self.pool.cpus:
            raise CPUShutDownError()

        assert self._last_reserved_time is None

        duration = cycles / self.freq

        self._last_reserved_time = self.collection.time

        if DEBUG:
            self.log(f"Busy for {duration} until {self.collection.time + duration}")

        self.pool.available_cpus.remove(self)
        await self.collection.yield_for(duration)

        if self in self.pool.cpus:
            self.pool.available_cpus.append(self)

        self._busy_time += min(duration, self.collection.time - self._last_reserved_time) * (1 - self.baseline_utilization)
        self._last_reserved_time = None

    @property
    def busy(self):
        return self._last_reserved_time is not None

    @property
    def enabled(self):
        return self._enabled

    def disable(self):
        self._enabled = False


class CPUPool(Actor):
    def __init__(self, worker: Worker, num_initial_cpus: int, cpu_spec: 'CPUSpec', queue_capacity: int):
        super().__init__(worker.collection)

        self.worker: Worker = worker

        self.cpu_spec = cpu_spec
        self.cpus: List[CPU] = [CPU(self, cpu_spec.frequency) for _ in range(num_initial_cpus)]
        self.available_cpus: List[CPU] = list(self.cpus)
        self.spinning_down = False

        self.queue: BoundedQueue[Future] = BoundedQueue(queue_capacity)

    @staticmethod
    async def spin_up(worker: Worker, initial_cpus: int, cpu_spec: 'CPUSpec', queue_capacity: int) -> 'CPUPool':
        pool = CPUPool(worker, 0, cpu_spec, queue_capacity)

        if DEBUG:
            pool.log(f"Spinning up CPUs in {pool}")
        cpu_spin_ups: List[Future] = [asyncio.ensure_future(pool.spin_up_cpu()) for _ in range(initial_cpus)]
        await asyncio.gather(*cpu_spin_ups)
        if DEBUG:
            pool.log(f"Completed spin-up of {pool}")

        return pool

    async def spin_down(self) -> None:
        if DEBUG:
            self.log(f"Scheduling spin-down.")
        self.spinning_down = True
        
        # Mark queue as final to prevent new items
        self.queue.push_final(None)  # Use None as a sentinel, not a future

        # Wait for all existing tasks in the queue to complete
        queue_futures = list(self.queue)
        if queue_futures:
            # TODO handle this correctly
            if DEBUG:
                self.log(f"Waiting for {len(queue_futures)} queued tasks to complete")
            for future in queue_futures:
                future.set_exception(WorkerShutDownError())

        if DEBUG:
            self.log("Begin spin-down.")

        # spin down the cpus
        cpu_spin_downs: List[Future] = [asyncio.create_task(self.spin_down_cpu(cpu)) for cpu in self.cpus]
        #await asyncio.gather(*cpu_spin_downs, return_exceptions=True)

        self.delete()

    async def spin_up_cpu(self) -> CPU:
        cpu = CPU(self, self.cpu_spec.frequency)

        await self.collection.yield_for(self.collection.config.CPU_SPIN_UP_DELAY)
        self._add_cpu(cpu)
        return cpu

    async def spin_down_cpu(self, cpu: Optional[CPU] = None) -> CPU:
        if len(self.cpus) == 1 and not self.spinning_down:
            raise CPUShutDownError("Cannot spin down last CPU")

        if cpu is None:
            if self.available_cpus:
                cpu = self.available_cpus.pop()
            else:
                cpu = self.cpus[-1]

        cpu.disable()
        self._remove_cpu(cpu)
        await self.collection.yield_for(self.collection.config.CPU_SPIN_DOWN_DELAY)
        return cpu

    def _add_cpu(self, cpu: CPU) -> None:
        assert cpu not in self.cpus
        self.cpus.append(cpu)
        self.available_cpus.append(cpu)

    def _remove_cpu(self, cpu: CPU) -> None:
        self.cpus.remove(cpu)
        if cpu in self.available_cpus:
            self.available_cpus.remove(cpu)

    async def reserve(self, cycles: float):
        """Request a CPU from the pool for a given number of cycles."""

        if self.available_cpus:
            cpu = self.available_cpus[0]
            assert cpu._last_reserved_time is None
        elif self.queue.full:
            raise QueueFullError(f"CPU request queue capacity ({self.queue.capacity}) exceeded")
        else:
            # No CPU available, create a future and wait
            future = asyncio.Future()
            self.queue.push(future)

            self._assign_if_available()

            # Wait until a CPU becomes available
            await future

            # At this point, the future has been resolved with a CPU
            cpu = future.result()
            assert cpu._last_reserved_time is None

        if DEBUG:
            self.log(f"Running for {cycles} cycles.")
        await cpu.reserve(cycles)

        if DEBUG:
            self.log(f"Completed request of {cycles} cycles.")
        self.release(cpu)

    def _assign_if_available(self):
        if self.available_cpus and self.queue:
            future = self.queue.pop()
            cpu = self.available_cpus[0]
            assert cpu._last_reserved_time is None
            future.set_result(cpu)

    def release(self, cpu: CPU):
        """Release a CPU back to the pool"""
        # If tasks are waiting, give the CPU to the next task

        if (cpu.enabled or self.spinning_down) and self.queue:
            next_future = self.queue.pop()
            next_future.set_result(cpu)
        else:
            # Otherwise return CPU to the available pool
            pass  # available_cpus is set elsewhere # self.available_cpus.append(cpu)

    @property
    def queue_length(self):
        """Return the current queue length"""
        return len(self.queue)


@dataclass(frozen=True)
class CPUSpec:
    frequency: float


class QueueFullError(DSEError):
    pass


class CPUShutDownError(DSEError):
    pass

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import asyncio
import sys
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterator, List, Optional, Type, TypeVar, Union

from termcolor import colored

from dse_sim.collection import DSEError
from dse_sim.config import Config, DEBUG
from dse_sim.event.clock import Clock

T = TypeVar('T', bound='Actor')


@dataclass
class DSECollectionConfig:
    num_workers: int
    half_ocus_per_worker: int
    cpu_frequency: float = Config.CPU_FREQUENCY
    queue_length: int = 100

    @property
    def cpus_per_worker(self):
        return max(1, self.half_ocus_per_worker // 2)

    @property
    def memory_per_worker(self):
        return self.half_ocus_per_worker * Config.MEMORY_PER_HALF_OCU


class DSECollection(object):
    config = Config

    def __init__(self,
                 name: str,
                 dse_config: DSECollectionConfig,
                 log_file: Union[None, bool, str] = None):

        from dse_sim.components.logical.worker import Worker
        from dse_sim.components.physical.cpu import CPUSpec

        self.steady = True
        self.dse_config = dse_config
        self.name: str = name
        self.actors: List['Actor'] = []
        self.actor_map: Dict[Type['Actor'], List['Actor']] = defaultdict(list)
        self.index_map: Dict[str, 'Index'] = {}
        self.clock = Clock()

        if log_file is False:
            self.log_file = False
        elif log_file is None:
            self.log_file = sys.stderr
        else:
            self.log_file = open(log_file, 'a')

        for i in range(self.dse_config.num_workers):
            w = Worker(self, self.dse_config.cpus_per_worker,
                       CPUSpec(frequency=self.dse_config.cpu_frequency), self.dse_config.memory_per_worker)
            self.register(w)

    def register(self, actor: 'Actor') -> None:
        if actor not in self.actors:
            if DEBUG:
                actor.log(f"Registering with {self}.")
            self.actors.append(actor)
            self.actor_map[actor.__class__].append(actor)

    def unregsiter(self, actor: 'Actor') -> None:
        if actor in self.actors:
            if DEBUG:
                actor.log(f"Unregistering with {self}.")
            self.actors.remove(actor)
            self.actor_map[actor.__class__].remove(actor)

    def get_by_type(self, actor_cls: Type[T]) -> Iterator[T]:
        for t in self.actor_map:
            if issubclass(t, actor_cls):
                yield from self.actor_map[t]

    @property
    def profilable_actors(self) -> Iterator['ProfilableActor']:
        from dse_sim.collection.actor import ProfilableActor
        return self.get_by_type(ProfilableActor)

    @property
    def time(self):
        return self.clock.time

    def __repr__(self):
        return f'DSECollection({self.name})'

    def log(self, message: str, actor: Optional['Actor'] = None):
        if self.log_file is False:
            return
        if actor is None:
            s = f"[{self.time:16.4f}] [{self.name:<20}] {message}"
        else:
            s = f"[{self.time:16.4f}] [{self.name + '/' + repr(actor):<20}] {message}"
        if self.log_file is sys.stderr:
            s = colored(s, 'red')
        print(s, file=self.log_file)

    async def add_index(self, name: str, instant: bool = False):
        from dse_sim.collection.index import Index
        if DEBUG:
            self.log(f"Creating index {name}")
        await self.clock.yield_for(self.config.INDEX_CREATION_TIME if not instant else 0)
        self.index_map[name] = Index(self, name, [])
        if DEBUG:
            self.log(f"Done creating index {name}")

        return self.index_map[name]

    async def scale_to(self, num_workers: int, half_ocus_per_worker: int):
        if not self.steady and not self.config.IGNORE_STEADY:
            raise DomainNotSteadyError("Cannot scale out while domain is not steady!")

        if (self.dse_config.num_workers == num_workers and
                self.dse_config.half_ocus_per_worker == half_ocus_per_worker):
            return '4xx'

        if (num_workers != self.dse_config.num_workers and
                half_ocus_per_worker != self.dse_config.half_ocus_per_worker):
            pass#raise InvalidScalingError("Cannot simultaneously change the number of workers and ocus per worker")

        if num_workers < 1:
            raise InvalidScalingError("Cannot scale in last worker")
        elif half_ocus_per_worker < 1:
            raise InvalidScalingError("Cannot scale in to < 0.5 OCUs")

        return await self.scale_workers(num_workers, half_ocus_per_worker)


    async def scale_workers(self, new_num_workers: int, new_half_ocus_per_worker: int):
        from dse_sim.components.logical.worker import Worker
        from dse_sim.components.physical.cpu import CPUSpec
        from dse_sim.components.logical.worker import WorkerStatus

        if not self.steady and not self.config.IGNORE_STEADY:
            raise DomainNotSteadyError("Cannot scale while domain is not steady!")

        self.steady = False
        try:
            # gather old workers
            old_workers = list(self.workers)
            for worker in old_workers:
                worker.cpu_pool.cpu_spec = CPUSpec(worker.cpu_pool.cpu_spec.frequency / 2)
                for cpu in worker.cpu_pool.cpus:
                    cpu.start_blue_green()

            self.dse_config.half_ocus_per_worker = new_half_ocus_per_worker
            # create green workers

            self.dse_config.num_workers += new_num_workers # higher count while rebalancing
            new_workers = await Worker.spin_up(self, self.dse_config.cpus_per_worker,
                                               CPUSpec(self.dse_config.cpu_frequency),
                                               self.dse_config.memory_per_worker,
                                               self.dse_config.queue_length,
                                               count=new_num_workers)

            # rebalance
            if DEBUG:
                self.log(f"Rebalancing shards across workers following creation of {new_workers}")
            rebalancing_tasks = [index.transfer_shards_for_blue_green(new_workers) for index in self.indices]
            result = await asyncio.gather(*rebalancing_tasks, return_exceptions=True)

            # update status
            for new_worker in new_workers:
                new_worker.status = WorkerStatus.BLUE
            for old_worker in old_workers:
                old_worker.status = WorkerStatus.OBSOLETE

            # spin down old workers
            spin_down_tasks = [old_worker.spin_down() for old_worker in old_workers]
            result = await asyncio.gather(*spin_down_tasks, return_exceptions=True)

            # update count
            self.dse_config.num_workers = new_num_workers
        finally:
            self.steady = True

        return new_workers

    async def scale_up_workers(self, count: int = 1):
        if not self.steady and not self.config.IGNORE_STEADY:
            raise DomainNotSteadyError("Cannot scale up while domain is not steady!")
        return await self.scale_to(self.dse_config.num_workers, self.dse_config.half_ocus_per_worker + count)

    async def scale_down_workers(self, count: int = 1):
        if not self.steady and not self.config.IGNORE_STEADY:
            raise DomainNotSteadyError("Cannot scale down while domain is not steady!")
        return await self.scale_to(self.dse_config.num_workers, self.dse_config.half_ocus_per_worker - count)

    async def scale_in_worker(self, count: int = 1):
        if not self.steady and not self.config.IGNORE_STEADY:
            raise DomainNotSteadyError("Cannot scale in while domain is not steady!")
        return await self.scale_to(self.dse_config.num_workers - count, self.dse_config.half_ocus_per_worker)

    async def scale_out_worker(self, count: int = 1):
        if not self.steady and not self.config.IGNORE_STEADY:
            raise DomainNotSteadyError("Cannot scale out while domain is not steady!")
        return await self.scale_to(self.dse_config.num_workers + count, self.dse_config.half_ocus_per_worker)

    async def run(self):
        return asyncio.create_task(self.clock.run())

    async def wrap_up(self):
        await asyncio.sleep(0)

    async def tear_down(self, when: Optional[float] = None):
        if when:
            await self.clock.yield_until(when - self.time)

        if DEBUG:
            self.log("Tearing down.")
        await self.clock.stop()
        for actor in list(self.actors):
            actor.tear_down()

    @property
    def workers(self) -> List['Worker']:
        from dse_sim.components.logical.worker import Worker
        return list(self.get_by_type(Worker))

    @property
    def indices(self) -> List['Index']:
        return list(self.index_map.values())

    def get_index(self, name: str) -> 'Index':
        return self.index_map[name]

    def yield_for(self, event: Union['Event', float]):
        return self.clock.yield_for(event)

    def yield_until(self, event: Union['Event', float]):
        return self.clock.yield_until(event)


class ScaleDownLastOCUError(DSEError):
    pass


class ScaleInLastWorkerError(DSEError):
    pass


class DomainNotSteadyError(DSEError):
    pass


class InvalidScalingError(DSEError):
    pass


class InvalidIndexError(DSEError):
    pass

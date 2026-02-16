# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import asyncio
import json
import math
import os
import sys
import traceback
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Callable, Coroutine, Optional, Union

import pandas

from dse_sim.collection.dse_collection import DSECollection, InvalidIndexError
from dse_sim.requests.actions.shut_down import ShutDownRequest
from dse_sim.requests.http.get import GetRequest
from dse_sim.requests.http.put import PutRequest
from dse_sim.requests.http.search import SearchRequest
from dse_sim.util import seeded_random, seeded_random as random


class WorkloadGenerator(ABC):
    def __init__(self, collection: DSECollection, *, save_path: Optional[str] = None):
        self.collection = collection
        self.count = 0
        self._active = True
        self.background_tasks = set()  # Track tasks

        self.save = save_path is not None
        if self.save:
            self.save_file = open(save_path, 'w')

    @abstractmethod
    async def next_request(self) -> Coroutine:
        pass

    def _task_done_callback(self, task):
        # Remove task from the set when done
        self.background_tasks.discard(task)
        # Handle any exceptions to prevent them from being silently ignored
        if not task.cancelled() and task.exception():
            print(f"Task failed with exception: {task.exception()}")
            # Or use logging: logging.error(f"Task failed: {task.exception()}")

    async def run_until(self, time: float):
        pending_tasks = set()
        try:
            while self._active and self.collection.time < time:
                self.count += 1
                coro = await self.next_request()
                task = asyncio.create_task(coro, name=f"Request_{self.count}")
                pending_tasks.add(task)
                # Remove task from set when done
                task.add_done_callback(pending_tasks.discard)

                # Periodically clean up completed tasks
                if self.count % 100 == 0:
                    # Clean up completed tasks from memory
                    _ = [t for t in pending_tasks if t.done()]

        finally:
            # Wait for pending tasks with a timeout
            if pending_tasks:
                done, pending = await asyncio.wait(pending_tasks, timeout=1)
                for task in pending:
                    task.cancel()


class PoissonRateGenerator(WorkloadGenerator, ABC):
    def __init__(self, collection: DSECollection,
                 cpu_size: Union[float, Callable[[], float]], mem_size: Union[float, Callable[[], float]],
                 *, save_path: Optional[str] = None):
        super().__init__(collection, save_path=save_path)

        self.cpu_size = (lambda: cpu_size) if isinstance(cpu_size, float) else cpu_size
        self.mem_size = (lambda: mem_size) if isinstance(mem_size, float) else mem_size

    @abstractmethod
    def get_inter_arrival_rate(self, time: float) -> float:
        pass

    async def next_request(self):
        inter_arrival = seeded_random.expovariate(self.get_inter_arrival_rate(self.collection.time))
        await self.collection.yield_for(inter_arrival)

        p_get = 0.35
        p_put = 0.5
        p_search = 0.15

        p_tot = p_get + p_put + p_search

        rnd = random.uniform(0, p_tot)

        indices = self.collection.indices
        index = random.choice(indices + indices[:1])

        if rnd <= p_get:
            request = GetRequest(self.collection.time, index,
                                 seeded_random.randint(0, 10 ** 10),
                                 self.cpu_size(), self.mem_size())
        elif rnd <= p_put + p_get:
            request = PutRequest(self.collection.time, index,
                                 seeded_random.randint(0, 10 ** 10),
                                 self.cpu_size(), self.mem_size())
        else:
            request = SearchRequest(self.collection.time, index,
                                    self.cpu_size() / 10, self.mem_size() / 10,
                                    self.cpu_size(), self.mem_size(),
                                    self.cpu_size() * 2
                                    )

        # TODO: move this saving code somewhere sane
        if self.save:
            self.save_file.write(request.to_json() + '\n')

        return request.handle()  # Return the coroutine, not a task


class ConstantRateGenerator(PoissonRateGenerator):
    def __init__(self, collection: DSECollection, rate: float,
                 cpu_size: Union[float, Callable[[], float]],
                 mem_size: Union[float, Callable[[], float]],
                 *, save_path: Optional[str] = None):
        super().__init__(collection, cpu_size, mem_size, save_path=save_path)
        self.rate = rate

    def get_inter_arrival_rate(self, time: float) -> float:
        return self.rate


class SinusoidalRateGenerator(PoissonRateGenerator):
    def __init__(self, collection: DSECollection,
                 rate_mean: float, rate_period: float,
                 cpu_size: Union[float, Callable[[], float]],
                 mem_size: Union[float, Callable[[], float]],
                 *, save_path: Optional[str] = None):
        super().__init__(collection, cpu_size, mem_size, save_path=save_path)
        self.rate_mean = rate_mean
        self.rate_period = rate_period

    def get_inter_arrival_rate(self, time: float) -> float:
        c1 = (1 + math.sin(2 * math.pi * time / self.rate_period)) / 2
        return (.5 + (1.5 * c1)) * self.rate_mean


class LoadedWorkloadGenerator(WorkloadGenerator):
    def __init__(self, collection: DSECollection, load_path: str):
        super().__init__(collection)
        self.load_path = load_path

        with open(self.load_path, 'r') as f:
            self.lines = [json.loads(i) for i in f if i]

        self.line_idx = 0


    async def next_request(self) -> Union[Coroutine, None]:
        if self.line_idx < len(self.lines):
            line = self.lines[self.line_idx]
            self.line_idx += 1

            try:
                line_type = line['type']
                if line_type == 'PUT':
                    request = PutRequest(line['time'], self.collection.get_index(line['index']), int(line['document_id']),
                                         line['cpu_size'], line['mem_size'], given_hash=(line['given_hash'] or [None])[0])
                elif line_type == 'GET':
                    request = GetRequest(line['time'], self.collection.get_index(line['index']), int(line['document_id']),
                                         line['cpu_size'], line['mem_size'], given_hash=(line['given_hash'] or [None])[0])
                elif line_type == 'SEARCH':
                    request = SearchRequest(line['time'], self.collection.get_index(line['index']), line['cpu_size_query'],
                                            line['mem_size_query'],
                                            line['cpu_size_response'], line['mem_size_response'], line['cpu_size_collate'],
                                            given_hashes=line['given_hashes'] or None)
            except InvalidIndexError as e:
                tb = traceback.extract_tb(e.__traceback__)
                file_name, line_number, func_name, text = tb[-1]

                print(
                    f'{self.collection.time},{e.__class__.__name__}({os.path.basename(file_name)}:{line_number}),{self.__class__.__name__},"{e}"',
                    file=sys.stderr)

            except Exception as e:

                tb = traceback.extract_tb(e.__traceback__)
                file_name, line_number, func_name, text = tb[-1]

                print(
                    f'{self.collection.time},{e.__class__.__name__}(Unexpected at {os.path.basename(file_name)}:{line_number}),{self.__class__.__name__},"{e}"',
                    file=sys.stderr)

        else:
            request = ShutDownRequest(self.collection)
            await self.collection.clock.stop()
            self._active = False

        await self.collection.yield_until(request.time)
        return request.handle()

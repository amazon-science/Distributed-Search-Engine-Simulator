# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import asyncio
from asyncio import Future, Task
from heapq import heappop, heappush
from typing import Coroutine, List, Tuple, Union

from dse_sim.event.event import Event


class Clock:
    def __init__(self):
        self.time: float = 0.0
        self.counter: int = 0
        self.active_tasks = set()
        self.pq: List[Tuple[Event, Future]] = []

        self._sleep_future: Future = None
        self.running = False

    async def yield_until(self, event: Union[Event, float]) -> None:
        self.counter += 1

        time = max(self.time, event.time if isinstance(event, Event) else event)

        if time > self.time:
            future = asyncio.Future()

            heappush(self.pq, (time, self.counter, future))

            if self._sleep_future is not None and not self._sleep_future.done():
                self._sleep_future.set_result(True)

            await future

    async def yield_for(self, time: float):
        if time > 0:
            await self.yield_until(time + self.time)

    async def process(self, coroutine: Coroutine) -> Task:
        task = asyncio.create_task(coroutine)
        self.active_tasks.add(task)

        task.add_done_callback(self.active_tasks.discard)
        return task

    async def run(self) -> None:
        self.running = True

        while self.running:
            if not self.pq:
                self._sleep_future = asyncio.Future()
                await self._sleep_future
                if self._sleep_future.result() is False:
                    break
                self._sleep_future = None

            await asyncio.sleep(0)
            event, count, future = heappop(self.pq)

            self.time = event if isinstance(event, float) else event.time

            future.set_result(True)
            # await asyncio.sleep(0)

    async def stop(self) -> None:
        await asyncio.sleep(0)
        self.running = False

        if self._sleep_future:
            self._sleep_future.set_result(False)

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

from __future__ import annotations

import itertools
import typing
from asyncio import Future
from typing import Iterable, List, Optional

import pandas

from dse_sim.collection.actor import Actor
from dse_sim.config import DEBUG

if typing.TYPE_CHECKING:
    from dse_sim.agent.agent import Agent


class Profiler(Actor):
    def __init__(self,
                 collection: 'DSECollection',
                 save_path: Optional[str] = None,
                 agents: Optional[List[Agent]] = None,
                 *,
                 save_mode: str = 'wt'):
        super().__init__(collection)

        self._history = []
        self.latest = []
        self._history_df = pandas.DataFrame()

        self.agents = agents or []

        self.save_f = open(save_path, save_mode) if save_path else None
        self.next_profile_event: Optional[Future] = None
        self._active = True

    async def run_periodically(self, period: float, num_iterations: Optional[int] = None, first: float = 0.0):
        it = range(num_iterations) if num_iterations else itertools.count()
        for _ in it:
            if DEBUG:
                self.log("Scheduling next profiling event.")

            self.next_profile_event = self.collection.yield_for(first if first > 0 else period)
            first = 0

            result = await self.next_profile_event
            if result is False:
                break
            else:
                r = self.profile(self.collection.time)
                if isinstance(r, (int, float)):
                    period = r

    def profile(self, time: float, actors: Optional[Iterable['ProfilableActor']] = None) -> Optional[float]:
        from dse_sim.collection.actor import ProfilableActor
        from dse_sim.components.logical.worker import WorkerStatus

        self.latest = []

        self.print(time, self.collection, 'num_workers', self.collection.dse_config.num_workers)
        self.print(time, self.collection, 'blue_workers', sum(1 for i in self.collection.workers if i.status == WorkerStatus.BLUE))
        self.print(time, self.collection, 'green_workers', sum(1 for i in self.collection.workers if i.status == WorkerStatus.GREEN))
        self.print(time, self.collection, 'ocus_per_worker', self.collection.dse_config.half_ocus_per_worker / 2)
        self.print(time, self.collection, 'steady', int(self.collection.steady))

        if actors is None:
            actors: Iterable[ProfilableActor] = self.collection.profilable_actors

        for actor in sorted(actors, key=repr):
            for key, value in actor.profile(time).items():
                self.print(time, actor, key, value)
        self.flush()

        r = None
        for agent in self.agents:
            r = r or agent.act(self.history)
        return r

    def print(self, time: float, actor: 'ProfilableActor', key: str, value: float):
        line = (time, repr(actor), key, value)
        self._history.append(line)

        self.latest.append(line)

        self._history_df = pandas.DataFrame()

        out = f'{time:.4f},{repr(actor)},{key},{value:.4f}'
        if self.save_f is not None:
            try:
                self.save_f.write(out + '\n')
            except ValueError as e:
                if self._active:
                    raise e
        else:
            print(out)

    @property
    def history(self):
        if self._history_df.shape[0] == 0 or self._history_df.iloc[-1]['time'] != self._history[-1][0]:
            self._history_df = pandas.DataFrame.from_records(self._history, columns=['time', 'actor', 'key', 'value'])
        return self._history_df

    def flush(self):
        try:
            if self.save_f:
                self.save_f.flush()
        except ValueError:
            pass

    def tear_down(self):
        self._active = False

        if isinstance(self.next_profile_event, Future):
            self.next_profile_event.set_result(False)

        if self.save_f:
            self.save_f.close()

        self.collection.unregsiter(self)

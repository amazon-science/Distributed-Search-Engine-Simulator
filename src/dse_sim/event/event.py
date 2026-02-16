# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

from abc import ABCMeta, abstractmethod

from dse_sim.util.counted import Counted


class Event(Counted, metaclass=ABCMeta):
    repr = [('t', 'time')]

    def __init__(self, time: float):
        self.time = time

    @abstractmethod
    async def handle(self) -> None:
        pass


class NullEvent(Event):
    async def handle(self) -> None:
        pass

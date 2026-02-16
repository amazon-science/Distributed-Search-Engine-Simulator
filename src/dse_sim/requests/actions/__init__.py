# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

from __future__ import annotations

import os
import sys
import traceback
import typing
from abc import ABC, abstractmethod

from dse_sim.collection import DSEError
from dse_sim.collection.dse_collection import DSECollection
from dse_sim.event.event import Event

if typing.TYPE_CHECKING:
    from dse_sim.agent.agent import Agent


class ActionRequest(Event, ABC):
    repr = [('t', 'time')]

    handle_count = 0

    def __init__(self, time: float, collection: DSECollection, agent: Agent, count: typing.Optional[int] = 1):
        super().__init__(time)
        self.collection = collection
        self.agent = agent

        self.completion_time = None
        self.workers = []
        self.count = count

    @abstractmethod
    async def process(self):
        pass

    async def handle(self):
        try:
            result = await self.process()
        except DSEError as e:
            result = '5xx'
            print(f'{self.time},{e.__class__.__name__},{self.__class__.__name__},"{e}"', file=sys.stderr)
        except Exception as e:
            result = '5xx'

            tb = traceback.extract_tb(e.__traceback__)
            file_name, line_number, func_name, text = tb[-1]

            print(f'{self.time},{e.__class__.__name__}(Unexpected at {os.path.basename(file_name)}:{line_number}),{self.__class__.__name__},"{e}"', file=sys.stderr)
        return result

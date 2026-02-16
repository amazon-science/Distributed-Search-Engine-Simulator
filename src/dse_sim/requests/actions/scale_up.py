# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import sys
from typing import Optional

from dse_sim.agent.agent import Agent
from dse_sim.collection import DSEError
from dse_sim.collection.dse_collection import DSECollection
from dse_sim.config import DEBUG
from dse_sim.requests.actions import ActionRequest


class ScaleUpRequest(ActionRequest):
    def __init__(self, collection: DSECollection, agent: Agent, count: Optional[int] = 1):
        super().__init__(collection.time, collection, agent, count=count)

    async def process(self):
        await self.collection.yield_until(self.time)

        try:
            if DEBUG:
                self.agent.log("Scaling up all workers")
            await self.collection.scale_up_workers(count=self.count)

            response = '4xx'
        except DSEError as e:
            response = '5xx'
            print(f'{self.time},{e.__class__.__name__},{self.__class__.__name__},"{e}"', file=sys.stderr)

        return response

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import sys
from typing import Optional

from dse_sim.agent.agent import Agent
from dse_sim.collection import DSEError
from dse_sim.collection.dse_collection import DSECollection
from dse_sim.config import DEBUG
from dse_sim.requests.actions import ActionRequest


class ScaleToRequest(ActionRequest):
    def __init__(self, collection: DSECollection, agent: Agent, num_workers: int, half_ocus_per_worker: int):
        super().__init__(collection.time, collection, agent)
        self.num_workers = num_workers
        self.half_ocus_per_worker = half_ocus_per_worker

    async def process(self):
        await self.collection.yield_until(self.time)

        try:
            if DEBUG: self.agent.log(f"Scaling to {self.num_workers}x{self.half_ocus_per_worker/2.0}")
            await self.collection.scale_to(num_workers=self.num_workers,
                                           half_ocus_per_worker=self.half_ocus_per_worker
                                           )

            response = '4xx'
        except DSEError as e:
            response = '5xx'
            print(f'{self.time},{e.__class__.__name__},{self.__class__.__name__},"{e}"', file=sys.stderr)

        return response

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import os
import sys

from dse_sim.agent.agent import Agent
from dse_sim.collection import DSEError
from dse_sim.collection.dse_collection import DSECollection
from dse_sim.requests.actions import ActionRequest


class ShutDownRequest(ActionRequest):
    def __init__(self, collection: DSECollection):
        super().__init__(collection.time, collection, None)

    async def process(self):
        os._exit(0)
        await self.collection.tear_down()
        return '4xx'

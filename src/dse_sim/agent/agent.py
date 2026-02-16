# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import asyncio
from abc import abstractmethod
from collections import defaultdict
from typing import Dict, Union

import pandas

from dse_sim.collection.actor import ProfilableActor
from dse_sim.collection.dse_collection import DSECollection
from dse_sim.config import DEBUG
from dse_sim.requests.actions import ActionRequest


class Agent(ProfilableActor):
    def __init__(self, collection: DSECollection):
        super().__init__(collection)
        self.request_counts = defaultdict(int)

    def run_profiler(self) -> Union[float, Dict[str, float]]:
        out = self.request_counts
        self.request_counts = defaultdict(int)
        return out

    def send_action_request(self, action: ActionRequest):
        if DEBUG: self.log(f"Taking action {action}")
        self.request_counts[action.__class__.__name__] += 1
        loop = asyncio.get_event_loop()
        return loop.create_task(action.handle())

    @abstractmethod
    def act(self, history: pandas.DataFrame):
        pass

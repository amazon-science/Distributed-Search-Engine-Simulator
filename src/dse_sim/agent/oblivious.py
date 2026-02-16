# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

from typing import Dict, List, Type

import pandas

from dse_sim.agent.agent import Agent
from dse_sim.collection.dse_collection import DSECollection
from dse_sim.requests.actions import ActionRequest


class ObliviousAgent(Agent):
    def __init__(self, collection: DSECollection, predefined_tasks: Dict[float, List[Type[ActionRequest]]]):
        super().__init__(collection)

        self.predefined_tasks = predefined_tasks

    def act(self, history: pandas.DataFrame):
        for time, task_list in list(self.predefined_tasks.items()):
            if self.collection.time >= time:
                for request in task_list:
                    if not isinstance(request, (tuple, list)):
                        request = [request, 1]
                    request = list(request)

                    if isinstance(request[1], int):
                        request[1] = {'count': request[1]}

                    request, kwargs = request[0], dict(request[1])
                    self.send_action_request(request(self.collection, self, **kwargs))
                del self.predefined_tasks[time]

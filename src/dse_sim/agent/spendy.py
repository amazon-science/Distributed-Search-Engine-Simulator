# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import pandas

from dse_sim.agent.agent import Agent
from dse_sim.collection.dse_collection import DSECollection


class SpendyAgent(Agent):
    def __init__(self, collection: DSECollection):
        super().__init__(collection)

    def act(self, history: pandas.DataFrame):
        from dse_sim.requests.actions.scale_up import ScaleUpRequest
        last_time = history['time'].iloc[-1]

        if (last_time // 60) % 5 == 0:
            cpu_df = history[history['actor'].str.contains('CPU')]
            current_mean_utilization = cpu_df[cpu_df['time'] == last_time]['value'].mean()
            if current_mean_utilization >= .5:
                self.send_action_request(ScaleUpRequest(self.collection, self))

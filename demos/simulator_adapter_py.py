# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: CC-BY-NC-4.0

import subprocess
import sys
from dataclasses import dataclass
from enum import Enum
from io import StringIO
from typing import List, Optional
from pathlib import Path

import pandas

@dataclass(frozen=True)
class State:
    time: float
    avg_rejection_rate: float
    avg_latency: float
    avg_shard_cnt: float
    avg_cpu_usage: float
    avg_mem_usage: float
    avg_rss_usage: float
    avg_disk_usage: float
    num_workers: int
    ocu_per_worker: float



def state_from_simulator_output(s: str) -> State:
    df = pandas.read_csv(StringIO(s))

    success = df[df['actor'].str.contains('Index') & (df['key'] == '4xx')]['value'].sum()
    reject = df[df['actor'].str.contains('Index') & (df['key'] == '5xx')]['value'].sum()

    avg_rejection_rate = reject / (success + reject)

    avg_latency = df[df['actor'].str.contains('Index') & (df['key'] == 'latency')]['value'].iloc[0]

    avg_shard_cnt = df[df['actor'].str.contains('Index') & (df['key'] == 'num_shards')]['value'].sum()

    avg_cpu_usage = df[df['actor'].str.contains('CPU\\(') & (df['key'] == 'utilization_frac')]['value'].mean()
    avg_mem_usage = df[df['actor'].str.contains('RAM\\(') & (df['key'] == 'utilization_avg')]['value'].sum()
    avg_rss_usage = avg_mem_usage
    avg_disk_usage = df[df['actor'].str.contains('Disk\\(') & (df['key'].str.contains('rate_bytes'))]['value'].sum()

    num_workers = df[df['key'] == 'num_workers']['value'].iloc[0]
    ocus_per_worker = df[df['key'] == 'ocus_per_worker']['value'].iloc[0]

    return State(float(df['time'].iloc[-1]),
                 float(avg_rejection_rate), float(avg_latency), float(avg_shard_cnt),
                 float(avg_cpu_usage),  float(avg_mem_usage), float(avg_rss_usage),
                 float(avg_disk_usage), int(num_workers), float(ocus_per_worker)
                 )


class ACTION_SPACE(Enum):
    SCALE_UP: str = 'ScaleUp'
    SCALE_DOWN: str = 'ScaleDown'
    SCALE_IN: str = 'ScaleIn'
    SCALE_OUT: str = 'ScaleOut'


class SimulatedSearchEnvironment:
    def __init__(self,
                 script_path: str,
                 num_workers: int,
                 ocus_per_worker: float,
                 shards_per_index: List[int],
                 first_delay: float,
                 workload_path: str,
                 index_names: Optional[List[str]] = None):

        self.script_path = script_path
        self.num_workers = num_workers
        self.ocus_per_worker = ocus_per_worker
        self.shards_per_index = shards_per_index
        self.first_delay = first_delay
        self.workload_path = workload_path
        self.index_names = index_names

        self.reset()

    def reset(self):
        self.clean()

        if self.index_names:
            index_names = self.index_names
        else:
            index_names = [f'Index{idx}' for idx in range(len(self.shards_per_index))]

        cmd = [sys.executable, self.script_path,
            '--num_workers', str(self.num_workers),
            '--half_ocus_per_worker', str(round(2 * self.ocus_per_worker)),
            '--indices', ",".join(index_names),
            '--shards_per_index', ",".join(f'{i}' for i in self.shards_per_index),
            '--profiler_output_path', "/dev/null",
            '--profiler_first_delay', str(self.first_delay),
            '--workload_path', self.workload_path
        ]
        self.subprocess = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
                                           bufsize=1)

        self.last_output = None
        self.needs_input = False

    def clean(self):
        try:
            self.subprocess.terminate()
        except:
            pass

    def control(self, actions: List[ACTION_SPACE], next_delay: float):
        self.subprocess.stdin.write(f'{next_delay},{",".join(i.value for i in actions)}\n')
        self.subprocess.stdin.flush()
        self.needs_input = False

    def _get_obs(self):
        if self.needs_input:
            return self.last_output

        while True:
            lines = []
            while True:
                output_line = self.subprocess.stdout.readline().rstrip('\n')
                if self.subprocess.poll() is not None and not output_line:
                    raise IOError("No output")
                elif output_line.strip() == '>':
                    break
                else:
                    lines.append(output_line)
            if not lines:
                raise IOError("No output")

            self.last_output = state_from_simulator_output('\n'.join(lines))
            self.needs_input = True
            return self.last_output


if __name__ == '__main__':
    demo_root = Path(__file__).parent
    env = SimulatedSearchEnvironment(f'{demo_root}/stdio_agent.py',
                                   2, 1, [6,6], 60,
                                   '/tmp/workload.jsonl', index_names=['hello', 'world'])
    while True:
        a = env._get_obs()
        print(a)
        action = ACTION_SPACE.SCALE_OUT
        env.control([action], 60)

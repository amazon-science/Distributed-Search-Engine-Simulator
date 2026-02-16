# Distributed Search Engine Simulator (Python)

A discrete-event simulation framework for distributed search engines designed for reinforcement learning research and capacity planning optimization.

## Overview

This simulator models the behavior of search engine clusters, including workers, shards, indices, and resource management. It provides an environment for developing and testing RL agents that make scaling decisions based on workload patterns and system metrics.

## Features

- **Discrete-Event Simulation**: Async-based event-driven architecture for efficient simulation
- **Resource Modeling**: Simulates CPU, memory, disk, and network resources with configurable parameters
- **Workload Generation**: Built-in generators for synthetic workload patterns (sinusoidal, custom)
- **RL Agent Framework**: Abstract agent interface for implementing custom scaling policies
- **Profiling & Metrics**: Comprehensive metrics collection and CSV export for analysis
- **HTTP Request Simulation**: Models GET, PUT, and SEARCH operations with realistic latency
- **Scaling Actions**: Support for scale up/down, scale in/out, and scale-to operations

## Architecture

```
src/dse_sim/
├── agent/          # RL agent implementations (base, spendy, oblivious, stdio)
├── collection/     # Collection, index, and actor management
├── components/     # Physical (CPU, RAM, disk) and logical (worker, shard) components
├── event/          # Event queue and clock for discrete-event simulation
├── generator/      # Workload generators
├── profiler/       # Metrics collection and profiling
├── requests/       # HTTP requests (GET, PUT, SEARCH) and scaling actions
└── util/           # Utilities (bounded queue, async debugging, counting)
```

## Installation

Requires Python 3.8 or higher.

```bash
pip install -e .
```

Dependencies:
- `pandas` - Data analysis and metrics handling
- `termcolor` - Colored terminal output

## Usage

### Basic Simulation

```python
import asyncio
from dse_sim.collection.dse_collection import DSECollection, DSECollectionConfig
from dse_sim.agent.spendy import SpendyAgent
from dse_sim.profiler.profiler import Profiler

async def main():
    # Create collection with 2 workers, 2 half-OCUs per worker
    config = DSECollectionConfig(num_workers=2, half_ocus_per_worker=2)
    collection = DSECollection("my-collection", config)
    
    # Start the simulation
    await collection.run()
    
    # Add indices and shards
    index = await collection.add_index('my-index', instant=True)
    for worker in collection.workers:
        await index.create_shards(worker, 6, instant=True)
    
    # Attach profiler and agent
    profiler = Profiler(collection, save_path='metrics.csv', agents=[SpendyAgent(collection)])
    asyncio.create_task(profiler.run_periodically(60))
    
    # Run simulation for 15000 seconds
    await asyncio.sleep(15000)

asyncio.run(main())
```

## Demo Scripts

The `demos/` directory contains example simulations:

- `run_sim_0430.py` - Full simulation with workload generation
- `spendy_agent.py` - Example using the SpendyAgent
- `stdio_agent.py` - Interactive agent controlled via stdin
- `scale_out_and_in.py` - Demonstrates horizontal scaling
- `scale_up_and_down.py` - Demonstrates vertical scaling
- `generate_workload.py` - Workload generation utilities
- `load_workload_from_jsonl.py` - Load workloads from files
- `plot_simulator.ipynb` - Jupyter notebook for visualization

## Configuration

Simulation parameters are defined in `src/dse_sim/config.py`:

- CPU/worker spin-up and spin-down delays
- Disk read/write speeds
- Memory per half-OCU
- CPU frequency
- Index/shard creation times
- Blue-green deployment CPU utilization threshold

## Metrics

The profiler collects metrics including:
- Request rejection rate (4xx, 5xx)
- Average latency
- Shard count
- CPU, memory, RSS, disk usage
- Number of workers and OCUs per worker
- System steady state

## License

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

SPDX-License-Identifier: CC-BY-NC-4.0

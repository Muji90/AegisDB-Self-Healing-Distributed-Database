# AegisDB

AegisDB is a five-node, self-healing distributed key-value database built as a SOFE4790U Distributed Systems final project. It combines data partitioning, replication, logical clocks, quorum operations, failure detection, leader election, automated repair, and a live observability dashboard in one demonstrable system.

## Core distributed-systems features

- Five independently running FastAPI nodes
- Consistent hashing with virtual nodes for deterministic key placement
- Replication factor of 5 (every record is stored on all five nodes)
- Read quorum and write quorum of 3 (a majority of the five nodes)
- Vector clocks for causal version metadata
- Persistent per-node SQLite storage
- Heartbeat-based node health monitoring
- Raft-style randomized leader election and leader heartbeats
- Simulated node failure and recovery
- Automatic self-healing of missing/stale replicas
- Read repair when a quorum read discovers a stale replica
- Replica rebalancing after failed nodes recover
- Tombstone-based distributed deletes
- Live cluster/event observability
- REST API plus interactive FastAPI API documentation

## Project structure

```text
AegisDB/
├── aegisdb/
│   ├── __init__.py
│   ├── cluster.py
│   ├── config.py
│   ├── hash_ring.py
│   ├── main.py
│   ├── models.py
│   ├── raft.py
│   ├── storage.py
│   └── vector_clock.py
├── static/
│   ├── index.html
│   ├── style.css
│   └── app.js
├── data/
├── tests/
├── requirements.txt
├── start_cluster.ps1
├── stop_cluster.ps1
└── README.md
```

## Quick start (Windows)

Double-click **`Start-AegisDB.bat`** in the AegisDB folder.

That one step:

1. Creates the `.venv` virtual environment and installs `requirements.txt` (first run only, needs Python 3.10+)
2. Stops any AegisDB nodes left over from a previous run
3. Starts the five nodes, each in its own window:
   - node1: http://127.0.0.1:8001
   - node2: http://127.0.0.1:8002
   - node3: http://127.0.0.1:8003
   - node4: http://127.0.0.1:8004
   - node5: http://127.0.0.1:8005
4. Waits until all five nodes pass their health check
5. Opens the dashboard at `http://127.0.0.1:8001` in your default browser

FastAPI documentation is available at `http://127.0.0.1:8001/docs`.

To stop the cluster, double-click **`Stop-AegisDB.bat`**.

Equivalent PowerShell commands:

```powershell
powershell -ExecutionPolicy Bypass -File .\start_cluster.ps1
powershell -ExecutionPolicy Bypass -File .\stop_cluster.ps1
```

## Run tests

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

## Recommended demonstration

1. Start all five nodes and show 100% cluster health.
2. Create a record in Data Explorer and show its five replica locations and vector clock.
3. Retrieve the same key from another node to demonstrate distributed access and read quorum.
4. Fail the current leader from Cluster Control and show automatic failure detection and a new leader election.
5. Keep reading the existing record while a node is unavailable to demonstrate fault tolerance.
6. Recover the failed node and watch self-healing restore the required replica set.
7. Open Live Events to show the failure, election, repair, and recovery sequence.
8. Use the Architecture page to connect the demonstration to the course concepts.

## Default distributed configuration

- Nodes: 5
- Replication factor: 5
- Read quorum: 3
- Write quorum: 3
- Virtual nodes per physical node: 64
- Heartbeat interval: 1 second
- Heartbeat timeout: 1.2 seconds
- Automatic recovery after a simulated failure: 25-35 seconds

These values can be overridden with environment variables defined in `aegisdb/config.py`.


## Performance Metrics

The Metrics page samples the running cluster every two seconds and visualizes request throughput, node availability, Raft leader/term progression, self-healing activity, read repair, rebalancing, average read/write latency, and quorum success rate. The charts are rendered directly in the AegisDB frontend and do not require Grafana or an external monitoring service.

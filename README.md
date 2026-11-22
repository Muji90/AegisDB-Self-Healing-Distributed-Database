# AegisDB

AegisDB is a five-node, self-healing distributed key-value database built as a SOFE4790U Distributed Systems final project. It combines replication, consistent hashing, logical clocks, quorum operations, failure detection, Bully leader election, automated repair, client-side failover, and a live observability dashboard in one demonstrable system.

## Core distributed-systems features

- Five independently running FastAPI nodes
- Consistent hashing with virtual nodes for deterministic replica ordering
- Replication factor of 5 (every record is stored on all five nodes)
- Read quorum and write quorum of 3 (a majority of the five nodes), so the cluster tolerates any two node failures
- Vector clocks for causal version metadata and conflict detection
- Persistent per-node SQLite storage
- Heartbeat-based node health monitoring
- Bully-algorithm leader election (Lecture 8): the highest-numbered live node is always leader, so succession is node5, node4, node3, node2, node1
- Leader heartbeats used as the followers' failure detector for the leader
- Client-side failover (failure transparency): if the connected node is down, the dashboard automatically sends record requests to the next live node
- Simulated node failure and automatic recovery
- Automatic self-healing of missing/stale replicas (run by the leader)
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
│   ├── bully.py          Bully leader election
│   ├── cluster.py        Node-to-node HTTP and health checks
│   ├── config.py         Settings and environment variables
│   ├── hash_ring.py      Consistent hashing
│   ├── main.py           API, quorum reads/writes, self-healing
│   ├── models.py         Request and message models
│   ├── storage.py        Per-node SQLite storage
│   └── vector_clock.py   Vector clocks
├── static/
│   ├── index.html
│   ├── style.css
│   └── app.js            Dashboard, including client-side failover
├── data/                 Created on first run (one database per node)
├── tests/
│   ├── test_bully.py
│   ├── test_hash_ring.py
│   ├── test_storage.py
│   └── test_vector_clock.py
├── Start-AegisDB.bat
├── Stop-AegisDB.bat
├── start_cluster.ps1
├── stop_cluster.ps1
├── requirements.txt
└── README.md
```

## Quick start (Windows)

**Before you start:**

- Windows 10 or 11
- Python 3.10 or newer from python.org (tick "Add python.exe to PATH" during install). Miniconda and Anaconda installs are also detected.
- Internet access the first time (to install the packages)
- If you received AegisDB as a .zip, right-click it, choose **Extract All**, and run it from the extracted folder. It will not start from inside the zip.

Double-click **`Start-AegisDB.bat`** in the AegisDB folder.

That one step:

1. Creates the `.venv` virtual environment and installs `requirements.txt` (first run, or when the folder was copied from another computer)
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

1. Start all five nodes and show 100% cluster health, with node5 as leader.
2. Create a record in Data Explorer and show its five replica locations and vector clock.
3. Retrieve the record and show the read quorum (5 responses, 3 required).
4. Fail node1 (the node the dashboard is connected to) and retrieve the record again. The dashboard automatically fails over and reports that node2 handled the request.
5. Fail the leader (node5) and show that node4, the next highest live node, wins the Bully election.
6. Fail a third node and show that reads and writes are refused because quorum (3 of 5) cannot be reached. No data is lost.
7. Recover the nodes. Show service resuming, node5 taking leadership back, and self-healing restoring any missing replicas.
8. Open Live Events to show the failure, election, repair and recovery sequence.
9. Use the Architecture page to connect the demonstration to the course concepts.

## Default distributed configuration

- Nodes: 5
- Replication factor: 5
- Read quorum: 3
- Write quorum: 3 (at least 3 nodes must be running to read or write)
- Virtual nodes per physical node: 64
- Heartbeat interval: 1 second
- Heartbeat timeout: 1.2 seconds
- Leader heartbeat interval: 0.6 seconds; leader failure timeout: 2 seconds
- Automatic recovery after a simulated failure: 25-35 seconds

These values can be overridden with environment variables defined in `aegisdb/config.py`.

## Performance Metrics

The Metrics page samples the running cluster every two seconds and visualizes request throughput, node availability, leader and election-term history, self-healing activity, read repair, rebalancing, average read/write latency, and quorum success rate. The charts are rendered directly in the AegisDB frontend and do not require Grafana or an external monitoring service.
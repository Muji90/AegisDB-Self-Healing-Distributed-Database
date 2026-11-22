from __future__ import annotations

import asyncio
import json
import random
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import quote

from fastapi import (
    FastAPI,
    HTTPException,
    Request,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import (
    FileResponse,
    JSONResponse,
)
from fastapi.staticfiles import StaticFiles

from aegisdb.cluster import ClusterManager
from aegisdb.config import load_settings
from aegisdb.hash_ring import ConsistentHashRing
from aegisdb.bully import BullyCoordinator
from aegisdb.models import (
    ElectionMessage,
    InternalRecord,
    LeaderMessage,
    PutRequest,
)
from aegisdb.storage import StorageEngine
from aegisdb.vector_clock import (
    compare,
    increment,
    merge,
)


settings = load_settings()

storage = StorageEngine(
    settings.database_path
)

cluster = ClusterManager(
    settings
)

hash_ring = ConsistentHashRing(
    list(settings.cluster_nodes.keys()),
    settings.virtual_nodes,
)

# Leader election uses the Bully algorithm (Lecture 8):
# the highest-numbered live node is always the leader.
election = BullyCoordinator(
    settings,
    cluster,
)


class RuntimeState:
    failure_mode = False

    total_requests = 0

    successful_writes = 0
    failed_writes = 0

    successful_reads = 0
    failed_reads = 0

    # Recovery metrics are intentionally split into low-level repair
    # counters and cluster-level recovery-cycle counters.  The Metrics
    # page reports the combined values so a node auto-recovery is visible
    # even when no record bytes need to be copied (for example RF=5 on a
    # five-node cluster, where the recovered node still has its local data).
    self_heal_repairs = 0
    node_recoveries = 0
    read_repairs = 0
    rebalance_removals = 0
    rebalance_cycles = 0

    read_latency_total_ms = 0.0
    write_latency_total_ms = 0.0
    read_latency_count = 0
    write_latency_count = 0


runtime = RuntimeState()

event_log = deque(
    maxlen=500
)

metrics_history = deque(
    maxlen=180
)

healing_task: asyncio.Task | None = None
observability_task: asyncio.Task | None = None
metrics_task: asyncio.Task | None = None
auto_recovery_tasks: dict[str, asyncio.Task] = {}


def record_event(
    event_type: str,
    category: str,
    message: str,
    severity: str = "info",
    details: dict | None = None,
) -> None:
    event_log.appendleft(
        {
            "id": (
                f"{settings.node_id}-"
                f"{uuid.uuid4().hex}"
            ),
            "timestamp": time.time(),
            "node_id": settings.node_id,
            "type": event_type,
            "category": category,
            "severity": severity,
            "message": message,
            "details": details or {},
        }
    )


def record_recovery_cycle(
    node_id: str,
    automatic: bool,
) -> None:
    """Record one completed node recovery and topology rebalance cycle.

    A simulated failure does not delete the node's local files. With RF=5
    on exactly five nodes, recovery can therefore require no physical
    replica copy/removal.  It still changes the healthy topology and causes
    AegisDB to re-evaluate replica placement.  Tracking that recovery cycle
    separately keeps the recovery chart meaningful without fabricating
    record-level repairs.
    """
    runtime.node_recoveries += 1
    runtime.rebalance_cycles += 1

    record_event(
        "cluster_rebalanced",
        "recovery",
        (
            f"Replica placement re-evaluated after {node_id} "
            f"rejoined the cluster."
        ),
        "success",
        {
            "node_id": node_id,
            "automatic": automatic,
            "replication_factor": settings.replication_factor,
        },
    )


@asynccontextmanager
async def lifespan(
    app: FastAPI,
):
    global healing_task
    global observability_task
    global metrics_task

    await cluster.start()
    await election.start()

    record_event(
        "node_started",
        "system",
        f"{settings.node_id} joined the AegisDB cluster.",
        "success",
        {
            "port": settings.node_port,
        },
    )

    healing_task = asyncio.create_task(
        self_healing_loop()
    )

    observability_task = asyncio.create_task(
        observability_loop()
    )

    metrics_task = asyncio.create_task(
        metrics_loop()
    )

    yield

    if healing_task is not None:
        healing_task.cancel()

        try:
            await healing_task
        except asyncio.CancelledError:
            pass

    if observability_task is not None:
        observability_task.cancel()

        try:
            await observability_task
        except asyncio.CancelledError:
            pass

    if metrics_task is not None:
        metrics_task.cancel()

        try:
            await metrics_task
        except asyncio.CancelledError:
            pass

    await election.stop()
    await cluster.stop()


app = FastAPI(
    title="AegisDB",
    description=(
        "Self-Healing Fault-Tolerant "
        "Distributed Database"
    ),
    version="1.3.0",
    lifespan=lifespan,
)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


BASE_DIR = (
    Path(__file__)
    .resolve()
    .parent
    .parent
)

STATIC_DIR = (
    BASE_DIR
    / "static"
)


app.mount(
    "/static",
    StaticFiles(
        directory=str(
            STATIC_DIR
        )
    ),
    name="static",
)


@app.middleware("http")
async def simulated_failure(
    request: Request,
    call_next,
):
    path = request.url.path

    blocked_prefixes = (
        "/internal/health",
        "/internal/store",
        "/internal/records",
        "/internal/events",
        "/internal/election",
        "/api/kv",
    )

    if (
        runtime.failure_mode
        and path.startswith(
            blocked_prefixes
        )
    ):
        return JSONResponse(
            status_code=503,
            content={
                "detail":
                    "Node is in simulated "
                    "failure mode."
            },
        )

    return await call_next(
        request
    )


@app.get("/")
async def dashboard():
    return FileResponse(
        STATIC_DIR
        / "index.html"
    )


@app.get("/internal/health")
async def internal_health():
    return {
        "node_id": settings.node_id,
        "status": "healthy",
        "time": time.time(),
    }


@app.get("/api/status")
async def status():
    return {
        "name": "AegisDB",
        "node_id": settings.node_id,
        "node_url": settings.node_url,
        "failure_mode": runtime.failure_mode,
        "replication_factor":
            settings.replication_factor,
        "read_quorum":
            settings.read_quorum,
        "write_quorum":
            settings.write_quorum,
        "records":
            storage.count(),
        "election":
            election.status(),
        "metrics": {
            "total_requests":
                runtime.total_requests,
            "successful_reads":
                runtime.successful_reads,
            "failed_reads":
                runtime.failed_reads,
            "successful_writes":
                runtime.successful_writes,
            "failed_writes":
                runtime.failed_writes,
            "self_heal_repairs":
                runtime.self_heal_repairs,
            "node_recoveries":
                runtime.node_recoveries,
            "read_repairs":
                runtime.read_repairs,
            "rebalance_removals":
                runtime.rebalance_removals,
            "rebalance_cycles":
                runtime.rebalance_cycles,
            "average_read_latency_ms":
                round(
                    runtime.read_latency_total_ms
                    / runtime.read_latency_count,
                    2,
                )
                if runtime.read_latency_count
                else 0.0,
            "average_write_latency_ms":
                round(
                    runtime.write_latency_total_ms
                    / runtime.write_latency_count,
                    2,
                )
                if runtime.write_latency_count
                else 0.0,
        },
    }


@app.get("/api/cluster")
async def cluster_status():
    result = []

    for node_id, url in (
        settings.cluster_nodes.items()
    ):
        health = cluster.health.get(
            node_id,
            {},
        )

        node_data = {
            "node_id": node_id,
            "url": url,
            "healthy":
                node_available(
                    node_id
                ),
            "last_seen":
                health.get(
                    "last_seen"
                ),
            "latency_ms":
                health.get(
                    "latency_ms"
                ),
        }

        try:
            if (
                node_id
                == settings.node_id
            ):
                node_status = {
                    "records":
                        storage.count(),
                    "election":
                        election.status(),
                    "failure_mode":
                        runtime.failure_mode,
                }

            else:
                response = (
                    await cluster.get(
                        node_id,
                        "/api/status",
                    )
                )

                if (
                    response.status_code
                    == 200
                ):
                    node_status = (
                        response.json()
                    )
                else:
                    node_status = {}

            node_data[
                "records"
            ] = node_status.get(
                "records",
                0,
            )

            node_data[
                "election"
            ] = node_status.get(
                "election",
                {},
            )

            node_data[
                "failure_mode"
            ] = node_status.get(
                "failure_mode",
                False,
            )

        except Exception:
            node_data[
                "records"
            ] = 0

            node_data[
                "election"
            ] = {}

            node_data[
                "failure_mode"
            ] = True

        result.append(
            node_data
        )

    return {
        "current_node":
            settings.node_id,
        "nodes":
            result,
    }


def ordered_nodes_for_key(
    key: str,
) -> list[str]:
    return hash_ring.get_nodes(
        key,
        len(
            settings.cluster_nodes
        ),
    )


def node_available(
    node_id: str,
) -> bool:
    if (
        node_id
        == settings.node_id
    ):
        return not (
            runtime.failure_mode
        )

    return cluster.is_healthy(
        node_id
    )


def record_signature(
    record: dict,
) -> tuple[str, str, bool]:
    clock = json.dumps(
        record[
            "vector_clock"
        ],
        sort_keys=True,
        separators=(",", ":"),
    )

    value = json.dumps(
        record.get(
            "value"
        ),
        sort_keys=True,
        separators=(",", ":"),
    )

    return (
        clock,
        value,
        bool(
            record.get(
                "tombstone",
                False,
            )
        ),
    )


def same_version(
    first: dict | None,
    second: dict | None,
) -> bool:
    if (
        first is None
        or second is None
    ):
        return False

    return (
        record_signature(first)
        == record_signature(second)
    )


async def fetch_record(
    node_id: str,
    key: str,
) -> dict | None:
    if not node_available(
        node_id
    ):
        return None

    if (
        node_id
        == settings.node_id
    ):
        return storage.get(
            key
        )

    try:
        response = await cluster.get(
            node_id,
            "/internal/store/"
            + quote(
                key,
                safe="",
            ),
        )

        if (
            response.status_code
            == 404
        ):
            return None

        if (
            response.status_code
            != 200
        ):
            return None

        return response.json()

    except Exception:
        return None


async def collect_records(
    key: str,
) -> list[dict]:
    node_order = (
        ordered_nodes_for_key(
            key
        )
    )

    tasks = [
        fetch_record(
            node_id,
            key,
        )
        for node_id
        in node_order
    ]

    results = await asyncio.gather(
        *tasks,
        return_exceptions=True,
    )

    records: list[dict] = []

    for result in results:
        if isinstance(
            result,
            dict,
        ):
            records.append(
                result
            )

    return records


def choose_latest(
    records: list[dict],
) -> tuple[
    dict | None,
    int,
]:
    if not records:
        return None, 0

    unique_records = []
    signatures = set()

    for record in records:
        signature = (
            record_signature(
                record
            )
        )

        if (
            signature
            in signatures
        ):
            continue

        signatures.add(
            signature
        )

        unique_records.append(
            record
        )

    maximal = []

    for candidate in unique_records:
        dominated = False

        for other in unique_records:
            if (
                other
                is candidate
            ):
                continue

            relationship = compare(
                candidate[
                    "vector_clock"
                ],
                other[
                    "vector_clock"
                ],
            )

            if (
                relationship
                == "before"
            ):
                dominated = True
                break

        if not dominated:
            maximal.append(
                candidate
            )

    maximal.sort(
        key=lambda item:
            item[
                "updated_at"
            ],
        reverse=True,
    )

    conflicts = max(
        0,
        len(maximal) - 1,
    )

    return (
        maximal[0],
        conflicts,
    )


async def store_on_node(
    node_id: str,
    record: dict,
) -> bool:
    if not node_available(
        node_id
    ):
        return False

    try:
        if (
            node_id
            == settings.node_id
        ):
            await apply_internal_record(
                InternalRecord(
                    **record
                )
            )

            return True

        response = await cluster.post(
            node_id,
            "/internal/store",
            record,
        )

        return (
            response.status_code
            == 200
        )

    except Exception:
        return False


async def physically_remove_record(
    node_id: str,
    key: str,
) -> bool:
    if not node_available(
        node_id
    ):
        return False

    try:
        if (
            node_id
            == settings.node_id
        ):
            return (
                storage.delete_physical(
                    key
                )
            )

        response = await cluster.delete(
            node_id,
            (
                "/internal/store/"
                + quote(
                    key,
                    safe="",
                )
                + "/physical"
            ),
        )

        return (
            response.status_code
            == 200
        )

    except Exception:
        return False


async def replicate_record(
    record: dict,
) -> tuple[
    int,
    list[str],
]:
    node_order = (
        ordered_nodes_for_key(
            record["key"]
        )
    )

    successes = 0
    stored_nodes = []

    for node_id in node_order:
        if (
            successes
            >= settings.replication_factor
        ):
            break

        if not node_available(
            node_id
        ):
            continue

        success = await store_on_node(
            node_id,
            record,
        )

        if success:
            successes += 1

            stored_nodes.append(
                node_id
            )

    return (
        successes,
        stored_nodes,
    )


async def desired_replica_nodes(
    key: str,
) -> list[str]:
    ordered = (
        ordered_nodes_for_key(
            key
        )
    )

    healthy = [
        node_id
        for node_id
        in ordered
        if node_available(
            node_id
        )
    ]

    return healthy[
        :settings.replication_factor
    ]


async def read_repair(
    key: str,
    latest: dict,
) -> None:
    targets = (
        await desired_replica_nodes(
            key
        )
    )

    for node_id in targets:
        existing = await fetch_record(
            node_id,
            key,
        )

        if same_version(
            existing,
            latest,
        ):
            continue

        should_repair = False

        if existing is None:
            should_repair = True

        else:
            relationship = compare(
                latest[
                    "vector_clock"
                ],
                existing[
                    "vector_clock"
                ],
            )

            if (
                relationship
                == "after"
            ):
                should_repair = True

            elif (
                relationship
                == "equal"
                and not same_version(
                    existing,
                    latest,
                )
            ):
                should_repair = True

            elif (
                relationship
                == "concurrent"
                and latest[
                    "updated_at"
                ]
                >= existing[
                    "updated_at"
                ]
            ):
                should_repair = True

        if should_repair:
            success = (
                await store_on_node(
                    node_id,
                    latest,
                )
            )

            if success:
                runtime.read_repairs += 1

                record_event(
                    "read_repair",
                    "recovery",
                    (
                        f"Read repair updated "
                        f"{key} on {node_id}."
                    ),
                    "success",
                    {
                        "key": key,
                        "target_node":
                            node_id,
                    },
                )


async def get_records_by_node(
) -> dict[
    str,
    dict[str, dict],
]:
    result = {}

    for node_id in (
        settings.cluster_nodes
    ):
        if not node_available(
            node_id
        ):
            continue

        try:
            if (
                node_id
                == settings.node_id
            ):
                records = (
                    storage.list_records(
                        limit=10000
                    )
                )

            else:
                response = (
                    await cluster.get(
                        node_id,
                        "/internal/records",
                    )
                )

                if (
                    response.status_code
                    != 200
                ):
                    continue

                records = (
                    response.json()
                    .get(
                        "records",
                        [],
                    )
                )

            result[
                node_id
            ] = {
                record["key"]:
                    record
                for record
                in records
            }

        except Exception:
            continue

    return result


async def heal_key(
    key: str,
    records_by_node: dict,
) -> None:
    versions = []

    for node_records in (
        records_by_node.values()
    ):
        record = (
            node_records.get(
                key
            )
        )

        if record is not None:
            versions.append(
                record
            )

    latest, _ = (
        choose_latest(
            versions
        )
    )

    if latest is None:
        return

    desired = (
        await desired_replica_nodes(
            key
        )
    )

    if not desired:
        return

    correct_desired = []

    for node_id in desired:
        existing = (
            records_by_node
            .get(
                node_id,
                {},
            )
            .get(
                key
            )
        )

        if same_version(
            existing,
            latest,
        ):
            correct_desired.append(
                node_id
            )

            continue

        success = await store_on_node(
            node_id,
            latest,
        )

        if success:
            runtime.self_heal_repairs += 1

            correct_desired.append(
                node_id
            )

            record_event(
                "replica_repaired",
                "recovery",
                (
                    f"Replica for {key} "
                    f"restored on {node_id}."
                ),
                "success",
                {
                    "key": key,
                    "target_node":
                        node_id,
                    "replication_factor":
                        settings.replication_factor,
                },
            )

    if (
        len(desired)
        < settings.replication_factor
    ):
        return

    if (
        len(correct_desired)
        < settings.replication_factor
    ):
        return

    for (
        node_id,
        node_records
    ) in records_by_node.items():

        if node_id in desired:
            continue

        if key not in node_records:
            continue

        removed = (
            await physically_remove_record(
                node_id,
                key,
            )
        )

        if removed:
            runtime.rebalance_removals += 1

            record_event(
                "replica_rebalanced",
                "recovery",
                (
                    f"Temporary replica "
                    f"for {key} removed "
                    f"from {node_id}."
                ),
                "info",
                {
                    "key": key,
                    "node_id":
                        node_id,
                },
            )


async def run_self_healing(
) -> None:
    if (
        runtime.failure_mode
        or election.state
        != election.LEADER
    ):
        return

    records_by_node = (
        await get_records_by_node()
    )

    all_keys = set()

    for node_records in (
        records_by_node.values()
    ):
        all_keys.update(
            node_records.keys()
        )

    for key in all_keys:
        await heal_key(
            key,
            records_by_node,
        )


async def self_healing_loop(
) -> None:
    while True:
        await asyncio.sleep(
            2.0
        )

        try:
            await run_self_healing()

        except asyncio.CancelledError:
            raise

        except Exception:
            pass


async def observability_loop(
) -> None:
    previous_health = {
        node_id:
            node_available(
                node_id
            )
        for node_id
        in settings.cluster_nodes
    }

    was_leader = False
    previous_term = -1

    while True:
        await asyncio.sleep(
            1.0
        )

        try:
            is_leader = (
                election.state
                == election.LEADER
                and not runtime.failure_mode
            )

            if (
                is_leader
                and (
                    not was_leader
                    or previous_term
                    != election.current_term
                )
            ):
                record_event(
                    "leader_elected",
                    "consensus",
                    (
                        f"{settings.node_id} "
                        f"elected leader "
                        f"(Bully algorithm) "
                        f"for term "
                        f"{election.current_term}."
                    ),
                    "success",
                    {
                        "leader":
                            settings.node_id,
                        "term":
                            election.current_term,
                    },
                )

            was_leader = is_leader
            previous_term = (
                election.current_term
            )

            current_health = {
                node_id:
                    node_available(
                        node_id
                    )
                for node_id
                in settings.cluster_nodes
            }

            if is_leader:
                for (
                    node_id,
                    healthy
                ) in current_health.items():

                    previous = (
                        previous_health.get(
                            node_id,
                            healthy,
                        )
                    )

                    if (
                        previous
                        and not healthy
                    ):
                        record_event(
                            "node_failed",
                            "system",
                            (
                                f"{node_id} "
                                f"failure detected."
                            ),
                            "danger",
                            {
                                "node_id":
                                    node_id,
                            },
                        )

                    elif (
                        not previous
                        and healthy
                    ):
                        record_event(
                            "node_recovered",
                            "system",
                            (
                                f"{node_id} "
                                f"recovered and "
                                f"rejoined the cluster."
                            ),
                            "success",
                            {
                                "node_id":
                                    node_id,
                            },
                        )

            previous_health = (
                current_health
            )

        except asyncio.CancelledError:
            raise

        except Exception:
            pass


async def metrics_loop(
) -> None:
    while True:
        try:
            healthy_nodes = {
                node_id: node_available(node_id)
                for node_id in settings.cluster_nodes
            }

            metrics_history.append(
                {
                    "timestamp": time.time(),
                    "total_requests": runtime.total_requests,
                    "successful_reads": runtime.successful_reads,
                    "failed_reads": runtime.failed_reads,
                    "successful_writes": runtime.successful_writes,
                    "failed_writes": runtime.failed_writes,
                    "healthy_nodes": healthy_nodes,
                    "healthy_count": sum(
                        1 for healthy in healthy_nodes.values() if healthy
                    ),
                    "leader": election.leader_id,
                    "election_state": election.state,
                    "term": election.current_term,
                    "self_heals": (
                        runtime.self_heal_repairs
                        + runtime.node_recoveries
                    ),
                    "read_repairs": runtime.read_repairs,
                    "rebalances": (
                        runtime.rebalance_removals
                        + runtime.rebalance_cycles
                    ),
                    "average_read_latency_ms": round(
                        runtime.read_latency_total_ms
                        / runtime.read_latency_count,
                        2,
                    ) if runtime.read_latency_count else 0.0,
                    "average_write_latency_ms": round(
                        runtime.write_latency_total_ms
                        / runtime.write_latency_count,
                        2,
                    ) if runtime.write_latency_count else 0.0,
                }
            )

            await asyncio.sleep(2.0)

        except asyncio.CancelledError:
            raise

        except Exception:
            await asyncio.sleep(2.0)


@app.get("/api/metrics")
async def api_metrics():
    samples = list(metrics_history)

    if samples:
        latest = samples[-1]
    else:
        latest = {
            "healthy_count": 0,
            "leader": election.leader_id,
            "term": election.current_term,
            "average_read_latency_ms": 0.0,
            "average_write_latency_ms": 0.0,
        }

    operations = (
        runtime.successful_reads
        + runtime.failed_reads
        + runtime.successful_writes
        + runtime.failed_writes
    )

    successful = (
        runtime.successful_reads
        + runtime.successful_writes
    )

    quorum_success_rate = (
        round((successful / operations) * 100, 1)
        if operations
        else 100.0
    )

    return {
        "samples": samples,
        "summary": {
            "average_read_latency_ms": latest.get(
                "average_read_latency_ms", 0.0
            ),
            "average_write_latency_ms": latest.get(
                "average_write_latency_ms", 0.0
            ),
            "quorum_success_rate": quorum_success_rate,
            "healthy_nodes": latest.get("healthy_count", 0),
            "total_nodes": len(settings.cluster_nodes),
            "leader": latest.get("leader") or "Electing",
            "term": latest.get("term", 0),
            "self_heals": (
                runtime.self_heal_repairs
                + runtime.node_recoveries
            ),
            "read_repairs": runtime.read_repairs,
            "rebalances": (
                runtime.rebalance_removals
                + runtime.rebalance_cycles
            ),
        },
    }


@app.post("/api/kv/{key}")
async def put_key(
    key: str,
    request: PutRequest,
):
    started_at = time.perf_counter()
    runtime.total_requests += 1

    existing_records = (
        await collect_records(
            key
        )
    )

    clocks = [
        record[
            "vector_clock"
        ]
        for record
        in existing_records
    ]

    if clocks:
        base_clock = merge(
            *clocks
        )
    else:
        base_clock = {}

    new_clock = increment(
        base_clock,
        settings.node_id,
    )

    record = {
        "key": key,
        "value": request.value,
        "vector_clock":
            new_clock,
        "updated_at":
            time.time(),
        "tombstone":
            False,
    }

    successes, nodes = (
        await replicate_record(
            record
        )
    )

    if (
        successes
        < settings.write_quorum
    ):
        runtime.failed_writes += 1

        record_event(
            "write_quorum_failed",
            "storage",
            (
                f"Write quorum failed "
                f"for {key}."
            ),
            "danger",
            {
                "key": key,
                "acks": successes,
                "required":
                    settings.write_quorum,
            },
        )

        raise HTTPException(
            status_code=503,
            detail={
                "message":
                    "Write quorum "
                    "was not reached.",
                "acks":
                    successes,
                "required":
                    settings.write_quorum,
            },
        )

    runtime.successful_writes += 1
    runtime.write_latency_total_ms += (
        time.perf_counter() - started_at
    ) * 1000
    runtime.write_latency_count += 1

    record_event(
        "record_written",
        "storage",
        (
            f"{key} replicated "
            f"to {successes} nodes."
        ),
        "success",
        {
            "key": key,
            "nodes": nodes,
            "vector_clock":
                new_clock,
        },
    )

    return {
        "success": True,
        "key": key,
        "value":
            request.value,
        "vector_clock":
            new_clock,
        "replicated_to":
            nodes,
        "acknowledgements":
            successes,
        "write_quorum":
            settings.write_quorum,
    }


@app.get("/api/kv/{key}")
async def get_key(
    key: str,
):
    started_at = time.perf_counter()
    runtime.total_requests += 1

    records = (
        await collect_records(
            key
        )
    )

    if (
        len(records)
        < settings.read_quorum
    ):
        runtime.failed_reads += 1

        record_event(
            "read_quorum_failed",
            "storage",
            (
                f"Read quorum failed "
                f"for {key}."
            ),
            "danger",
            {
                "key": key,
                "responses":
                    len(records),
                "required":
                    settings.read_quorum,
            },
        )

        raise HTTPException(
            status_code=503,
            detail={
                "message":
                    "Read quorum "
                    "was not reached.",
                "responses":
                    len(records),
                "required":
                    settings.read_quorum,
            },
        )

    latest, conflicts = (
        choose_latest(
            records
        )
    )

    if (
        latest is None
        or latest[
            "tombstone"
        ]
    ):
        runtime.failed_reads += 1

        raise HTTPException(
            status_code=404,
            detail="Key not found.",
        )

    runtime.successful_reads += 1
    runtime.read_latency_total_ms += (
        time.perf_counter() - started_at
    ) * 1000
    runtime.read_latency_count += 1

    asyncio.create_task(
        read_repair(
            key,
            latest,
        )
    )

    return {
        "success": True,
        "key": key,
        "value":
            latest[
                "value"
            ],
        "vector_clock":
            latest[
                "vector_clock"
            ],
        "updated_at":
            latest[
                "updated_at"
            ],
        "replica_responses":
            len(records),
        "read_quorum":
            settings.read_quorum,
        "conflict_versions":
            conflicts,
    }


@app.delete("/api/kv/{key}")
async def delete_key(
    key: str,
):
    runtime.total_requests += 1

    existing_records = (
        await collect_records(
            key
        )
    )

    clocks = [
        record[
            "vector_clock"
        ]
        for record
        in existing_records
    ]

    if clocks:
        base_clock = merge(
            *clocks
        )
    else:
        base_clock = {}

    new_clock = increment(
        base_clock,
        settings.node_id,
    )

    record = {
        "key": key,
        "value": None,
        "vector_clock":
            new_clock,
        "updated_at":
            time.time(),
        "tombstone":
            True,
    }

    successes, nodes = (
        await replicate_record(
            record
        )
    )

    if (
        successes
        < settings.write_quorum
    ):
        runtime.failed_writes += 1

        raise HTTPException(
            status_code=503,
            detail=(
                "Delete quorum "
                "was not reached."
            ),
        )

    runtime.successful_writes += 1

    record_event(
        "record_deleted",
        "storage",
        (
            f"Distributed delete "
            f"committed for {key}."
        ),
        "warning",
        {
            "key": key,
            "nodes": nodes,
        },
    )

    return {
        "success": True,
        "key": key,
        "deleted": True,
        "replicated_to":
            nodes,
        "vector_clock":
            new_clock,
    }


@app.get("/api/keys")
async def list_local_keys():
    return {
        "node_id":
            settings.node_id,
        "records":
            storage.list_records(
                limit=100
            ),
    }


@app.get("/internal/records")
async def internal_records():
    return {
        "node_id":
            settings.node_id,
        "records":
            storage.list_records(
                limit=10000
            ),
    }


@app.get("/internal/events")
async def internal_events():
    return {
        "node_id":
            settings.node_id,
        "events":
            list(event_log),
    }


@app.get("/api/events")
async def api_events():
    events = []

    for node_id in (
        settings.cluster_nodes
    ):
        try:
            if (
                node_id
                == settings.node_id
            ):
                node_events = (
                    list(event_log)
                )

            else:
                if not node_available(
                    node_id
                ):
                    continue

                response = (
                    await cluster.get(
                        node_id,
                        "/internal/events",
                    )
                )

                if (
                    response.status_code
                    != 200
                ):
                    continue

                node_events = (
                    response.json()
                    .get(
                        "events",
                        [],
                    )
                )

            events.extend(
                node_events
            )

        except Exception:
            continue

    unique = {}

    for event in events:
        event_id = (
            event.get("id")
        )

        if event_id:
            unique[
                event_id
            ] = event

    merged = list(
        unique.values()
    )

    merged.sort(
        key=lambda item:
            item.get(
                "timestamp",
                0,
            ),
        reverse=True,
    )

    counts = {
        "failures": 0,
        "recoveries": 0,
        "leader_elections": 0,
        "self_heals": 0,
        "read_repairs": 0,
        "rebalances": 0,
    }

    for event in merged:
        event_type = (
            event.get(
                "type"
            )
        )

        if (
            event_type
            == "node_failed"
        ):
            counts[
                "failures"
            ] += 1

        elif (
            event_type
            == "node_recovered"
        ):
            counts[
                "recoveries"
            ] += 1

        elif (
            event_type
            == "leader_elected"
        ):
            counts[
                "leader_elections"
            ] += 1

        elif event_type in (
            "replica_repaired",
            "node_auto_recovered",
        ):
            counts[
                "self_heals"
            ] += 1

        elif (
            event_type
            == "read_repair"
        ):
            counts[
                "read_repairs"
            ] += 1

        elif event_type in (
            "replica_rebalanced",
            "cluster_rebalanced",
        ):
            counts[
                "rebalances"
            ] += 1

    return {
        "events":
            merged[:200],
        "counts":
            counts,
    }


@app.get("/internal/store/{key}")
async def internal_get(
    key: str,
):
    record = storage.get(
        key
    )

    if record is None:
        raise HTTPException(
            status_code=404,
            detail="Not found.",
        )

    return record


async def apply_internal_record(
    record: InternalRecord,
) -> dict:
    existing = storage.get(
        record.key
    )

    if existing is not None:
        relationship = compare(
            record.vector_clock,
            existing[
                "vector_clock"
            ],
        )

        if (
            relationship
            == "before"
        ):
            return {
                "success":
                    True,
                "stored":
                    False,
                "reason":
                    "Older version ignored.",
            }

        if (
            relationship
            == "concurrent"
            and record.updated_at
            < existing[
                "updated_at"
            ]
        ):
            return {
                "success":
                    True,
                "stored":
                    False,
                "reason":
                    "Concurrent older "
                    "version ignored.",
            }

    storage.put(
        key=
            record.key,
        value=
            record.value,
        vector_clock=
            record.vector_clock,
        updated_at=
            record.updated_at,
        tombstone=
            record.tombstone,
    )

    return {
        "success": True,
        "stored": True,
        "node_id":
            settings.node_id,
    }


@app.post("/internal/store")
async def internal_store(
    record: InternalRecord,
):
    return await apply_internal_record(
        record
    )


@app.delete(
    "/internal/store/{key}/physical"
)
async def internal_physical_delete(
    key: str,
):
    removed = (
        storage.delete_physical(
            key
        )
    )

    return {
        "success": True,
        "removed": removed,
        "node_id":
            settings.node_id,
        "key": key,
    }


@app.post(
    "/internal/election/election"
)
async def bully_election(
    request: ElectionMessage,
):
    # ELECTION from a lower-priority node -> reply with ANSWER.
    return await election.handle_election(
        request.term,
        request.candidate_id,
    )


@app.post(
    "/internal/election/coordinator"
)
async def bully_coordinator(
    request: LeaderMessage,
):
    # COORDINATOR: a node announces it has won the election.
    return await election.handle_coordinator(
        request.term,
        request.leader_id,
    )


@app.post(
    "/internal/election/heartbeat"
)
async def bully_heartbeat(
    request: LeaderMessage,
):
    # HEARTBEAT: the leader's periodic "I am alive" message.
    return await election.handle_heartbeat(
        request.term,
        request.leader_id,
    )


async def schedule_automatic_recovery(
    node_id: str,
) -> float:
    if not settings.auto_recovery_enabled:
        return 0.0

    existing = auto_recovery_tasks.get(
        node_id
    )

    if (
        existing is not None
        and not existing.done()
    ):
        return float(
            getattr(
                existing,
                "recovery_delay",
                settings.auto_recovery_min_seconds,
            )
        )

    delay = random.uniform(
        settings.auto_recovery_min_seconds,
        settings.auto_recovery_max_seconds,
    )

    async def recover_later() -> None:
        try:
            await asyncio.sleep(delay)

            if node_id == settings.node_id:
                if runtime.failure_mode:
                    runtime.failure_mode = False
                    await election.resume()

                    record_event(
                        "node_auto_recovered",
                        "recovery",
                        (
                            f"{node_id} automatically "
                            f"recovered after "
                            f"{delay:.1f} seconds."
                        ),
                        "success",
                        {
                            "node_id": node_id,
                            "downtime_seconds": round(delay, 1),
                        },
                    )

                    record_recovery_cycle(
                        node_id,
                        automatic=True,
                    )

                return

            response = await cluster.post(
                node_id,
                "/internal/admin/recover",
            )

            if response.status_code == 200:
                record_event(
                    "node_auto_recovered",
                    "recovery",
                    (
                        f"{node_id} automatically "
                        f"recovered after "
                        f"{delay:.1f} seconds."
                    ),
                    "success",
                    {
                        "node_id": node_id,
                        "downtime_seconds": round(delay, 1),
                    },
                )

                record_recovery_cycle(
                    node_id,
                    automatic=True,
                )

        except asyncio.CancelledError:
            raise

        except Exception as exc:
            record_event(
                "automatic_recovery_failed",
                "recovery",
                (
                    f"Automatic recovery for "
                    f"{node_id} failed: {exc}"
                ),
                "danger",
                {
                    "node_id": node_id,
                },
            )

        finally:
            auto_recovery_tasks.pop(
                node_id,
                None,
            )

    task = asyncio.create_task(
        recover_later()
    )
    task.recovery_delay = delay
    auto_recovery_tasks[node_id] = task

    return delay


def cancel_automatic_recovery(
    node_id: str,
) -> None:
    task = auto_recovery_tasks.pop(
        node_id,
        None,
    )

    if (
        task is not None
        and not task.done()
    ):
        task.cancel()


@app.post(
    "/internal/admin/fail"
)
async def internal_fail():
    runtime.failure_mode = True

    await election.pause()

    return {
        "success": True,
        "node_id":
            settings.node_id,
        "failure_mode": True,
    }


@app.post(
    "/internal/admin/recover"
)
async def internal_recover():
    runtime.failure_mode = False

    await election.resume()

    return {
        "success": True,
        "node_id":
            settings.node_id,
        "failure_mode": False,
    }


@app.post(
    "/api/admin/fail/{node_id}"
)
async def fail_node(
    node_id: str,
):
    if (
        node_id
        not in settings.cluster_nodes
    ):
        raise HTTPException(
            status_code=404,
            detail="Unknown node.",
        )

    if (
        node_id
        == settings.node_id
    ):
        runtime.failure_mode = True
        await election.pause()

        delay = await schedule_automatic_recovery(
            node_id
        )

        return {
            "success": True,
            "node_id": node_id,
            "failure_mode": True,
            "automatic_recovery": True,
            "auto_recovery_in_seconds": round(delay, 1),
        }

    try:
        response = await cluster.post(
            node_id,
            "/internal/admin/fail",
        )

        if response.status_code != 200:
            raise HTTPException(
                status_code=response.status_code,
                detail=(
                    "Remote node could "
                    "not enter failure mode."
                ),
            )

        delay = await schedule_automatic_recovery(
            node_id
        )

        result = response.json()
        result["automatic_recovery"] = True
        result["auto_recovery_in_seconds"] = round(
            delay,
            1,
        )

        return result

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=str(exc),
        )


@app.post(
    "/api/admin/recover/{node_id}"
)
async def recover_node(
    node_id: str,
):
    if (
        node_id
        not in settings.cluster_nodes
    ):
        raise HTTPException(
            status_code=404,
            detail="Unknown node.",
        )

    if (
        node_id
        == settings.node_id
    ):
        cancel_automatic_recovery(
            node_id
        )

        runtime.failure_mode = False

        await election.resume()

        record_recovery_cycle(
            node_id,
            automatic=False,
        )

        return {
            "success": True,
            "node_id":
                node_id,
            "failure_mode":
                False,
        }

    cancel_automatic_recovery(
        node_id
    )

    try:
        response = await cluster.post(
            node_id,
            "/internal/admin/recover",
        )

        if (
            response.status_code
            != 200
        ):
            raise HTTPException(
                status_code=
                    response.status_code,
                detail=(
                    "Remote node could "
                    "not recover."
                ),
            )

        record_recovery_cycle(
            node_id,
            automatic=False,
        )

        return response.json()

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=str(exc),
        )


@app.get(
    "/api/placement/{key}"
)
async def placement(
    key: str,
):
    ordered = (
        ordered_nodes_for_key(
            key
        )
    )

    healthy = [
        node_id
        for node_id
        in ordered
        if node_available(
            node_id
        )
    ]

    return {
        "key": key,
        "preferred_replicas":
            ordered[
                :settings.replication_factor
            ],
        "healthy_replicas":
            healthy[
                :settings.replication_factor
            ],
        "ring_order":
            ordered,
    }


@app.post("/api/admin/heal")
async def manual_heal():
    if (
        election.state
        != election.LEADER
    ):
        raise HTTPException(
            status_code=409,
            detail=(
                "Self-healing must "
                "run on the current leader."
            ),
        )

    before = (
        runtime.self_heal_repairs
    )

    await run_self_healing()

    after = (
        runtime.self_heal_repairs
    )

    return {
        "success": True,
        "leader":
            settings.node_id,
        "new_repairs":
            after - before,
        "total_repairs":
            after,
    }
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_CLUSTER = (
    "node1=http://127.0.0.1:8001,"
    "node2=http://127.0.0.1:8002,"
    "node3=http://127.0.0.1:8003,"
    "node4=http://127.0.0.1:8004,"
    "node5=http://127.0.0.1:8005"
)


def parse_cluster(value: str) -> dict[str, str]:
    result: dict[str, str] = {}

    for item in value.split(","):
        item = item.strip()

        if not item:
            continue

        node_id, url = item.split("=", 1)

        result[node_id.strip()] = (
            url.strip().rstrip("/")
        )

    return result


@dataclass(frozen=True)
class Settings:
    node_id: str
    node_port: int
    cluster_nodes: dict[str, str]
    replication_factor: int
    read_quorum: int
    write_quorum: int
    heartbeat_interval: float
    heartbeat_timeout: float
    virtual_nodes: int
    data_dir: Path
    auto_recovery_enabled: bool
    auto_recovery_min_seconds: float
    auto_recovery_max_seconds: float

    @property
    def node_url(self) -> str:
        return self.cluster_nodes[
            self.node_id
        ]

    @property
    def database_path(self) -> Path:
        return (
            self.data_dir
            / f"{self.node_id}.db"
        )


def load_settings() -> Settings:
    node_id = os.getenv(
        "NODE_ID",
        "node1",
    )

    node_port = int(
        os.getenv(
            "NODE_PORT",
            "8001",
        )
    )

    cluster_nodes = parse_cluster(
        os.getenv(
            "CLUSTER_NODES",
            DEFAULT_CLUSTER,
        )
    )

    if node_id not in cluster_nodes:
        cluster_nodes[node_id] = (
            f"http://127.0.0.1:{node_port}"
        )

    replication_factor = min(
        int(
            os.getenv(
                "REPLICATION_FACTOR",
                "5",
            )
        ),
        len(cluster_nodes),
    )

    read_quorum = int(
        os.getenv(
            "READ_QUORUM",
            "3",
        )
    )

    write_quorum = int(
        os.getenv(
            "WRITE_QUORUM",
            "3",
        )
    )

    if replication_factor < 1:
        raise ValueError(
            "Replication factor must be at least 1."
        )

    if (
        read_quorum < 1
        or read_quorum > replication_factor
    ):
        raise ValueError(
            "READ_QUORUM must be between 1 "
            "and the replication factor."
        )

    if (
        write_quorum < 1
        or write_quorum > replication_factor
    ):
        raise ValueError(
            "WRITE_QUORUM must be between 1 "
            "and the replication factor."
        )

    data_dir = Path(
        os.getenv(
            "DATA_DIR",
            "data",
        )
    ).resolve()

    data_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    return Settings(
        node_id=node_id,
        node_port=node_port,
        cluster_nodes=cluster_nodes,
        replication_factor=(
            replication_factor
        ),
        read_quorum=read_quorum,
        write_quorum=write_quorum,
        heartbeat_interval=float(
            os.getenv(
                "HEARTBEAT_INTERVAL",
                "1.0",
            )
        ),
        heartbeat_timeout=float(
            os.getenv(
                "HEARTBEAT_TIMEOUT",
                "1.2",
            )
        ),
        virtual_nodes=int(
            os.getenv(
                "VIRTUAL_NODES",
                "64",
            )
        ),
        data_dir=data_dir,
        auto_recovery_enabled=(
            os.getenv(
                "AUTO_RECOVERY_ENABLED",
                "true",
            )
            .strip()
            .lower()
            in {
                "1",
                "true",
                "yes",
                "on",
            }
        ),
        auto_recovery_min_seconds=float(
            os.getenv(
                "AUTO_RECOVERY_MIN_SECONDS",
                "25.0",
            )
        ),
        auto_recovery_max_seconds=float(
            os.getenv(
                "AUTO_RECOVERY_MAX_SECONDS",
                "35.0",
            )
        ),
    )
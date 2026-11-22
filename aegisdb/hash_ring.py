from __future__ import annotations

import hashlib
import bisect


class ConsistentHashRing:
    def __init__(
        self,
        nodes: list[str],
        virtual_nodes: int = 64,
    ):
        self.virtual_nodes = virtual_nodes
        self._ring: list[int] = []
        self._owners: dict[int, str] = {}

        for node in nodes:
            self.add_node(node)

    @staticmethod
    def _hash(value: str) -> int:
        digest = hashlib.sha256(
            value.encode("utf-8")
        ).hexdigest()

        return int(digest, 16)

    def add_node(self, node_id: str) -> None:
        for replica in range(self.virtual_nodes):
            position = self._hash(
                f"{node_id}#{replica}"
            )

            if position in self._owners:
                continue

            bisect.insort(self._ring, position)
            self._owners[position] = node_id

    def remove_node(self, node_id: str) -> None:
        positions = [
            position
            for position, owner in self._owners.items()
            if owner == node_id
        ]

        for position in positions:
            self._ring.remove(position)
            del self._owners[position]

    def get_nodes(
        self,
        key: str,
        count: int,
    ) -> list[str]:
        if not self._ring:
            return []

        count = max(1, count)

        position = self._hash(key)

        index = bisect.bisect_left(
            self._ring,
            position,
        )

        result: list[str] = []
        seen: set[str] = set()

        total_positions = len(self._ring)

        for offset in range(total_positions):
            ring_index = (
                index + offset
            ) % total_positions

            node_id = self._owners[
                self._ring[ring_index]
            ]

            if node_id in seen:
                continue

            seen.add(node_id)
            result.append(node_id)

            if len(result) >= count:
                break

        return result
from aegisdb.hash_ring import ConsistentHashRing


def test_hash_ring_returns_unique_nodes():
    ring = ConsistentHashRing(["node1", "node2", "node3"], virtual_nodes=16)
    nodes = ring.get_nodes("user:101", 3)

    assert len(nodes) == 3
    assert len(set(nodes)) == 3


def test_hash_is_deterministic():
    ring = ConsistentHashRing(["node1", "node2", "node3"])

    assert ring.get_nodes("important-key", 2) == ring.get_nodes("important-key", 2)


def test_requested_count_is_capped_by_available_nodes():
    ring = ConsistentHashRing(["node1", "node2", "node3"], virtual_nodes=8)

    nodes = ring.get_nodes("key", 10)

    assert len(nodes) == 3
    assert set(nodes) == {"node1", "node2", "node3"}


def test_remove_node_changes_ownership_set():
    ring = ConsistentHashRing(["node1", "node2", "node3"], virtual_nodes=16)
    ring.remove_node("node2")

    nodes = ring.get_nodes("key", 3)

    assert "node2" not in nodes
    assert set(nodes) == {"node1", "node3"}

from types import SimpleNamespace

from aegisdb.bully import BullyCoordinator


NODES = {f"node{i}": "" for i in range(1, 6)}


def make(node_id: str) -> BullyCoordinator:
    settings = SimpleNamespace(node_id=node_id, cluster_nodes=NODES)
    return BullyCoordinator(settings, cluster=None)


def test_priority_follows_node_number():
    order = sorted(NODES, key=BullyCoordinator.priority, reverse=True)
    assert order == ["node5", "node4", "node3", "node2", "node1"]


def test_election_is_sent_only_to_higher_nodes():
    assert make("node2")._higher_nodes() == ["node3", "node4", "node5"]
    assert make("node5")._higher_nodes() == []


def test_double_digit_ids_compare_numerically():
    assert BullyCoordinator.priority("node10") > BullyCoordinator.priority("node9")
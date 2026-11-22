from pathlib import Path

from aegisdb.storage import StorageEngine


def test_storage_round_trip(tmp_path: Path):
    engine = StorageEngine(tmp_path / "node.db")
    engine.put("student:101", {"name": "Test"}, {"node1": 1}, 1.0)

    record = engine.get("student:101")

    assert record is not None
    assert record["value"] == {"name": "Test"}
    assert record["vector_clock"] == {"node1": 1}
    assert record["tombstone"] is False


def test_storage_tombstone(tmp_path: Path):
    engine = StorageEngine(tmp_path / "node.db")
    engine.put("key", None, {"node1": 2}, 2.0, tombstone=True)

    record = engine.get("key")

    assert record is not None
    assert record["tombstone"] is True
    assert engine.count() == 0

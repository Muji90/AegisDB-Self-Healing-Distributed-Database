from aegisdb.vector_clock import compare, increment, merge


def test_increment_does_not_mutate_original():
    clock = {"node1": 1}
    result = increment(clock, "node1")
    
    assert result == {"node1": 2}
    assert clock == {"node1": 1}


def test_merge_uses_component_wise_maximum():
    result = merge({"node1": 2, "node2": 1}, {"node1": 1, "node2": 3})

    assert result == {"node1": 2, "node2": 3}


def test_equal():
    assert compare({"node1": 1}, {"node1": 1}) == "equal"


def test_before():
    assert compare({"node1": 1, "node2": 1}, {"node1": 2, "node2": 1}) == "before"


def test_after():
    assert compare({"node1": 3, "node2": 1}, {"node1": 2, "node2": 1}) == "after"


def test_concurrent():
    assert compare({"node1": 2, "node2": 1}, {"node1": 1, "node2": 2}) == "concurrent"

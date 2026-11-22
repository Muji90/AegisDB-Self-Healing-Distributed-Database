from __future__ import annotations


Clock = dict[str, int]


def increment(clock: Clock, node_id: str) -> Clock:
    result = dict(clock)
    result[node_id] = result.get(node_id, 0) + 1
    return result


def merge(*clocks: Clock) -> Clock:
    result: Clock = {}

    for clock in clocks:
        for node_id, value in clock.items():
            result[node_id] = max(
                result.get(node_id, 0),
                value,
            )

    return result


def compare(a: Clock, b: Clock) -> str:
    """
    Returns:
        equal
        before
        after
        concurrent
    """

    nodes = set(a) | set(b)

    a_less = False
    a_greater = False

    for node in nodes:
        av = a.get(node, 0)
        bv = b.get(node, 0)

        if av < bv:
            a_less = True

        if av > bv:
            a_greater = True

    if not a_less and not a_greater:
        return "equal"

    if a_less and not a_greater:
        return "before"

    if a_greater and not a_less:
        return "after"

    return "concurrent"
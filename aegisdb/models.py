from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class PutRequest(BaseModel):
    value: Any


class InternalRecord(BaseModel):
    key: str
    value: Any = None
    vector_clock: dict[str, int] = Field(default_factory=dict)
    updated_at: float
    tombstone: bool = False


class ElectionMessage(BaseModel):
    """Bully ELECTION message, sent to higher-priority nodes."""

    term: int
    candidate_id: str


class LeaderMessage(BaseModel):
    """Bully COORDINATOR or HEARTBEAT message, sent by the leader."""

    term: int
    leader_id: str
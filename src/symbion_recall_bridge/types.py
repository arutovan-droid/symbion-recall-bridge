from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List


@dataclass
class RecallSnapshot:
    operator_id: str
    session_id: str
    timestamp_utc: str
    operator_essence_delta: Dict[str, Any] = field(default_factory=dict)
    open_threads: List[Dict[str, Any]] = field(default_factory=list)
    state_vector_shifts: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_snapshot_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_snapshot_dict(cls, data: Dict[str, Any]) -> "RecallSnapshot":
        return cls(
            operator_id=str(data.get("operator_id", "unknown")),
            session_id=str(data.get("session_id", "unknown")),
            timestamp_utc=str(data.get("timestamp_utc", "")),
            operator_essence_delta=dict(data.get("operator_essence_delta", {})),
            open_threads=list(data.get("open_threads", [])),
            state_vector_shifts=list(data.get("state_vector_shifts", [])),
            metadata=dict(data.get("metadata", {})),
        )

from __future__ import annotations

from typing import Any, Dict, List

from .types import RecallSnapshot


def snapshot_from_distillation(
    *,
    operator_id: str,
    session_id: str,
    timestamp_utc: str,
    distilled: Dict[str, Any],
) -> RecallSnapshot:
    operator_essence_delta = dict(distilled.get("operator_essence_delta", {}))

    open_threads = [
        {
            "packet_id": item.get("packet_id"),
            "thread_type": item.get("thread_type"),
            "payload": item.get("payload", {}),
        }
        for item in distilled.get("open_threads", [])
    ]

    state_vector_shifts = [
        {
            "packet_id": item.get("packet_id"),
            "shift_type": item.get("shift_type"),
            "payload": item.get("payload", {}),
        }
        for item in distilled.get("state_vector_shifts", [])
    ]

    metadata = {
        "distilled_packets_total": distilled.get("metadata", {}).get("packets_total", 0),
        "distilled_crystal_count": len(distilled.get("crystal_candidates", [])),
        "distilled_open_threads_count": len(open_threads),
        "distilled_state_vector_shifts_count": len(state_vector_shifts),
    }

    return RecallSnapshot(
        operator_id=operator_id,
        session_id=session_id,
        timestamp_utc=timestamp_utc,
        operator_essence_delta=operator_essence_delta,
        open_threads=open_threads,
        state_vector_shifts=state_vector_shifts,
        metadata=metadata,
    )

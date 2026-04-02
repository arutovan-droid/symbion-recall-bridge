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

    distilled_metadata = dict(distilled.get("metadata", {}) or {})

    metadata = {
        "distilled_packets_total": distilled_metadata.get("packets_total", 0),
        "distilled_crystal_count": len(distilled.get("crystal_candidates", [])),
        "distilled_open_threads_count": len(open_threads),
        "distilled_state_vector_shifts_count": len(state_vector_shifts),
    }

    # preserve selected metadata from upstream distillation/session close
    if "density_profile" in distilled_metadata:
        metadata["density_profile"] = distilled_metadata["density_profile"]
    if "session_envelope" in distilled_metadata:
        metadata["session_envelope"] = distilled_metadata["session_envelope"]

    return RecallSnapshot(
        operator_id=operator_id,
        session_id=session_id,
        timestamp_utc=timestamp_utc,
        operator_essence_delta=operator_essence_delta,
        open_threads=open_threads,
        state_vector_shifts=state_vector_shifts,
        metadata=metadata,
    )

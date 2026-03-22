from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException

from .adapters import snapshot_from_distillation
from .store import RecallHotStore
from .types import RecallSnapshot


def create_app(store: Optional[RecallHotStore] = None) -> FastAPI:
    app = FastAPI(title="symbion-recall-bridge", version="0.1.0")
    recall_store = store or RecallHotStore()

    @app.get("/api/recall/{operator_id}/status")
    def get_status(operator_id: str) -> Dict[str, Any]:
        return recall_store.get_status(operator_id)

    @app.get("/api/recall/{operator_id}/context")
    def get_context(
        operator_id: str,
        max_open_threads: int = 5,
        max_state_vector_shifts: int = 5,
    ) -> Dict[str, Any]:
        return recall_store.get_context(
            operator_id=operator_id,
            max_open_threads=max_open_threads,
            max_state_vector_shifts=max_state_vector_shifts,
        )

    @app.post("/api/recall/{operator_id}/save")
    def save_snapshot(operator_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        mode = str(payload.get("mode", "snapshot"))

        if mode == "distilled":
            distilled = payload.get("distilled")
            session_id = payload.get("session_id")
            timestamp_utc = payload.get("timestamp_utc")

            if not isinstance(distilled, dict):
                raise HTTPException(status_code=400, detail="distilled payload must be a dict")
            if not session_id or not timestamp_utc:
                raise HTTPException(status_code=400, detail="session_id and timestamp_utc are required")

            snapshot = snapshot_from_distillation(
                operator_id=operator_id,
                session_id=str(session_id),
                timestamp_utc=str(timestamp_utc),
                distilled=distilled,
            )
        else:
            snapshot = RecallSnapshot.from_snapshot_dict(payload)
            if snapshot.operator_id != operator_id:
                raise HTTPException(status_code=400, detail="operator_id mismatch")

        path = recall_store.save_hot_snapshot(snapshot)
        return {
            "ok": True,
            "operator_id": operator_id,
            "session_id": snapshot.session_id,
            "saved_path": str(path),
        }

    @app.delete("/api/recall/{operator_id}/purge")
    def purge_operator(operator_id: str) -> Dict[str, Any]:
        recall_store.purge_operator(operator_id)
        return {
            "ok": True,
            "operator_id": operator_id,
            "purged": True,
        }

    return app


app = create_app()

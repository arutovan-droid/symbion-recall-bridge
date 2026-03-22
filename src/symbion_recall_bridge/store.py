from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from .types import RecallSnapshot


class RecallHotStore:
    def __init__(self, base_dir: str | Path = "integration/recall/hot", max_snapshots: int = 3) -> None:
        self.base_dir = Path(base_dir)
        self.max_snapshots = max_snapshots

    def _operator_dir(self, operator_id: str) -> Path:
        return self.base_dir / operator_id

    def save_hot_snapshot(self, snapshot: RecallSnapshot) -> Path:
        operator_dir = self._operator_dir(snapshot.operator_id)
        operator_dir.mkdir(parents=True, exist_ok=True)

        file_path = operator_dir / f"{snapshot.timestamp_utc}_{snapshot.session_id}.json"
        file_path.write_text(
            json.dumps(snapshot.to_snapshot_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        snapshots = sorted(operator_dir.glob("*.json"))
        while len(snapshots) > self.max_snapshots:
            oldest = snapshots.pop(0)
            oldest.unlink(missing_ok=True)

        return file_path

    def load_hot_context(self, operator_id: str) -> Dict[str, Any]:
        operator_dir = self._operator_dir(operator_id)
        if not operator_dir.exists():
            return {
                "operator_id": operator_id,
                "snapshots": [],
                "recall_context": {
                    "operator_essence_delta": {},
                    "open_threads": [],
                    "state_vector_shifts": [],
                },
            }

        snapshots = sorted(operator_dir.glob("*.json"))
        loaded: List[RecallSnapshot] = [
            RecallSnapshot.from_snapshot_dict(json.loads(path.read_text(encoding="utf-8")))
            for path in snapshots
        ]

        latest = loaded[-1] if loaded else None
        recent_threads: List[Dict[str, Any]] = []
        recent_shifts: List[Dict[str, Any]] = []

        for snap in loaded[-3:]:
            recent_threads.extend(snap.open_threads)
            recent_shifts.extend(snap.state_vector_shifts)

        return {
            "operator_id": operator_id,
            "snapshots": [snap.to_snapshot_dict() for snap in loaded],
            "recall_context": {
                "operator_essence_delta": latest.operator_essence_delta if latest else {},
                "open_threads": recent_threads[:10],
                "state_vector_shifts": recent_shifts[:10],
            },
        }

    def purge_operator(self, operator_id: str) -> None:
        operator_dir = self._operator_dir(operator_id)
        if not operator_dir.exists():
            return
        for path in operator_dir.glob("*.json"):
            path.unlink(missing_ok=True)
        try:
            operator_dir.rmdir()
        except OSError:
            pass

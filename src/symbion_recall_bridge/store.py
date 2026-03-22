from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

from .types import RecallSnapshot, WarmEssence


def _dedupe_dict_list(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    result: List[Dict[str, Any]] = []
    for item in items:
        key = json.dumps(item, sort_keys=True, ensure_ascii=False)
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


class RecallHotStore:
    def __init__(
        self,
        base_dir: str | Path = "integration/recall/hot",
        warm_dir: str | Path = "integration/recall/warm",
        max_snapshots: int = 3,
    ) -> None:
        self.base_dir = Path(base_dir)
        self.warm_dir = Path(warm_dir)
        self.max_snapshots = max_snapshots

    def _operator_dir(self, operator_id: str) -> Path:
        return self.base_dir / operator_id

    def _warm_path(self, operator_id: str) -> Path:
        return self.warm_dir / operator_id / "operator_essence.json"

    def load_warm_essence(self, operator_id: str) -> WarmEssence:
        path = self._warm_path(operator_id)
        if not path.exists():
            return WarmEssence(operator_id=operator_id)
        return WarmEssence.from_snapshot_dict(json.loads(path.read_text(encoding="utf-8")))

    def save_warm_essence(self, essence: WarmEssence) -> Path:
        path = self._warm_path(essence.operator_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(essence.to_snapshot_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return path

    def _merge_snapshot_into_warm(self, snapshot: RecallSnapshot) -> None:
        warm = self.load_warm_essence(snapshot.operator_id)

        if snapshot.operator_essence_delta:
            warm.operator_essence_delta = snapshot.operator_essence_delta

        warm.open_threads = _dedupe_dict_list(warm.open_threads + snapshot.open_threads)
        warm.state_vector_shifts = _dedupe_dict_list(warm.state_vector_shifts + snapshot.state_vector_shifts)
        session_envelope = dict(snapshot.metadata.get("session_envelope", {})) if snapshot.metadata else {}

        warm.history.append(
            {
                "session_id": snapshot.session_id,
                "timestamp_utc": snapshot.timestamp_utc,
                "operator_id": snapshot.operator_id,
                "recall_loaded": session_envelope.get("recall_loaded"),
                "publish_count": session_envelope.get("publish_count"),
                "last_event_type": session_envelope.get("last_event_type"),
                "last_payload_keys": session_envelope.get("last_payload_keys", []),
            }
        )

        warm.metadata = {
            "history_count": len(warm.history),
            "open_threads_count": len(warm.open_threads),
            "state_vector_shifts_count": len(warm.state_vector_shifts),
        }

        self.save_warm_essence(warm)

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
            oldest_snapshot = RecallSnapshot.from_snapshot_dict(
                json.loads(oldest.read_text(encoding="utf-8"))
            )
            self._merge_snapshot_into_warm(oldest_snapshot)
            oldest.unlink(missing_ok=True)

        return file_path

    def get_status(self, operator_id: str) -> Dict[str, Any]:
        operator_dir = self._operator_dir(operator_id)
        warm = self.load_warm_essence(operator_id)

        if not operator_dir.exists():
            return {
                "operator_id": operator_id,
                "hot_exists": False,
                "snapshots_count": 0,
                "latest_session_id": None,
                "latest_timestamp_utc": None,
                "warm_exists": bool(warm.history),
                "warm_history_count": len(warm.history),
            }

        snapshots = sorted(operator_dir.glob("*.json"))
        latest_data: Dict[str, Any] | None = None
        if snapshots:
            latest_data = json.loads(snapshots[-1].read_text(encoding="utf-8"))

        return {
            "operator_id": operator_id,
            "hot_exists": True,
            "snapshots_count": len(snapshots),
            "latest_session_id": latest_data.get("session_id") if latest_data else None,
            "latest_timestamp_utc": latest_data.get("timestamp_utc") if latest_data else None,
            "warm_exists": bool(warm.history),
            "warm_history_count": len(warm.history),
        }

    def get_context(
        self,
        operator_id: str,
        max_open_threads: int = 5,
        max_state_vector_shifts: int = 5,
    ) -> Dict[str, Any]:
        operator_dir = self._operator_dir(operator_id)
        warm = self.load_warm_essence(operator_id)

        if not operator_dir.exists():
            return {
                "operator_id": operator_id,
                "recall_context": {
                    "operator_essence_delta": warm.operator_essence_delta,
                    "open_threads": warm.open_threads[:max_open_threads],
                    "state_vector_shifts": warm.state_vector_shifts[:max_state_vector_shifts],
                    "continuity_trend": warm.history[-3:],
                },
                "caps": {
                    "max_open_threads": max_open_threads,
                    "max_state_vector_shifts": max_state_vector_shifts,
                },
            }

        snapshots = sorted(operator_dir.glob("*.json"))
        loaded: List[RecallSnapshot] = [
            RecallSnapshot.from_snapshot_dict(json.loads(path.read_text(encoding="utf-8")))
            for path in snapshots
        ]

        latest = loaded[-1] if loaded else None
        recent_threads: List[Dict[str, Any]] = list(warm.open_threads)
        recent_shifts: List[Dict[str, Any]] = list(warm.state_vector_shifts)

        for snap in loaded[-3:]:
            recent_threads.extend(snap.open_threads)
            recent_shifts.extend(snap.state_vector_shifts)

        recent_threads = _dedupe_dict_list(recent_threads)
        recent_shifts = _dedupe_dict_list(recent_shifts)

        return {
            "operator_id": operator_id,
            "recall_context": {
                "operator_essence_delta": latest.operator_essence_delta if latest and latest.operator_essence_delta else warm.operator_essence_delta,
                "open_threads": recent_threads[:max_open_threads],
                "state_vector_shifts": recent_shifts[:max_state_vector_shifts],
                "continuity_trend": warm.history[-3:],
            },
            "caps": {
                "max_open_threads": max_open_threads,
                "max_state_vector_shifts": max_state_vector_shifts,
            },
        }

    def load_hot_context(self, operator_id: str) -> Dict[str, Any]:
        operator_dir = self._operator_dir(operator_id)
        if not operator_dir.exists():
            return {
                "operator_id": operator_id,
                "snapshots": [],
                "recall_context": self.get_context(operator_id)["recall_context"],
            }

        snapshots = sorted(operator_dir.glob("*.json"))
        loaded: List[RecallSnapshot] = [
            RecallSnapshot.from_snapshot_dict(json.loads(path.read_text(encoding="utf-8")))
            for path in snapshots
        ]

        return {
            "operator_id": operator_id,
            "snapshots": [snap.to_snapshot_dict() for snap in loaded],
            "recall_context": self.get_context(operator_id)["recall_context"],
        }

    def purge_operator(self, operator_id: str) -> None:
        operator_dir = self._operator_dir(operator_id)
        if operator_dir.exists():
            for path in operator_dir.glob("*.json"):
                path.unlink(missing_ok=True)
            try:
                operator_dir.rmdir()
            except OSError:
                pass

        warm_path = self._warm_path(operator_id)
        if warm_path.exists():
            warm_path.unlink(missing_ok=True)
            try:
                warm_path.parent.rmdir()
            except OSError:
                pass

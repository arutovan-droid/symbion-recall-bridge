from fastapi.testclient import TestClient

from symbion_recall_bridge.api import create_app
from symbion_recall_bridge.store import RecallHotStore


def test_recall_api_smoke(tmp_path):
    store = RecallHotStore(
        base_dir=tmp_path / "hot",
        warm_dir=tmp_path / "warm",
        max_snapshots=3,
    )
    client = TestClient(create_app(store))

    distilled = {
        "operator_essence_delta": {
            "dominant_crystal_principle": {"packet_id": "p1", "principle": "restore power"},
            "dominant_state_shift": {"packet_id": "p1", "shift_type": "continuity_shift"},
            "dominant_open_thread": {"packet_id": "p1", "thread_type": "unresolved_thread"},
        },
        "open_threads": [
            {"packet_id": "p1", "thread_type": "unresolved_thread", "payload": {"continuity_event": {"open_thread": True}}}
        ],
        "state_vector_shifts": [
            {"packet_id": "p1", "shift_type": "continuity_shift", "payload": {"state_vector": {"mode": "recovery"}}}
        ],
        "crystal_candidates": [
            {"packet_id": "p1", "principle": "restore power", "support": {}}
        ],
        "metadata": {
            "packets_total": 1
        },
    }

    save_resp = client.post(
        "/api/recall/op1/save",
        json={
            "mode": "distilled",
            "session_id": "sess-001",
            "timestamp_utc": "2026-03-22T16-00-00Z",
            "distilled": distilled,
        },
    )
    assert save_resp.status_code == 200
    assert save_resp.json()["ok"] is True

    status_resp = client.get("/api/recall/op1/status")
    assert status_resp.status_code == 200
    assert status_resp.json()["hot_exists"] is True
    assert status_resp.json()["snapshots_count"] == 1

    context_resp = client.get("/api/recall/op1/context")
    assert context_resp.status_code == 200
    assert (
        context_resp.json()["recall_context"]["operator_essence_delta"]["dominant_crystal_principle"]["packet_id"]
        == "p1"
    )

    purge_resp = client.delete("/api/recall/op1/purge")
    assert purge_resp.status_code == 200
    assert purge_resp.json()["purged"] is True

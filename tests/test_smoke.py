from symbion_recall_bridge import RecallHotStore, RecallSnapshot


def test_recall_hot_store_smoke(tmp_path):
    store = RecallHotStore(base_dir=tmp_path / "hot", max_snapshots=3)

    store.save_hot_snapshot(
        RecallSnapshot(
            operator_id="op1",
            session_id="s1",
            timestamp_utc="2026-03-22T10-00-00Z",
            operator_essence_delta={"dominant_crystal_principle": {"packet_id": "p1"}},
            open_threads=[{"thread_id": "t1"}],
            state_vector_shifts=[{"shift_id": "sv1"}],
        )
    )
    store.save_hot_snapshot(
        RecallSnapshot(
            operator_id="op1",
            session_id="s2",
            timestamp_utc="2026-03-22T11-00-00Z",
            operator_essence_delta={"dominant_crystal_principle": {"packet_id": "p2"}},
            open_threads=[{"thread_id": "t2"}],
            state_vector_shifts=[{"shift_id": "sv2"}],
        )
    )
    store.save_hot_snapshot(
        RecallSnapshot(
            operator_id="op1",
            session_id="s3",
            timestamp_utc="2026-03-22T12-00-00Z",
            operator_essence_delta={"dominant_crystal_principle": {"packet_id": "p3"}},
            open_threads=[{"thread_id": "t3"}],
            state_vector_shifts=[{"shift_id": "sv3"}],
        )
    )
    store.save_hot_snapshot(
        RecallSnapshot(
            operator_id="op1",
            session_id="s4",
            timestamp_utc="2026-03-22T13-00-00Z",
            operator_essence_delta={"dominant_crystal_principle": {"packet_id": "p4"}},
            open_threads=[{"thread_id": "t4"}],
            state_vector_shifts=[{"shift_id": "sv4"}],
        )
    )

    status = store.get_status("op1")
    assert status["hot_exists"] is True
    assert status["snapshots_count"] == 3
    assert status["latest_session_id"] == "s4"

    context = store.get_context("op1", max_open_threads=2, max_state_vector_shifts=2)
    assert context["operator_id"] == "op1"
    assert context["recall_context"]["operator_essence_delta"]["dominant_crystal_principle"]["packet_id"] == "p4"
    assert len(context["recall_context"]["open_threads"]) == 2
    assert len(context["recall_context"]["state_vector_shifts"]) == 2

    loaded = store.load_hot_context("op1")
    assert loaded["operator_id"] == "op1"
    assert len(loaded["snapshots"]) == 3
    assert loaded["recall_context"]["operator_essence_delta"]["dominant_crystal_principle"]["packet_id"] == "p4"
    assert {"thread_id": "t4"} in loaded["recall_context"]["open_threads"]

    store.purge_operator("op1")
    purged = store.load_hot_context("op1")
    assert purged["snapshots"] == []

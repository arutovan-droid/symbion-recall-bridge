from symbion_recall_bridge import RecallHotStore, RecallSnapshot, snapshot_from_distillation


def test_recall_hot_store_smoke(tmp_path):
    store = RecallHotStore(
        base_dir=tmp_path / "hot",
        warm_dir=tmp_path / "warm",
        max_snapshots=3,
    )

    store.save_hot_snapshot(
        RecallSnapshot(
            operator_id="op1",
            session_id="s1",
            timestamp_utc="2026-03-22T10-00-00Z",
            operator_essence_delta={"dominant_crystal_principle": {"packet_id": "p1"}},
            open_threads=[{"thread_id": "t1"}],
            state_vector_shifts=[{"shift_id": "sv1"}],
            metadata={
                "session_envelope": {
                    "operator_id": "op1",
                    "session_id": "s1",
                    "recall_loaded": False,
                    "publish_count": 1,
                    "last_event_type": "alchemist.ingress.transmuted",
                    "last_payload_keys": ["state_vector"],
                }
            },
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
            metadata={
                "session_envelope": {
                    "operator_id": "op1",
                    "session_id": "s2",
                    "recall_loaded": True,
                    "publish_count": None,
                    "last_event_type": None,
                    "last_payload_keys": [],
                }
            },
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

    store.save_hot_snapshot(
        RecallSnapshot(
            operator_id="op1",
            session_id="s5",
            timestamp_utc="2026-03-22T14-00-00Z",
            operator_essence_delta={"dominant_crystal_principle": {"packet_id": "p5"}},
            open_threads=[{"thread_id": "t5"}],
            state_vector_shifts=[{"shift_id": "sv5"}],
        )
    )

    status = store.get_status("op1")
    assert status["hot_exists"] is True
    assert status["snapshots_count"] == 3
    assert status["latest_session_id"] == "s5"
    assert status["warm_exists"] is True
    assert status["warm_history_count"] == 2

    warm = store.load_warm_essence("op1")
    assert warm.operator_id == "op1"
    assert warm.history[0]["session_id"] == "s1"
    assert warm.history[0]["operator_id"] == "op1"
    assert warm.history[0]["publish_count"] == 1
    assert warm.history[0]["last_event_type"] == "alchemist.ingress.transmuted"
    assert "state_vector" in warm.history[0]["last_payload_keys"]
    assert warm.history[0]["empty_session"] is False

    assert warm.history[1]["session_id"] == "s2"
    assert warm.history[1]["empty_session"] is True

    context = store.get_context("op1", max_open_threads=5, max_state_vector_shifts=5)
    assert context["operator_id"] == "op1"
    assert context["recall_context"]["operator_essence_delta"]["dominant_crystal_principle"]["packet_id"] == "p5"
    assert {"thread_id": "t5"} in context["recall_context"]["open_threads"]
    assert context["recall_context"]["continuity_trend"][0]["session_id"] == "s1"

    loaded = store.load_hot_context("op1")
    assert loaded["operator_id"] == "op1"
    assert len(loaded["snapshots"]) == 3
    assert loaded["recall_context"]["operator_essence_delta"]["dominant_crystal_principle"]["packet_id"] == "p5"

    distilled = {
        "operator_essence_delta": {
            "dominant_crystal_principle": {"packet_id": "p9", "principle": "restore power"},
            "dominant_state_shift": {"packet_id": "p9", "shift_type": "continuity_shift"},
            "dominant_open_thread": {"packet_id": "p9", "thread_type": "unresolved_thread"},
        },
        "open_threads": [
            {"packet_id": "p9", "thread_type": "unresolved_thread", "payload": {"continuity_event": {"open_thread": True}}}
        ],
        "state_vector_shifts": [
            {"packet_id": "p9", "shift_type": "continuity_shift", "payload": {"state_vector": {"mode": "recovery"}}}
        ],
        "crystal_candidates": [
            {"packet_id": "p9", "principle": "restore power", "support": {}}
        ],
        "metadata": {
            "packets_total": 1
        },
    }

    adapted = snapshot_from_distillation(
        operator_id="op2",
        session_id="s9",
        timestamp_utc="2026-03-22T15-00-00Z",
        distilled=distilled,
    )

    assert adapted.operator_id == "op2"
    assert adapted.session_id == "s9"
    assert adapted.operator_essence_delta["dominant_crystal_principle"]["packet_id"] == "p9"
    assert adapted.metadata["distilled_packets_total"] == 1
    assert adapted.metadata["distilled_crystal_count"] == 1

    store.save_hot_snapshot(adapted)
    op2 = store.get_context("op2")
    assert op2["recall_context"]["operator_essence_delta"]["dominant_crystal_principle"]["packet_id"] == "p9"

    store.purge_operator("op1")
    purged = store.load_hot_context("op1")
    assert purged["snapshots"] == []

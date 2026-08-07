"""RenderGate 两阶段渲染闸门的单元测试。"""

import pytest

from app.manager.render_gate import RenderGate


def test_collect_executes_staged_jobs_in_order() -> None:
    gate = RenderGate()
    gate.stage(lambda: 1)
    gate.stage(lambda: 2)

    assert gate.pending_count == 2
    assert gate.collect() == [1, 2]
    assert gate.pending_count == 0


def test_collect_without_jobs_returns_empty_list() -> None:
    assert RenderGate().collect() == []


def test_collect_clears_queue_before_execution() -> None:
    gate = RenderGate()
    gate.stage(lambda: "payload")

    first = gate.collect()
    second = gate.collect()

    assert first == ["payload"]
    assert second == []


def test_collect_memoized_miss_executes_and_stores(monkeypatch) -> None:
    import streamlit as st

    monkeypatch.setattr(st, "session_state", {})
    gate = RenderGate()
    gate.stage(lambda: "built")

    result = gate.collect_memoized("memo_key", "sig-v1")

    assert result == ["built"]
    assert st.session_state["memo_key"] == {"signature": "sig-v1", "payloads": ["built"]}


def test_collect_memoized_hit_skips_jobs_and_spinner(monkeypatch) -> None:
    import streamlit as st

    state = {"memo_key": {"signature": "sig-v1", "payloads": ["cached"]}}
    monkeypatch.setattr(st, "session_state", state)
    gate = RenderGate()
    gate.stage(lambda: pytest.fail("memo 命中时不应执行构建任务"))

    assert gate.collect_memoized("memo_key", "sig-v1") == ["cached"]
    assert gate.pending_count == 0


def test_collect_memoized_rebuilds_on_signature_change(monkeypatch) -> None:
    import streamlit as st

    state = {"memo_key": {"signature": "sig-v1", "payloads": ["old"]}}
    monkeypatch.setattr(st, "session_state", state)
    gate = RenderGate()
    gate.stage(lambda: "new")

    assert gate.collect_memoized("memo_key", "sig-v2") == ["new"]
    assert state["memo_key"]["signature"] == "sig-v2"

"""RenderGate 两阶段渲染闸门的单元测试。"""

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

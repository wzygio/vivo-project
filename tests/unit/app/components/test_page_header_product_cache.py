import functools
import sys
import types
from pathlib import Path
from types import SimpleNamespace

from app.components import page_header
from app.manager.session_manager import SessionManager


class _CachedFunctionStub:
    def __init__(self) -> None:
        self.clear_count = 0

    def clear(self) -> None:
        self.clear_count += 1


def test_product_cache_revision_changes_only_the_selected_product(tmp_path: Path) -> None:
    m626_before = page_header.build_product_cache_signature(
        "report-v1",
        "M626",
        revision_dir=tmp_path,
    )
    m673_before = page_header.build_product_cache_signature(
        "report-v1",
        "M673",
        revision_dir=tmp_path,
    )

    page_header.bump_product_cache_revision("M626", revision_dir=tmp_path)

    m626_after = page_header.build_product_cache_signature(
        "report-v1",
        "M626",
        revision_dir=tmp_path,
    )
    m673_after = page_header.build_product_cache_signature(
        "report-v1",
        "M673",
        revision_dir=tmp_path,
    )

    assert m626_after != m626_before
    assert m673_after == m673_before


def test_product_cache_signature_includes_data_forward_policy(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        page_header.ConfigLoader,
        "get_data_forward_policy",
        lambda: SimpleNamespace(signature="data-forward-v1:enabled:4d"),
    )

    signature = page_header.build_product_cache_signature(
        "report-v1",
        "M626",
        revision_dir=tmp_path,
    )

    assert signature.endswith("|data_forward=data-forward-v1:enabled:4d")


def test_product_scoped_invalidation_does_not_clear_whole_function_cache(
    monkeypatch,
) -> None:
    cached_function = _CachedFunctionStub()
    bumped_products: list[str] = []
    monkeypatch.setattr(
        page_header,
        "bump_product_cache_revision",
        lambda product_code: bumped_products.append(product_code),
    )

    scope = page_header.invalidate_page_cache(
        cached_funcs=[cached_function],
        product_code="M673",
    )

    assert scope == "product"
    assert bumped_products == ["M673"]
    assert cached_function.clear_count == 0


def test_unscoped_invalidation_preserves_legacy_function_clear_behavior() -> None:
    cached_function = _CachedFunctionStub()

    scope = page_header.invalidate_page_cache(cached_funcs=[cached_function])

    assert scope == "global"
    assert cached_function.clear_count == 1


class _ContainerStub:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _StreamlitStub:
    """最小 streamlit 替身：驱动 render_page_header 并捕获按钮 on_click 回调。"""

    def __init__(self, session_state: dict | None = None) -> None:
        self.session_state = dict(session_state or {})
        self.query_params = {"admin": "true"}
        self.toasts: list[str] = []
        self.button_callbacks: dict[str, object] = {}
        self.selectbox_labels: list[str] = []
        self.cache_data = SimpleNamespace(clear=lambda: None)
        self.cache_resource = SimpleNamespace(clear=lambda: None)

    def markdown(self, *args, **kwargs) -> None:
        return None

    def title(self, *args, **kwargs) -> None:
        return None

    def caption(self, *args, **kwargs) -> None:
        return None

    def columns(self, spec, **kwargs) -> list:
        return [_ContainerStub() for _ in spec]

    def container(self, **kwargs) -> _ContainerStub:
        return _ContainerStub()

    def selectbox(self, label, *, options, index=0, **kwargs):
        self.selectbox_labels.append(label)
        return options[index]

    def button(self, label, *, key=None, on_click=None, **kwargs) -> None:
        self.button_callbacks[key] = on_click

    def toast(self, message, *, icon=None) -> None:
        self.toasts.append(message)

    def rerun(self) -> None:
        raise AssertionError("渲染页头时不应触发 rerun")


def _render_header_and_get_refresh_callback(
    monkeypatch,
    *,
    refresh_handlers,
    cached_funcs=None,
    product_cache_scope=None,
    session_state=None,
):
    """渲染页头并返回「刷新数据」按钮的 on_click 回调与 streamlit 替身。"""
    st_stub = _StreamlitStub(session_state)
    monkeypatch.setattr(page_header, "st", st_stub)
    monkeypatch.setattr(
        page_header,
        "detect_project_changes",
        lambda *args, **kwargs: False,
    )
    monkeypatch.setattr(SessionManager, "AVAILABLE_PRODUCTS", ["M626"])
    config = SimpleNamespace(data_source=SimpleNamespace(product_code="M626"))

    page_header.render_page_header(
        title="测试页",
        config=config,
        cached_funcs=cached_funcs,
        refresh_handlers=refresh_handlers,
        product_cache_scope=product_cache_scope,
    )

    return st_stub.button_callbacks["btn_refresh_测试页"], st_stub


def _redirect_revision_writes_to_tmp(monkeypatch, tmp_path: Path) -> None:
    """将产品 revision 写入重定向到 tmp 目录，避免污染真实 output/。"""
    real_bump = page_header.bump_product_cache_revision
    monkeypatch.setattr(
        page_header,
        "bump_product_cache_revision",
        functools.partial(real_bump, revision_dir=tmp_path),
    )


def test_refresh_data_success_bumps_product_revision_and_clears_view_model(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _redirect_revision_writes_to_tmp(monkeypatch, tmp_path)
    handled: list[str] = []
    refresh_callback, st_stub = _render_header_and_get_refresh_callback(
        monkeypatch,
        refresh_handlers=[lambda: handled.append("snapshot") or True],
        cached_funcs=[_CachedFunctionStub()],
        product_cache_scope="M626",
        session_state={"inline_view_model_1": object(), "unrelated_key": 1},
    )
    revision_before = page_header.get_product_cache_revision(
        "M626", revision_dir=tmp_path
    )

    refresh_callback()

    revision_after = page_header.get_product_cache_revision(
        "M626", revision_dir=tmp_path
    )
    assert handled == ["snapshot"]
    assert revision_after != revision_before
    assert "inline_view_model_1" not in st_stub.session_state
    assert st_stub.session_state["unrelated_key"] == 1
    assert "✅ L1 快照与 L2 缓存已刷新。" in st_stub.toasts


def test_refresh_data_failure_keeps_revision_and_session_cache(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _redirect_revision_writes_to_tmp(monkeypatch, tmp_path)
    refresh_callback, st_stub = _render_header_and_get_refresh_callback(
        monkeypatch,
        refresh_handlers=[lambda: True, lambda: False],
        cached_funcs=None,
        product_cache_scope="M626",
        session_state={"inline_view_model_1": object()},
    )
    revision_before = page_header.get_product_cache_revision(
        "M626", revision_dir=tmp_path
    )

    refresh_callback()

    revision_after = page_header.get_product_cache_revision(
        "M626", revision_dir=tmp_path
    )
    assert revision_after == revision_before
    assert "inline_view_model_1" in st_stub.session_state
    assert "❌ 数据库连接或快照更新失败，已保留当前缓存视图。" in st_stub.toasts


def test_refresh_data_without_product_scope_clears_cached_funcs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _redirect_revision_writes_to_tmp(monkeypatch, tmp_path)
    cached_function = _CachedFunctionStub()
    refresh_callback, st_stub = _render_header_and_get_refresh_callback(
        monkeypatch,
        refresh_handlers=[lambda: True],
        cached_funcs=[cached_function],
        product_cache_scope=None,
        session_state={"agg_view_model": object()},
    )

    refresh_callback()

    assert cached_function.clear_count == 1
    assert "agg_view_model" not in st_stub.session_state
    assert "✅ L1 快照与 L2 缓存已刷新。" in st_stub.toasts
    assert (
        page_header.get_product_cache_revision("M626", revision_dir=tmp_path) == "0"
    )


def test_refresh_data_does_not_reload_modules_or_config(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _redirect_revision_writes_to_tmp(monkeypatch, tmp_path)
    reloaded: list[str] = []
    fake_reloader = types.ModuleType("app.utils.reloader")
    fake_reloader.deep_reload_modules = lambda: reloaded.append("reload")  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "app.utils.reloader", fake_reloader)
    config_loads: list[str] = []
    monkeypatch.setattr(
        SessionManager,
        "load_and_set_config",
        staticmethod(lambda product: config_loads.append(product)),
    )
    refresh_callback, _ = _render_header_and_get_refresh_callback(
        monkeypatch,
        refresh_handlers=[lambda: True],
        cached_funcs=[_CachedFunctionStub()],
        product_cache_scope="M626",
    )

    refresh_callback()

    assert reloaded == []
    assert config_loads == []


def test_refresh_data_handler_exception_is_treated_as_failure(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """handler 抛异常按失败处理：不推进 revision、不清 L2、提示失败。"""
    _redirect_revision_writes_to_tmp(monkeypatch, tmp_path)

    def exploding_handler():
        raise RuntimeError("snapshot refresh blew up")

    refresh_callback, st_stub = _render_header_and_get_refresh_callback(
        monkeypatch,
        refresh_handlers=[exploding_handler],
        cached_funcs=None,
        product_cache_scope="M626",
        session_state={"inline_view_model_1": object()},
    )
    revision_before = page_header.get_product_cache_revision(
        "M626", revision_dir=tmp_path
    )

    refresh_callback()

    assert (
        page_header.get_product_cache_revision("M626", revision_dir=tmp_path)
        == revision_before
    )
    assert "inline_view_model_1" in st_stub.session_state
    assert "❌ 数据库连接或快照更新失败，已保留当前缓存视图。" in st_stub.toasts


def test_refresh_data_db_failure_with_snapshot_fallback_keeps_revision(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """DB 失败但降级返回非空旧快照时，页头仍按失败处理（假成功回归）。"""
    import pandas as pd

    from src.inline_domain import composition
    from src.inline_domain.infrastructure.shared.measurement_snapshot_repository import (
        InlineMeasurementSnapshotRepository,
    )

    _redirect_revision_writes_to_tmp(monkeypatch, tmp_path)

    snapshot_dir = tmp_path / "snapshots"
    snapshot_dir.mkdir()
    stale = pd.DataFrame(
        [
            {
                "factory": "ARRAY",
                "prod_code": "M626",
                "start_time": pd.Timestamp("2026-08-13 08:00:00"),
                "sheet_id": "SHEET-1",
                "lot_id": "FALLBACK",
                "step_id": "11620",
                "param_name": "TDSUM",
                "site_name": "S1",
                "unit_id": "EQ-1",
                "param_value": 3.0,
            }
        ]
    )
    stale.to_parquet(snapshot_dir / "inline_measurements_M626.parquet", index=False)

    def failing_loader(*_args):
        raise RuntimeError("database unavailable")

    repository = InlineMeasurementSnapshotRepository(
        snapshot_dir=snapshot_dir,
        db_manager=SimpleNamespace(engine=object()),
        measurement_loader=failing_loader,
    )
    monkeypatch.setattr(
        composition,
        "build_raw_measurement_repository",
        lambda *_args, **_kwargs: repository,
    )

    refresh_callback, st_stub = _render_header_and_get_refresh_callback(
        monkeypatch,
        refresh_handlers=[
            lambda: composition.refresh_raw_measurements(object(), "M626", "2026-08-13")
        ],
        cached_funcs=None,
        product_cache_scope="M626",
        session_state={"inline_view_model_1": object()},
    )
    revision_before = page_header.get_product_cache_revision(
        "M626", revision_dir=tmp_path
    )

    refresh_callback()

    assert (
        page_header.get_product_cache_revision("M626", revision_dir=tmp_path)
        == revision_before
    )
    assert "inline_view_model_1" in st_stub.session_state
    assert "❌ 数据库连接或快照更新失败，已保留当前缓存视图。" in st_stub.toasts
    assert "✅ L1 快照与 L2 缓存已刷新。" not in st_stub.toasts


def test_show_product_filter_false_skips_product_selectbox(monkeypatch) -> None:
    """show_product_filter=False：无产品筛选框，管理员操作列保持正常布局。"""
    st_stub = _StreamlitStub()
    monkeypatch.setattr(page_header, "st", st_stub)
    monkeypatch.setattr(
        page_header,
        "detect_project_changes",
        lambda *args, **kwargs: False,
    )
    monkeypatch.setattr(SessionManager, "AVAILABLE_PRODUCTS", ["M626"])
    config = SimpleNamespace(data_source=SimpleNamespace(product_code="M626"))

    page_header.render_page_header(
        title="测试页",
        config=config,
        show_product_filter=False,
    )

    assert st_stub.selectbox_labels == []
    assert "btn_refresh_测试页" in st_stub.button_callbacks
    assert "btn_clear_测试页" in st_stub.button_callbacks


def test_show_product_filter_default_keeps_product_selectbox(monkeypatch) -> None:
    """默认行为不变：其余页面仍渲染产品筛选 selectbox。"""
    st_stub = _StreamlitStub()
    monkeypatch.setattr(page_header, "st", st_stub)
    monkeypatch.setattr(
        page_header,
        "detect_project_changes",
        lambda *args, **kwargs: False,
    )
    monkeypatch.setattr(SessionManager, "AVAILABLE_PRODUCTS", ["M626"])
    config = SimpleNamespace(data_source=SimpleNamespace(product_code="M626"))

    page_header.render_page_header(title="测试页", config=config)

    assert st_stub.selectbox_labels == ["📦 当前产品型号"]


def test_hard_reset_clears_alert_matrix_loaded_state(monkeypatch) -> None:
    """「刷新缓存」后矩阵已加载状态被清除（页面回到按钮门控）。"""
    st_stub = _StreamlitStub(
        {"alert_matrix_board_loaded": True, "unrelated_key": 1}
    )
    monkeypatch.setattr(page_header, "st", st_stub)
    fake_reloader = types.ModuleType("app.utils.reloader")
    fake_reloader.deep_reload_modules = lambda: None  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "app.utils.reloader", fake_reloader)

    page_header.perform_hard_reset([], None)

    assert "alert_matrix_board_loaded" not in st_stub.session_state
    assert st_stub.session_state["unrelated_key"] == 1


def test_refresh_data_clears_alert_matrix_loaded_state(monkeypatch) -> None:
    """「刷新数据」成功后同样清除矩阵已加载状态。"""
    refresh_callback, st_stub = _render_header_and_get_refresh_callback(
        monkeypatch,
        refresh_handlers=[lambda: True],
        cached_funcs=[_CachedFunctionStub()],
        product_cache_scope=None,
        session_state={"alert_matrix_board_loaded": True},
    )

    refresh_callback()

    assert "alert_matrix_board_loaded" not in st_stub.session_state


def test_hard_reset_clears_monitor_query_state(monkeypatch) -> None:
    """「刷新缓存」后超规片自动预警的查询已提交状态被清除（回到查询门控）。"""
    st_stub = _StreamlitStub(
        {"monitor_query_signature": ("ALL", ("M678",), ("ARRAY",)), "unrelated_key": 1}
    )
    monkeypatch.setattr(page_header, "st", st_stub)
    fake_reloader = types.ModuleType("app.utils.reloader")
    fake_reloader.deep_reload_modules = lambda: None  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "app.utils.reloader", fake_reloader)

    page_header.perform_hard_reset([], None)

    assert "monitor_query_signature" not in st_stub.session_state
    assert st_stub.session_state["unrelated_key"] == 1


def test_refresh_data_clears_monitor_query_state(monkeypatch) -> None:
    """「刷新数据」成功后同样清除查询已提交状态。"""
    refresh_callback, st_stub = _render_header_and_get_refresh_callback(
        monkeypatch,
        refresh_handlers=[lambda: True],
        cached_funcs=[_CachedFunctionStub()],
        product_cache_scope=None,
        session_state={"monitor_query_signature": ("ALL", ("M678",), ("ARRAY",))},
    )

    refresh_callback()

    assert "monitor_query_signature" not in st_stub.session_state

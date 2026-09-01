from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
import tomllib

import pytest

from app.components import page_header
from app.utils import reloader
from src.shared_kernel.config import ConfigLoader


PROJECT_ROOT = Path(__file__).resolve().parents[4]


def test_streamlit_run_on_save_is_disabled():
    """手动热重载模式：保存源码不再自动触发整页 rerun。"""
    config_path = PROJECT_ROOT / ".streamlit" / "config.toml"

    with config_path.open("rb") as config_file:
        config = tomllib.load(config_file)

    assert config["server"]["runOnSave"] is False


def test_render_page_header_checks_project_changes_before_rendering(monkeypatch):
    class ChangeDetectionRan(Exception):
        pass

    class UnexpectedStreamlitAccess:
        def __getattr__(self, name):
            pytest.fail(f"Streamlit rendering started before change detection: {name}")

    def stop_after_detection():
        raise ChangeDetectionRan

    monkeypatch.setattr(page_header, "detect_project_changes", stop_after_detection)
    monkeypatch.setattr(page_header, "st", UnexpectedStreamlitAccess())

    with pytest.raises(ChangeDetectionRan):
        page_header.render_page_header()


def test_detect_project_changes_marks_pending_without_reloading_or_rerunning(monkeypatch):
    session_state = {"last_code_revision": "old-revision"}
    events = []
    fake_streamlit = SimpleNamespace(
        session_state=session_state,
        rerun=lambda: events.append(("rerun", None)),
    )

    monkeypatch.setattr(page_header, "st", fake_streamlit)
    monkeypatch.setattr(
        ConfigLoader,
        "get_project_root",
        staticmethod(lambda: PROJECT_ROOT),
    )
    monkeypatch.setattr(
        reloader,
        "get_project_revision",
        lambda _project_root: "new-revision",
    )
    monkeypatch.setattr(
        reloader,
        "deep_reload_modules",
        lambda: events.append(("unload", None)),
    )

    changed = page_header.detect_project_changes()

    assert changed is True
    assert session_state["last_code_revision"] == "new-revision"
    assert session_state["code_update_pending"] is True
    assert events == []  # 不卸载模块、不 rerun


def test_detect_project_changes_only_records_the_initial_revision(monkeypatch):
    session_state = {}
    fake_streamlit = SimpleNamespace(session_state=session_state)

    monkeypatch.setattr(page_header, "st", fake_streamlit)
    monkeypatch.setattr(
        ConfigLoader,
        "get_project_root",
        staticmethod(lambda: PROJECT_ROOT),
    )
    monkeypatch.setattr(
        reloader,
        "get_project_revision",
        lambda _project_root: "initial-revision",
    )

    changed = page_header.detect_project_changes()

    assert changed is False
    assert session_state["last_code_revision"] == "initial-revision"
    assert "code_update_pending" not in session_state


def test_hard_reset_callback_reloads_code_and_config_and_clears_pending(monkeypatch):
    """刷新缓存按钮：缓存失效 + 代码重载 + 配置重读 + 清除变更提示标记。"""
    events: list[tuple[str, object]] = []
    session_state = {
        "code_update_pending": True,
        "selected_product_code": "M626",
        "view_model_cache": object(),
    }
    button_callbacks = {}

    class FakeSt:
        query_params = {"admin": "true"}

        def markdown(self, *_args, **_kwargs):
            return None

        def title(self, *_args, **_kwargs):
            return None

        def container(self, **_kwargs):
            return nullcontext()

        def columns(self, spec, **_kwargs):
            count = spec if isinstance(spec, int) else len(spec)
            return [nullcontext() for _ in range(count)]

        def selectbox(self, *_args, **kwargs):
            return kwargs.get("options", ["M626"])[0]

        def button(self, label, **kwargs):
            button_callbacks[label] = kwargs.get("on_click")
            return False

        def write(self, *_args, **_kwargs):
            return None

        def toast(self, *_args, **_kwargs):
            return None

        def caption(self, *_args, **_kwargs):
            return None

    fake_st = FakeSt()
    fake_st.session_state = session_state
    monkeypatch.setattr(page_header, "st", fake_st)
    monkeypatch.setattr(
        page_header,
        "invalidate_page_cache",
        lambda *args, **kwargs: events.append(("invalidate", kwargs.get("product_code"))) or "product",
    )
    monkeypatch.setattr(
        reloader,
        "deep_reload_modules",
        lambda: events.append(("deep_reload", None)),
    )
    monkeypatch.setattr(
        page_header.SessionManager,
        "load_and_set_config",
        staticmethod(lambda product: events.append(("load_config", product))),
    )

    page_header.render_page_header(
        title="SPC监控报表",
        config=SimpleNamespace(data_source=SimpleNamespace(product_code="M626")),
        product_cache_scope="M626",
    )
    hard_reset = button_callbacks["🔄 刷新缓存"]
    assert hard_reset is not None

    hard_reset()

    assert ("invalidate", "M626") in events
    assert ("deep_reload", None) in events
    assert ("load_config", "M626") in events
    assert "code_update_pending" not in session_state
    assert "view_model_cache" not in session_state


@pytest.mark.parametrize(
    ("query_params", "expected_buttons", "expected_groups"),
    [
        ({}, [], ["产品筛选"]),
        (
            {"admin": "true"},
            ["🔄 刷新数据", "🔄 刷新缓存"],
            ["产品筛选", "管理员操作"],
        ),
        ({"admin": "True"}, [], ["产品筛选"]),
    ],
)
def test_page_header_separates_product_filter_and_gates_admin_actions(
    monkeypatch,
    query_params,
    expected_buttons,
    expected_groups,
):
    """产品筛选始终可见，刷新操作仅在严格的 admin=true 下单独显示。"""
    button_calls = []
    captions = []
    bordered_containers = []

    class FakeSt:
        session_state = {}

        def __init__(self):
            self.query_params = query_params

        def markdown(self, *_args, **_kwargs):
            return None

        def title(self, *_args, **_kwargs):
            return None

        def container(self, **kwargs):
            if kwargs.get("border"):
                bordered_containers.append(kwargs)
            return nullcontext()

        def columns(self, spec, **_kwargs):
            count = spec if isinstance(spec, int) else len(spec)
            return [nullcontext() for _ in range(count)]

        def selectbox(self, *_args, **kwargs):
            return kwargs["options"][0]

        def button(self, label, **kwargs):
            button_calls.append((label, kwargs))
            return False

        def caption(self, text, **_kwargs):
            captions.append(text)

    monkeypatch.setattr(page_header, "detect_project_changes", lambda: False)
    monkeypatch.setattr(page_header, "st", FakeSt())

    page_header.render_page_header(
        title="测试页面",
        config=SimpleNamespace(data_source=SimpleNamespace(product_code="M626")),
    )

    assert [label for label, _kwargs in button_calls] == expected_buttons
    assert captions == expected_groups
    assert len(bordered_containers) == len(expected_groups)
    assert all(kwargs.get("width") == "stretch" for _label, kwargs in button_calls)


def test_every_streamlit_page_uses_the_shared_page_header():
    """所有业务页面都必须通过共享页头获得一致的产品与管理员控件。"""
    pages_dir = PROJECT_ROOT / "app" / "pages"
    page_files = sorted(pages_dir.glob("*.py"))
    pages_without_header = [
        page_file.name
        for page_file in page_files
        if "render_page_header(" not in page_file.read_text(encoding="utf-8")
    ]

    assert len(page_files) == 12
    assert pages_without_header == []

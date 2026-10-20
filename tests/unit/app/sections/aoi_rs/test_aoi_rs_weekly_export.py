"""Exercise the independent weekly PPT export without data or template I/O."""

from contextlib import nullcontext
from datetime import date
import json
from types import SimpleNamespace

import pytest
from streamlit.testing.v1 import AppTest

from app.sections.inline_domain.aoi_rs import aoi_rs_weekly_export as weekly


START = date(2026, 9, 1)
END = date(2026, 10, 19)
ENABLED = ["M678", "M626"]
TEMPLATE = b"isolated PPT template fixture"


def _app(*, admin=False):
    app = AppTest.from_string("""
from datetime import date
from app.sections.inline_domain.aoi_rs.aoi_rs_weekly_export import render_aoi_rs_weekly_export
render_aoi_rs_weekly_export(
    current_product="M626", start_date=date(2026, 9, 1), end_date=date(2026, 10, 19),
    db_manager=None, step_desc_map={"11629": "PSI_FI_REV"},
)
""")
    if admin:
        app.query_params["admin"] = "true"
    return app


@pytest.fixture
def isolated_export(monkeypatch):
    calls = []
    monkeypatch.setattr(weekly.ConfigLoader, "get_enabled_products", lambda: ENABLED.copy())
    monkeypatch.setattr(weekly, "load_weekly_ppt_template", lambda: TEMPLATE)

    def build(products, **kwargs):
        calls.append((products, kwargs))
        return b"complete weekly PPT fixture"

    monkeypatch.setattr(weekly, "build_aoi_rs_weekly_ppt", build)
    return calls


@pytest.mark.parametrize("admin", [None, "false", "True", "true"])
def test_weekly_export_available_in_all_display_modes(isolated_export, admin):
    app = _app(admin=False)
    if admin is not None:
        app.query_params["admin"] = admin
    app.run()
    assert not app.exception
    assert app.expander[0].label == "RS 周报导出（PPT）"
    assert app.multiselect(key=weekly.PRODUCT_SELECTION_KEY).value == ENABLED
    assert not isolated_export
    app.button(key="aoi_rs_weekly_ppt_generate").click().run()
    assert not app.exception
    assert len(app.get("download_button")) == 1
    assert len(isolated_export) == 1
    visible_text = " ".join(item.value for item in app.caption)
    assert "admin" not in visible_text.lower()


def test_defaults_and_generate_then_download_survive_rerun(isolated_export):
    app = _app().run()
    assert not app.exception
    assert app.multiselect(key=weekly.PRODUCT_SELECTION_KEY).value == ENABLED
    assert app.multiselect(key=weekly.FACTORY_SELECTION_KEY).value == ["ARRAY"]
    assert app.multiselect(key=weekly.STEP_SELECTION_KEY).value == []
    assert len(app.multiselect(key=weekly.STEP_SELECTION_KEY).options) == 5
    assert not isolated_export
    assert not app.get("download_button")
    app.button(key="aoi_rs_weekly_ppt_generate").click().run()
    assert not app.exception
    assert len(isolated_export) == 1
    products, kwargs = isolated_export[0]
    assert products == tuple(ENABLED)
    assert kwargs["factories"] == ("ARRAY",)
    assert kwargs["steps"] == ()
    assert kwargs["reference_date"] == END
    assert kwargs["template_bytes"] == TEMPLATE
    assert callable(kwargs["load_report"])
    downloads = app.get("download_button")
    assert len(downloads) == 1
    assert downloads[0].proto.url
    app.run()
    assert not app.exception
    assert len(app.get("download_button")) == 1
    assert len(isolated_export) == 1


def test_selected_products_factories_and_array_station_are_forwarded(isolated_export):
    app = _app().run()
    app.multiselect(key=weekly.PRODUCT_SELECTION_KEY).set_value(["M626"]).run()
    app.multiselect(key=weekly.FACTORY_SELECTION_KEY).set_value(["ARRAY", "OLED", "TP"]).run()
    app.multiselect(key=weekly.STEP_SELECTION_KEY).set_value(["12629"]).run()
    assert not isolated_export
    app.button(key="aoi_rs_weekly_ppt_generate").click().run()
    assert not app.exception
    products, kwargs = isolated_export[0]
    assert products == ("M626",)
    assert kwargs["factories"] == ("ARRAY", "OLED", "TP")
    assert kwargs["steps"] == ("12629",)


def test_empty_product_and_factory_selection_mean_full_enabled_scope(isolated_export):
    app = _app().run()
    app.multiselect(key=weekly.PRODUCT_SELECTION_KEY).set_value([]).run()
    app.multiselect(key=weekly.FACTORY_SELECTION_KEY).set_value([]).run()
    app.button(key="aoi_rs_weekly_ppt_generate").click().run()
    assert not app.exception
    products, kwargs = isolated_export[0]
    assert products == tuple(ENABLED)
    assert kwargs["factories"] == ("ARRAY", "OLED", "TP")


def test_array_station_selection_is_omitted_for_non_array_exports(isolated_export):
    app = _app().run()
    app.multiselect(key=weekly.STEP_SELECTION_KEY).set_value(["12629"]).run()
    app.multiselect(key=weekly.FACTORY_SELECTION_KEY).set_value(["OLED", "TP"]).run()
    assert [widget.key for widget in app.multiselect] == [weekly.PRODUCT_SELECTION_KEY, weekly.FACTORY_SELECTION_KEY]
    app.button(key="aoi_rs_weekly_ppt_generate").click().run()
    assert not app.exception
    assert isolated_export[0][1]["steps"] == ()


def test_no_enabled_products_has_no_export_controls(isolated_export, monkeypatch):
    monkeypatch.setattr(weekly.ConfigLoader, "get_enabled_products", lambda: [])
    app = _app().run()
    assert not app.exception
    assert not app.multiselect
    assert not app.button
    assert not app.get("download_button")
    assert not isolated_export


def test_disabled_products_and_obsolete_stations_are_removed_before_widgets(isolated_export):
    app = _app()
    app.session_state[weekly.PRODUCT_SELECTION_KEY] = ["DISABLED", "M626"]
    app.session_state[weekly.STEP_SELECTION_KEY] = ["OBSOLETE", "13629"]
    app.run()
    assert not app.exception
    assert app.multiselect(key=weekly.PRODUCT_SELECTION_KEY).value == ["M626"]
    assert app.multiselect(key=weekly.STEP_SELECTION_KEY).value == ["13629"]
    app.button(key="aoi_rs_weekly_ppt_generate").click().run()
    assert not app.exception
    assert isolated_export[0][0] == ("M626",)
    assert isolated_export[0][1]["steps"] == ("13629",)


@pytest.mark.parametrize("failure_site", ["build_aoi_rs_weekly_ppt", "load_weekly_ppt_template"])
def test_generation_failure_shows_safe_business_message(isolated_export, monkeypatch, failure_site):
    def fail(*args, **kwargs):
        raise OSError(r"SYNTHETIC_SECRET at C:\private\configuration.yaml")

    monkeypatch.setattr(weekly, failure_site, fail)
    app = _app().run()
    app.button(key="aoi_rs_weekly_ppt_generate").click().run()
    assert not app.exception
    assert len(app.error) == 1
    assert "失败" in app.error[0].value
    assert "SYNTHETIC_SECRET" not in app.error[0].value
    assert "configuration.yaml" not in app.error[0].value
    assert not app.get("download_button")
    assert not app.session_state[weekly.STATUS_KEY].busy


def test_failed_rebuild_preserves_previous_complete_download(isolated_export, monkeypatch):
    app = _app().run()
    app.button(key="aoi_rs_weekly_ppt_generate").click().run()
    previous_url = app.get("download_button")[0].proto.url

    def fail(*args, **kwargs):
        raise RuntimeError("failed replacement")

    monkeypatch.setattr(weekly, "build_aoi_rs_weekly_ppt", fail)
    app.button(key="aoi_rs_weekly_ppt_generate").click().run()
    assert not app.exception
    assert app.error
    assert app.get("download_button")[0].proto.url == previous_url


def test_changed_enabled_scope_removes_old_report_from_downloads(isolated_export, monkeypatch):
    app = _app().run()
    app.button(key="aoi_rs_weekly_ppt_generate").click().run()
    assert app.get("download_button")
    monkeypatch.setattr(weekly.ConfigLoader, "get_enabled_products", lambda: ["M626"])
    app.run()
    assert not app.exception
    assert app.multiselect(key=weekly.PRODUCT_SELECTION_KEY).value == ["M626"]
    assert not app.get("download_button")
    assert app.session_state[weekly.STATUS_KEY].data is None


def test_generation_loads_each_product_with_its_own_query_and_cache_signatures(isolated_export, monkeypatch):
    service_calls, repository_calls, signature_calls = [], [], []
    ports = {product: object() for product in ENABLED}
    reports = {product: object() for product in ENABLED}

    def repository(manager, product):
        assert manager is None
        repository_calls.append(product)
        return ports[product]

    def report_service(**kwargs):
        service_calls.append(kwargs)
        return reports[json.loads(kwargs["query_config_json"])["prod_code"]]

    def signature(namespace, product, scopes):
        signature_calls.append((namespace, product, scopes))
        return f"snapshot:{product}"

    def build(products, *, load_report, **kwargs):
        assert all(load_report(product) is reports[product] for product in products)
        return b"complete isolated PPT"

    monkeypatch.setattr(weekly, "build_aoi_rs_repository", repository)
    monkeypatch.setattr(weekly.AoiRsReportService, "get_aoi_rs_report_data", report_service)
    monkeypatch.setattr(weekly, "build_indicator_product_cache_signature", signature)
    monkeypatch.setattr(weekly, "get_indicator_product_revision", lambda scope, product: f"revision:{scope}:{product}")
    monkeypatch.setattr(weekly, "get_scope_decision_signature", lambda scope, product: f"decision:{scope}:{product}")
    monkeypatch.setattr(weekly, "build_aoi_rs_weekly_ppt", build)
    app = _app().run()
    assert not repository_calls
    app.button(key="aoi_rs_weekly_ppt_generate").click().run()
    assert not app.exception
    assert repository_calls == ENABLED
    assert signature_calls == [("aoi_rs_report_v1", product, ("aoi_rs_sheet_oos",)) for product in ENABLED]
    assert len(service_calls) == len(ENABLED)
    for call, product in zip(service_calls, ENABLED):
        assert call["_data_port"] is ports[product]
        assert json.loads(call["query_config_json"]) == {
            "prod_code": product, "start_date": START.isoformat(), "end_date": END.isoformat(),
            "factory": None, "step_id": None, "rs_code": None,
        }
        assert call["snapshot_signature"] == f"snapshot:{product}"
        assert call["product_revision"] == f"revision:aoi_rs_sheet_oos:{product}"
        assert call["decision_signature"] == f"decision:aoi_rs:{product}"


def test_baseexception_propagates_resets_busy_and_keeps_download(isolated_export, monkeypatch):
    state, buttons, downloads = {}, [], []
    ui = SimpleNamespace(
        query_params={"admin": "true"}, session_state=state,
        expander=lambda *args, **kwargs: nullcontext(),
        spinner=lambda *args, **kwargs: nullcontext(),
        multiselect=lambda label, *args, key, **kwargs: state[key],
        caption=lambda *args, **kwargs: None,
        success=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
        button=lambda label, **kwargs: buttons.append(kwargs),
        download_button=lambda label, **kwargs: downloads.append(kwargs),
    )
    monkeypatch.setattr(weekly, "st", ui)

    def render():
        weekly.render_aoi_rs_weekly_export(
            current_product="M626", start_date=START, end_date=END, db_manager=None,
        )

    render()
    generate = buttons[-1]["on_click"]
    generate()
    render()
    assert downloads[-1]["data"] == b"complete weekly PPT fixture"
    assert downloads[-1]["on_click"] == "ignore"
    assert downloads[-1]["mime"] == "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    assert downloads[-1]["file_name"].endswith(".pptx")

    class Interrupted(BaseException):
        pass

    def interrupt(*args, **kwargs):
        assert state[weekly.STATUS_KEY].busy
        raise Interrupted()

    monkeypatch.setattr(weekly, "build_aoi_rs_weekly_ppt", interrupt)
    with pytest.raises(Interrupted):
        generate()
    assert not state[weekly.STATUS_KEY].busy
    render()
    assert downloads[-1]["data"] == b"complete weekly PPT fixture"
    monkeypatch.setattr(weekly, "build_aoi_rs_weekly_ppt", lambda *args, **kwargs: b"replacement PPT")
    buttons[-1]["on_click"]()
    render()
    assert downloads[-1]["data"] == b"replacement PPT"

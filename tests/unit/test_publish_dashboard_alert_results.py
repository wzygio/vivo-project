from datetime import date

from tools.publish_dashboard_alert_results import refresh_results


TODAY = date(2026, 11, 3)


def test_refreshes_stale_yield_kinds_and_each_ijp_region_once():
    states = {("yield_lot_oos", "M673"): "error",
              ("yield_trend_fluctuation", "M673"): "error"}
    for row in ("ijp_aa_oos", "ijp_hole_oos"):
        for product in ("M673", "M678"):
            states[row, product] = "error"
    calls = []

    def reader(row, product, reference):
        assert reference == TODAY
        return {"state": states.get((row, product), "ok")}

    def yield_producer(product, *, kinds):
        calls.append((product, kinds))
        for kind in kinds:
            states[("yield_lot_oos" if kind == "lot" else "yield_trend_fluctuation", product)] = "ok"
        return {"status": "success"}

    def ijp_producer(row):
        calls.append(row)
        for product in ("M673", "M678"):
            states[row, product] = "ok"

    outcomes = refresh_results(("M673", "M678"), TODAY, reader=reader,
                               yield_producer=yield_producer, ijp_producer=ijp_producer)
    assert calls == [("M673", ("trend", "lot")), "ijp_aa_oos", "ijp_hole_oos"]
    assert all(item["status"] in {"success", "current"} for item in outcomes)


def test_one_producer_failure_does_not_prevent_other_regions_or_products():
    published = set()

    def reader(row, product, reference):
        return {"state": "ok" if (row, product) in published else "error"}

    def yield_producer(product, *, kinds):
        if product == "M673":
            raise RuntimeError("private connection details")
        for row in ("yield_lot_oos", "yield_trend_fluctuation"):
            published.add((row, product))

    def ijp_producer(row):
        if row == "ijp_aa_oos":
            return  # A successful call alone does not prove valid publication.
        published.update((row, product) for product in ("M673", "M678"))

    outcomes = refresh_results(("M673", "M678"), TODAY, reader=reader,
                               yield_producer=yield_producer, ijp_producer=ijp_producer)
    assert [item["status"] for item in outcomes] == ["failed", "success", "failed", "success"]
    assert outcomes[0]["error_type"] == "RuntimeError"
    assert "private" not in str(outcomes)


def test_fresh_results_do_not_construct_or_call_producers():
    def forbidden(*args, **kwargs):
        raise AssertionError("Unexpected production")

    outcomes = refresh_results(("M673",), TODAY, reader=lambda *args: {"state": "ok"},
                               yield_producer=forbidden, ijp_producer=forbidden)
    assert all(item["status"] == "current" for item in outcomes)


def test_yield_empty_success_is_current_but_ijp_unpublished_template_is_produced():
    done = set()

    def reader(row, *args):
        return {"state": "ok" if row in done else "no_data"}

    outcomes = refresh_results(("M673",), TODAY, reader=reader,
                               yield_producer=lambda *a, **kw: None, ijp_producer=done.add)
    assert done == {"ijp_aa_oos", "ijp_hole_oos"}
    assert [item["status"] for item in outcomes] == ["current", "success", "success"]


def test_check_does_not_load_or_write_business_results(tmp_path, monkeypatch):
    from src.shared_kernel.config import ConfigLoader
    from tools.publish_dashboard_alert_results import main

    monkeypatch.setattr(ConfigLoader, "get_enabled_products", lambda **kwargs: ["M673"])
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", lambda *args: tmp_path / args[1])

    def forbidden(*args, **kwargs):
        raise AssertionError("Unexpected business access")

    assert main(["--check"], reader=forbidden, yield_producer=forbidden,
                ijp_producer=forbidden, log_dir=tmp_path) == 0
    assert list(tmp_path.iterdir()) == []


def test_main_logs_partial_failure_without_private_error_text(tmp_path, monkeypatch):
    import json
    from src.shared_kernel.config import ConfigLoader
    from tools.publish_dashboard_alert_results import main

    monkeypatch.setattr(ConfigLoader, "get_enabled_products", lambda **kwargs: ["M673"])
    monkeypatch.setattr(ConfigLoader, "get_domain_resource_path", lambda *args: tmp_path / args[1])

    def broken(*args, **kwargs):
        raise RuntimeError("private database information")

    assert main([], reader=broken, yield_producer=broken, ijp_producer=broken, log_dir=tmp_path) == 1
    files = list(tmp_path.glob("*.json"))
    assert len(files) == 1
    report = json.loads(files[0].read_text(encoding="utf-8"))
    assert report["status"] == "partial"
    assert len(report["results"]) == 3
    assert "private" not in files[0].read_text(encoding="utf-8")

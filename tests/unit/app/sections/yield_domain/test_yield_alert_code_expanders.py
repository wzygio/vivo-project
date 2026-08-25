"""自动预警缺陷图像（render_alert_code_expanders）单元测试。

覆盖：
- collect_alert_hit_codes 命中集合汇总（code 级 / group 级展开 / Lot 超规 / 去重排序 / 空输入）；
- 无命中不渲染任何内容；
- 有命中时外层 expander 标题数量正确，payload 构建走独立 chart key 前缀；
- RenderGate.collect_memoized 命中时不重复构建图表。
"""

from types import SimpleNamespace

import pandas as pd
import plotly.graph_objects as go

from app.sections.yield_domain import yield_dashboard


class _Context:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _FakeStreamlit:
    def __init__(self):
        self.session_state = {}
        self.expander_labels = []
        self.captions = []
        self.plotly_keys = []

    def expander(self, label, **kwargs):
        self.expander_labels.append(label)
        return _Context()

    def caption(self, *args, **kwargs):
        self.captions.append(args[0] if args else "")

    def markdown(self, *args, **kwargs):
        return None

    def warning(self, *args, **kwargs):
        return None

    def info(self, *args, **kwargs):
        return None

    def divider(self, *args, **kwargs):
        return None

    def spinner(self, *args, **kwargs):
        return _Context()

    def columns(self, spec):
        count = spec if isinstance(spec, int) else len(spec)
        return [_Context() for _ in range(count)]

    def tabs(self, labels, **kwargs):
        return [_Context() for _ in labels]

    def plotly_chart(self, *args, **kwargs):
        self.plotly_keys.append(kwargs.get("key"))
        return None


def _mapping_code_data() -> dict:
    monthly = pd.DataFrame(
        {
            "defect_group": ["G1", "G1", "G2"],
            "defect_desc": ["CODE-A", "CODE-B", "CODE-C"],
            "defect_rate": [0.001, 0.002, 0.003],
            "time_period": ["2026-06", "2026-06", "2026-06"],
        }
    )
    return {"monthly": monthly, "weekly": monthly.copy(), "daily": monthly.copy()}


class TestCollectAlertHitCodes:
    def test_empty_inputs_return_empty(self):
        assert yield_dashboard.collect_alert_hit_codes([], [], _mapping_code_data()) == []
        assert (
            yield_dashboard.collect_alert_hit_codes(None, None, _mapping_code_data()) == []
        )

    def test_code_level_record_collected(self):
        records = [
            {
                "level": "code",
                "defect_group": "G1",
                "defect_desc": "CODE-A",
                "time_period": "2026-06",
            }
        ]
        assert yield_dashboard.collect_alert_hit_codes(
            records, [], _mapping_code_data()
        ) == [("G1", "CODE-A")]

    def test_code_level_record_without_group_resolved_via_mapping(self):
        records = [{"level": "code", "defect_group": None, "defect_desc": "CODE-C"}]
        assert yield_dashboard.collect_alert_hit_codes(
            records, [], _mapping_code_data()
        ) == [("G2", "CODE-C")]

    def test_group_level_record_expands_to_all_codes(self):
        records = [{"level": "group", "defect_group": "G1", "defect_desc": None}]
        assert yield_dashboard.collect_alert_hit_codes(
            records, [], _mapping_code_data()
        ) == [("G1", "CODE-A"), ("G1", "CODE-B")]

    def test_lot_oos_record_resolved_via_mapping(self):
        lot_records = [{"异常 Code": "CODE-C", "超规 Lot ID": "LOT-1"}]
        assert yield_dashboard.collect_alert_hit_codes(
            [], lot_records, _mapping_code_data()
        ) == [("G2", "CODE-C")]

    def test_dedup_and_stable_sorted_order(self):
        trend_records = [
            {"level": "code", "defect_group": "G2", "defect_desc": "CODE-C"},
            {"level": "group", "defect_group": "G1", "defect_desc": None},
            {"level": "code", "defect_group": "G2", "defect_desc": "CODE-C"},
        ]
        lot_records = [{"异常 Code": "CODE-A"}, {"异常 Code": "CODE-C"}]
        assert yield_dashboard.collect_alert_hit_codes(
            trend_records, lot_records, _mapping_code_data()
        ) == [("G1", "CODE-A"), ("G1", "CODE-B"), ("G2", "CODE-C")]

    def test_unresolvable_codes_are_skipped(self):
        trend_records = [
            {"level": "code", "defect_group": None, "defect_desc": "CODE-UNKNOWN"},
            {"level": "group", "defect_group": "G-UNKNOWN", "defect_desc": None},
        ]
        lot_records = [{"异常 Code": "CODE-UNKNOWN"}, {"异常 Code": ""}]
        assert (
            yield_dashboard.collect_alert_hit_codes(
                trend_records, lot_records, _mapping_code_data()
            )
            == []
        )

    def test_weekly_fallback_when_monthly_missing(self):
        code_data = {
            "monthly": pd.DataFrame(),
            "weekly": pd.DataFrame(
                {"defect_group": ["G9"], "defect_desc": ["CODE-Z"]}
            ),
        }
        lot_records = [{"异常 Code": "CODE-Z"}]
        assert yield_dashboard.collect_alert_hit_codes(
            [], lot_records, code_data
        ) == [("G9", "CODE-Z")]


class TestRenderAlertCodeExpanders:
    def _call_args(self, **overrides):
        args = {
            "trend_records": [],
            "lot_oos_records": [],
            "warning_lines": None,
            "mwd_code_data": _mapping_code_data(),
            "lot_data": {},
            "sheet_data": {},
            "mapping_data": None,
            "hotspot_scripts": [],
            "product_code": "M626",
        }
        args.update(overrides)
        return args

    def test_no_hits_renders_nothing(self, monkeypatch):
        fake_st = _FakeStreamlit()
        monkeypatch.setattr(yield_dashboard, "st", fake_st)
        created = []

        class _Gate:
            def __init__(self):
                created.append(self)

        monkeypatch.setattr(yield_dashboard, "RenderGate", _Gate)

        yield_dashboard.render_alert_code_expanders(**self._call_args())

        assert fake_st.expander_labels == []
        assert created == []

    def test_hits_render_expander_with_count_and_isolated_keys(self, monkeypatch):
        fake_st = _FakeStreamlit()
        monkeypatch.setattr(yield_dashboard, "st", fake_st)

        created = []

        class _Gate:
            def __init__(self):
                self.jobs = []
                self.memo_calls = []
                created.append(self)

            def stage(self, job):
                self.jobs.append(job)

            def collect_memoized(self, state_key, signature):
                self.memo_calls.append((state_key, signature))
                return [job() for job in self.jobs]

        monkeypatch.setattr(yield_dashboard, "RenderGate", _Gate)

        build_calls = []

        def _fake_build(**kwargs):
            build_calls.append(kwargs)
            return {
                "curr_group": kwargs["curr_group"],
                "curr_code": kwargs["curr_code"],
            }

        rendered = []
        monkeypatch.setattr(yield_dashboard, "_build_compact_render_payload", _fake_build)
        monkeypatch.setattr(
            yield_dashboard,
            "_render_compact_payload",
            lambda payload: rendered.append(payload),
        )

        trend_records = [
            {"level": "code", "defect_group": "G1", "defect_desc": "CODE-A"},
            {"level": "group", "defect_group": "G2", "defect_desc": None},
        ]
        lot_records = [{"异常 Code": "CODE-A"}]
        yield_dashboard.render_alert_code_expanders(
            **self._call_args(trend_records=trend_records, lot_oos_records=lot_records)
        )

        assert fake_st.expander_labels == ["🚨 自动预警缺陷图像（2 个 Code）"]
        assert fake_st.captions
        assert [call["curr_code"] for call in build_calls] == ["CODE-A", "CODE-C"]
        assert all(call["key_prefix"] == "yield_alert" for call in build_calls)
        assert [(p["curr_group"], p["curr_code"]) for p in rendered] == [
            ("G1", "CODE-A"),
            ("G2", "CODE-C"),
        ]
        assert len(created) == 1
        memo_calls = created[0].memo_calls
        assert len(memo_calls) == 1
        state_key, signature = memo_calls[0]
        assert state_key == "yield_alert_charts_memo"
        assert "product=M626" in signature
        assert "codes=" in signature


def _trend_frame(code: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "defect_group": ["GROUP-A"],
            "defect_desc": [code],
            "defect_rate": [0.0002],
            "time_period": ["2026-06"],
        }
    )


def _lot_frame(code: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "defect_desc": [code],
            "defect_rate": [0.0002],
            "lot_id": ["LOT-1"],
            "warehousing_time": ["20260601"],
            "array_input_time": ["2026-06-01 08:00:00"],
            "defect_panel_count": [1],
        }
    )


def _full_render_inputs():
    mwd_code_data = {
        period: pd.concat(
            [_trend_frame("CODE-A"), _trend_frame("CODE-B")], ignore_index=True
        )
        for period in ("monthly", "weekly", "daily")
    }
    lot_data = {
        "code_level_details": {
            "GROUP-A": pd.concat([_lot_frame("CODE-A"), _lot_frame("CODE-B")])
        }
    }
    sheet_data = {
        "group_level_summary_for_table": pd.DataFrame(
            {
                "sheet_id": ["SHEET-1"],
                "lot_id": ["LOT-1"],
                "warehousing_time": ["20260601"],
                "array_input_time": ["2026-06-01 08:00:00"],
            }
        ),
        "code_level_details": {
            "GROUP-A": pd.DataFrame(
                {
                    "sheet_id": ["SHEET-1", "SHEET-1"],
                    "lot_id": ["LOT-1", "LOT-1"],
                    "defect_desc": ["CODE-A", "CODE-B"],
                    "defect_rate": [0.0002, 0.0003],
                    "defect_panel_count": [1, 1],
                }
            )
        },
    }
    mapping_data = pd.DataFrame(
        {
            "defect_group": ["GROUP-A", "GROUP-A"],
            "defect_desc": ["CODE-A", "CODE-B"],
            "batch_no": ["BATCH-1", "BATCH-1"],
            "batch_total_input": [100, 100],
            "panel_id": ["1-1", "1-1"],
        }
    )
    return mwd_code_data, lot_data, sheet_data, mapping_data


def test_alert_render_assigns_prefixed_unique_plotly_keys_and_memoizes(monkeypatch):
    fake_st = _FakeStreamlit()
    monkeypatch.setattr(yield_dashboard, "st", fake_st)
    trend_chart_calls = []

    def _trend_chart(*args, **kwargs):
        trend_chart_calls.append(1)
        return go.Figure()

    monkeypatch.setattr(yield_dashboard, "create_code_trend_chart", _trend_chart)
    monkeypatch.setattr(
        yield_dashboard, "create_mapping_heatmap", lambda *a, **k: go.Figure()
    )
    monkeypatch.setattr(
        yield_dashboard, "create_lot_defect_chart", lambda *a, **k: go.Figure()
    )
    monkeypatch.setattr(
        yield_dashboard, "create_sheet_defect_chart", lambda *a, **k: go.Figure()
    )

    mwd_code_data, lot_data, sheet_data, mapping_data = _full_render_inputs()
    call_args = dict(
        trend_records=[
            {"level": "code", "defect_group": "GROUP-A", "defect_desc": "CODE-A"}
        ],
        lot_oos_records=[{"异常 Code": "CODE-B"}],
        warning_lines=None,
        mwd_code_data=mwd_code_data,
        lot_data=lot_data,
        sheet_data=sheet_data,
        mapping_data=mapping_data,
        hotspot_scripts=[],
        product_code="TEST",
    )

    yield_dashboard.render_alert_code_expanders(**call_args)

    # 首个 expander 为外层预警图像区，其后为每个命中 Code 的子折叠面板
    assert fake_st.expander_labels[0] == "🚨 自动预警缺陷图像（2 个 Code）"
    assert len(fake_st.expander_labels) == 3
    keys = [key for key in fake_st.plotly_keys if key]
    assert keys, "预警区应渲染至少一张图表"
    assert all("yield_alert" in key for key in keys)
    assert len(keys) == len(set(keys))

    # 第二次同数据 rerun：memo 命中，不重复构建图表，仅复用 payload 重新渲染
    build_count = len(trend_chart_calls)
    yield_dashboard.render_alert_code_expanders(**call_args)
    assert len(trend_chart_calls) == build_count

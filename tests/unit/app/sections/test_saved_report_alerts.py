"""Report displays consume published rows even when new production fails."""

from contextlib import nullcontext
from datetime import date, datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from app.sections.equipment_domain import parts_alerts
from app.sections.indicator_domain.ijp import alert_center as ijp_alerts
from app.sections.iqc_domain.eva_materials import evaporation_dashboard as iqc
from app.sections.yield_domain import published_alert_center as yield_alerts


class UI:
    def __init__(self):
        self.errors = []
        self.warnings = []
        self.tables = []
        self.column_config = SimpleNamespace(NumberColumn=lambda **kwargs: None)

    def expander(self, *args, **kwargs):
        return nullcontext()

    def error(self, value):
        self.errors.append(value)

    def warning(self, value):
        self.warnings.append(value)

    def info(self, value):
        pass

    def success(self, value):
        pass

    def caption(self, value):
        pass

    def dataframe(self, value, **kwargs):
        self.tables.append(value)


def saved(frame):
    return {'state': 'alert', 'alerts': frame, 'records': frame,
            'message': '', 'alert_factories': []}


def test_parts_failed_publication_still_consumes_saved_result(monkeypatch):
    ui = UI()
    committed = pd.DataFrame([{'备件类型': 'saved target'}])
    seen = []
    monkeypatch.setattr(parts_alerts, 'st', ui)
    monkeypatch.setattr(parts_alerts, 'publish_parts_alerts', lambda *args, **kwargs: (_ for _ in ()).throw(OSError()))
    monkeypatch.setattr(parts_alerts, 'read_alert_result', lambda *args: seen.append(args) or saved(committed))
    displayed = []
    monkeypatch.setattr(parts_alerts, 'render_parts_alert_center', lambda frame, **kwargs: displayed.append(frame))

    parts_alerts.render_parts_alerts(pd.DataFrame(), store_factory=lambda: object(),
                                   as_of_date='2026-10-27', last_update='')

    assert seen == [('parts_oos', '', date(2026, 10, 27))]
    pd.testing.assert_frame_equal(displayed[0], committed)
    assert ui.warnings


@pytest.mark.parametrize('region,row', [('AA区', 'ijp_aa_oos'), ('孔区', 'ijp_hole_oos')])
def test_ijp_consumer_can_run_without_calling_the_producer(monkeypatch, region, row):
    ui = UI()
    committed = pd.DataFrame([{'region': region, 'productcode': 'P', 'line': 'L',
                               'printer': 'SAVED', 'rs_code': 'C3DM1', 'ratio': .5, 'minimum': .9}])
    monkeypatch.setattr(ijp_alerts, 'st', ui)
    calls = []
    monkeypatch.setattr(ijp_alerts, 'read_alert_result', lambda *args: calls.append(args) or saved(committed))
    monkeypatch.setattr(ijp_alerts, 'get_ijp_code_descriptions', lambda region: {})
    service = SimpleNamespace(get_reporting_window=lambda: (datetime(2026, 9, 1), datetime(2026, 10, 27)))
    # No get_alert_report method: a consumer must not ask for analysis.
    ijp_alerts.render_ijp_alert_center(service, region, produce=False)
    assert calls == [(row, '', date(2026, 10, 27))]
    assert ui.tables[0]['Printer'].tolist() == ['SAVED']


def test_iqc_failed_source_still_consumes_saved_result(monkeypatch):
    ui = UI()
    committed = pd.DataFrame([{'物料号': 'saved-material'}])
    monkeypatch.setattr(iqc, 'st', ui)
    monkeypatch.setattr(iqc, '_load_report', lambda *args: (_ for _ in ()).throw(RuntimeError()))
    calls = []
    monkeypatch.setattr(iqc, 'read_alert_result', lambda *args: calls.append(args) or saved(committed))
    displayed = []
    monkeypatch.setattr(iqc, '_render_report_table', lambda frame, **kwargs: displayed.append(frame))
    iqc._render_automatic_alerts(None, today=date(2026, 10, 27), enabled=('P',), work_orders=(), alert_store=None)
    assert calls == [('iqc_evaporation_ng', '', date(2026, 10, 27))]
    pd.testing.assert_frame_equal(displayed[0], committed)
    assert ui.warnings


def test_yield_saved_center_needs_neither_report_nor_chart_data(monkeypatch):
    ui = UI()
    monkeypatch.setattr(yield_alerts, 'st', ui)
    lot = pd.DataFrame([{'超规 Lot ID': 'SAVED', '异常 Code': 'C1'}])
    trend = pd.DataFrame([{'level': 'code', 'defect_group': 'G', 'defect_desc': 'C2',
                           'time_period': '2026-W43', 'curr_rate': .2, 'prev_rate': .1,
                           'rules': 'rise', 'period_scope': 'weekly', 'is_current': True}])
    calls = []
    monkeypatch.setattr(yield_alerts, 'read_alert_result', lambda row, product, reference, **selection:
                        calls.append((row, product, reference, selection)) or saved(lot if row == 'yield_lot_oos' else trend))
    result = yield_alerts.render_saved_yield_alerts('P', date(2026, 10, 27))
    assert [call[0] for call in calls] == ['yield_lot_oos', 'yield_trend_fluctuation']
    assert calls[0][3] == {} and calls[1][3] == {}
    assert result['lot_records'][0]['超规 Lot ID'] == 'SAVED'
    assert 'is_current' not in ui.tables[1]
    assert ui.tables[1]['缺陷名称'].tolist() == ['C2']


def test_yield_saved_center_selects_complete_previous_iso_week_across_year(monkeypatch, tmp_path):
    from datetime import timedelta, timezone
    from src.yield_domain import composition
    from src.yield_domain.application.alert_results import YieldAlertResults
    from src.yield_domain.infrastructure.alert_result_workbooks import YieldAlertWorkbookStore
    from src.shared_kernel.utils.excel_tools import replace_workbook_sheets

    day = date(2027, 1, 4)
    now = datetime(2027, 1, 4, 10, tzinfo=timezone(timedelta(hours=8)))
    publisher = YieldAlertResults(
        YieldAlertWorkbookStore({'lot': tmp_path / 'lots.xlsx', 'trend': tmp_path / 'trends.xlsx'}),
        policy_signature=lambda product: 'fixture', ttl_seconds=43200, clock=lambda: now,
    )
    rows = [{'超规 Lot ID': identifier, '异常 Code': 'D1', '管控规格线': '1.00%',
             '实际不良率': '2.00%', '不良panel数': 2, '入库时间': value,
             '阵列投入时间': '2026/12/01'} for identifier, value in (
                 ('older', '2026/12/27'), ('monday', '2026/12/28'),
                 ('sunday', '2027/01/03'), ('current', '2027/01/04'))]
    for kind, records in (('lot', rows), ('trend', [])):
        publisher.publish('P', kind, records, reference_date=day,
                          coverage_start='2026-12-01', coverage_end=str(day), has_data=True,
                          health={'status': 'fresh', 'refreshed_at': now.isoformat()},
                          calculation_signature='fixture')
    path = tmp_path / 'lots.xlsx'
    flags = pd.read_excel(path, sheet_name='P__flags').assign(flag=False)
    assert replace_workbook_sheets(path, {'P__flags': flags}).written
    monkeypatch.setattr(composition, 'build_yield_alert_results', lambda: publisher)
    ui = UI()
    monkeypatch.setattr(yield_alerts, 'st', ui)
    output = yield_alerts.render_saved_yield_alerts('P', day)
    assert [record['超规 Lot ID'] for record in output['lot_records']] == ['monday', 'sunday']
    assert output['trend_records'] == []
    assert ui.errors == ['检测到 2 条预警，请关注。']

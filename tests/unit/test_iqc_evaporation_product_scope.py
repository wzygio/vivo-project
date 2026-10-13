"""Product scope is a material membership union using selected product codes."""
from datetime import date

import pandas as pd
import pytest

from src.iqc_domain.application.eva_materials.evaporation import EvaporationReportService
from src.iqc_domain.core.eva_materials.evaporation import REPORT_COLUMNS
from tests.unit.test_iqc_evaporation import source_frame


class ProductSource:
    def __init__(self):
        self.scopes = []

    def read_report(self, start, end, products):
        self.scopes.append(products)
        return source_frame().assign(prod_code='、'.join(products))


@pytest.mark.parametrize('selected,expected', [
    ((), ('M678', 'M626')),
    (('M626',), ('M626',)),
    (('M626', 'M678', 'M626'), ('M678', 'M626')),
    (('DISABLED', 'M678'), ('M678',)),
])
def test_scope_is_enabled_intersection_in_config_order_and_keeps_projected_products(selected, expected):
    source = ProductSource()
    service = EvaporationReportService(source, enabled_products=('M678', 'M626'))
    report = service.get_report(date(2026, 8, 1), date(2026, 8, 31), selected)
    assert source.scopes == [expected]
    assert report['产品型号'].tolist() == ['、'.join(expected)]
    assert '蒸镀批次' not in report
    assert report['IQC结果'].tolist() == ['OK']


@pytest.mark.parametrize('enabled,selected', [((), ()), (('M678',), ('DISABLED',))])
def test_empty_effective_scope_returns_public_empty_schema_without_reading(enabled, selected):
    source = ProductSource()
    service = EvaporationReportService(source, enabled_products=enabled)
    result = service.get_report(date(2026, 8, 1), date(2026, 8, 31), selected)
    assert not source.scopes
    assert result.empty and tuple(result.columns) == REPORT_COLUMNS


def test_scope_configuration_is_copied_and_invalid_scalar_selection_rejected():
    source = ProductSource()
    configured = ['M678']
    service = EvaporationReportService(source, enabled_products=configured)
    configured.append('DISABLED')
    service.get_report(date(2026, 8, 1), date(2026, 8, 31))
    assert source.scopes == [('M678',)]
    with pytest.raises(ValueError, match='IQC_PRODUCT_SCOPE_INVALID'):
        service.get_report(date(2026, 8, 1), date(2026, 8, 31), 'M678')

"""Public Sheet/Lot projection over injected, already-published result readers."""
from __future__ import annotations

from collections.abc import Callable
from datetime import date
import hashlib
import json
from typing import Any

import pandas as pd

from api.schemas import AnomalyQuery, AnomalyRecord, CapabilityRecord
from src.inline_domain.core.monitor.cpk_summary import CPK_THRESHOLD


class ResultUnavailable(ValueError):
    """A source cannot satisfy the complete public contract."""


def _text(value: Any) -> str:
    return '' if value is None or pd.isna(value) else str(value).strip()


def _released(value: Any) -> bool:
    return _text(value).lower() in {'false', '0', '0.0', 'no', 'n'}


class PublishedAnomalyService:
    def __init__(self, *, source_reader: Callable, hidden: Callable, metadata: Any,
                 capability_hidden: Callable | None = None) -> None:
        self._source_reader = source_reader
        self._hidden = hidden
        self._metadata = metadata
        self._capability_hidden = capability_hidden

    def read(self, query: AnomalyQuery, factories: tuple[str, ...], reference_date: date) -> list[dict]:
        if query.product is None or query.sub_domain is None:
            raise ResultUnavailable('Source query must be resolved')
        # Deliberately precedes source access: hidden state cannot leak through
        # source failures, publication dates, counts, or pagination versions.
        capability = query.event_type in ('CPK', 'CPM')
        if capability:
            if self._capability_hidden is None:
                raise ResultUnavailable('Capability public policy unavailable')
            hidden = self._capability_hidden(query.event_type, query.product)
        else:
            hidden = self._hidden(query.sub_domain, query.product)
        if hidden:
            return []
        result = self._source_reader(query, reference_date, factories)
        if (result.get('state') == 'error'
                or result.get('metadata', {}).get('availability') == 'missing'
                or result.get('release_evidence') is False):
            raise ResultUnavailable('Published result unavailable')
        records = result.get('release_records', result.get('records'))
        if not isinstance(records, pd.DataFrame):
            raise ResultUnavailable('Published result unavailable')
        if 'flag' not in records:
            raise ResultUnavailable('Published release evidence unavailable')
        if records.empty:
            return []
        rows = records.loc[records['flag'].map(_released)].to_dict('records')
        public: list[dict] = []
        for row in rows:
            normalized = self._identity(query, row)
            # Filter known factory scope before enrichment; invalid inaccessible
            # station metadata must not change an authorized caller's result.
            if normalized['factory'] and normalized['factory'] not in factories:
                continue
            if query.entity_type and query.entity_type != normalized['entity_type']:
                continue
            if query.entity_id and query.entity_id != normalized['entity_id']:
                continue
            if query.step_id and query.step_id != normalized['step_id']:
                continue
            if query.parameter_name and query.parameter_name != normalized['parameter_name']:
                continue
            if query.sub_domain == 'Yield':
                enriched = {'factory': normalized['factory'], 'step_id': None, 'layer': None}
            else:
                enriched = self._metadata.enrich(
                    query.sub_domain, query.product, normalized['entity_id'], normalized['parameter_name'],
                    normalized['factory'], normalized['step_id'],
                )
            if enriched['factory'] not in factories:
                continue
            if query.layer and query.layer != enriched['layer']:
                continue
            record_model = CapabilityRecord if capability else AnomalyRecord
            model = record_model(
                sub_domain=query.sub_domain, product=query.product, event_type=query.event_type,
                **{**normalized, **enriched},
            )
            public.append(model.model_dump(mode='json', by_alias=True))
        # All sort keys are public. Source file signatures never become tokens.
        public.sort(key=lambda row: tuple(str(row.get(name) or '') for name in (
            '发生时间', '对象编号', '发生站点', '监控参数', '命中边界', '对象类型',
        )), reverse=True)
        return public

    @staticmethod
    def _identity(query: AnomalyQuery, row: dict) -> dict:
        if query.event_type in ('CPK', 'CPM'):
            return PublishedAnomalyService._capability_identity(query, row)
        domain = query.sub_domain
        boundary = None
        precision = 'second'
        if domain in ('SPC', 'CTQ', 'AOI_TT', 'AOI_RS'):
            entity_type = _text(row.get('chart_kind')) if domain == 'AOI_RS' else 'sheet'
            entity_id = _text(row.get('point_id' if domain == 'AOI_RS' else 'sheet_id'))
            parameter = _text(row.get({'AOI_RS': 'rs_code', 'AOI_TT': 'tt_name'}.get(domain, 'param_name')))
            stamp = row.get('start_time' if domain == 'AOI_TT' else 'sheet_start_time')
            factory, step = _text(row.get('factory')).upper(), _text(row.get('step_id'))
            boundary = _text(row.get('oos_type' if query.event_type == 'OOS' else 'ooc_type')) or None
        elif domain == 'Q-Time':
            entity_type, entity_id = 'lot', _text(row.get('lot_id'))
            parameter = _text(row.get('step_desc'))
            factory, step = _text(row.get('shop')).upper(), _text(row.get('f_step'))
            stamp = pd.to_datetime(_text(row.get('timekey'))[:14], format='%Y%m%d%H%M%S', errors='coerce')
        elif domain == 'Yield':
            entity_type, entity_id = 'lot', _text(row.get('超规 Lot ID'))
            parameter = _text(row.get('异常 Code'))
            factory, step = _text(row.get('factory')).upper(), ''
            if not factory:
                raise ResultUnavailable('Published Yield factory unavailable')
            stamp = pd.to_datetime(row.get('入库时间'), format='%Y/%m/%d', errors='coerce')
            precision = 'day'
        else:
            raise ResultUnavailable('Unknown published source')
        if not entity_id or not parameter or entity_type not in ('sheet', 'lot'):
            raise ResultUnavailable('Required published identity unavailable')
        timestamp = pd.to_datetime(stamp, errors='coerce')
        if pd.isna(timestamp):
            raise ResultUnavailable('Required event time unavailable')
        timestamp = timestamp.tz_localize('Asia/Shanghai') if timestamp.tzinfo is None else timestamp.tz_convert('Asia/Shanghai')
        # A source that only published a date is represented as a date, never a
        # fabricated exact occurrence timestamp at midnight.
        occurred_at = timestamp.date().isoformat() if precision == 'day' else timestamp.isoformat()
        return dict(entity_type=entity_type, entity_id=entity_id, parameter_name=parameter,
                    factory=factory, step_id=step or None, occurred_at=occurred_at,
                    boundary=boundary)

    @staticmethod
    def _capability_identity(query: AnomalyQuery, row: dict) -> dict:
        factory = _text(row.get('factory')).upper()
        step, parameter = _text(row.get('step_id')), _text(row.get('param_name'))
        period_type, period_label = _text(row.get('period_type')), _text(row.get('period_label'))
        if not factory or not step or not parameter:
            raise ResultUnavailable('Required capability identity unavailable')
        start = pd.to_datetime(row.get('calendar_start'), errors='coerce')
        end = pd.to_datetime(row.get('calendar_end'), errors='coerce')
        if pd.isna(start) or pd.isna(end):
            raise ResultUnavailable('Required capability period unavailable')
        # Identity contains only the visible business keys, not values or files.
        keys = (query.event_type, query.product, factory, step, parameter, period_type, period_label)
        digest = hashlib.sha256(json.dumps(keys, ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
        return dict(entity_type='capability', entity_id='capability:' + digest,
                    parameter_name=parameter, factory=factory, step_id=step,
                    occurred_at=end.date().isoformat(), boundary='LOWER',
                    metric_value=row.get(query.event_type.lower()), lower_bound=CPK_THRESHOLD,
                    period_type=period_type, period_label=period_label,
                    period_start_date=start.date(), period_end_date=end.date())

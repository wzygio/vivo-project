"""Maintain candidate rows and durable per-business-key decisions in one publication."""

from dataclasses import dataclass
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path

import pandas as pd

from src.shared_kernel.core.decoration_flags import parse_decoration_flag
from src.shared_kernel.core.decoration_revision import VERSION_SHEET, decision_revision, upload_revision
from src.shared_kernel.infrastructure.alert_workbook import SnapshotChangedError, edit_alert_workbook, workbook_bytes
from src.shared_kernel.infrastructure.file_lock import publication_lock
from src.shared_kernel.utils.excel_tools import read_uploaded_excel_sheet, read_workbook_sheets


class AlertDecisionInvalid(ValueError):
    """An actionable error for the decision-workbook editor."""


@dataclass(frozen=True)
class AlertDecisionSheet:
    sheet: str
    keys: tuple[str, ...]

    @property
    def candidates(self) -> str:
        return f'{self.sheet}候选'

    @property
    def decisions(self) -> str:
        return f'{self.sheet}决策'

    def _keys(self, frame: pd.DataFrame) -> pd.DataFrame:
        if not set(self.keys) <= set(frame):
            raise AlertDecisionInvalid('缺少用于匹配预警的业务列。')
        keys = frame.loc[:, list(self.keys)].copy()
        for name in self.keys:
            keys[name] = keys[name].map(_key_text)
        return keys

    def _ledger(self, sheets: dict, candidates: pd.DataFrame) -> pd.DataFrame:
        current = self._keys(candidates).drop_duplicates()
        if self.decisions not in sheets:
            if self.candidates in sheets:
                raise AlertDecisionInvalid('缺少已发布预警的决策工作表。')
            return current.assign(Flag=False)
        ledger = self._validate(sheets[self.decisions])
        new = ~pd.MultiIndex.from_frame(current).isin(pd.MultiIndex.from_frame(ledger[list(self.keys)]))
        return pd.concat([ledger, current.loc[new].assign(Flag=False)], ignore_index=True)

    def _validate(self, frame: pd.DataFrame) -> pd.DataFrame:
        if set(frame) != {*self.keys, 'Flag'} or not frame.columns.is_unique:
            raise AlertDecisionInvalid(f'请保留“{self.decisions}”工作表及下载的完整表头。')
        keys = self._keys(frame)
        if keys.duplicated().any():
            raise AlertDecisionInvalid('同一业务记录不能填写多行决定。')
        try:
            return keys.assign(Flag=frame['Flag'].map(parse_decoration_flag))
        except ValueError as exc:
            raise AlertDecisionInvalid('Flag 须填写 True、False 或 Delete。') from exc

    def released(self, candidates: pd.DataFrame, ledger: pd.DataFrame) -> pd.DataFrame:
        index = pd.MultiIndex.from_frame(self._keys(candidates))
        flags = ledger.set_index(list(self.keys))['Flag'].reindex(index)
        if flags.isna().any():
            raise AlertDecisionInvalid('候选记录缺少对应的预警决定。')
        return candidates.loc[flags.eq(False).to_numpy()].reset_index(drop=True)

    def publish(self, sheets: dict, candidates: pd.DataFrame) -> pd.DataFrame:
        ledger = self._ledger(sheets, candidates)
        public = self.released(candidates, ledger)
        sheets[self.candidates] = candidates
        sheets[self.decisions] = ledger
        sheets[self.sheet] = public
        return public

    def read(self, sheets: dict, meta: pd.Series, *, verify_publication: bool = False) -> pd.DataFrame:
        """Display reflects direct decision edits; inspection also checks the committed count."""
        published = sheets[self.sheet]
        if int(meta.alert_count) != len(published):
            raise ValueError('ALERT_PUBLICATION_COUNT_MISMATCH')
        old_format = pd.isna(meta.get('candidate_count')) or meta.get('candidate_count') == ''
        if self.candidates not in sheets and self.decisions not in sheets and old_format:
            return published
        candidates = sheets[self.candidates]
        if list(candidates) != list(published) or len(candidates) != int(meta.candidate_count):
            raise ValueError('ALERT_CANDIDATE_SCOPE_INVALID')
        ledger = self._validate(sheets[self.decisions])
        public = self.released(candidates, ledger)
        if verify_publication and len(public) != len(published):
            raise ValueError('ALERT_DECISION_COUNT_MISMATCH')
        return public

    def download(self, path: Path) -> tuple[bytes, str]:
        with publication_lock(path):
            if path.exists():
                version = sha256(path.read_bytes()).hexdigest()
                sheets = read_workbook_sheets(path, preserve_text=True)
            else:
                version, sheets = '', {}
            candidates = sheets.get(self.candidates, sheets.get(self.sheet, pd.DataFrame(columns=self.keys)))
            ledger = self._ledger(sheets, candidates)
            revision = decision_revision(self.sheet, ledger, self.keys, controls=('Flag',))
            return workbook_bytes({
                self.decisions: ledger, self.candidates: candidates,
                VERSION_SHEET: pd.DataFrame({'revision': [revision]}),
            }), version

    def apply_upload(self, path: Path, payload: memoryview | bytes, version: str) -> int:
        updates = self._validate(read_uploaded_excel_sheet(payload, sheet_name=self.decisions))
        revision = upload_revision(read_uploaded_excel_sheet(payload, sheet_name=VERSION_SHEET))
        with edit_alert_workbook(path, self.sheet, expected_digest=version, preserve_text=True) as sheets:
            if self.sheet not in sheets or '__meta__' not in sheets:
                raise AlertDecisionInvalid('请先生成预警结果后再维护决定。')
            candidates = sheets.get(self.candidates, sheets[self.sheet])
            ledger = self._ledger(sheets, candidates)
            current = decision_revision(self.sheet, ledger, self.keys, controls=('Flag',))
            if revision != current:
                raise SnapshotChangedError('决定版本已变化或缺失，请重新下载最新台账。')
            ledger = ledger.set_index(list(self.keys))
            flags = updates.set_index(list(self.keys))['Flag']
            if not flags.index.isin(ledger.index).all():
                raise AlertDecisionInvalid('上传包含台账中不存在的业务记录，请下载最新台账。')
            ledger['Flag'] = ledger['Flag'].astype(object)
            ledger.loc[flags.index, 'Flag'] = flags
            ledger = ledger.reset_index()
            public = self.released(candidates, ledger)
            sheets[self.candidates], sheets[self.decisions], sheets[self.sheet] = candidates, ledger, public
            metadata = sheets['__meta__']
            selected = metadata.region.eq(self.sheet) if 'region' in metadata else pd.Series(True, index=metadata.index)
            if selected.sum() != 1:
                raise AlertDecisionInvalid('请先生成本区域预警结果后再维护决定。')
            metadata.loc[selected, ['alert_count', 'candidate_count']] = [len(public), len(candidates)]
        return len(updates)


def _key_text(value: object) -> str:
    if pd.isna(value):
        return ''
    if isinstance(value, (datetime, date)):
        stamp = pd.Timestamp(value).round('ms')
        return str(stamp.date()) if stamp == stamp.normalize() else str(stamp)
    return str(value).strip()

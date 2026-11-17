"""Read-only source for the fixed 1A200-to-21100 TrackIn interval.

Only source preparation lives here. The returned frame enters the existing
Q-Time snapshot, monitoring, decision and display pipelines unchanged.
"""

from __future__ import annotations

from datetime import datetime
import logging
from typing import Any

import pandas as pd
from sqlalchemy import bindparam, text

from src.indicator_domain.application.qtime.dtos import QTimeStepOption
from src.indicator_domain.application.qtime.errors import QTimeDataAccessError


logger = logging.getLogger(__name__)
SOURCE_VERSION = "array-transfer-trackin-v1"
MAX_EVENT_ROWS = 100_000
MAX_MASTER_ROWS = 10_000
DETAIL_COLUMNS = [
    "step_desc", "lot_id", "prod_qty", "sub_prod_type", "f_step", "t_step",
    "q_spec", "wait_time", "timekey", "shop", "prodcode",
]
SAFE_ERROR = "Q-Time 数据读取失败，请稍后重试。"
_IDENTITY = ["prod_id", "prod_ver", "factory"]
_MASTER_IDENTITY = ["productspecname", "productspecversion", "factoryname"]


class ArrayTransferQTimeSource:
    """Supplement the standard Lot facts without requiring a Q-Time master row."""

    def __init__(self, engine: Any, *, enabled_products: tuple[str, ...]) -> None:
        self._engine = engine
        self._enabled_products = tuple(dict.fromkeys(enabled_products))

    @property
    def step_option(self) -> QTimeStepOption:
        return QTimeStepOption("1A200->21100", "1A200", "21100")

    def source_signature(self) -> tuple[object, ...]:
        return SOURCE_VERSION, self._enabled_products

    def read(self, start_time: pd.Timestamp, end_time: pd.Timestamp) -> pd.DataFrame:
        """Read the bounded source window; key completed rows by start TrackIn."""
        if not self._enabled_products:
            return pd.DataFrame(columns=DETAIL_COLUMNS)
        try:
            start, end = pd.Timestamp(start_time), pd.Timestamp(end_time)
            if pd.isna(start) or pd.isna(end) or start >= end:
                raise ValueError("Invalid fixed-path source window")
            if self._engine is None:
                raise RuntimeError("Database engine unavailable")
            with self._engine.connect() as connection:
                with connection.begin():
                    connection.exec_driver_sql("SET TRANSACTION READ ONLY")
                    connection.exec_driver_sql("SET LOCAL statement_timeout = '20s'")
                    events = self._read_events(connection, start, end)
                    starts = self._completed_starts(events, start, end)
                    if starts.empty:
                        return pd.DataFrame(columns=DETAIL_COLUMNS)
                    names = starts["prod_id"].drop_duplicates().tolist()
                    master = self._read_master(connection, names)
            mapped = self._map_products(starts, master)
            return self._standard_details(mapped)
        except Exception as exc:
            logger.error("Q-Time fixed-path source unavailable: cause=%s: %s",
                         type(exc).__name__, exc, exc_info=True)
            raise QTimeDataAccessError(SAFE_ERROR) from exc

    @staticmethod
    def _read_events(connection: Any, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
        statement = text("""
            SELECT lot_id, oper_code, event_time, event_timekey,
                   prod_id, prod_ver, factory, prod_qty
            FROM mdw.dwr_mes_lot_hist
            WHERE event_time >= :start_time AND event_time < :end_time
              AND event_name = :event_name AND oper_code IN :stations
            LIMIT :row_limit
        """).bindparams(bindparam("stations", expanding=True))
        events = pd.read_sql(statement, connection, params={
            "start_time": start, "end_time": end, "event_name": "TrackIn",
            "stations": ("1A200", "21100"), "row_limit": MAX_EVENT_ROWS + 1,
        })
        if len(events) > MAX_EVENT_ROWS:
            raise ValueError("Fixed-path event source exceeds bounded capacity")
        return events

    @staticmethod
    def _read_master(connection: Any, names: list[str]) -> pd.DataFrame:
        statement = text("""
            SELECT DISTINCT productspecname, productspecversion,
                            productcode, factoryname
            FROM mdw.dwr_mes_productspec
            WHERE productspecname IN :specs AND createtime <= :as_of
            LIMIT :row_limit
        """).bindparams(bindparam("specs", expanding=True))
        master = pd.read_sql(statement, connection, params={
            "specs": names, "as_of": datetime.now(), "row_limit": MAX_MASTER_ROWS + 1,
        })
        if len(master) > MAX_MASTER_ROWS:
            raise ValueError("Fixed-path product mapping exceeds bounded capacity")
        return master

    @staticmethod
    def _completed_starts(
        events: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp,
    ) -> pd.DataFrame:
        if events.empty:
            return events.copy()
        frame = events.copy()
        frame["event_time"] = pd.to_datetime(frame["event_time"], errors="coerce")
        if frame["event_time"].isna().any():
            raise ValueError("Invalid fixed-path event timestamp")
        frame = frame.loc[frame["event_time"].ge(start) & frame["event_time"].lt(end)].copy()
        for column in ["lot_id", "oper_code", *_IDENTITY]:
            frame[column] = frame[column].astype("string").str.strip()
        if frame["lot_id"].isna().any() or frame["lot_id"].eq("").any():
            raise ValueError("Missing fixed-path Lot identity")
        # CPT chooses the earliest target for each Lot/start, without introducing
        # a next-start boundary or restricting target reuse between start records.
        targets = (
            frame.loc[frame["oper_code"].eq("21100")]
            .groupby("lot_id", as_index=False)["event_time"].min()
            .rename(columns={"event_time": "to_time"})
        )
        starts = frame.loc[frame["oper_code"].eq("1A200")].copy()
        starts = starts.sort_values(["event_time", "event_timekey"], kind="stable")
        starts = starts.drop_duplicates(["lot_id", "prod_id", "event_time"])
        completed = starts.merge(targets, on="lot_id", how="inner", validate="many_to_one")
        if completed["to_time"].lt(completed["event_time"]).any():
            raise ValueError("Negative fixed-path duration under CPT source rule")
        if completed[_IDENTITY].isna().any().any() or completed[_IDENTITY].eq("").any().any():
            raise ValueError("Missing fixed-path product mapping identity")
        return completed

    def _map_products(self, starts: pd.DataFrame, master: pd.DataFrame) -> pd.DataFrame:
        mapping = master.copy()
        for column in [*_MASTER_IDENTITY, "productcode"]:
            mapping[column] = mapping[column].astype("string").str.strip()
        mapping = mapping.drop_duplicates([*_MASTER_IDENTITY, "productcode"])
        identities = starts[_IDENTITY].drop_duplicates().rename(
            columns=dict(zip(_IDENTITY, _MASTER_IDENTITY)),
        )
        mapping = mapping.merge(identities, on=_MASTER_IDENTITY, how="inner", validate="many_to_one")
        if mapping.duplicated(_MASTER_IDENTITY, keep=False).any():
            raise ValueError("Ambiguous fixed-path product mapping")
        mapped = starts.merge(
            mapping, left_on=_IDENTITY, right_on=_MASTER_IDENTITY,
            how="left", validate="many_to_one",
        )
        if mapped["productcode"].isna().any() or mapped["productcode"].eq("").any():
            raise ValueError("Missing fixed-path product mapping")
        return mapped.loc[mapped["productcode"].isin(self._enabled_products)].copy()

    def _standard_details(self, mapped: pd.DataFrame) -> pd.DataFrame:
        result = pd.DataFrame(index=mapped.index)
        result["step_desc"] = self.step_option.step_desc
        result["lot_id"] = mapped["lot_id"]
        result["prod_qty"] = pd.to_numeric(mapped["prod_qty"], errors="coerce")
        result["sub_prod_type"] = None
        result["f_step"], result["t_step"] = "1A200", "21100"
        result["q_spec"] = None
        result["wait_time"] = (mapped["to_time"] - mapped["event_time"]).dt.total_seconds() / 3600
        # Preserve source precision until the normal repository normalization.
        result["timekey"] = mapped["event_time"].dt.strftime("%Y%m%d%H%M%S%f")
        result["shop"] = "ARRAY"
        result["prodcode"] = mapped["productcode"]
        keys = ["prodcode", "step_desc", "lot_id"]
        if result.assign(_seconds=result["timekey"].str[:14]).duplicated([*keys, "_seconds"]).any():
            raise ValueError("Fixed-path rows collide with the existing second-level decision key")
        return result.reindex(columns=DETAIL_COLUMNS).reset_index(drop=True)

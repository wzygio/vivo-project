"""WMS material facts; supplied FineReport joins with bound date parameters."""

import logging
from datetime import date

import pandas as pd
from sqlalchemy import text

from src.iqc_domain.application.eva_materials.evaporation import IqcSourceUnavailable, validate_dates
from src.shared_kernel.data_forward import DataForwardPolicy
from src.shared_kernel.infrastructure.db_handler import DatabaseManager
from src.shared_kernel.report_cutoff import ReportCutoffPolicy

logger = logging.getLogger(__name__)

# Select only the page's business fields. In particular, no report URLs, internal
# flags, remarks or unshifted timestamps cross the repository output boundary.
REPORT_SQL = """
SELECT Q.EATTRIBUTE10 AS prod_code, Q.EATTRIBUTE15 AS materialtype,
       Q.REQUESTDATE AS requestdate, Q.CHECKEDDATE AS checkeddate,
       Q.MITEMNAME AS mitemname, U.MITEMDESC AS mitemdesc,
       S.EATTRIBUTE1 AS check_user, S.EATTRIBUTE12 AS mater_type,
       Q.EATTRIBUTE5 AS factory, M.CHA_ITEM AS cha_item,
       T.SPEC_REQ1 AS spec_req1, T.LOW_SPEC AS low_spec,
       T.SPEC_REQ2 AS spec_req2, T.UPP_SPEC AS upp_spec,
       T.IQC_1 AS iqc_1, T.IQC_2 AS iqc_2, T.IQC_3 AS iqc_3,
       T.IQC_4 AS iqc_4, T.IQC_5 AS iqc_5,
       T.COA_1 AS coa_1, T.COA_2 AS coa_2, T.COA_3 AS coa_3,
       T.COA_4 AS coa_4, T.COA_5 AS coa_5,
       T.IQC_RESULT AS iqc_result, T.COA_RESULT AS coa_result
FROM mdw.dwr_wms_tblqcticket Q
LEFT JOIN mdw.dwr_wms_tblmitem_b U
       ON U.MITEMNAME = Q.MITEMNAME AND U.ORGID = 5000
LEFT JOIN mdw.dwr_wms_tblqcticketsample S
       ON S.TICKETNO = Q.TICKETNO AND S.LOTNO = Q.LOTNO
      AND S.ITEMNAME = Q.MITEMNAME
      AND S.EATTRIBUTE12 IN (SELECT DISTINCT SETCODE FROM mdw.dwr_wms_tblmitemset)
LEFT JOIN (SELECT DISTINCT MAT_TYPE, CHA_ITEM, TO_COMMENT FROM mdw.imp_iqc_mat_info) M
       ON S.EATTRIBUTE12 = M.MAT_TYPE
      AND (S.EATTRIBUTE12 <> '有机'
           OR U.MITEMDESC LIKE '%' || COALESCE(M.TO_COMMENT, '') || '%')
LEFT JOIN mdw.imp_iqc_mat_info_tbtj T
       ON S.EATTRIBUTE12 = T.MAT_TYPE AND Q.TICKETNO = T.TICKETNO
      AND M.CHA_ITEM = T.CHA_ITEM
WHERE Q.ORGID = 5000 AND Q.TICKETTYPE LIKE '%IQC%'
  AND Q.CHECKEDDATE >= :start_time AND Q.CHECKEDDATE < :end_time
  AND Q.CHECKEDSAMPLEQTY IS NOT NULL
  AND (Q.EATTRIBUTE1 NOT LIKE '%免检%' OR Q.EATTRIBUTE1 IS NULL)
  AND (Q.EATTRIBUTE10 <> '/' OR Q.EATTRIBUTE10 IS NULL)
  AND (Q.EATTRIBUTE12 <> '清除统计' OR Q.EATTRIBUTE12 IS NULL)
  AND S.CHECKMODE NOT LIKE '%免检%'
  AND Q.EATTRIBUTE5 = 'V3'
  AND S.EATTRIBUTE12 = '有机'
"""


class EvaporationRepository:
    def __init__(
        self, db_manager: DatabaseManager,
        forward_policy: DataForwardPolicy, cutoff_policy: ReportCutoffPolicy,
    ) -> None:
        self._db_manager = db_manager
        self._forward = forward_policy
        self._cutoff = cutoff_policy

    def read_report(self, start: date, end: date) -> pd.DataFrame:
        validate_dates(start, end)
        display_start = pd.Timestamp(start)
        display_end = pd.Timestamp(end) + pd.Timedelta(days=1)
        source_start, source_end = self._forward.to_source_window(display_start, display_end)
        try:
            engine = self._db_manager.engine
            if engine is None:
                raise IqcSourceUnavailable("IQC_SOURCE_UNAVAILABLE")
            with engine.connect() as connection, connection.begin():
                connection.execute(text("SET TRANSACTION READ ONLY"))
                connection.execute(text("SET LOCAL statement_timeout = '60s'"))
                frame = pd.read_sql_query(text(REPORT_SQL), connection, params={
                    "start_time": source_start.to_pydatetime(),
                    "end_time": source_end.to_pydatetime(),
                })
            frame.columns = frame.columns.str.lower()
            for column in ("requestdate", "checkeddate"):
                frame[column] = pd.to_datetime(frame[column], errors="raise")
            display = self._forward.shift_frame(frame, ("requestdate", "checkeddate"))
            display = self._cutoff.filter_frame(display, "checkeddate")
            return display.loc[
                display.checkeddate.ge(display_start) & display.checkeddate.lt(display_end)
            ].copy()
        except Exception as exc:
            # Database exception strings can contain connection and SQL details.
            logger.error("IQC_SOURCE_UNAVAILABLE exception_type=%s", type(exc).__name__)
            raise IqcSourceUnavailable("IQC_SOURCE_UNAVAILABLE") from None

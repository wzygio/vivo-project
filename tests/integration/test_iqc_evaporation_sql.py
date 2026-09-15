"""Execute the report SELECT on a local relational fixture (no production writes)."""

import sqlite3

import pandas as pd

from src.iqc_domain.infrastructure.evaporation_repository import REPORT_SQL


def test_catalog_expands_unmeasured_items_and_preserves_result_source():
    with sqlite3.connect(":memory:") as connection:
        connection.execute("ATTACH DATABASE ':memory:' AS mdw")
        connection.executescript("""
        CREATE TABLE mdw.dwr_wms_tblqcticket (
            eattribute10 TEXT, eattribute15 TEXT, requestdate TEXT, checkeddate TEXT,
            mitemname TEXT, eattribute5 TEXT, ticketno TEXT, lotno TEXT, orgid INTEGER,
            tickettype TEXT, checkedsampleqty INTEGER, eattribute1 TEXT, eattribute12 TEXT);
        INSERT INTO mdw.dwr_wms_tblqcticket VALUES
            ('通用','量产','2026-08-20','2026-08-27','100','V3','T1','L1',5000,'IQC',1,NULL,NULL);
        CREATE TABLE mdw.dwr_wms_tblmitem_b (mitemname TEXT, orgid INTEGER, mitemdesc TEXT);
        INSERT INTO mdw.dwr_wms_tblmitem_b VALUES ('100',5000,'有机材料_RH1080');
        CREATE TABLE mdw.dwr_wms_tblqcticketsample (
            ticketno TEXT, lotno TEXT, itemname TEXT, eattribute1 TEXT,
            eattribute12 TEXT, checkmode TEXT);
        INSERT INTO mdw.dwr_wms_tblqcticketsample VALUES ('T1','L1','100','检验员','有机','常规检验');
        CREATE TABLE mdw.dwr_wms_tblmitemset (setcode TEXT);
        INSERT INTO mdw.dwr_wms_tblmitemset VALUES ('有机');
        CREATE TABLE mdw.imp_iqc_mat_info (mat_type TEXT, cha_item TEXT, to_comment TEXT);
        INSERT INTO mdw.imp_iqc_mat_info VALUES
            ('有机','HPLC-A','RH1080'), ('有机','HPLC-A','RH1080'),
            ('有机','未测项目','RH1080'), ('有机','其它物料项目','OTHER');
        CREATE TABLE mdw.imp_iqc_mat_info_tbtj (
            mat_type TEXT, ticketno TEXT, cha_item TEXT,
            spec_req1 TEXT, low_spec REAL, spec_req2 TEXT, upp_spec REAL,
            iqc_1 REAL, iqc_2 REAL, iqc_3 REAL, iqc_4 REAL, iqc_5 REAL,
            coa_1 REAL, coa_2 REAL, coa_3 REAL, coa_4 REAL, coa_5 REAL,
            iqc_result TEXT, coa_result TEXT);
        INSERT INTO mdw.imp_iqc_mat_info_tbtj VALUES
            ('有机','T1','HPLC-A','>=',62,'<=',64,63,NULL,NULL,NULL,NULL,62,NULL,NULL,NULL,NULL,'NG',NULL);
        """)
        params = {"start_time": "2026-08-01", "end_time": "2026-09-01"}
        frame = pd.read_sql_query(REPORT_SQL, connection, params=params)
        assert len(frame) == 2
        measured = frame.loc[frame.cha_item.eq("HPLC-A")].iloc[0]
        assert measured.iqc_result == "NG"  # Stored decision, even with an in-spec value.
        assert pd.isna(measured.coa_result)
        assert frame.loc[frame.cha_item.eq("未测项目"), "iqc_1"].isna().all()
        # Exemption and cleanup flags are SQL exclusions, not UI hiding.
        for column, value in (("eattribute1", "免检"), ("eattribute12", "清除统计"), ("eattribute10", "/")):
            connection.execute(f"UPDATE mdw.dwr_wms_tblqcticket SET {column}=?", (value,))
            assert pd.read_sql_query(REPORT_SQL, connection, params=params).empty
            connection.execute(f"UPDATE mdw.dwr_wms_tblqcticket SET {column}=NULL")
        assert pd.read_sql_query(REPORT_SQL, connection, params={
            "start_time": "2026-08-01", "end_time": "2026-08-27",
        }).empty
        for mode in ('免检', None):
            connection.execute('UPDATE mdw.dwr_wms_tblqcticketsample SET checkmode=?', (mode,))
            assert pd.read_sql_query(REPORT_SQL, connection, params=params).empty
        connection.execute("UPDATE mdw.dwr_wms_tblqcticketsample SET checkmode='常规检验'")
        connection.execute("UPDATE mdw.dwr_wms_tblqcticketsample SET eattribute12='MASK'")
        connection.execute("INSERT INTO mdw.dwr_wms_tblmitemset VALUES ('MASK')")
        assert pd.read_sql_query(REPORT_SQL, connection, params=params).empty
        connection.execute("UPDATE mdw.dwr_wms_tblqcticketsample SET eattribute12='有机'")
        connection.execute('DELETE FROM mdw.dwr_wms_tblmitemset')
        assert pd.read_sql_query(REPORT_SQL, connection, params=params).empty

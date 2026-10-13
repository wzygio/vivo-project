"""Execute the complete bridge against many-to-many relational source fixtures."""
from datetime import datetime
import re
import sqlite3

from sqlalchemy import create_engine

from src.iqc_domain.infrastructure.eva_materials.product_scope import material_product_statement


def bridge_connection():
    connection = sqlite3.connect(':memory:')
    connection.create_function('substring', 2, lambda value, regex: (
        match.group(0) if value and (match := re.search(regex, value)) else None
    ))
    connection.create_function('to_date', 2, lambda value, fmt: (
        datetime.strptime(value, '%Y/%m/%d').strftime('%Y-%m-%d') if value else None
    ))
    connection.create_function('to_char', 2, lambda value, fmt: value.replace('-', '') if value else None)
    connection.execute("ATTACH DATABASE ':memory:' AS mdw")
    connection.executescript("""
        CREATE TABLE mdw.dwr_mes_ct_cruciblelothistory (
            planorganicspec TEXT, cycletime TEXT, cruciblelotname TEXT, eventname TEXT, machinename TEXT);
        INSERT INTO mdw.dwr_mes_ct_cruciblelothistory VALUES
            ('A','20260801','C1','KitBatchCrucible','M1'),
            ('A','20260801','C1','KitBatchCrucible','M1'),
            ('A','20260803','C1','KitBatchCrucible','M1'),
            ('B','20260801','C2','KitBatchCrucible','M1'),
            ('DISABLED-MATERIAL','20260802','C3','KitBatchCrucible','M1'),
            ('BAD-MACHINE','20260801','C1','KitBatchCrucible','3CEE03'),
            ('BAD-EVENT','20260801','C1','OtherEvent','M1'),
            ('NO-HISTORY','20260801','NO','KitBatchCrucible','M1'),
            ('NULL-CYCLE',NULL,'C1','KitBatchCrucible','M1');
        CREATE TABLE mdw.dwr_mes_consumablehistory (cruciblelotname TEXT, eventname TEXT);
        INSERT INTO mdw.dwr_mes_consumablehistory VALUES
            ('C1','KitBatchCrucible'),('C1','KitBatchCrucible'),
            ('C2','KitBatchCrucible'),('C3','KitBatchCrucible');
        CREATE TABLE mdw.dwr_mes_productrequest (
            productrequestname TEXT, description TEXT, productspecname TEXT, subproductiontype TEXT);
        INSERT INTO mdw.dwr_mes_productrequest VALUES
            ('R1','2026/8/1 production','S1','P'),
            ('R2','2026/08/01 production','S2','P'),
            ('R3','2026/08/02 production','S3','P'),
            ('R4','2026/08/03 production','S1','P'),
            ('NO-BATCH',NULL,'S1','P');
        CREATE TABLE mdw.dwr_mes_productspec (productspecname TEXT, productcode TEXT);
        INSERT INTO mdw.dwr_mes_productspec VALUES ('S1','M678'),('S2','M626'),('S3','DISABLED');
    """)
    return connection


def test_shared_cycle_many_products_many_materials_returns_distinct_memberships():
    engine = create_engine('sqlite://', creator=bridge_connection)
    try:
        with engine.connect() as connection:
            params = {'materials': ('A','B','DISABLED-MATERIAL','BAD-MACHINE','BAD-EVENT','NO-HISTORY','NULL-CYCLE'),
                      'products': ('M678','M626'), 'work_orders': ('P',)}
            rows = connection.execute(material_product_statement(), params).all()
            assert set(rows) == {('A','M678','20260801'),('A','M678','20260803'),('A','M626','20260801'),('B','M678','20260801'),('B','M626','20260801')}
            assert len(rows) == 5
            params['products'] = ('M626',)
            assert set(connection.execute(material_product_statement(), params).all()) == {('A','M626','20260801'),('B','M626','20260801')}
            params['work_orders'] = ('NOT-ALLOWED',)
            assert not connection.execute(material_product_statement(), params).all()
    finally:
        engine.dispose()

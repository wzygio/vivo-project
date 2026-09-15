"""Synthetic SQL facts used by integration and browser tests; no production data."""

from sqlalchemy import create_engine, event, text
from sqlalchemy.pool import StaticPool


def build_hole_engine():
    engine = create_engine('sqlite://', poolclass=StaticPool,
                           connect_args={'check_same_thread': False})

    @event.listens_for(engine, 'connect')
    def register_split_part(connection, _record):
        def split_part(value, separator, index):
            if value is None:
                return None
            parts = value.split(separator)
            return parts[index - 1] if len(parts) >= index else ''
        connection.create_function('split_part', 3, split_part)

    with engine.begin() as c:
        for sql in (
            "ATTACH DATABASE ':memory:' AS eda",
            'CREATE TABLE eda.oled_ijp_defect_t (shop TEXT, step_id TEXT, glass_id TEXT, '
            'img_dir TEXT, image_name TEXT, code TEXT, update_time TEXT)',
            'CREATE TABLE eda.oled_chamber_hst_t (cut_id TEXT, cut_start_time TEXT, '
            'product_id TEXT, equip_id TEXT, sub_equip_id TEXT, item5 TEXT)',
            'CREATE TABLE dwr_mes_productspec (productspecname TEXT, productcode TEXT)',
            'CREATE TABLE dwr_mes_productrequest_v (sub_prod_id TEXT, sub_prod_type TEXT)',
            'CREATE TABLE eda.dwd_glass_oled_cycle_v3 '
            '(glass_id TEXT, pici TEXT, prod_code TEXT, event_time TEXT)',
            "INSERT INTO dwr_mes_productspec VALUES ('S1','M626'),('S2','DISABLED')",
            "INSERT INTO dwr_mes_productrequest_v VALUES ('R1','MP'),('R1','MP')",
            "INSERT INTO eda.dwd_glass_oled_cycle_v3 VALUES "
            "('G1','LOT1','M626','2026-09-01 08:00:00'),"
            "('G1','LOT1','M626','2026-09-01 09:00:00'),"
            "('G2','LOT2','M626','2026-09-02 08:00:00')",
        ):
            c.execute(text(sql))
        for glass, cut, spec, printer in (
            ('G1', '2026-09-01 07:00:00', 'S1', '3CEE01-IK2-PR1'),
            ('G1', '2026-09-01 07:01:00', 'S1', '3CEE01-IK2-PR1'),
            ('G2', '2026-07-28 07:00:00', 'S1', '3CEE01-IK2-PR2'),
            ('G3', '2026-09-01 07:00:00', 'S2', '3CEE02-IK2-PR1'),
            ('G4', '2026-09-01 07:00:00', 'S1', 'OTHER'),
        ):
            c.execute(text('INSERT INTO eda.oled_chamber_hst_t VALUES '
                           '(:g,:t,:s,:e,:p,\'R1\')'),
                      dict(g=glass, t=cut, s=spec, e=printer[:6], p=printer))
        rows = []
        for glass in ('G1', 'G2', 'G3', 'G4'):
            for index, code in enumerate(('C3RA1', 'C3RA2', 'C3RA2', 'C3RA3')):
                rows.append(dict(g=glass, code=code, step='21200',
                                 image='ABCDEFGHIJKLMNHT0_' + str(index),
                                 t='2026-09-01 08:00:00'))
        # Small holes, unclassified paths, other step/code and duplicate source row.
        for digit, code, step in [('5','C3RA1','21200'), ('9','C3RA2','21200'),
                                  ('x','C3RA1','21200'), ('0','C3RA1','OTHER'),
                                  ('0','C3DM1','21200')]:
            rows.append(dict(g='G1', code=code, step=step,
                             image='ABCDEFGHIJKLMNHT' + digit, t='2026-09-01 08:00:00'))
        rows.append(rows[0].copy())
        c.execute(text("INSERT INTO eda.oled_ijp_defect_t VALUES "
                       "('OLED',:step,:g,'xxxA/B/C/D/E/F/G/H/I',:image,:code,:t)"), rows)
    return engine

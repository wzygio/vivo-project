"""SQLite ATTACH contract tests for the IJP overflow repository SQL."""

from datetime import datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text

from src.indicator_domain.application.ijp.dtos import IjpQuery
from src.indicator_domain.application.ijp.errors import IjpDataAccessError
from src.indicator_domain.infrastructure.ijp.repository import IjpRepository

START = datetime(2026, 8, 31, 7, 0)
END = datetime(2026, 9, 1, 7, 0)


def _image(suffix: str, panel: str = "L3N464E03182CA") -> str:
    return (
        "C/VIEW/2W4A9/3CTV01/L3N4/64/E03/SOURCE/L3N464E03182.IMG/"
        f"{panel}{suffix}_2W400_68_PT_20260803_030008_FVG_C3DM1_RS.JPG"
    )


def _build_engine():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(text("ATTACH DATABASE ':memory:' AS eda"))
        connection.execute(
            text(
                "CREATE TABLE eda.spot_eda_oled_view_dft_v ("
                "glass_start_time TEXT, glass_id TEXT, product_spec TEXT, "
                "rs_code TEXT, rs_defect_image_name TEXT)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE dwr_mes_productspec ("
                "productspecname TEXT, productcode TEXT)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE eda.oled_chamber_hst_t ("
                "cut_id TEXT, cut_start_time TEXT, sub_equip_id TEXT, item5 TEXT)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE dwr_mes_productrequest_v ("
                "sub_prod_id TEXT, sub_prod_type TEXT, factory TEXT)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE eda.dwd_glass_oled_cycle_v3 ("
                "glass_id TEXT, pici TEXT, cycle_id TEXT, prod_code TEXT, "
                "event_time TEXT)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE dwr_mes_productspec_v ("
                "prod_id TEXT, product_code TEXT, factory TEXT)"
            )
        )

        # 缺陷明细：G1 三行（2×C3DM1 后缀 B0，1×C3RA1 后缀 HT1），G2 一行（C3DM2 LT），
        # 另含白名单外设备、白名单外 CODE、窗口外与闭区间边界的对照行。
        connection.execute(
            text(
                "INSERT INTO eda.spot_eda_oled_view_dft_v VALUES "
                f"('2026-08-27 08:00:00','G1','SPEC1','C3DM1','{_image('B0')}'),"
                f"('2026-08-27 08:05:00','G1','SPEC1','C3DM1','{_image('B0')}'),"
                f"('2026-08-27 08:10:00','G1','SPEC1','C3RA1','{_image('HT1')}'),"
                f"('2026-08-27 09:00:00','G2','SPEC2','C3DM2','{_image('LT')}'),"
                f"('2026-08-27 10:00:00','G3','SPEC1','C3DM1','{_image('T0')}'),"
                f"('2026-08-27 11:00:00','G4','SPEC1','X9XX9','{_image('T0')}'),"
                f"('2026-08-25 08:00:00','G5','SPEC1','C3DM1','{_image('R2')}'),"
                f"('2026-08-27 07:00:00','G6','SPEC1','C3DM3','{_image('L3')}'),"
                f"('2026-08-28 07:00:00','G7','SPEC1','C3DM4','{_image('RB')}')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO dwr_mes_productspec VALUES "
                "('SPEC1','M626'),('SPEC2','M678')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO eda.oled_chamber_hst_t VALUES "
                "('G1','2026-08-27 07:30:00','3CEE01-IK2-PR1','REQ1'),"
                "('G2','2026-08-27 08:30:00','3CEE02-IK2-PR2','REQ2'),"
                "('G3','2026-08-27 09:30:00','9XXXX9-XX9-XX9','REQ1'),"
                "('G4','2026-08-27 10:30:00','3CEE01-IK2-PR1','REQ1'),"
                "('G5','2026-08-25 07:30:00','3CEE01-IK2-PR2','REQ1'),"
                "('G6','2026-08-27 07:00:00','3CEE04-IKT-PRT','REQ1'),"
                "('G7','2026-08-28 06:30:00','3CEE04-IKT-PRT','REQ1')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO dwr_mes_productrequest_v VALUES "
                "('REQ1','P','OLED'),('REQ2','E','OLED'),('REQ3','X','ARRAY'),"
                "('REQ4',NULL,'OLED')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO eda.dwd_glass_oled_cycle_v3 VALUES "
                "('G1','LOT1','CYC1','M626','2026-08-27 12:00:00'),"
                "('G2','LOT2','CYC2','M678','2026-08-27 13:00:00'),"
                "('G9','LOT9','CYC9','M626','2026-08-01 00:00:00'),"
                "('G8','LOT8','CYC8','M626','NaT')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO dwr_mes_productspec_v VALUES "
                "('PROD-A','M626','OLED'),('PROD-B','M678','OLED'),"
                "('PROD-C','Z517','ARRAY')"
            )
        )
    return engine


def _repository(engine) -> IjpRepository:
    return IjpRepository(SimpleNamespace(engine=engine))


def _query(**overrides) -> IjpQuery:
    return IjpQuery(start_time=START, end_time=END, **overrides)


def test_fetch_details_applies_whitelists_closed_interval_and_ratio_contract() -> None:
    repository = _repository(_build_engine())

    details = repository.fetch_details(_query())

    # G3 白名单外设备、G4 白名单外 CODE、G5 窗口前 7 天内但明细不扩窗 → 均排除；
    # G6/G7 命中闭区间边界 → 保留；G1 的两行 B0 各展开一行 BOTTOM0。
    assert list(details.columns) == [
        "print_time",
        "productcode",
        "glass_id",
        "printer",
        "panel_id",
        "image_url",
        "panel_location",
        "rs_code",
        "code_ratio",
    ]
    assert sorted(details["glass_id"].unique()) == ["G1", "G2", "G6", "G7"]

    g1 = details[details["glass_id"] == "G1"]
    assert len(g1) == 5  # 2×BOTTOM + 2×BOTTOM0 展开 + 1×KONGTOP
    assert sorted(g1["panel_location"].tolist()) == [
        "BOTTOM", "BOTTOM", "BOTTOM0", "BOTTOM0", "KONGTOP",
    ]
    assert g1["code_ratio"].tolist() == [0.667] * 4 + [0.333]

    row = details.iloc[0]
    assert row["panel_id"] == _image("B0")[56:70]
    assert row["image_url"].startswith("http://10.73.17.41/IMG_WEB/V3/C/VIEW/")
    g2 = details[details["glass_id"] == "G2"]
    assert g2["panel_location"].tolist() == ["LEFTTOP"]
    assert g2["code_ratio"].tolist() == [1.0]


def test_fetch_details_honours_every_optional_filter() -> None:
    repository = _repository(_build_engine())

    assert set(repository.fetch_details(_query(lines=("3CEE02",)))["glass_id"]) == {"G2"}
    assert set(repository.fetch_details(_query(equipments=("3CEE04-IKT-PRT",)))["glass_id"]) == {"G6", "G7"}
    assert set(repository.fetch_details(_query(codes=("C3RA1",)))["rs_code"]) == {"C3RA1"}
    assert set(repository.fetch_details(_query(glass_ids=("G2",)))["glass_id"]) == {"G2"}
    assert set(repository.fetch_details(_query(product_names=("SPEC2",)))["glass_id"]) == {"G2"}
    assert set(repository.fetch_details(_query(product_codes=("M678",)))["glass_id"]) == {"G2"}
    assert set(repository.fetch_details(_query(sub_prod_types=("E",)))["glass_id"]) == {"G2"}
    assert set(repository.fetch_details(_query(picis=("LOT2",)))["glass_id"]) == {"G2"}
    assert set(repository.fetch_details(_query(cycles=("CYC2",)))["glass_id"]) == {"G2"}

    bottom = repository.fetch_details(_query(panel_locations=("BOTTOM",)))
    assert sorted(bottom["panel_location"].unique()) == ["BOTTOM", "BOTTOM0"]
    assert set(bottom["glass_id"]) == {"G1"}
    kong = repository.fetch_details(_query(panel_locations=("LEFTTOP",)))
    assert set(kong["glass_id"]) == {"G2"}


def test_fetch_details_limits_the_result_and_reports_truncation_size() -> None:
    repository = _repository(_build_engine())

    details = repository.fetch_details(_query(detail_limit=3))

    assert len(details) >= 3  # SQL LIMIT 作用于原始行，BOTTOM 展开可能略多
    assert len(details) <= 6


def test_fetch_daily_ratios_expands_the_start_by_seven_days() -> None:
    repository = _repository(_build_engine())

    ratios = repository.fetch_daily_ratios(_query(codes=("C3DM1",)))

    assert list(ratios.columns) == ["day", "rs_code", "code_num", "ratio"]
    # G5（2026-08-29）落在扩窗 7 天内 → 出现在 By天 聚合但不在明细中
    assert set(ratios["day"]) == {"2026-08-29", "2026-08-31"}
    by_day = ratios.set_index("day")
    assert by_day.loc["2026-08-29", "code_num"] == 1
    assert by_day.loc["2026-08-29", "ratio"] == 1.0
    assert by_day.loc["2026-08-31", "code_num"] == 2
    assert by_day.loc["2026-08-31", "ratio"] == 1.0


def test_filter_option_queries_follow_the_finereport_datasets() -> None:
    repository = _repository(_build_engine())

    assert repository.list_product_codes() == ("M626", "M678")
    assert repository.list_product_names(()) == ("PROD-A", "PROD-B")
    assert repository.list_product_names(("M678",)) == ("PROD-B",)
    assert repository.list_sub_prod_types() == ("E", "P")
    assert repository.list_picis(START, END, ()) == ("LOT1", "LOT2")
    assert repository.list_picis(START, END, ("M678",)) == ("LOT2",)
    assert repository.list_cycles(START, END, (), ()) == ("CYC1", "CYC2")
    assert repository.list_cycles(START, END, (), ("LOT1",)) == ("CYC1",)


def test_database_failures_are_exposed_as_a_safe_domain_error() -> None:
    repository = _repository(SimpleNamespace(engine=None))

    with pytest.raises(IjpDataAccessError) as caught:
        repository.fetch_details(_query())

    assert str(caught.value) == "IJP 溢流数据读取失败，请联系系统管理员确认数据库权限。"

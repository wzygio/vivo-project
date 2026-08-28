# tests/unit/test_modifier_table.py
"""入库良率修饰表管理器的行为测试。"""
import pandas as pd
import pytest

from src.yield_domain.core.mwd_trend.modifier_table import parse_rate_value


class TestParseRateValue:
    """指定良损/当月良损 的数值解析：兼容百分比字符串与 >1 防呆。"""

    def test_percent_string_is_converted_to_decimal(self):
        assert parse_rate_value("1.03%") == pytest.approx(0.0103)

    def test_decimal_passes_through(self):
        assert parse_rate_value(0.0103) == pytest.approx(0.0103)

    def test_value_above_one_is_treated_as_percent(self):
        # 业务人员手滑输入 1.5（没带 %），强制按 1.5% 处理
        assert parse_rate_value(1.5) == pytest.approx(0.015)

    @pytest.mark.parametrize("raw", [None, "", "nan", float("nan")])
    def test_empty_values_return_none(self, raw):
        assert parse_rate_value(raw) is None


from pathlib import Path

from src.yield_domain.core.mwd_trend.modifier_table import (
    MODIFIER_TABLE_COLUMNS,
    read_modifier_table,
)


def _write_table(path: Path, sheet_name: str, rows: list[dict]):
    df = pd.DataFrame(rows, columns=MODIFIER_TABLE_COLUMNS)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)


def _row(defect, month, raw_loss=None, specified=None):
    return {
        "不良类型": defect,
        "周期类型": "月度",
        "时间标签": month,
        "当月良损": raw_loss,
        "指定良损": specified,
        "缩放倍数": None,
    }


class TestReadModifierTable:
    """读取修饰表：按 <产品>_Group级 / <产品>_Code级 分 Sheet。"""

    def test_reads_group_and_code_sheets(self, tmp_path):
        path = tmp_path / "modifier.xlsx"
        _write_table(path, "M999_Group级", [_row("Array_Line", "2026-07", 0.01)])
        # 同文件追加 Code 级 sheet
        code_df = pd.DataFrame([_row("B暗点", "2026-07", 0.001)], columns=MODIFIER_TABLE_COLUMNS)
        with pd.ExcelWriter(path, engine="openpyxl", mode="a") as writer:
            code_df.to_excel(writer, index=False, sheet_name="M999_Code级")

        table = read_modifier_table(path, "M999")

        assert set(table) == {"group", "code"}
        assert table["group"].iloc[0]["不良类型"] == "Array_Line"
        assert table["code"].iloc[0]["不良类型"] == "B暗点"

    def test_missing_file_returns_empty_frames(self, tmp_path):
        table = read_modifier_table(tmp_path / "not_exists.xlsx", "M999")
        assert table["group"].empty and table["code"].empty
        assert list(table["group"].columns) == MODIFIER_TABLE_COLUMNS

    def test_missing_sheet_returns_empty_without_starting_com(
        self,
        tmp_path,
        monkeypatch,
    ):
        path = tmp_path / "modifier.xlsx"
        _write_table(path, "M999_Group级", [_row("Array_Line", "2026-07", 0.01)])
        com_calls = 0

        def fail_if_com_starts(*args, **kwargs):
            nonlocal com_calls
            com_calls += 1
            raise AssertionError("标准工作簿缺 Sheet 不应启动 COM")

        monkeypatch.setattr(
            "src.shared_kernel.utils.excel_tools._read_encrypted_xlsx_via_com",
            fail_if_com_starts,
        )

        table = read_modifier_table(path, "M999")

        assert not table["group"].empty
        assert table["code"].empty
        assert com_calls == 0

    def test_unreadable_workbook_is_not_silently_treated_as_empty(
        self, tmp_path, monkeypatch
    ):
        path = tmp_path / "modifier.xlsx"
        path.write_bytes(b"broken")

        monkeypatch.setattr(
            "src.yield_domain.core.mwd_trend.modifier_table._read_sheet",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                ValueError("workbook format is unreadable")
            ),
        )

        with pytest.raises(ValueError, match="format is unreadable"):
            read_modifier_table(path, "M999")


from src.yield_domain.core.mwd_trend.modifier_table import (
    compute_current_month_loss,
    compute_current_month_losses,
)


def _panel_rows():
    # 2026-07 月：panel P1/P2/P3 投入；P1 有两个 Code 不良，P2 有一个
    return pd.DataFrame(
        {
            "panel_id": ["P1", "P1", "P2", "P4"],
            "defect_group": ["Array_Line", "OLED_Mura", "Array_Line", "Array_Line"],
            "defect_desc": ["G向单亮线", "HBM亮点", "G向单暗线", "G向单亮线"],
            "warehousing_time": ["20260701", "20260702", "20260715", "20260801"],
        }
    )


class TestComputeCurrentMonthLoss:
    """当月原始良损 = 当月不良 Panel 去重数 / 当月投入 Panel 去重数。"""

    def test_code_level_loss_for_target_month(self):
        loss = compute_current_month_loss(_panel_rows(), level="code", month="2026-07")
        # 投入 = {P1,P2} = 2；G向单亮线 {P1}、HBM亮点 {P1}、G向单暗线 {P2}
        assert loss["G向单亮线"] == pytest.approx(0.5)
        assert loss["HBM亮点"] == pytest.approx(0.5)
        assert loss["G向单暗线"] == pytest.approx(0.5)
        # 若误把 2026-08 的 P4 算入，G向单亮线 会变成 2/3 而非 1/2

    def test_group_level_loss(self):
        loss = compute_current_month_loss(_panel_rows(), level="group", month="2026-07")
        # Array_Line {P1,P2} = 2/2；OLED_Mura {P1} = 1/2
        assert loss["Array_Line"] == pytest.approx(1.0)
        assert loss["OLED_Mura"] == pytest.approx(0.5)

    def test_month_without_data_returns_empty(self):
        loss = compute_current_month_loss(_panel_rows(), level="code", month="2026-06")
        assert loss.empty

    def test_both_levels_share_one_date_parse(self, monkeypatch):
        """Group/Code 当月良损必须复用同一份日期解析和当月切片。"""
        import src.yield_domain.core.mwd_trend.modifier_table as module

        real_to_datetime = pd.to_datetime
        calls = 0

        def counting_to_datetime(*args, **kwargs):
            nonlocal calls
            calls += 1
            return real_to_datetime(*args, **kwargs)

        monkeypatch.setattr(module.pd, "to_datetime", counting_to_datetime)

        losses = compute_current_month_losses(_panel_rows(), month="2026-07")

        assert calls == 1
        assert losses["group"]["Array_Line"] == pytest.approx(1.0)
        assert losses["code"]["G向单亮线"] == pytest.approx(0.5)


from src.yield_domain.core.mwd_trend.modifier_table import (
    ModifierTableValidationError,
    compute_scale_factors,
    resolve_monthly_targets,
)


class TestResolveMonthlyTargets:
    """目标良损回退链：当月指定 → 最近上月指定 → 当月原始 → 无目标。"""

    def test_specified_current_month_wins(self):
        df = pd.DataFrame(
            [_row("B暗点", "2026-07", raw_loss=0.01, specified=0.02)],
            columns=MODIFIER_TABLE_COLUMNS,
        )
        targets = resolve_monthly_targets(df, ["2026-07"])
        assert targets["B暗点"]["2026-07"] == pytest.approx(0.02)

    def test_fallback_to_nearest_previous_specified_month(self):
        df = pd.DataFrame(
            [
                _row("B暗点", "2026-05", raw_loss=0.01, specified=0.02),
                _row("B暗点", "2026-06", raw_loss=0.011),
                _row("B暗点", "2026-07", raw_loss=0.012),
            ],
            columns=MODIFIER_TABLE_COLUMNS,
        )
        targets = resolve_monthly_targets(df, ["2026-06", "2026-07"])
        assert targets["B暗点"]["2026-06"] == pytest.approx(0.02)
        assert targets["B暗点"]["2026-07"] == pytest.approx(0.02)

    def test_fallback_to_raw_loss_when_never_specified(self):
        df = pd.DataFrame(
            [_row("B暗点", "2026-07", raw_loss=0.013)],
            columns=MODIFIER_TABLE_COLUMNS,
        )
        targets = resolve_monthly_targets(df, ["2026-07"])
        assert targets["B暗点"]["2026-07"] == pytest.approx(0.013)

    def test_defect_without_any_rows_is_absent(self):
        df = pd.DataFrame(
            [_row("B暗点", "2026-07", raw_loss=0.013)],
            columns=MODIFIER_TABLE_COLUMNS,
        )
        targets = resolve_monthly_targets(df, ["2026-07"])
        assert "HBM亮点" not in targets

    def test_month_without_row_and_no_specified_yields_no_modifier_target(self):
        # 表中无该月行且从未指定 → 不给修饰目标，由日度生成器使用原始月度良损。
        df = pd.DataFrame(
            [_row("B暗点", "2026-06", raw_loss=0.013)],
            columns=MODIFIER_TABLE_COLUMNS,
        )
        targets = resolve_monthly_targets(df, ["2026-07"])
        assert "2026-07" not in targets["B暗点"]


def test_read_modifier_table_rejects_negative_rate_with_row_context(tmp_path):
    path = tmp_path / "modifier.xlsx"
    _write_table(
        path,
        "M999_Code级",
        [_row("B暗点", "2026-07", raw_loss=0.1, specified=-0.1)],
    )

    with pytest.raises(ModifierTableValidationError) as exc_info:
        read_modifier_table(path, "M999")

    message = str(exc_info.value)
    assert "M999" in message
    assert "M999_Code级" in message
    assert "B暗点" in message
    assert "2026-07" in message
    assert "-0.1" in message


class TestComputeScaleFactors:
    """缩放倍数 = round(指定良损 / 当月良损, 3)；异常口径记 1.0。"""

    def test_factor_rounded_to_three_decimals(self):
        df = pd.DataFrame(
            [_row("B暗点", "2026-07", raw_loss=0.013, specified=0.02)],
            columns=MODIFIER_TABLE_COLUMNS,
        )
        factors = compute_scale_factors(df)
        assert factors[("B暗点", "2026-07")] == pytest.approx(round(0.02 / 0.013, 3))

    def test_unspecified_rows_have_factor_one(self):
        df = pd.DataFrame(
            [_row("B暗点", "2026-07", raw_loss=0.013)],
            columns=MODIFIER_TABLE_COLUMNS,
        )
        factors = compute_scale_factors(df)
        assert factors[("B暗点", "2026-07")] == 1.0

    def test_zero_raw_loss_with_specified_falls_back_to_one(self):
        df = pd.DataFrame(
            [_row("B暗点", "2026-07", raw_loss=0.0, specified=0.02)],
            columns=MODIFIER_TABLE_COLUMNS,
        )
        factors = compute_scale_factors(df)
        assert factors[("B暗点", "2026-07")] == 1.0

    def test_fallback_month_uses_previous_specified_over_current_raw(self):
        # 当月未指定但上月指定过：倍数 = 上月指定 / 当月原始（与趋势的回退口径一致）
        df = pd.DataFrame(
            [
                _row("B暗点", "2026-06", raw_loss=0.01, specified=0.02),
                _row("B暗点", "2026-07", raw_loss=0.008),
            ],
            columns=MODIFIER_TABLE_COLUMNS,
        )
        factors = compute_scale_factors(df)
        assert factors[("B暗点", "2026-07")] == pytest.approx(round(0.02 / 0.008, 3))


from src.yield_domain.core.mwd_trend.modifier_table import (
    specified_signature,
    sync_modifier_table,
)


class TestSyncModifierTable:
    """写回 orchestration：更新当月良损，并仅在持久化成功后推进签名。"""

    @pytest.fixture
    def captured_writes(self, monkeypatch):
        writes = {}

        def fake_replace(path, sheet_name, df):
            writes[sheet_name] = df.copy()
            return True

        monkeypatch.setattr(
            "src.yield_domain.core.mwd_trend.modifier_table.replace_workbook_sheet",
            fake_replace,
        )
        return writes

    def _run_sync(self, tmp_path, table_rows, sheet="M999_Code级", month="2026-07"):
        path = tmp_path / "modifier.xlsx"
        if table_rows is not None:
            _write_table(path, sheet, table_rows)
        return sync_modifier_table(
            path,
            "M999",
            _panel_rows(),
            current_month=month,
            signature_path=tmp_path / "sig.json",
        )

    def test_first_sync_appends_current_month_and_writes_signature(
        self, tmp_path, captured_writes
    ):
        table = self._run_sync(tmp_path, None)  # 文件不存在 → 从零建表

        written = captured_writes["M999_Code级"]
        july = written[written["时间标签"] == "2026-07"]
        # 当月三个 Code 都应追加，良损 = 0.5
        assert set(july["不良类型"]) == {"G向单亮线", "HBM亮点", "G向单暗线"}
        assert sorted(july["当月良损"].tolist()) == pytest.approx([0.5, 0.5, 0.5])
        assert (july["缩放倍数"] == 1.0).all()  # 未指定 → 1.0
        assert (tmp_path / "sig.json").exists()
        assert not table["code"].empty

    def test_existing_specified_value_is_preserved_and_factor_computed(
        self, tmp_path, captured_writes
    ):
        rows = [_row("G向单亮线", "2026-07", raw_loss=0.4, specified=0.8)]
        self._run_sync(tmp_path, rows)

        written = captured_writes["M999_Code级"]
        line = written[written["不良类型"] == "G向单亮线"].iloc[0]
        assert line["指定良损"] == 0.8  # 用户指定不被覆盖
        assert line["当月良损"] == pytest.approx(0.5)  # 当月良损被刷新
        assert line["缩放倍数"] == pytest.approx(round(0.8 / 0.5, 3))

    def test_no_rewrite_when_nothing_changed(self, tmp_path, captured_writes):
        rows = [_row("G向单亮线", "2026-07", raw_loss=0.5, specified=0.8)]
        # 先补全其余 Code 行，使表与计算结果完全一致
        rows += [
            _row("HBM亮点", "2026-07", raw_loss=0.5),
            _row("G向单暗线", "2026-07", raw_loss=0.5),
        ]
        path = tmp_path / "modifier.xlsx"
        _write_table(path, "M999_Code级", rows)
        # 预置相同签名
        sig = specified_signature(
            read_modifier_table(path, "M999")["code"]
        )
        (tmp_path / "sig.json").write_text(
            __import__("json").dumps({"M999:code": sig, "M999:group": ""}), encoding="utf-8"
        )

        sync_modifier_table(
            path, "M999", _panel_rows(),
            current_month="2026-07", signature_path=tmp_path / "sig.json",
        )
        assert "M999_Code级" not in captured_writes  # 内容无变化 → 不写回

    def test_specified_edit_triggers_rewrite(self, tmp_path, captured_writes):
        rows = [_row("G向单亮线", "2026-07", raw_loss=0.5, specified=0.8)]
        path = tmp_path / "modifier.xlsx"
        _write_table(path, "M999_Code级", rows)
        # 预置"旧"签名（与当前指定不符）
        (tmp_path / "sig.json").write_text(
            __import__("json").dumps({"M999:code": "stale", "M999:group": ""}), encoding="utf-8"
        )

        sync_modifier_table(
            path, "M999", _panel_rows(),
            current_month="2026-07", signature_path=tmp_path / "sig.json",
        )
        assert "M999_Code级" in captured_writes

    def test_write_failure_is_tolerated(self, tmp_path, monkeypatch):
        def boom(path, sheet_name, df):
            return False

        monkeypatch.setattr(
            "src.yield_domain.core.mwd_trend.modifier_table.replace_workbook_sheet",
            boom,
        )
        signature_path = tmp_path / "sig.json"
        signature_path.write_text(
            __import__("json").dumps(
                {"M999:code": "stale-code", "M999:group": "stale-group"}
            ),
            encoding="utf-8",
        )

        # 不抛异常，仍返回内存中的更新表；失败 sheet 的签名保留旧值以便重试。
        table = self._run_sync(tmp_path, None)
        assert not table["code"].empty
        stored = __import__("json").loads(signature_path.read_text(encoding="utf-8"))
        assert stored == {"M999:code": "stale-code", "M999:group": "stale-group"}

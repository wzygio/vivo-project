from __future__ import annotations

from os import utime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from src.inline_domain.infrastructure.shared import outlier_filter_rules
from src.inline_domain.infrastructure.shared.outlier_filter_rules import (
    OutlierFilterConfigurationError,
    apply_outlier_filter_rules,
    load_outlier_filter_rules,
)


def _measurements() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"step_id": "100", "param_name": "PARAM_A", "param_value": -3.0},
            {"step_id": "100", "param_name": "PARAM_B", "param_value": 8.0},
            {"step_id": "200", "param_name": "PARAM_A", "param_value": 5.0},
        ]
    )


def test_step_only_rule_removes_every_parameter_at_that_step() -> None:
    rules = pd.DataFrame(
        [
            {
                "prod_col": "ALL",
                "step_col": "100",
                "param_col": "",
                "lower_col": "",
                "upper_col": "",
            }
        ]
    )

    result = apply_outlier_filter_rules(_measurements(), "M678", rules)

    assert result[["step_id", "param_name"]].to_dict("records") == [
        {"step_id": "200", "param_name": "PARAM_A"}
    ]


def test_parameter_rule_keeps_existing_inclusive_boundary_semantics() -> None:
    rules = pd.DataFrame(
        [
            {
                "prod_col": "M678",
                "step_col": "100.0",
                "param_col": "PARAM_A",
                "lower_col": "0",
                "upper_col": "0",
            }
        ]
    )

    result = apply_outlier_filter_rules(_measurements(), "M678", rules)

    assert result["param_name"].tolist() == ["PARAM_B", "PARAM_A"]


def test_offset_uses_product_scope_and_combines_matching_rules() -> None:
    measurements = _measurements()
    common_rule = {"step_col": "100", "param_col": "PARAM_A", "lower_col": "-4"}
    rules = pd.DataFrame([
        {**common_rule, "prod_code": " all ", "offset": "2.5"},
        {**common_rule, "prod_code": " m678 ", "prod_col": "OTHER", "offset": "-1"},
        {**common_rule, "prod_code": "Z622", "offset": "1"},
    ])

    m678 = apply_outlier_filter_rules(measurements, "M678", rules)
    z622 = apply_outlier_filter_rules(measurements, "Z622", rules)

    assert m678["param_value"].tolist() == [-1.5, 8.0, 5.0]
    assert z622["param_value"].tolist() == [0.5, 8.0, 5.0]
    assert measurements["param_value"].tolist() == [-3.0, 8.0, 5.0]


@pytest.mark.parametrize("offset", [-50, 0])
def test_workbook_offset_without_bounds_preserves_rows_and_raw_input(tmp_path, offset) -> None:
    source = tmp_path / "rules.xlsx"
    pd.DataFrame([{
        "prod_code": "Z622", "step_col": "11140", "param_col": "SE_L2T",
        "lower_col": "", "upper_col": "", "offset": offset,
    }]).to_excel(source, index=False)
    rules = load_outlier_filter_rules(source, tmp_path)
    points = pd.DataFrame({
        "step_id": ["11140", "11140", "10140"],
        "param_name": ["SE_L2T", "OTHER", "SE_L2T"],
        "param_value": [110, 120, 130],
    })
    expected = points.assign(param_value=[110 + offset, 120, 130])

    for _ in range(2):
        actual = apply_outlier_filter_rules(points, "Z622", rules)
        pd.testing.assert_frame_equal(actual, expected, check_dtype=False)
    pd.testing.assert_frame_equal(apply_outlier_filter_rules(points, "M626", rules), points)
    assert points["param_value"].tolist() == [110, 120, 130]


@pytest.mark.parametrize(("lower", "offset"), [("", ""), ("invalid", "-50")])
def test_missing_bounds_require_an_explicit_offset_and_no_malformed_bound(lower, offset) -> None:
    rules = pd.DataFrame([{
        "step_col": "100", "param_col": "PARAM_A", "lower_col": lower, "offset": offset,
    }])
    with pytest.raises(OutlierFilterConfigurationError):
        apply_outlier_filter_rules(_measurements(), "Z622", rules)


@pytest.mark.parametrize("offset", ["invalid", "inf"])
def test_invalid_nonempty_offset_fails_fast(offset: str) -> None:
    rules = pd.DataFrame([
        {"step_col": "100", "param_col": "PARAM_A", "lower_col": "-4", "offset": offset},
    ])

    with pytest.raises(OutlierFilterConfigurationError, match="offset"):
        apply_outlier_filter_rules(_measurements(), "M678", rules)


def test_rule_input_signature_changes_when_workbook_is_saved(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    source = tmp_path / "rules.xlsx"
    source.touch()
    monkeypatch.setattr(outlier_filter_rules, "outlier_filter_workbook_path", lambda: source)
    initial_signature = outlier_filter_rules.outlier_filter_input_signature()

    stat = source.stat()
    utime(source, ns=(stat.st_atime_ns, stat.st_mtime_ns + 2_000_000_000))

    assert outlier_filter_rules.outlier_filter_input_signature() != initial_signature


def test_encrypted_workbook_is_decrypted_before_excel_read(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = tmp_path / "rules.xlsx"
    source.write_bytes(b"encrypted")
    decrypted = tmp_path / "decrypted" / "rules.xlsx"
    calls: list[tuple[str, Path]] = []

    monkeypatch.setattr(outlier_filter_rules, "is_encrypted_file", lambda path: True)

    def fake_decrypt_file(path: Path, *, output_dir: Path):
        calls.append(("decrypt", Path(path)))
        decrypted.parent.mkdir(parents=True)
        decrypted.write_bytes(b"standard-xlsx")
        return SimpleNamespace(output_path=decrypted)

    def fake_read_excel(path: Path, **kwargs) -> pd.DataFrame:
        calls.append(("read", Path(path)))
        return pd.DataFrame(
            columns=["prod_col", "step_col", "param_col", "lower_col", "upper_col"]
        )

    monkeypatch.setattr(outlier_filter_rules, "decrypt_file", fake_decrypt_file)
    monkeypatch.setattr(outlier_filter_rules.pd, "read_excel", fake_read_excel)

    load_outlier_filter_rules(source, tmp_path / "decrypted")

    assert calls == [("decrypt", source), ("read", decrypted)]
    assert not decrypted.exists()


def test_decryption_failure_is_not_silently_ignored(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = tmp_path / "rules.xlsx"
    source.write_bytes(b"encrypted")
    monkeypatch.setattr(outlier_filter_rules, "is_encrypted_file", lambda path: True)
    monkeypatch.setattr(
        outlier_filter_rules,
        "decrypt_file",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("COM failed")),
    )

    with pytest.raises(OutlierFilterConfigurationError, match="无法加载异常值过滤配置"):
        load_outlier_filter_rules(source, tmp_path / "decrypted")


def test_invalid_rule_schema_is_not_silently_ignored(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    source = tmp_path / "rules.xlsx"
    source.write_bytes(b"PK")
    monkeypatch.setattr(outlier_filter_rules, "is_encrypted_file", lambda path: False)
    monkeypatch.setattr(
        outlier_filter_rules.pd,
        "read_excel",
        lambda *args, **kwargs: pd.DataFrame({"step_col": ["100"]}),
    )

    with pytest.raises(OutlierFilterConfigurationError, match="缺少必需列"):
        load_outlier_filter_rules(source, tmp_path / "decrypted")


def test_parameter_rule_without_a_numeric_boundary_fails_fast() -> None:
    rules = pd.DataFrame(
        [
            {
                "prod_col": "M678",
                "step_col": "100",
                "param_col": "PARAM_A",
                "lower_col": "",
                "upper_col": "not-a-number",
            }
        ]
    )

    with pytest.raises(OutlierFilterConfigurationError, match="至少配置一个有效数值边界"):
        apply_outlier_filter_rules(_measurements(), "M678", rules)

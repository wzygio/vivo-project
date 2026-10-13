import pandas as pd
import pytest

from src.equipment_domain.infrastructure.cvd_parts_loader import load_cvd_parts


def source_row(value=2477, factory="old-factory"):
    return pd.DataFrame({
        "厂别": [factory], "备件类型": ["Mask"], "设备类型": ["CVD"],
        "膜层": ["SIN"], "制程": ["SIN DEPO"], "寿命规格": [6500],
        "站点": [21200], "机台号": ["M1"], "测量值": [value],
        "进度": [352], "测量时间": ["2026-09-21"],
    })


def test_sheet_name_overrides_factory_and_old_time_with_offset_header(tmp_path):
    path = tmp_path / "cvd.xlsx"
    with pd.ExcelWriter(path) as writer:
        source_row().to_excel(writer, sheet_name="OLED-1012 ", index=False, startrow=6, startcol=2)
    loaded = load_cvd_parts(path, as_of_date="2026-10-13")
    assert len(loaded) == 1
    assert loaded.iloc[0]["机台号"] == "M1"
    assert loaded.iloc[0]["进度"] == 352
    assert loaded.iloc[0]["厂别"] == "OLED"
    assert loaded.iloc[0]["测量时间"] == pd.Timestamp("2026-10-12")
    assert loaded.iloc[0]["测量值"] == 2477
    assert not any(str(c).startswith("Unnamed") for c in loaded.columns)


def test_latest_version_per_factory_is_selected_independent_of_sheet_order(tmp_path):
    path = tmp_path / "cvd.xlsx"
    with pd.ExcelWriter(path) as writer:
        source_row(100).to_excel(writer, sheet_name="OLED-0921", index=False)
        source_row(300).to_excel(writer, sheet_name="ARRAY-1011", index=False)
        source_row(200).to_excel(writer, sheet_name="OLED-1012", index=False)
    loaded = load_cvd_parts(path, as_of_date="2026-10-13").set_index("厂别")
    assert len(loaded) == 2
    assert loaded.loc["OLED", "测量值"] == 200
    assert loaded.loc["OLED", "测量时间"] == pd.Timestamp("2026-10-12")
    assert loaded.loc["ARRAY", "测量值"] == 300
    assert loaded.loc["ARRAY", "测量时间"] == pd.Timestamp("2026-10-11")


def test_sheet_identity_and_date_columns_are_optional_in_source_table(tmp_path):
    path = tmp_path / "cvd.xlsx"
    with pd.ExcelWriter(path) as writer:
        source_row().drop(columns=["厂别", "测量时间"]).to_excel(
            writer, sheet_name="ARRAY-1012", index=False,
        )
    loaded = load_cvd_parts(path, as_of_date="2026-10-13")
    assert loaded.iloc[0]["厂别"] == "ARRAY"
    assert loaded.iloc[0]["测量时间"] == pd.Timestamp("2026-10-12")


@pytest.mark.parametrize("sheet", ["first", "OLED-0230", "OLED-101", "-1012"])
def test_invalid_sheet_identity_fails_instead_of_using_old_baseline(tmp_path, sheet):
    path = tmp_path / "cvd.xlsx"
    with pd.ExcelWriter(path) as writer:
        source_row().to_excel(writer, sheet_name=sheet, index=False)
    with pytest.raises(ValueError, match="Sheet"):
        load_cvd_parts(path, as_of_date="2026-10-13")


def test_new_sheet_version_resets_value_and_elapsed_time_without_accumulation(tmp_path):
    from src.equipment_domain.config import get_equipment_runtime_config
    from src.equipment_domain.core.cvd_parts import build_cvd_report

    path = tmp_path / "cvd.xlsx"

    def compute(sheet, value):
        with pd.ExcelWriter(path) as writer:
            source_row(value).to_excel(writer, sheet_name=sheet, index=False)
        return build_cvd_report(
            load_cvd_parts(path, as_of_date="2026-10-13"),
            as_of_date="2026-10-13", baseline_date="2026-09-21",
            policy=get_equipment_runtime_config().alert_policy,
        )

    old = compute("OLED-1001", 2477)
    assert old.iloc[0]["测量值"] == pytest.approx(2477 + 352 / 30 * 12)
    latest = compute("OLED-1012", 2800)
    assert latest.iloc[0]["测量值"] == pytest.approx(2800 + 352 / 30)
    pd.testing.assert_frame_equal(latest, compute("OLED-1012", 2800))


def test_duplicate_active_factory_version_is_rejected(tmp_path):
    path = tmp_path / "cvd.xlsx"
    with pd.ExcelWriter(path) as writer:
        source_row().to_excel(writer, sheet_name="OLED-1012", index=False)
        source_row(3000).to_excel(writer, sheet_name="OLED-1012 ", index=False)
    with pytest.raises(ValueError, match="重复"):
        load_cvd_parts(path, as_of_date="2026-10-13")


def test_invalid_latest_sheet_does_not_silently_use_old_version(tmp_path):
    path = tmp_path / "cvd.xlsx"
    with pd.ExcelWriter(path) as writer:
        source_row().to_excel(writer, sheet_name="OLED-0921", index=False)
        source_row().drop(columns="测量值").to_excel(writer, sheet_name="OLED-1012", index=False)
    with pytest.raises(ValueError, match="完整字段表头"):
        load_cvd_parts(path, as_of_date="2026-10-13")


def test_encrypted_source_is_isolated_from_excel_side_effects_and_copies_are_cleaned(
    tmp_path, monkeypatch,
):
    from types import SimpleNamespace

    import fr_file_decryption
    from src.equipment_domain.infrastructure import cvd_parts_loader

    source_path = tmp_path / "cvd.xlsx"
    source_bytes = b"\x00\x00\x00\x00encrypted-content"
    source_path.write_bytes(source_bytes)
    temporary_paths = []

    def decrypt_copy(path, *, output_dir, strategy):
        assert path != source_path
        assert path.read_bytes() == source_bytes
        assert strategy == "excel-values"
        temporary_paths.append(path)
        path.write_bytes(b"encryption-layer-rewrites-container")
        output_dir.mkdir(parents=True)
        output = output_dir / "values.xlsx"
        with pd.ExcelWriter(output) as writer:
            source_row().to_excel(writer, sheet_name="OLED-1012 ", index=False)
        return SimpleNamespace(output_path=output)

    monkeypatch.setattr(cvd_parts_loader.ConfigLoader, "get_project_root", lambda: tmp_path)
    monkeypatch.setattr(fr_file_decryption, "decrypt_file", decrypt_copy)
    loaded = load_cvd_parts(source_path, as_of_date="2026-10-13")
    assert loaded.iloc[0]["测量时间"] == pd.Timestamp("2026-10-12")
    assert source_path.read_bytes() == source_bytes
    assert all(not path.exists() for path in temporary_paths)
    assert not list((tmp_path / "output/decrypted_files").iterdir())


def test_missing_workbook_is_not_reported_as_empty(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_cvd_parts(tmp_path / "missing.xlsx")

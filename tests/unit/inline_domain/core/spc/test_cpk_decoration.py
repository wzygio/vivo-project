from pathlib import Path
from zipfile import BadZipFile

import pandas as pd

from src.inline_domain.core.spc import cpk_decoration
from src.inline_domain.core.spc.cpk_decoration import (
    apply_cpk_decoration,
    load_cpk_decoration,
    prepare_cpk_decoration,
)


def _capability_frame(cpk: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "prod_code": "M678",
                "factory": "ARRAY",
                "step_id": "12140",
                "param_name": "SE_L1T_UNI",
                "period_type": "day",
                "period_label": "2026-07-20",
                "period_sort": 320,
                "period_start": "2026-07-20",
                "period_end": "2026-07-20",
                "cpk": cpk,
            }
        ]
    )


def test_cpk_decoration_defaults_to_real_cpk_until_an_admin_enables_a_row(tmp_path: Path) -> None:
    real_df = _capability_frame(0.82)
    corrected_df = _capability_frame(1.46)

    result = prepare_cpk_decoration(
        real_period_capability_df=real_df,
        corrected_period_capability_df=corrected_df,
        product_dir=tmp_path,
        persist_files=False,
    )

    assert result.decoration_df["flag"].tolist() == [False]
    assert result.period_capability_df["cpk"].tolist() == [0.82]
    assert result.period_capability_df["cpk_decorated"].tolist() == [False]

    enabled_df = result.decoration_df.assign(flag=True)
    decorated_df = apply_cpk_decoration(real_df, corrected_df, enabled_df)

    assert decorated_df["cpk"].tolist() == [1.46]
    assert decorated_df["cpk_decorated"].tolist() == [True]


def test_cpk_decoration_accepts_existing_ture_flag_typo() -> None:
    real_df = _capability_frame(0.82)
    corrected_df = _capability_frame(1.46)
    decoration_df = prepare_cpk_decoration(
        real_period_capability_df=real_df,
        corrected_period_capability_df=corrected_df,
        product_dir=Path("."),
        persist_files=False,
    ).decoration_df.assign(cpk_corrected=1.72, flag="TURE")

    decorated_df = apply_cpk_decoration(real_df, corrected_df, decoration_df)

    assert decorated_df["cpk"].tolist() == [1.72]
    assert decorated_df["cpk_decorated"].tolist() == [True]


def test_load_cpk_decoration_falls_back_to_excel_com_for_enterprise_encrypted_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    product_dir = tmp_path / "Z571"
    product_dir.mkdir()
    decoration_path = product_dir / cpk_decoration.CPK_DECORATION_FILE_NAME
    decoration_path.write_bytes(b"\x00\x00\x00\x00enterprise-encrypted")
    expected_df = prepare_cpk_decoration(
        real_period_capability_df=_capability_frame(0.82),
        corrected_period_capability_df=_capability_frame(1.46),
        product_dir=tmp_path,
        persist_files=False,
    ).decoration_df.assign(flag="TURE")

    monkeypatch.setattr(
        cpk_decoration.pd,
        "read_excel",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(BadZipFile("File is not a zip file")),
    )
    monkeypatch.setattr(
        cpk_decoration,
        "_read_encrypted_xlsx_via_com",
        lambda path: expected_df if path == decoration_path else pd.DataFrame(),
    )

    loaded_df = load_cpk_decoration(product_dir)

    assert loaded_df.equals(expected_df)


def test_prepare_cpk_decoration_never_rewrites_an_existing_user_file(tmp_path: Path) -> None:
    real_df = _capability_frame(0.82)
    corrected_df = _capability_frame(1.46)
    decoration_path = tmp_path / cpk_decoration.CPK_DECORATION_FILE_NAME
    existing_df = prepare_cpk_decoration(
        real_period_capability_df=real_df,
        corrected_period_capability_df=corrected_df,
        product_dir=tmp_path,
        persist_files=False,
    ).decoration_df.assign(cpk_corrected=1.72, flag="TURE")
    existing_df.to_excel(decoration_path, index=False)
    original_bytes = decoration_path.read_bytes()

    result = prepare_cpk_decoration(
        real_period_capability_df=real_df,
        corrected_period_capability_df=corrected_df,
        product_dir=tmp_path,
    )

    assert decoration_path.read_bytes() == original_bytes
    assert not (tmp_path / "spc_cpk_detail.xlsx").exists()
    assert result.period_capability_df["cpk"].tolist() == [1.72]
    assert result.period_capability_df["cpk_decorated"].tolist() == [True]


def test_prepare_cpk_decoration_preserves_an_unreadable_existing_user_file(
    monkeypatch,
    tmp_path: Path,
) -> None:
    decoration_path = tmp_path / cpk_decoration.CPK_DECORATION_FILE_NAME
    encrypted_bytes = b"\x00\x00\x00\x00enterprise-encrypted"
    decoration_path.write_bytes(encrypted_bytes)
    monkeypatch.setattr(
        cpk_decoration.pd,
        "read_excel",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(BadZipFile("File is not a zip file")),
    )
    monkeypatch.setattr(
        cpk_decoration,
        "_read_encrypted_xlsx_via_com",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("Excel COM unavailable")),
    )

    result = prepare_cpk_decoration(
        real_period_capability_df=_capability_frame(0.82),
        corrected_period_capability_df=_capability_frame(1.46),
        product_dir=tmp_path,
    )

    assert decoration_path.read_bytes() == encrypted_bytes
    assert result.period_capability_df["cpk"].tolist() == [0.82]
    assert result.period_capability_df["cpk_decorated"].tolist() == [False]

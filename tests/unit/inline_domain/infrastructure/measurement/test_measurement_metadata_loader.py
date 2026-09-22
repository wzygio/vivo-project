from __future__ import annotations

from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src.inline_domain.infrastructure.shared.measurement_metadata_loader import (
    load_parameter_catalog,
    load_parameter_specs,
)


@pytest.mark.parametrize("target", ["2.5", "0", None, "invalid"])
def test_metadata_loader_returns_unclassified_catalog_and_complete_specs(target) -> None:
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(text("ATTACH DATABASE ':memory:' AS eda"))
        connection.execute(text("ATTACH DATABASE ':memory:' AS mdw"))
        connection.execute(
            text(
                "CREATE TABLE mdw.dwr_mes_productspec ("
                "productspecname TEXT, productcode TEXT)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE eda.IMP_SPC_TZBJX ("
                "productspecname TEXT, parmtername TEXT, data_type TEXT)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE mdw.dwd_imp_dv_param_spec ("
                "prod_code TEXT, step_id TEXT, param_name TEXT, param_type TEXT, "
                "usl TEXT, lsl TEXT, ucl TEXT, lcl TEXT, target TEXT, "
                "main_step_id TEXT, main_eqp_type TEXT)"
            )
        )
        connection.execute(text("INSERT INTO mdw.dwr_mes_productspec VALUES ('S-M678', 'M678')"))
        connection.execute(text("INSERT INTO eda.IMP_SPC_TZBJX VALUES ('S-M678', 'TDSUM', NULL)"))
        connection.execute(
            text(
                "INSERT INTO mdw.dwd_imp_dv_param_spec VALUES "
                "('M678', '11620', 'TDSUM', NULL, '5', NULL, '3', NULL, :target, NULL, NULL)"
            ),
            {"target": target},
        )

    db = SimpleNamespace(engine=engine)
    catalog = load_parameter_catalog(db, "M678")
    specs = load_parameter_specs(db, "M678")

    assert catalog.to_dict("records") == [{"ref_param_name": "TDSUM", "data_type": None}]
    assert specs.loc[0, "param_type"] is None
    assert specs.loc[0, "usl"] == 5
    if target in (None, "invalid"):
        assert pd.isna(specs.loc[0, "target"])
    else:
        assert specs.loc[0, "target"] == float(target)
    assert specs.loc[0, "main_step_id"] == "11620"
    assert specs.loc[0, "main_eqp_type"] == "EQP"
    assert "target" in load_parameter_specs(db, "UNKNOWN").columns
    engine.dispose()

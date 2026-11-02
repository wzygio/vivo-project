"""Public visibility rules are shared by page and API adapters."""

import pandas as pd
import pytest


def test_policy_projects_public_state_without_mutating_source():
    from src.shared_kernel.core.alert_visibility import apply_matrix_compliance, parse_matrix_config

    rules = parse_matrix_config(pd.DataFrame({"监控参数": ["SPC 单片异常"], "P1": ["True"]}))
    original = {
        "rows": [{"row_key": "spc_sheet_oos", "display_name": "SPC 单片异常"}],
        "cells": {("spc_sheet_oos", "P1"): {"state": "alert", "message": "internal"}},
    }
    projected = apply_matrix_compliance(original, rules)

    assert projected["cells"][("spc_sheet_oos", "P1")] == {"state": "ok", "message": ""}
    assert original["cells"][("spc_sheet_oos", "P1")] == {"state": "alert", "message": "internal"}


@pytest.mark.parametrize("cell, expected", [(True, True), (False, False), ("TRUE", True), (" false ", False)])
def test_visibility_parser_keeps_explicit_boolean_semantics(cell, expected):
    from src.shared_kernel.core.alert_visibility import parse_matrix_config

    result = parse_matrix_config(pd.DataFrame({"监控参数": ["SPC 单片异常"], "P1": [cell]}))
    assert result == {("SPC 单片异常", "P1"): expected}


@pytest.mark.parametrize("cell", [None, "", "yes", "0", 0, "enabled"])
def test_visibility_parser_rejects_ambiguous_or_absent_decisions(cell):
    from src.shared_kernel.core.alert_visibility import parse_matrix_config

    with pytest.raises(ValueError):
        parse_matrix_config(pd.DataFrame({"监控参数": ["SPC 单片异常"], "P1": [cell]}))


def test_visibility_parser_rejects_duplicate_names_and_columns():
    from src.shared_kernel.core.alert_visibility import parse_matrix_config

    with pytest.raises(ValueError):
        parse_matrix_config(pd.DataFrame({"监控参数": ["SPC", "SPC"], "P1": [False, False]}))
    with pytest.raises(ValueError):
        parse_matrix_config(pd.DataFrame([["SPC", False, False]], columns=["监控参数", "P1", "P1"]))


def test_unconfigured_cell_keeps_current_page_compatibility_without_source_mutation():
    from src.shared_kernel.core.alert_visibility import apply_matrix_compliance

    original = {
        "rows": [{"row_key": "spc_sheet_oos", "display_name": "SPC 单片异常"}],
        "cells": {("spc_sheet_oos", "P1"): {"state": "error", "message": "unavailable"}},
    }
    projected = apply_matrix_compliance(original, {})
    assert projected == original
    projected["cells"][("spc_sheet_oos", "P1")]["state"] = "ok"
    assert original["cells"][("spc_sheet_oos", "P1")]["state"] == "error"

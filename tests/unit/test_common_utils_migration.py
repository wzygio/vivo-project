from fr_common_utils.excel import xlsx_to_csv as canonical_xlsx_to_csv
from src.shared_kernel.utils import data_inspector


def test_probe_csv_export_uses_the_packaged_excel_contract() -> None:
    assert data_inspector.xlsx_to_csv is canonical_xlsx_to_csv

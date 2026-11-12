"""Production coverage cannot silently shrink below the maintained dashboard."""


def test_registry_covers_dashboard_and_api_events():
    from app.sections.inline_domain.monitor.alert_matrix_service import MATRIX_ROWS
    from api.inspection.application import MODULES
    owners = {
        'spc_sheet_oos':'spc','ctq_sheet_oos':'ctq','aoi_tt_sheet_oos':'aoi_tt','aoi_rs_sheet_oos':'aoi_rs',
        'spc_cpk_trend':'spc','spc_cpm_trend':'spc','yield_lot_oos':'yield',
        'yield_trend_fluctuation':'yield','qtime_sheet_oos':'qtime',
        'ijp_aa_oos':'ijp_aa','ijp_hole_oos':'ijp_hole','parts_oos':'parts','iqc_evaporation_ng':'iqc_evaporation'}
    assert {row.row_key for row in MATRIX_ROWS} == set(owners)
    assert set(owners.values()) == set(MODULES)


def test_migrated_producers_do_not_import_tools():
    import ast
    from pathlib import Path
    for path in Path('api/inspection').glob('*.py'):
        tree=ast.parse(path.read_text(encoding='utf-8'))
        assert not any(isinstance(node,ast.ImportFrom) and (node.module or '').startswith('tools.')
                       for node in ast.walk(tree)),path

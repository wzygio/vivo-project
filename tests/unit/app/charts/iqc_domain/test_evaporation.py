import pandas as pd

from app.charts.iqc_domain.evaporation import (
    build_evaporation_figure,
    prepare_evaporation_points,
)


def readings():
    return pd.DataFrame({
        '物料号': ['A'] * 5, '物料描述': ['有机材料'] * 5,
        '产品型号': ['M678'] * 5, '特性项目': ['HPLC-A'] * 5,
        '检验时间': ['2026-11-03 12:00', '2026-11-01 08:00',
                     '2026-11-01 08:00', '2026-11-02 10:00', None],
        'IQC-1': [65, 0, None, None, 63],
        'COA-1': [63, 62.5, 62.6, None, 63],
        '规格上限': [64] * 5, '规格下限': [62] * 5,
        '规格样式1': ['<='] * 5, '规格样式2': ['>='] * 5,
        'flag': ['private'] * 5, 'batch_no': ['private'] * 5,
    })


def test_each_inspection_keeps_its_own_position_and_missing_is_not_zero():
    source = readings()
    before = source.copy(deep=True)
    points = prepare_evaporation_points(source)
    assert len(points) == 3
    assert points['IQC-1'].iloc[0] == 0
    assert points['IQC-1'].isna().sum() == 1
    assert not {'flag', 'batch_no'} & set(points)
    pd.testing.assert_frame_equal(source, before)
    figure = build_evaporation_figure(points, characteristic='HPLC-A')
    iqc, coa = figure.data[:2]
    assert iqc.name == 'IQC-1' and coa.name == 'COA-1'
    assert iqc.type == coa.type == 'scatter'
    assert iqc.mode == coa.mode == 'markers'
    assert list(iqc.x) == [0, 2] and list(iqc.y) == [0, 65]
    assert list(coa.x) == [0, 1, 2] and list(coa.y) == [62.5, 62.6, 63]
    assert figure.layout.xaxis.type == 'linear'
    assert figure.layout.yaxis.range is None  # Never clip an out-of-spec point.
    assert 'private' not in figure.to_json()
    assert '2026-11-01 08:00:00' in figure.to_json()
    assert {shape.y0 for shape in figure.layout.shapes} == {62, 64}


def test_changing_specs_follow_the_corresponding_inspection_and_break_at_missing():
    frame = pd.concat([readings().iloc[[1]]] * 4, ignore_index=True)
    frame['规格上限'] = [64, 65, None, 66]
    figure = build_evaporation_figure(frame, characteristic='HPLC-A')
    upper = next(trace for trace in figure.data if trace.name == '规格上限')
    assert list(upper.y) == [64, 65, None, 66]
    assert upper.line.shape == 'hv' and upper.line.dash == 'dash'
    assert not upper.connectgaps
    assert {shape.y0 for shape in figure.layout.shapes} == {62}


def test_single_series_without_specs_and_non_numeric_values():
    frame = readings().iloc[[0, 1, 2]].copy()
    frame['IQC-1'] = ['bad', float('inf'), None]
    frame['规格上限'] = None
    frame['规格下限'] = None
    figure = build_evaporation_figure(frame, characteristic='COA only')
    assert [trace.name for trace in figure.data] == ['COA-1']
    assert len(figure.data[0].y) == 3
    assert not figure.layout.shapes


def test_empty_points_create_no_measurements_or_specs():
    frame = readings().assign(**{'IQC-1': None, 'COA-1': None})
    assert prepare_evaporation_points(frame).empty
    figure = build_evaporation_figure(frame, characteristic='未测')
    assert not figure.data and not figure.layout.shapes


def test_only_the_four_verified_characteristics_are_plotted():
    frame = pd.concat([readings().iloc[[1]]] * 6, ignore_index=True)
    frame['特性项目'] = [
        'DSC-Tg中点', 'HPLC-A', 'HPLC-A+B', 'HPLC-B', 'DSC-Tg起始点', 'TGA-1%失重',
    ]
    points = prepare_evaporation_points(frame)
    assert points['特性项目'].tolist() == ['DSC-Tg中点', 'HPLC-A', 'HPLC-A+B', 'HPLC-B']

"""Over-limit selection from the current critical-parts report projection."""

import pandas as pd

PARTS_PUBLIC_COLUMNS = (
    '厂别', '备件类型', '设备类型', '膜层', '制程',
    '寿命规格', '站点', '机台号-腔室',
    '测量值', '使用进度', '预警状态', '测量时间',
)
OVER_PROGRESS_LIMIT = 100.0


def build_parts_alerts(report_df: pd.DataFrame) -> pd.DataFrame:
    """Keep finite progress strictly above 100%; never inspect source flags."""
    if report_df.empty:
        return pd.DataFrame(columns=PARTS_PUBLIC_COLUMNS)
    if '使用进度' not in report_df:
        raise ValueError('备件报表缺少使用进度')
    progress = pd.to_numeric(report_df['使用进度'], errors='coerce')
    result = report_df.assign(使用进度=progress).loc[
        progress.gt(OVER_PROGRESS_LIMIT) & progress.lt(float('inf')),
    ].reindex(columns=PARTS_PUBLIC_COLUMNS).copy()
    result['预警状态'] = '超规'
    result['厂别'] = result['厂别'].astype('string').str.strip().str.upper()
    return result.reset_index(drop=True)

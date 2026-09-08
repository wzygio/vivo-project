"""历史迁移工具不得再次自动填充人工维护的指定良损。"""

import pandas as pd
import pytest

from src.yield_domain.core.mwd_trend.modifier_table import MODIFIER_TABLE_COLUMNS
from tools.backfill_modifier_table_specified import (
    LegacySpecifiedBackfillDisabledError,
    backfill_sheet,
)


def test_legacy_backfill_is_disabled() -> None:
    table = pd.DataFrame(
        [
            {
                "不良类型": "B暗点",
                "周期类型": "月度",
                "时间标签": "2026-09",
                "当月良损": 0.00001,
                "指定良损": None,
                "缩放倍数": 1.0,
            }
        ],
        columns=MODIFIER_TABLE_COLUMNS,
    )

    with pytest.raises(LegacySpecifiedBackfillDisabledError, match="仅允许人工维护"):
        backfill_sheet(
            table,
            {("B暗点", "2026-09"): 0.0625},
            {("B暗点", "2026-09"): 0.5},
        )

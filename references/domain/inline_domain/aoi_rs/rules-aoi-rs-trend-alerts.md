# AOI_RS 趋势预警规则

核验日期：2026-10-20。本文负责 AOI_RS 页面的“当日超规”日密度预警。按用户最新修订，目标日必须超过上次密度的 1.2 倍，且上次密度必须超过五天均值的 1.2 倍；删除独立的连续两日上升判定。均值仍为算术平均、跳过缺测及零密度；相邻比较允许跨日、向前最多追溯七天。既有上一 ISO 周单片异常规则继续独立运行；本功能不写入报警历史、周报警率或全指标矩阵。

## 1. 日期、指标与密度

- 每次评估的参考日期 R 来自报表查询结束日，页面正常运行时为当天；目标日 `T = R−1`，即昨天。
- 不自动寻找“最新有数据的日期”。昨天缺测时不使用前天代替，也不使用今天未完成的数据作目标值。
- 按当前产品内的 `factory + step_id + rs_code` 独立评估。
- 密度单位为 RS/片：`SUM(code_qty) / 同厂别同站点同日 distinct sheet/glass 过货量`，允许大于 1，不是百分比。
- 判定使用当前报表投影的日密度。日期前推、厂别日期排除在基础设施输出时已经执行；Core 不重复变换日期。

## 2. 当日超规：两个条件同时满足

用 `D(t)` 表示日期 t 的有效日密度。定义 P 为 `[T−7,T)` 内最近的有效日期，Q 为同一区间内 P 之前最近的有效日期。P 是判定中的“前一天”，允许跨过缺测日期，不能早于 T−7；Q 仅保留供表格 T-2 展示，不再参与另一条判定。真实零密度仍可作为比较记录，不因数值为零而跳过。

所有倍数比较都严格使用 `>`，恰好 1.2 倍不触发。实现用 1e-12 相对容差消除浮点边界误差，例如 `1/5` 对 `1/6` 的密度正好上涨 20%，不能因二进制舍入误报；不设置绝对密度下限。

| 规则名称 | 条件 | 数据门槛 |
|---|---|---|
| 当日超规 | `D(T) > 1.2 × D(P)` **且** `D(P) > 1.2 × M` | T 和 P 均有效；计算窗 `[T−7,T]` 至少 3 个有效日；五天基线至少 1 个正密度 |

其中 M 是固定窗口 `[T−6,T−2]` 中正密度的**算术平均**，各有效日权重相同，不按过货量加权；仍是原来的五个自然日。缺测、零密度及无效数值均跳过；不是补足五条有效记录，也不延长窗口寻找正值。五天均无正密度时 M 不可用，本规则不可评估，不再由另一条规则单独报警。

均值窗口不随跨日选出的 P 移动；因此 P 落在此五天内时，也属于均值候选。保留原窗口位置，避免跨日后额外引入另一套基线窗口。

只有上述两条不等式同时满足才报警；不再使用目标日直接与均值比较，也不再使用 P 与 Q 的连续上升独立判定。每个命中指标只显示一条记录，预警原因统一为“当日超规”。

“当日超规”是用户对原双比较逻辑指定的名称，并不增加 `D(T) > spec` 的要求。即使未超过 MWD_RATIO 规格、甚至缺少规格，也可以触发本功能。单片规格异常的原规则不受此命名影响。

### 示例

| 场景 | 数据 | 结果 |
|---|---|---|
| 上次尚未超过基线 | M=0.10，P=0.08，T=0.15 | 目标日虽大幅反弹，第二个条件不满足，不报警 |
| 连续上涨但基线过高 | M=10，Q=0.10，P=0.13，T=0.17 | P 未超过均值的 1.2 倍，不报警 |
| 两条件均满足 | M=0.10，P=0.13，T=0.17 | 一条指标预警，原因为“当日超规” |
| 前一天缺失 | T=10月19日，18日缺测，17日为0.20，14日为0.10，T为0.30 | P=17日、Q=14日，固定五天均值 M=0.15；两个条件均满足 |
| 回溯界限 | T=10月19日，历史记录最早可用12日，11日及更早不能作为P/Q | 包含恰好七天前；同一界限约束P和Q，不能逐次向前各追七天 |
| 基线全零 | 五天内只有有效零值，P=0.13，T=0.17 | 零值不参与均值，M为空；数据不足，不报警 |
| 当日严格边界 | M=0.10，P=0.125，T=0.15 | T 恰好为 P 的 1.2 倍，不报警 |
| 基线严格边界 | M=0.10，P=0.12，T=0.20 | P 恰好为 M 的 1.2 倍，不报警 |

## 3. 有效数据与缺测

- 有效日要求该指标存在日趋势行、密度是有限非负数、过货量是有限正数。真实零密度属于有效比较数据，计入有效天数，但不参与均值。
- 缺行、无效日期、NaN、无穷值、负密度、零/负/无效过货量不作为有效日，不能伪造为零。
- 比较回溯范围为 `[T−7,T)`，即目标日前七个自然日；加上目标日，计算输入覆盖 `[T−7,T]` 共八个自然日。图轴为了补足展示而返回更早日期时，检测器仍排除窗口外数据。
- 先排除缺测或无效行，再按日期排序取最后两条历史记录；P/Q 的间隔可以超过一天。T 本身仍固定为昨天，昨天缺测时不会把 P 当作目标日报警。
- 同指标同日重复行或非空输入缺少必要列属于数据契约错误，应用服务明确失败；不能伪装成“无预警”。
- 没有额外单日过货量下限。目标日过货量和有效日数量保留在后端评估结果中；按用户要求不再传给前端表格。
- 不计算零值分母上的相对增幅；页面直接展示参与比较的密度。

## 4. 预警与现有图表保持一致

现有主图日轴保留最近 7 个有 RS 明细的日期。今天或其他 Code 存在数据时，可能挤掉预警所需的 T−7。

预警聚合通过 `daily_start_date=T−7`、`daily_end_date=T` 明确生成完整八天日轴，不受主图最近七个有数据日期的限制；某指标缺明细的日期仍不生成密度，不补零。月/周聚合的截止仍为报表结束日 R，使相同日期的日密度与主图一致。主图及 PDF 不传显式日期范围，其日期轴不变。

开启专用厂别规则时，预警沿用当前明细投影及日值限幅。日值和主图使用相同、截至 R 的月基准，不能改成截至 T 的月基准，否则在月内今天有数据时可能出现图值与预警不一致。专用规则详情见 [AOI_RS 专用修饰规则](rules-aoi-rs-special-decoration.md)。

## 5. 分层、缓存和页面行为

- Core 的 `evaluate_aoi_rs_trends()` 接收趋势帧、指标帧和明确参考日期，返回每指标一行评估结果：实际 P/Q 日期、比较密度、目标日过货量、有效天数、当日超规可评估状态和命中状态；不访问数据库、配置或工作簿。输出不再含 `rising_evaluable` 或 `rising_alert`。
- Application 在现有 ReportService 中完成投影、共享趋势计算及规则评估；`trend_evaluation_df` 作为 DataFrame 进入原生 payload，在缓存外构造 ViewModel，遵循 ADR-0001。
- 查询结束日、产品 revision、决策签名、日期排除、专用厂别/窗口及新增趋势策略版本参与缓存身份。跨天或上述输入变化后重新评估。
- 页面保留既有单片异常区，新增趋势预警中心及命中指标的三图区域，均在手动查询门控前运行。趋势区使用独立图表 key 前缀 `aoi_rs_trend_alert`。
- 自动图像按厂别、站点、RS Code 精确匹配；过货量仅按厂别、站点匹配，每个命中指标只输出一组图像。
- 按用户的展示精简要求，命中时仅显示预警明细，移除规则说明、可评估数量、数据不足数量、命中数量提示及自动图像说明。无数据、全部不足或可评估规则无命中时保留简短状态，不把全部不足显示为正常。
- 前端表格在进入 Streamlit 前只保留业务字段，站点描述复用既有映射，不传递内部决策字段、源值对比或原始异常详情。
- 表格采用“当日／当日密度”“T-1有效日／T-1密度”“T-2有效日／T-2密度”“基线5日均值”标签。前端实际数据共 11 列，不含过货量、计算窗有效天数和基线有效天数，不通过隐藏列保留这些字段。T-1/T-2 展示 P/Q，名称简化不改变跨日规则；“当日”仍展示 T（昨天）。接口字段 `previous_density`、`two_days_before_density`、`previous_date`、`two_days_before_date` 保留，Q 仅用于展示。
- 读取或计算失败沿用报表加载失败提示，技术原因保留在服务端日志。

## 6. 与提供的参考程序的关系

保留其 1.2 倍双比较思路和五天内仅平均正密度的规则，按用户最新要求将第二个条件改为“上次密度超过均值的 1.2 倍”，统一命名为“当日超规”。独立的连续两日上升规则已删除。未复制参考程序的窗口偏移和缺日当零行为；真实零值只从均值中跳过，不从比较序列中跳过。P 仍可在七天内跨日选取，Q 仅供展示。

本项目分子继续使用 `SUM(code_qty)`，没有照搬参考程序的缺陷行数，也没有照搬 Yield 的百分点阈值或引入跨域检测器依赖。

## 7. 验证入口

- 纯规则：`tests/unit/inline_domain/core/aoi_rs/test_aoi_rs_trend_alerts.py`。
- 用例、月基准一致性、最早基线日与缓存：`tests/unit/inline_domain/application/aoi_rs/test_aoi_rs_service.py`。
- 页面正常/不足/失败、字段白名单与精确自动图像：`tests/unit/app/sections/aoi_rs/test_aoi_rs_trend_warning_ui.py`。
- 查询门控：`tests/unit/app/pages/test_aoi_rs_page.py`。
- 无数据库 UI 夹具：`tests/e2e/fixtures/aoi_rs_trend_app.py`。

## 8. 实现程序与接入位置

业务判定由 `src/inline_domain/core/shared/density_trend_alerts.py` 统一实现，RS 由 `core/aoi_rs/trend_alerts.py` 绑定指标键；TT 使用仅 Total 的三键适配器，分子、分母聚合各归原模块。共享策略版本 `density-previous-over-five-day-mean-v4` 同时使 RS/TT 原判定缓存失效。下方依次为共享判定和 RS 适配器的完整内容，不另建第二套运行逻辑；规则变更时同步此代码段。

应用接入位于 `application/aoi_rs/aoi_rs_service.py`：以报表结束日减一天得到 T，将 `daily_start_date=T−HISTORY_LOOKBACK_DAYS`、`daily_end_date=T` 传给 `build_decorated_period_trend_df()`，再调用 `evaluate_aoi_rs_trends()`。`HISTORY_LOOKBACK_DAYS=max(7, 5+2−1)=7`，同时覆盖比较和均值所需日期。

聚合接入位于 `core/aoi_rs/aoi_rs_calculator.py:build_period_trend_df()` 与 `aoi_rs_special_decoration.py:build_decorated_period_trend_df()`。显式日轴与原月/周轴组合，保留现有分子、分母、专用厂别和月基准处理。展示接入位于 `app/sections/inline_domain/aoi_rs/trend_alerts.py`，仅映射业务列和输出实际比较日期。

<!-- AOI_RS_TREND_IMPLEMENTATION_START -->

共享判定 `core/shared/density_trend_alerts.py`：

```python
"""Yesterday-density warnings with bounded sparse-date comparisons (no I/O)."""

from __future__ import annotations

from datetime import date, timedelta
from math import isclose
from collections.abc import Sequence

import numpy as np
import pandas as pd

DENSITY_TREND_POLICY_VERSION = "density-previous-over-five-day-mean-v4"
RISE_FACTOR = 1.2
LOOKBACK_DAYS = 7
BASELINE_DAYS = 5
BASELINE_LAG_DAYS = 2
HISTORY_LOOKBACK_DAYS = max(LOOKBACK_DAYS, BASELINE_DAYS + BASELINE_LAG_DAYS - 1)
EVALUATION_VALUE_COLUMNS = [
    "target_date", "target_density",
    "previous_date", "previous_density", "two_days_before_date",
    "two_days_before_density", "baseline_density",
    "target_sheet_qty", "valid_days", "baseline_days", "daily_evaluable",
    "daily_alert", "is_alert",
]


def _exceeds_rise(value: float, baseline: float) -> bool:
    """Treat binary roundoff at exactly 20% as equality (e.g. 1/5 vs 1/6)."""
    threshold = RISE_FACTOR * baseline
    return value > threshold and not isclose(value, threshold, rel_tol=1e-12, abs_tol=0)


def _evaluate_series(series: pd.DataFrame, target: pd.Timestamp) -> dict[str, object]:
    values = (
        series.set_index("day")["value"].sort_index() if not series.empty
        else pd.Series(dtype=float, index=pd.DatetimeIndex([]))
    )
    current_value = values.get(target, np.nan)
    comparisons = values.loc[
        (values.index >= target - pd.Timedelta(days=LOOKBACK_DAYS))
        & (values.index < target)
    ]
    previous_value = comparisons.iloc[-1] if len(comparisons) >= 1 else np.nan
    two_before_value = comparisons.iloc[-2] if len(comparisons) >= 2 else np.nan
    baseline_end = target - pd.Timedelta(days=BASELINE_LAG_DAYS)
    baseline_start = baseline_end - pd.Timedelta(days=BASELINE_DAYS - 1)
    baseline = values.loc[
        (values.index >= baseline_start) & (values.index <= baseline_end) & values.gt(0)
    ]
    baseline_mean = float(baseline.mean()) if not baseline.empty else np.nan
    has_pair = target in values.index and not comparisons.empty
    daily_evaluable = has_pair and len(values) >= 3 and baseline_mean > 0
    daily_alert = bool(
        daily_evaluable
        and _exceeds_rise(current_value, previous_value)
        and _exceeds_rise(previous_value, baseline_mean)
    )
    target_rows = series.loc[series["day"].eq(target)] if not series.empty else series
    return {
        "target_date": target.date().isoformat(),
        "target_density": current_value,
        "previous_date": comparisons.index[-1].date().isoformat() if len(comparisons) >= 1 else "",
        "previous_density": previous_value,
        "two_days_before_date": comparisons.index[-2].date().isoformat() if len(comparisons) >= 2 else "",
        "two_days_before_density": two_before_value,
        "baseline_density": baseline_mean,
        "target_sheet_qty": int(target_rows["sheet_qty"].iloc[0]) if not target_rows.empty else 0,
        "valid_days": len(values),
        "baseline_days": len(baseline),
        "daily_evaluable": bool(daily_evaluable),
        "daily_alert": daily_alert,
        "is_alert": daily_alert,
    }


def evaluate_density_trends(
    trend_df: pd.DataFrame,
    indicators_df: pd.DataFrame,
    *,
    reference_date: date,
    indicator_keys: Sequence[str],
) -> pd.DataFrame:
    """Evaluate yesterday, not the latest observed date, for every scoped indicator.

    P and Q are the latest two valid dates in [T-7, T), including real zero
    densities. Daily: D(T)>1.2*D(P) AND D(P)>1.2*positive_mean(T-6..T-2),
    with at least three valid observations in the calculation window. Q is
    retained for display only. Missing days never become zero; T is fixed.
    """
    keys = list(indicator_keys)
    columns = ["prod_code", *keys, *EVALUATION_VALUE_COLUMNS]
    if indicators_df.empty:
        return pd.DataFrame(columns=columns)
    target = pd.Timestamp(reference_date - timedelta(days=1)).normalize()
    start = target - pd.Timedelta(days=HISTORY_LOOKBACK_DAYS)
    daily = pd.DataFrame(columns=[*keys, "day", "value", "sheet_qty"])
    if not trend_df.empty:
        required = {*keys, "period_type", "period_label", "value", "sheet_qty"}
        if not required.issubset(trend_df.columns):
            raise ValueError("AOI density trend input is missing required columns")
        daily = trend_df.loc[trend_df["period_type"].eq("day")].copy()
        daily["day"] = pd.to_datetime(daily["period_label"], errors="coerce").dt.normalize()
        daily = daily.loc[daily["day"].between(start, target)].copy()
        if daily.duplicated([*keys, "day"]).any():
            raise ValueError("AOI density trend input has duplicate indicator dates")
        for column in ("value", "sheet_qty"):
            daily[column] = pd.to_numeric(daily[column], errors="coerce")
        daily = daily.loc[
            np.isfinite(daily["value"]) & daily["value"].ge(0)
            & np.isfinite(daily["sheet_qty"]) & daily["sheet_qty"].gt(0)
        ]
    groups = {
        tuple(str(value) for value in key): group
        for key, group in daily.groupby(keys)
    }
    records = []
    for indicator in indicators_df.drop_duplicates(keys).to_dict("records"):
        key = tuple(str(indicator[name]) for name in keys)
        series = groups.get(key, daily.iloc[0:0])
        records.append({
            "prod_code": indicator.get("prod_code", ""),
            **{name: indicator[name] for name in keys},
            **_evaluate_series(series, target),
        })
    return pd.DataFrame(records, columns=columns)
```

RS 适配器 `core/aoi_rs/trend_alerts.py`：

```python
"""RS adapter for the shared AOI density-trend rules."""
from __future__ import annotations

from datetime import date
import pandas as pd

from src.inline_domain.core.shared.density_trend_alerts import (
    BASELINE_DAYS, BASELINE_LAG_DAYS, DENSITY_TREND_POLICY_VERSION,
    EVALUATION_VALUE_COLUMNS, HISTORY_LOOKBACK_DAYS, LOOKBACK_DAYS, RISE_FACTOR,
    evaluate_density_trends,
)

AOI_RS_TREND_POLICY_VERSION = DENSITY_TREND_POLICY_VERSION
INDICATOR_KEYS = ["factory", "step_id", "rs_code"]
EVALUATION_COLUMNS = ["prod_code", *INDICATOR_KEYS, *EVALUATION_VALUE_COLUMNS]


def evaluate_aoi_rs_trends(
    trend_df: pd.DataFrame, indicators_df: pd.DataFrame, *, reference_date: date,
) -> pd.DataFrame:
    """Keep the RS public schema while sharing calendar and threshold rules."""
    return evaluate_density_trends(
        trend_df, indicators_df, reference_date=reference_date, indicator_keys=INDICATOR_KEYS,
    )
```

<!-- AOI_RS_TREND_IMPLEMENTATION_END -->

# AOI_RS 报表 — 数据源 Spec

> 任务来源：`docs/dev_docs/dev_prompt/feat-AOI_RS.md`
> 参考链路：`app/pages/SPC监控报表.py` → `SpcReportService` → `SpcRepository` / `data_loader`
> 状态：✅ 全部数据项已确认数据源（探查于 2026-08-10，脚本与原始输出见 `.scratch/probe_aoi_rs*.py`、`.scratch/probe_aoi_rs_result.md`）

## 0. 报表逻辑链摘要（Step 1 结论）

AOI_RS 报表与 SPC 监控报表同构，差异在于：

| 维度 | SPC 报表 | AOI_RS 报表 |
| --- | --- | --- |
| 测量值 | `param_value`（连续值，箱线图） | `code_qty`（计数，直接加和，点线图） |
| 指标粒度 | 厂别+站点+参数(param_name) | 厂别+站点+RS Code(rs_code) |
| 趋势图 | 按 period 聚合 CPK/均值/std | 按 period 聚合：Σcode_qty ÷ 过货 sheet/glass 数 |
| 规格线 | `dwd_imp_dv_param_spec` | `mdw.dwd_imp_rs_code_xishu_fo_tzsbjx` |
| 时间范围 | 页面给定（上月1日~当前） | 固定：上一自然月 1 日 ~ 当前日期（含当天），无时间筛选框 |
| 筛选框 | 厂别、站点、参数名 | 厂别、站点、Code 名称（rs_code + code_desc） |
| 月周天切分 | `build_available_period_axis`（跳过空值向前补全，最近 2 月/3 周/7 天，`src/inline_domain/core/spc/spc_calculator.py:142-197`） | 复用同一逻辑 |

图表清单：
1. By 月周天趋势图（两月、三周、七天，跳过空值向前补全）：分母=该 period 过货 sheet/glass 数（distinct），分子=平均每 sheet/glass 的 RS 个数（按 RS Code 分线），叠加规格线（type_flag=`MWD_RATIO`）。
2. By Lot 别点线图：每个 Lot 的 RS 个数，规格线 type_flag=`LOT_RATIO`。
3. By Sheet 别点线图：每个 sheet 的 RS 个数，规格线 type_flag=`SHEET_ID`/`GLASS_ID`。

颗粒度（从大到小）：产品型号 → 厂别 → 站点 → RS Code → RS 个数（明细）。

## 1. 数据源确认清单

### A. RS Code 明细（分子 / By Lot / By Sheet / 明细表）✅

| 厂别 | 表 | ID 字段 | 时间字段 |
| --- | --- | --- | --- |
| ARRAY | `eda.spc_tzbjx_rs_array` | `sheet_id` | `sheet_start_time` |
| OLED | `eda.spc_tzbjx_rs_oled` | `glass_id` | `glass_start_time` |
| TP | `eda.spc_tzbjx_rs_tsp` | `glass_id` | `glass_start_time` |

三表结构完全一致（15 列）：

| 列 | 类型 | 用途 | 探查结论 |
| --- | --- | --- | --- |
| `product_spec` | varchar | 产品规格名 → join 产品字典得 prod_code | ✅ 必需（见下方注意 1）；array 表有脏值（如 `'1'`,`'55'`），join 不上即被排除 |
| `productcode` | varchar | 产品型号 | ⚠️ array 表**全为空**；OLED/TP 部分有值（M626/M673/M678/Z517/Z571）。**统一不用此列**，改走 product_spec join 字典 |
| `step_id` | varchar | 站点筛选 | ✅ array: 11629/12629/13629/15629/18629；oled: 21329/43629；tsp: 43629 |
| `lot_id` | varchar | By Lot 点线图 | ✅ 三表均有（任务文档未列出，探查确认存在），样例 `L3MR57E0HAA`/`F3MR58E13AB`/`T3MR5900PAA` |
| `sheet_id`/`glass_id` | varchar | By Sheet 点线图、分子 distinct | ✅ |
| `sheet_start_time`/`glass_start_time` | timestamp | period 切分 | ✅ 数据覆盖 2025-07 ~ 当前，近一月行数 array≈7.0 万 / oled≈3.5 千 / tsp≈4.2 千 |
| `rs_code` | varchar | RS Code（指标维度+筛选框） | ✅ 基本为 5 位代码（如 `A1PPS`/`C4BP3`/`T3DMR`），个别 6 位（`C4PCP3`） |
| `code_qty` | integer | RS 个数（加和） | ⚠️ **并非恒为 1**：实测分布 0~65；OLED 表大量 `code_qty=0` 的记录（约 40%）。直接 `SUM(code_qty)` 即可，但需与业务确认 0 值行语义（复判后无缺陷？） |
| `code_qty1` | numeric | — | 与 code_qty 重复的冗余列，忽略 |
| `spec` | numeric | — | 样例均为 None，预留列，忽略 |
| `subproductiontype`/`wororder`/`eqp_id`/`sub_eqp`/`update_time` | — | — | 本报表不用 |

统一查询模式（仿 `data_loader.py:88-92` 的 factory_meta 三元组）：
`ARRAY→(spc_tzbjx_rs_array, sheet_id, sheet_start_time)`，`OLED→(spc_tzbjx_rs_oled, glass_id, glass_start_time)`，`TP→(spc_tzbjx_rs_tsp, glass_id, glass_start_time)`，三段 UNION ALL，每段 `LEFT JOIN mdw.dwr_mes_productspec dmp ON dmp.productspecname = t.product_spec` 后按 `dmp.productcode = :prod_code` 过滤。

### B. 过货明细（趋势图分母）✅

| 厂别 | 视图 | ID 字段 | 时间字段 |
| --- | --- | --- | --- |
| ARRAY | `eda.spot_eda_array_view_sht_v` | `sheet_id` | `sheet_start_time` |
| OLED | `eda.spot_eda_oled_view_gls_v` | `glass_id` | `glass_start_time` |
| TP | `eda.spot_eda_tp_view_gls_v` | `glass_id` | `glass_start_time` |

> ⚠️ 任务文档写的是 `eda.spot_eda_tsp_view_gls_v`，**该视图不存在**。eda schema 命名规律为 `spot_eda_{array|oled|tp}_view_{sht|gls}_v`（tsp 开头的只有 `spc_tzbjx_rs_tsp` 等少数表），TP 正确视图是 `eda.spot_eda_tp_view_gls_v`。

关键字段（三视图同构，37~38 列）：`sheet_id`/`glass_id`、`sheet_start_time`/`glass_start_time`、`step_id`、`product_spec`、`lot_id`。

口径校验：TP 视图含 `step_id='43629'`（与 RS 明细表 tsp 的站点一致），近一月该站点 distinct glass 数 ≈5.8 万，可覆盖 RS 明细的 3396 个 glass。分母查询同样按 `step_id + 时间窗` 过滤后 `COUNT(DISTINCT id)`，产品过滤同 A 节 join 字典。

注意：过货视图的 step_id 是该站点全部过货记录（含非 AOI 站点），必须按选定站点过滤，不能整表计数。

### C. RS Code 规格表（规格线 + Code 名称）✅

表：`mdw.dwd_imp_rs_code_xishu_fo_tzsbjx`（任务文档未标 schema，实际在 `mdw`；共 555 行）

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `prod_code` | varchar | 产品型号（M626/M673/M678/Z517/Z571） |
| `factory` | varchar | 厂别（ARRAY/OLED/TP） |
| `type_flag` | varchar | **规格适用的图类型**：`MWD_RATIO`（月周天趋势，185 行）、`LOT_RATIO`（By Lot，185 行）、`SHEET_ID`（By Sheet，120 行）、`GLASS_ID`（65 行，疑为 OLED/TP 的 By Glass 规格） |
| `step_id` | varchar | 站点 |
| `rs_code` | varchar | RS Code |
| `code_desc` | varchar | **Code 中文名称**（如"PHT责M3残留"）——筛选框"Code名称"的数据源 |
| `spec` | numeric | 规格上限值（0.02~80，单边上限；MWD_RATIO 为小数比率，LOT/SHEET 为个数） |
| `main_step_id`/`main_eqp_type`/`owner_id`/`interface_time` | — | 责任站点/设备类型等，本报表不用 |

规格粒度 = `prod_code + factory + step_id + rs_code + type_flag`。画线时按图类型取对应 type_flag 的 spec 值，作为单边上限线。

### D. 产品字典 ✅

表：`mdw.dwr_mes_productspec`（SPC 链路同款）。映射字段：`productspecname → productcode`（另有 `factoryname`、`description` 中文描述、`activestate`）。

join 方式（与任务文档一致）：

```sql
LEFT JOIN mdw.dwr_mes_productspec dmp
    ON dmp.productspecname = t.product_spec
-- 过滤：dmp.productcode = :prod_code
```

## 2. 与任务文档的偏差记录（开发注意）

1. **`code_qty` 不恒为 1**：实测 0~65，OLED 约 40% 记录为 0。加和逻辑不变，但 0 值行语义待业务确认。
2. **TP 过货视图名**：文档的 `eda.spot_eda_tsp_view_gls_v` 不存在，实际为 `eda.spot_eda_tp_view_gls_v`。
3. **RS 明细表比文档多出关键列**：`lot_id`（By Lot 图必需，文档未提）、`product_spec`（产品过滤必需，因 array 表 `productcode` 全空）。
4. **规格表 schema 为 `mdw`**，且规格按 `type_flag` 区分适用图类型（MWD_RATIO/LOT_RATIO/SHEET_ID/GLASS_ID），`code_desc` 顺便解决了"Code名称"筛选框的数据源，无需另找码表。
5. RS 明细站点与过货视图站点口径一致（如 TP 的 43629 两边都存在），分母按站点+时间窗过滤即可对齐。

## 3. 探查资产

- 探查脚本：`.scratch/probe_aoi_rs.py`（主）、`.scratch/probe_aoi_rs_supp.py` / `supp2` / `supp3`（补充）
- 原始输出：`.scratch/probe_aoi_rs_result.md`（完整列清单、样例、distinct 值）
- 已知坑：`pd.read_sql` 直传含 `%` 的 SQL（ILIKE）会触发 psycopg2 参数格式化 TypeError，须用 `sqlalchemy.text()` + connection。

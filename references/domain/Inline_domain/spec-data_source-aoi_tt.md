# AOI_TT 报表 — 数据源 Spec

> 任务来源：`docs/dev_docs/dev_prompt/feat-AOI_TT.md`
> 参考链路：`app/pages/AOI_RS监控报表.py` → `AoiRsReportService` → `src/inline_domain/infrastructure/aoi_rs/data_loader.py`
> 状态：✅ 全部数据项已确认数据源（探查于 2026-08-10，脚本与原始输出见 `.scratch/probe_aoi_tt*.py`、`.scratch/probe_aoi_tt*_result.md`）

## 0. 报表逻辑链摘要（Step 1 结论）

AOI_TT 报表与 AOI_RS 报表同构（样式完全一致），差异在于：

| 维度 | AOI_RS 报表 | AOI_TT 报表 |
| --- | --- | --- |
| 明细表 | `eda.spc_tzbjx_rs_{array,oled,tsp}` | `eda.spc_tzbjx_{array,oled,tsp}`（SPC 测量明细表） |
| 测量值 | `code_qty`（RS Code 个数，加和） | `param_value`（AOI Total Defect 个数，整数计数） |
| 指标粒度 | 厂别+站点+RS Code(rs_code) | 厂别+站点+TT(param_name) |
| 指标识别 | 明细表 rs_code 全量 | **规格表 `param_type IS NULL` 的 (step,param) 组合即 TT 参数全集**（见 §1-A） |
| 规格表 | `mdw.dwd_imp_rs_code_xishu_fo_tzsbjx`（type_flag 区分图类型） | `mdw.dwd_imp_dv_param_spec`（粒度 prod+step+param，取 **USL/UCL** 两条上限线，适用全部图） |
| 趋势分母 | 过货视图 distinct sheet（按站点过滤） | **测量表自身 distinct sheet**（过货视图不含 AOI 站点记录，见 §2-2） |
| 筛选框 | 厂别、站点、Code 名称 | 厂别、站点、Code 名称 + Particle Size（ARRAY/TP：Total/S/M/L/H；OLED：Total） |
| 时间范围 | 固定：上一自然月 1 日 ~ 当前日期 | 相同 |

图表清单（与 AOI_RS 一致）：
1. By 月周天趋势图（两月、三周、七天，跳过空值向前补全）：值 = Σparam_value ÷ 同 period 同站点 distinct sheet/glass 数（AOI 检测片数），按 TT 参数分线，叠加 USL/UCL 规格线 + 检测片数柱状（双 Y 轴）。
2. By Lot 别点线图：每个 Lot 的平均每片 TT 个数（ΣTT ÷ distinct Sheet），叠加 USL/UCL。
3. By Sheet 别点线图：每个 sheet/glass 的 TT 个数（每 (step,sheet,param) 恰一行，Σ 即原值），叠加 USL/UCL。

颗粒度（从大到小）：产品型号 → 厂别 → 站点 → TT → TT 个数（明细）。
参数与规格：「产品型号、厂别、站点、TT」经规格表锁定唯一参数及 USL/UCL（step_id 全局唯一，厂别由明细来源表决定）。

## 1. 数据源确认清单

### A. TT 明细（分子 / By Lot / By Sheet / 明细）✅

| 厂别 | 表 | ID 字段 | 时间字段 |
| --- | --- | --- | --- |
| ARRAY | `eda.spc_tzbjx_array` | `sheet_id` | `sheet_start_time` |
| OLED | `eda.spc_tzbjx_oled` | `glass_id` | `glass_start_time` |
| TP | `eda.spc_tzbjx_tsp` | `glass_id` | `glass_start_time` |

三表同构（14~15 列），关键列：

| 列 | 用途 | 探查结论 |
| --- | --- | --- |
| `product_spec` | join 产品字典得 prod_code | ✅ 同 RS 链路 |
| `step_id` | 站点 | ✅ AOI 站点：ARRAY `11620/12620/13620/15620/18620`，OLED `21320`，TP `43620` |
| `lot_id` | By Lot 点线图 | ✅ 三表均有 |
| `sheet_id`/`glass_id` | By Sheet 点线图、分母 distinct | ✅ |
| `sheet_start_time`/`glass_start_time` | period 切分 | ✅ |
| `param_name` | **TT 参数名（指标维度+Code名称筛选框）** | ✅ TT 参数全集 = `TDSUM`（ARRAY/TP）、`DSUM_L`/`DSUM_O`（OLED）；spec 表另有 `TOTAL_O_L` 但近 40 天无测量数据（疑旧名），不出现即可 |
| `param_value` | **TT 个数（缺陷计数）** | ✅ 整数值分布（ARRAY 24~2630、OLED 75~…），每 (step,sheet,param) 恰好 1 行（ARRAY 每 sheet 1~5 行是因跨 5 个 AOI 站点；OLED DSUM_L/DSUM_O 各 1 行/glass） |
| `site_name`/`unit_id`/`equip_id`/… | — | 本报表不用 |

**TT 参数识别规则（关键决策）**：测量表含全部 SPC 参数（SE_L1T、CD1、OVL、PPA…），必须过滤。
规格表 `mdw.dwd_imp_dv_param_spec` 中 `param_type IS NULL` 的 72 行全部是 AOI TT 参数
（TDSUM/DSUM_L/DSUM_O/TOTAL_O_L × 8 产品 × 7 站点），与测量表中实际出现的 TT 参数
（`%SUM%` 命名）完全对应。因此以规格表 `param_type IS NULL` 的 (prod_code, step_id, param_name)
驱动 TT 明细查询。风险：若未来其它 `param_type IS NULL` 的非 TT 参数入表会混入，届时需收紧规则。

### B. By Lot 维度 ✅

明细表自带 `lot_id`（样例 `L3MY6800HAA`/`C3MQ67M3NAF`/`T3Z668004AA`），直接 group by，无需 join。

### C. 趋势图分母（检测片数）✅（口径变更，见 §2-2）

**分母 = TT 测量表自身按 (factory, step_id, period) 的 `COUNT(DISTINCT sheet_id/glass_id)`**。
依据：TDSUM/DSUM 是每片必测项（distinct sheet == 行数，每日口径验证一致），
故 distinct sheet 数即 AOI 检测片数，Σparam_value ÷ 检测片数 = 平均每片 TT 个数，数学口径干净。

### D. 规格 USL/UCL ✅

表：`mdw.dwd_imp_dv_param_spec`（SPC 链路同款，schema 为 `mdw`）。

| 列 | 说明 |
| --- | --- |
| `prod_code` / `step_id` / `param_name` | 规格粒度键（无 factory 列；step_id 全局唯一隐含厂别） |
| `usl` | 规格上限（如 M626 12620 TDSUM usl=770） |
| `ucl` | 管控上限（如 ucl=500） |
| `lsl`/`lcl`/`target`/`cl`/`sigma_*` | TT 为越小越好型，不用 |
| `param_type` | TT 参数恒为 NULL（即 §1-A 的识别规则） |

样例（M626）：`11620 TDSUM usl=618 ucl=468`、`12620 TDSUM usl=770 ucl=500`、`13620 TDSUM usl=1308 ucl=600`、`21320 DSUM_L/DSUM_O/TOTAL_O_L usl=1700 ucl=1300`、`43620 TDSUM usl=4999 ucl=4500`。
8 个产品（M626/M673/M678/Z517/Z553/Z559/Z571/Z576）× 7 站点均有规格行。

### E. 产品字典 ✅

表：`mdw.dwr_mes_productspec`（productspecname → productcode），RS/SPC 链路同款 join：

```sql
JOIN mdw.dwr_mes_productspec P ON T.product_spec = P.productspecname
WHERE P.productcode = :prod_code
```

### F. Particle Size（ARRAY/TP）✅

| 模式 | 厂别 | 数据源与口径 |
| --- | --- | --- |
| 两种模式共同 | 三厂 | `Total` 始终来自 SPC `param_value`，并先完成单片三态修饰 |
| 比例生成（默认） | ARRAY/TP | 按“比例规格表”的站点 S/M/L/H 比例，对每片 Total 做稳定扰动后分配；同一业务键结果不变，四档合计等于 Total |
| 实表 | ARRAY | `eda.ARRAY_DEFECT_T.item119`，保留 `item51='AOI'` 且粒径属于 S/M/L/H，每条缺陷事实计 1 |
| 实表 | TP | `eda.TSP_DEFECT_T.item2`，粒径属于 S/M/L/H，每条缺陷事实计 1；`cut_id` 对齐 SPC `glass_id` |
| 两种模式共同 | OLED | 暂不区分 Particle Size，仅展示 Total |

实表模式下，产品通过各厂 SPC 表的 `product_spec` 再映射 `mdw.dwr_mes_productspec`。SPC 同一 Sheet 存在多条参数记录，因此必须先取得唯一的 Sheet/Glass 到产品映射，再连接缺陷事实；不能对连接后的缺陷事实去重，也不能直接连接完整 SPC 明细。

实表 S/M/L/H 按 `(product, sheet, step, particle_size)` 汇总；没有对应粒径缺陷时补 0。比例生成模式若某站点规格缺失或无效，该站点保持 Total-only，不借用其他站点比例。

## 2. 与任务文档/RS 报表的偏差记录（开发注意）

1. **任务文档字段描述笔误**：文档称 `param_name` 为"RS Code代码"，实测 TT 明细表的 `param_name`
   是 TT 参数名（TDSUM/DSUM_L/DSUM_O），与 RS Code 无关（RS Code 在 `spc_tzbjx_rs_*` 表的 `rs_code` 列）。
2. **趋势分母不用过货视图**：`eda.spot_eda_*_view_*` 中**不存在** AOI 测量站点记录
   （ARRAY 视图无 xx620、OLED 无 21320、TP 无 43620；视图里 xx629/21329/43629 是 RS 站点）。
   改用测量表自身 distinct sheet 作分母（AOI 检测片数）。因此 TT 报表**无需加载过货明细**，
   比 RS 少一路查询；趋势图柱状图语义为"检测片数"。
3. **规格无 type_flag 维度**：RS 规格按图类型区分（MWD_RATIO/LOT_RATIO/SHEET_ID），
   TT 规格为 (prod,step,param) 唯一 USL/UCL，三张图共用同一组上限线。
4. **OLED 站点一个、TT 参数两个**：21320 站点下 DSUM_L/DSUM_O 并存（粒度=站点×TT 的必要性）；
   spec 表的 `TOTAL_O_L` 近 40 天无测量数据，指标列表由明细驱动，自然不出现。
5. **param_value 直接即个数**：每 (step,sheet,param) 恰 1 行，By Sheet 值 = param_value 本身；
   只有 ARRAY 同 sheet 跨多站点出现多行，属正常（站点是粒度的一部分）。

## 3. 探查资产

- 探查脚本：`.scratch/probe_aoi_tt.py`（主）、`.scratch/probe_aoi_tt_supp.py`（TT 识别规则）、`.scratch/probe_aoi_tt_supp2.py`（分母口径）
- 原始输出：`.scratch/probe_aoi_tt_result.md`、`.scratch/probe_aoi_tt_supp_result.md`、`.scratch/probe_aoi_tt_supp2_result.md`
- 已知坑（沿用 RS 结论）：`pd.read_sql` 直传含 `%` 的 SQL 须用 `sqlalchemy.text()`。

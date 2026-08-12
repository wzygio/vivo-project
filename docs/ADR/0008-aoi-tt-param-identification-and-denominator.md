# ADR-0008：AOI_TT 报表的 TT 参数识别规则与趋势分母口径

- Status: Accepted
- Date: 2026-08-10
- Scope: `src/inline_domain/{infrastructure,application,core}/aoi_tt/`、`app/sections/aoi_tt/`、`app/pages/AOI_TT监控报表.py`

## Context

任务 `docs/dev_docs/dev_prompt/feat-AOI_TT.md` 要求仿照 AOI_RS 报表（ADR-0007）制作 AOI Total Defect（TT）报表，样式与 AOI_RS 完全一致。开发前已对 Greenplum 实库三轮探查（证据：`.scratch/probe_aoi_tt*_result.md`，结论固化于 `references/domain/aoi_tt/spec-data_source.md`），发现两处无法照搬 RS 链路的事实：

1. **TT 明细混在 SPC 测量大表中**。`eda.spc_tzbjx_{array,oled,tsp}` 承载全部 SPC 参数（SE_L1T、CD1、OVL、PPA…），TT 参数（TDSUM / DSUM_L / DSUM_O）只是其中一小部分，必须有过滤规则。任务文档所称"param_name：RS Code代码"为笔误，TT 明细表与 RS Code 无关。
2. **过货视图不含 AOI 站点记录**。`eda.spot_eda_*_view_*` 中不存在 AOI 测量站点（ARRAY xx620、OLED 21320、TP 43620；视图中的 xx629/21329/43629 是 RS 站点），RS 报表的"过货视图 distinct sheet"分母口径对 TT 不可用。
3. 规格表 `mdw.dwd_imp_dv_param_spec`（SPC 同款）无 factory 列、无 type_flag 维度；TT 规格为 (prod_code, step_id, param_name) 唯一的 usl/ucl，且全部 TT 规格行的 `param_type` 恒为 NULL（72 行探查样本全为 TT 参数）。

## Decision

1. AOI_TT 镜像 AOI_RS 五层子模块链路：`infrastructure/aoi_tt/data_loader.py` → `application/aoi_tt/aoi_tt_service.py`（缓存 payload → ViewModel，ADR-0001）→ `core/aoi_tt/aoi_tt_calculator.py` → `app/sections/aoi_tt/aoi_tt_dashboard.py` → `app/pages/AOI_TT监控报表.py`。
2. **TT 参数识别规则**：以规格表 `mdw.dwd_imp_dv_param_spec` 中 `param_type IS NULL` 的 (step_id, param_name) 组合为 TT 参数全集，先查参数集、再按组合过滤明细查询。该规则与测量表实际出现的 `%SUM%` 参数交叉验证一致（探查 §1-A）。
3. 三厂明细以三元组 `(表名, ID列, 时间列)` 抹平 sheet/glass 差异后 UNION ALL，统一逻辑列为 `factory, prod_code, start_time, sheet_id, lot_id, step_id, tt_name, tt_qty`；产品过滤统一 `product_spec` JOIN `mdw.dwr_mes_productspec`（同 ADR-0007）。
4. **趋势分母 = TT 测量表自身按 (factory, step_id, period) 的 `COUNT(DISTINCT sheet_id)`（AOI 检测片数）**。依据：TDSUM/DSUM 为每片必测项，每 (step, sheet, param) 恰一行（探查验证每日 distinct sheet == 行数）。因此 TT 报表不加载过货视图，比 RS 少一路查询；趋势图柱状图语义为"检测片数"。
5. 月周天趋势值 = period 内 Σtt_qty ÷ 同 period 同站点检测 distinct sheet 数；分母 0 → NaN 不除零；period 轴复用 `build_available_period_axis`（跳过空值向前补全）。
6. 规格线取 `usl`/`ucl` 两条上限（越小越好型，不用 lsl/lcl），按 (step_id, tt_name) 匹配（规格表无 factory 列，step_id 全局唯一隐含厂别），三张图共用；无规格不画线（NaN 降级）。
7. 时间窗固定为上一自然月 1 日 ~ 当前日期，页面无时间筛选控件；筛选框（厂别/站点/Code名称=TT 参数名）与查询门控交互同 AOI_RS。

## Alternatives considered

- 按 `%SUM%` 命名模式识别 TT 参数：拒绝。命名规则是隐式约定（TOTAL_O_L 即反例），规格表 `param_type IS NULL` 是数据驱动的显式规则，且顺带提供规格。
- 按 AOI 站点清单（xx620/21320/43620）硬编码过滤：拒绝。站点清单同样需要硬编码且随产品/工艺变化，不如规格表驱动。
- 分母改用 RS 站点（xx629）过货视图计数：拒绝。RS 站点过货与 AOI 检测片数口径不同（探查显示同站点量级不一致），测量表自身分母数学口径干净（平均每片 TT 个数）。
- 复用 AOI_RS 模块加参数开关：拒绝。两个报表的数据源、规格结构、分母口径均不同，独立子模块避免语义耦合（同 ADR-0007 的决策逻辑）。

## Consequences

- 正面：TT 参数与规格同源（同一张规格表驱动），新增 TT 参数/站点/产品零代码接入；分母自洽无需跨表对齐；period 切分与 SPC/RS 同一实现。
- 负面/后续约束：
  - `param_type IS NULL` 作为 TT 识别规则依赖规格表维护惯例；若未来其它 param_type 为 NULL 的非 TT 参数入表会混入报表，届时需收紧规则（如增加参数名白名单）。
  - 规格表 `TOTAL_O_L` 无对应测量数据（疑 DSUM_L 旧名），当前由明细驱动指标列表自然不出现；若历史数据需要展示需另做映射。
  - 任务文档的字段描述笔误已在 `references/domain/aoi_tt/spec-data_source.md` §2 登记，后续以 spec 为准。

## Traceability

- Issue: `.scratch/aoi-tt-report/issues/01-create-aoi-tt-report.md`
- Plan: `.planning/2026-08-10-aoi-tt-report/`
- 数据源 spec: `references/domain/aoi_tt/spec-data_source.md`
- 关键代码: `src/inline_domain/infrastructure/aoi_tt/data_loader.py`、`src/inline_domain/core/aoi_tt/aoi_tt_calculator.py`、`src/inline_domain/application/aoi_tt/aoi_tt_service.py`、`app/sections/aoi_tt/aoi_tt_dashboard.py`、`app/pages/AOI_TT监控报表.py`、`app/static/config.js`（门户注册 AOI_TT_REPORT）
- 测试: `tests/unit/inline_domain/{infrastructure,application,core}/aoi_tt/`、`tests/unit/app/sections/aoi_tt/`、`tests/unit/app/pages/test_aoi_tt_page.py`（新增 31 项全绿；inline_domain+app 171 passed；全量 319 passed / 5 个既有跨域失败与 RS 交付登记一致）
- E2E: `tests/e2e/aoi_tt_report.js`（playwright-cli，localhost:8503，通过），截图 `output/screenshots/aoi_tt_e2e.png`

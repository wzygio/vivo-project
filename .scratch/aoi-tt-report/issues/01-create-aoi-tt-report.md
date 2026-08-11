# 创建 AOI_TT 监控报表

Category: `enhancement`  
Status: `ready-for-agent`  
Created: `2026-08-10`  
Source: `docs/dev_docs/dev_prompt/feat-AOI_TT.md`  
Related: `references/domain/aoi_tt/spec-data_source.md`（数据源已探查确认）、`references/domain/aoi_rs/spec-data_source.md`、`docs/ADR/0001-streamlit-cache-native-payload-boundary.md`、`docs/ADR/0007-aoi-rs-report-data-source-and-counting.md`、AOI_RS 报表全链路

## Current context

- AOI_RS 报表已于 2026-08-10 交付（页面 → `AoiRsReportService` → `data_loader` → Dashboard），AOI_TT 与其"样式完全一致"，镜像该链路即可。
- 任务文档 Workflow 前三步已完成：逻辑链分析、数据需求梳理、数据库实探（3 轮探查），结论固化在 `references/domain/aoi_tt/spec-data_source.md`。
- 数据源全部确认：
  - TT 明细 `eda.spc_tzbjx_{array,oled,tsp}`（SPC 测量明细表，含 `lot_id`、`param_name`、`param_value`、`step_id`、`product_spec`）；
  - TT 参数识别规则 = 规格表 `mdw.dwd_imp_dv_param_spec` 中 `param_type IS NULL` 的 (prod,step,param) 组合（72 行全为 TDSUM/DSUM_L/DSUM_O/TOTAL_O_L，与测量表 `%SUM%` 参数完全对应）；
  - 规格 = 同表 `usl`/`ucl`（粒度 prod+step+param，三张图共用）；
  - 趋势分母 = 测量表自身 distinct sheet/glass（过货视图不含 AOI 站点 xx620/21320/43620 记录，已验证 TDSUM/DSUM 每片必测，distinct sheet 即检测片数）；
  - 产品字典 `mdw.dwr_mes_productspec`。
- 月周天"跳过空值向前补全"切分复用 `build_available_period_axis`；缓存边界遵循 ADR-0001。

## Desired outcome

- 新增独立 AOI_TT 报表页面（`app/pages/AOI_TT监控报表.py`），Streamlit 自动发现，Header/产品切换/缓存刷新与 AOI_RS 页面一致。
- 固定时间窗"上一自然月 1 日 ~ 当前日期（含当天）"，无时间筛选框。
- 三张图（与 AOI_RS 布局一致，expander 按 站点×TT 展开、三图并列）：
  1. By 月周天趋势图：值 = Σ`param_value` ÷ 同 period 同站点 distinct sheet/glass 数（检测片数），按 TT 参数分线，叠加 USL/UCL 两条上限线，双 Y 轴检测片数柱状。
  2. By Lot 点线图：每 Lot Σ`param_value`，叠加 USL/UCL。
  3. By Sheet 点线图：每 sheet/glass 的 TT 个数，叠加 USL/UCL。
- 筛选框：厂别、站点、Code 名称（TT 参数名），查询按钮门控与 AOI_RS 一致。
- 颗粒度：产品型号 → 厂别 → 站点 → TT → TT 个数（明细）。

## Acceptance criteria

- [x] 页面可独立发现并打开，无时间筛选框，窗口为上一自然月 1 日至当前日期；门户 `resources/static/config.js` 注册。
- [x] TT 明细查询由规格表 `param_type IS NULL` 组合驱动（(step_id, param_name) 过滤），三厂 UNION ALL + 产品字典 join。
- [x] 月周天趋势：分母为测量表自身 distinct sheet（分母 0 → NaN 不除零）；period 轴跳过空值向前补全；USL/UCL 正确叠加。
- [x] By Lot / By Sheet 聚合正确；每 (step,sheet,param) 单行时 By Sheet 值 = param_value 本身。
- [x] 筛选级联与查询门控行为与 AOI_RS 一致；空数据/无规格安全降级。
- [x] 缓存只返回原生 payload（ADR-0001）。
- [x] RED→GREEN 测试覆盖 DAO 契约、聚合、规格匹配、筛选门控、页面渲染；inline_domain 回归无新增失败；Streamlit E2E 烟测通过。

## Out of scope

- 不修改 AOI_RS/SPC/CTQ 报表行为与共享 period 切分语义。
- 不新增数据库表；不重新定义 `param_type IS NULL` 之外的业务语义。
- 不处理 `TOTAL_O_L` 历史命名迁移（无测量数据自然不出现）。

## Questions to resolve

- [x] TT 参数识别：规格表 `param_type IS NULL`（72 行全为 TT 参数）↔ 测量表 `%SUM%` 参数交叉验证一致，采用规格表驱动。
- [x] 趋势分母：过货视图无 AOI 站点记录 → 测量表自身 distinct sheet（每片必测已验证）。
- [x] 规格：USL/UCL 取自 `mdw.dwd_imp_dv_param_spec`，无 type_flag 维度，三图共用。
- [x] 任务文档笔误（`param_name` 非"RS Code代码"）已在 spec §2-1 记录。

## Agent Brief

**Category:** enhancement
**Summary:** 镜像 AOI_RS 链路新增 AOI_TT 监控报表，数据源为三张 SPC 测量明细表（TT 参数子集）+ 参数规格表 USL/UCL，全部已探查确认。

**Current behavior:** 系统有 AOI_RS 复判报表，但 AOI Total Defect（拍照缺陷总数）无报表入口。TT 数据以 `TDSUM`/`DSUM_L`/`DSUM_O` 参数存在于 `eda.spc_tzbjx_{array,oled,tsp}`，规格在 `mdw.dwd_imp_dv_param_spec`（`param_type IS NULL` 行）。

**Desired behavior:** 独立 AOI_TT 页面，固定窗口（上一自然月 1 日~当前），三张图（月周天趋势=Σparam_value÷检测片数、By Lot、By Sheet），USL/UCL 双上限线，筛选（厂别/站点/Code名称=TT 参数名）+ 查询门控，expander 按站点×TT 展开三图并列，与 AOI_RS 视觉完全一致。

**Key interfaces:**

- `AoiTtQueryConfig`：prod_code + start/end 日期（固定窗），可选 factory/step_id/tt_name。
- 厂别三元组：ARRAY→(`spc_tzbjx_array`,`sheet_id`,`sheet_start_time`)；OLED→(`spc_tzbjx_oled`,`glass_id`,`glass_start_time`)；TP→(`spc_tzbjx_tsp`,`glass_id`,`glass_start_time`)。
- TT 参数集：先查规格表 `param_type IS NULL` 得 (step_id, param_name) 组合，再据此过滤明细；产品过滤统一 `JOIN mdw.dwr_mes_productspec ON productspecname = product_spec`。
- 分母：明细表自身 `COUNT(DISTINCT sheet_id)` 按 period+站点；无过货视图查询。
- 规格：`usl`/`ucl` 按 (prod,step,param) 左连接，NaN 不画线。
- Period 轴：复用 `build_available_period_axis`（入参列名 sheet_start_time）。
- 统一逻辑模型：factory / prod_code / start_time / sheet_id / lot_id / step_id / tt_name / tt_qty。

**Edge cases:**

- 分母 0（period 无数据）→ 该 period 跳过（轴由明细可用性决定，天然规避）。
- 无 USL/UCL（NaN）不画线；空数据/空筛选显示明确空状态。
- `TOTAL_O_L` 有规格无测量 → 指标由明细驱动，不出现。
- OLED 单站点双 TT 参数（DSUM_L/DSUM_O）→ 粒度为站点×TT，各自一条线。

**Out of scope:** 修改 AOI_RS/SPC/CTQ 行为；新增数据库表；共享 period 函数语义变更。

**Verification:**

- RED→GREEN 单测：DAO（sqlite ATTACH 契约）、period 比值与分母、USL/UCL 匹配、筛选门控、页面渲染。
- `tests/unit/inline_domain` 回归无新增失败；全量单测记录既有失败基线。
- playwright-cli 烟测（localhost:8503）：页面打开、筛选级联、查询门控、三图渲染、产品切换。

## Comments

### 2026-08-10 — Triage outcome

任务文档 Workflow 前三步完成，3 轮探查关闭全部数据缺口（脚本 `.scratch/probe_aoi_tt*.py`，输出 `.scratch/probe_aoi_tt*_result.md`）。两处与 RS 链路的实质偏差（TT 识别规则、分母口径）已记录于 spec §2 并有探查证据支撑。用户在任务文档中明确授权按 development-flow 执行到底。状态：`ready-for-agent`。

### 2026-08-10 — AOI_TT delivery evidence

已按 development-flow 模块 3 完成全栈交付：infrastructure（`data_loader.py`，TT 参数集/明细/规格加载）、core（`aoi_tt_calculator.py`，趋势自含分母/throughput/By Lot/By Sheet/USL-UCL 匹配）、application（`aoi_tt_service.py`，缓存 payload→ViewModel）、Dashboard（筛选级联+查询门控+三图，USL/UCL 双虚线）、页面（`AOI_TT监控报表.py`）与门户注册（config.js `AOI_TT_REPORT`）。

测试证据：新增 31 项单测一次全绿（sqlite ATTACH DAO 契约 + 聚合/规格/筛选门控/页面）；`tests/unit/inline_domain + tests/unit/app` 171 passed（基线 72 无回归）；全量 319 passed / 5 failed（与 AOI_RS 交付时登记的同一批既有跨域失败：code_selector×2、compliance_xlsx×1、yield_global_data_policy×2；`test_shadow_ema` 收集错误同为既有问题排除）。

E2E（playwright-cli，localhost:8503，`tests/e2e/aoi_tt_report.js`）：页面发现/打开、厂别→站点→Code 级联与查询门控、ARRAY 11620 TDSUM 三图渲染（月周天趋势双轴+检测片数柱、By Lot、By Sheet）、USL=618/UCL=468 规格线全部通过；截图 `output/screenshots/aoi_tt_e2e.png`。

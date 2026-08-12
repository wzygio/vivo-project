# ADR-0007：AOI_RS 报表的数据源映射与计数口径

- Status: Accepted
- Date: 2026-08-10
- Scope: `src/inline_domain/{infrastructure,application,core}/aoi_rs/`、`app/sections/aoi_rs/`、`app/pages/AOI_RS监控报表.py`

## Context

任务 `docs/dev_docs/dev_prompt/feat-AOI_RS.md` 要求仿照 SPC 监控报表制作 AOI_RS 报表（月周天趋势 / By Lot / By Sheet 三张图）。任务文档给出的表名与字段清单部分失准，开发前已对 Greenplum 实库探查（证据：`.scratch/probe_aoi_rs_result.md`，结论固化于 `references/domain/aoi_rs/spec-data_source.md`）：

1. 任务文档所写 TP 过货视图 `eda.spot_eda_tsp_view_gls_v` 不存在；eda schema 命名规律为 `spot_eda_{array|oled|tp}_view_{sht|gls}_v`，TP 正确视图是 `eda.spot_eda_tp_view_gls_v`。
2. RS 明细表 `eda.spc_tzbjx_rs_array` 的 `productcode` 列全空（OLED/TP 部分有值），无法用于产品过滤；三表均有 `product_spec` 与任务文档未提及的 `lot_id`（By Lot 图必需）。
3. `code_qty` 并非文档所述"取值为 1"：实测 0~65，OLED 约 40% 记录为 0。
4. 规格表 `dwd_imp_rs_code_xishu_fo_tzsbjx` 实际位于 `mdw` schema，且按 `type_flag`（MWD_RATIO / LOT_RATIO / SHEET_ID / GLASS_ID）区分规格适用的图类型，`code_desc` 提供 Code 中文名。

同时，RS 数据量（月级 ≤10 万行）远小于 SPC 测量明细，SPC 链路的 parquet 快照机制（8h TTL + 策略版本戳）对此报表属于过度设计。

## Decision

1. AOI_RS 采用与 SPC/CTQ 同构的五层独立子模块链路：`infrastructure/aoi_rs/data_loader.py` → `application/aoi_rs/aoi_rs_service.py`（缓存 payload → ViewModel，遵循 ADR-0001）→ `core/aoi_rs/aoi_rs_calculator.py` → `app/sections/aoi_rs/aoi_rs_dashboard.py` → `app/pages/AOI_RS监控报表.py`。
2. 三厂 RS 明细以三元组 `(表名, ID列, 时间列)` 抹平 sheet/glass 差异后 UNION ALL，统一逻辑列为 `factory, prod_code, start_time, sheet_id, lot_id, step_id, rs_code, code_qty`。
3. 产品过滤统一走 `product_spec` LEFT JOIN `mdw.dwr_mes_productspec`（productspecname → productcode）；**不使用** RS 表自带的 `productcode` 列。join 不上的脏 product_spec（如 `'1'`, `'55'`）自然排除。
4. 分母使用 `eda.spot_eda_{array,oled|tp}_view_{sht,gls}_v`（TP 为 `tp` 非 `tsp`），按同站点同时间窗 `COUNT(DISTINCT id)`。
5. 月周天趋势口径：值 = period 内 Σcode_qty ÷ 同 period 同站点过货 distinct sheet/glass 数；分母为 0 记 NaN 不除零。period 轴复用 `build_available_period_axis`（跳过空值向前补全，最近 2 月/3 周/7 天），不重写切分逻辑。
6. 规格线按图类型取 `type_flag`：月周天→`MWD_RATIO`，By Lot→`LOT_RATIO`，By Sheet→`SHEET_ID`（ARRAY）与 `GLASS_ID`（OLED/TP），作单边上限线；`code_desc` 作为筛选框"Code名称"与图例的显示源。无规格的 Code 不画线（NaN 降级）。
7. `code_qty` 按 `SUM` 加和口径（含 0 值行自然计入分子），不做"恒为 1"假设，不对 rs_code 做长度假设（存在 6 位个体）。
8. 不引入 parquet 快照；`st.cache_data`（max_entries=3）直查库，payload 仅含 DataFrame。
9. 时间窗固定为上一自然月 1 日 ~ 当前日期（`get_period_window_start`），页面不提供时间筛选控件。

## Alternatives considered

- 复用 `SpcRepository` + `data_type_filter="AOI"`：拒绝。`SpcQueryConfig.data_type_filter` 的 AOI 口子面向的是 SPC 测量大宽表（`spc_tzbjx_*` 连续量测值），与 RS 明细表（计数型、含 lot_id、独立规格表）结构不同，强行复用会把两套语义耦合进同一仓储。
- 使用 RS 明细表自带 `productcode` 过滤：拒绝。array 表该列全空，三厂口径无法统一。
- 按"每条记录算一个"（COUNT 行数）而非 SUM(code_qty)：拒绝。实测 code_qty 分布 0~65，COUNT 会丢失每行点数且把 0 值行计为 1；任务文档明确"可以直接对 code_qty 加和"。

## Consequences

- 正面：三厂口径完全统一，新增厂别只需扩充三元组字典；规格/码表/分母/分子数据源全部经实库验证；period 切分与 SPC 保持同一实现，语义一致。
- 负面/后续约束：
  - `code_qty=0` 行的业务语义（复判无缺陷？）未经业务确认，当前按加和口径处理；若业务重定义，仅需调整 data_loader 过滤。
  - 规格线与图类型通过 `type_flag` 硬映射耦合；规格表新增 type_flag 时需同步 `SPEC_TYPE_BY_CHART`。
  - ~~By Lot/By Sheet 指标按任务文档取"个数"（Σcode_qty），而规格名 LOT_RATIO 暗示比率语义；若后续确认规格为比率口径，需同步调整指标计算。~~ **已于 2026-08-11 兑现并修正**：By Lot 改为 Lot 内平均每片（Σcode_qty ÷ 该 Lot 同站点过货 distinct sheet/glass 数，分母 0 记 NaN），与 LOT_RATIO 比率语义对齐；By Sheet 保持每片个数（对应 SHEET_ID/GLASS_ID 规格）。
  - 任务文档的 TP 视图笔误已在 `references/domain/aoi_rs/spec-data_source.md` 第 2 节登记，后续报表引用文档时以 spec 为准。

## Traceability

- Issue: `.scratch/aoi-rs-report/issues/01-create-aoi-rs-report.md`（ready-for-agent，验收全勾选）
- Plan: `.planning/2026-08-10-aoi-rs-report/`（task_plan / findings / progress）
- 数据源 spec: `references/domain/aoi_rs/spec-data_source.md`
- 关键代码: `src/inline_domain/infrastructure/aoi_rs/data_loader.py`、`src/inline_domain/core/aoi_rs/aoi_rs_calculator.py`、`src/inline_domain/application/aoi_rs/aoi_rs_service.py`、`app/sections/aoi_rs/aoi_rs_dashboard.py`、`app/pages/AOI_RS监控报表.py`
- 测试: `tests/unit/inline_domain/{infrastructure,application,core}/aoi_rs/`、`tests/unit/app/sections/aoi_rs/`、`tests/unit/app/pages/test_aoi_rs_page.py`（新增 21 项全绿；inline_domain 69 passed；全量 283 passed / 5 个既有跨域失败登记于 progress.md）
- UI QA: `output/screenshots/aoi_rs_*.png`（ARRAY/OLED 两链路、门控、窄视口、视觉缺陷修复复验）

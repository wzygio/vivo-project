# Findings — SPC 主制程设备/腔室追溯

## Requirements

- Task2 只优化现有第二幅图：从检测站点量测 `unit_id` 改为参数对应主制程站点的过货设备/腔室。
- 时间窗、点位明细粒度和厂别/站点/参数筛选保持现状。
- 必须补全 `references/domain/spc/spec-data_source.md` 并完成 E2E。

## Current implementation

- 页面通过 `SpcReportService` 一次加载量测、规格、Sheet 特征和能力结果。
- `load_spc_measurements` 统一三厂量测表，当前输出量测 `unit_id`。
- `load_spc_spec_limits` 当前仅输出规格线，尚未输出 `main_step_id/main_eqp_type`。
- 第二幅图 `_create_sheet_points_box_chart(..., sort_mode="按腔室排序")` 使用 `_resolve_chamber_column`，首选 `unit_id`，再由 `_derive_chamber_label` 截取首个连字符前缀；这是检测量测设备近似值。
- 第三幅图按 `sheet_start_time` 排序，与主站点追溯无关。

## Verified data sources

- 所需表均位于 `mdw` schema：`dwd_imp_dv_param_spec`、`dwt_inout_sht`、`dwt_inout_sub_unit_sht`、`dwt_inout_unit_sht`、`dwt_inout_gls`、`dwt_inout_sub_unit_gls`、`dwd_mes_oled_oper_layer_v`。
- 规格三元组 `prod_code + step_id + param_name`：1352 行 / 1352 个 distinct key，无重复。
- 规格路由全库：CHAMBER 440 行、EQP 904 行、空 8 行；空值按当前站点/EQP 回退。
- 配置产品 M626/M673/M678/Z517/Z571 均有规格数据；除 Z517 外均同时含 EQP/CHAMBER。
- `event_timekey` 为 20 位数字时间键，前 14 位是 `YYYYMMDDHHMMSS`；`date_timekey` 可利用履历表主键首列索引限制窗口。

## Route contract

| Factory | Route | Source | Value / priority |
|---|---|---|---|
| ARRAY | EQP | `dwt_inout_sht` | `eqp_id`，缺失回退量测 `unit_id` |
| ARRAY | CHAMBER | `dwt_inout_sub_unit_sht` then `dwt_inout_unit_sht` | `sub_unit_id` 优先、`unit_id` 次之 |
| OLED | EQP | `dwt_inout_gls` | `factory LIKE 'OLED%'`，取 `eqp_id`，缺失回退量测 `unit_id` |
| TP | EQP | `dwt_inout_gls` | `factory='TP'`，取 `eqp_id`，缺失回退量测 `unit_id` |
| TP | CHAMBER | `dwt_inout_sub_unit_gls` | `sub_unit_id` |
| OLED | CHAMBER | `dwt_inout_sub_unit_gls` + `dwd_mes_oled_oper_layer_v` | `sub_unit_id`，CVD1~4 可归一到 CVD |

## Probe evidence

- M626 最近报表窗口的去重点位路由抽样：ARRAY EQP 3835、ARRAY CHAMBER 361、OLED EQP 2567、OLED CHAMBER 416、TP EQP 780、TP CHAMBER 164。
- CHAMBER 三厂匹配覆盖均为 100%；ARRAY EQP 最近先前 OUT 覆盖 99.2%。OLED/TP EQP 存在历史缺口，因此需要北极星既有量测设备回退。
- 履历表规模很大；实现必须按日期、物料 ID、主站点和 OUT 条件下推，不能全表拉取。

## Technical decisions

| Decision | Rationale |
|---|---|
| 先加载量测，再按去重物料/主站点批量查询履历并在 Python 选择最近先前 OUT | 避免 SQL 多对多放大，并能明确测试选择规则与回退。 |
| CHAMBER 缺失不回退检测 `unit_id` | 检测设备不等同主站点腔室，伪回退会产生错误业务标签。 |
| 保留 trace source/time 字段 | 让 UNKNOWN、回退和具体履历来源可诊断、可测试。 |
| 提升 snapshot policy version 并检查追溯列 | 防止旧快照静默继续用检测站点腔室。 |

## Resources

- `.scratch/spc-main-process-chamber/issues/01-trace-main-process-equipment-chamber.md`
- `docs/dev_docs/dev_prompt/feat-SPC_CPM.md`
- `docs/dev_docs/北极星-过货腔室/北极星-过货腔室_SQL解析报告.md`
- `docs/dev_docs/北极星-过货腔室/long_text_BA373569-2FFE-4848-AC4D-363C2B462531.txt`
- `docs/ADR/0001-streamlit-cache-native-payload-boundary.md`

## Visual/browser findings

- OLED/21200 实际渲染的主站点图例包含 `3AFC12-CVD-CHB`、`3AFC16-CVD-CHC`、`3CEE02-BLF` 等完整主设备/腔室 ID，没有使用检测站点前缀。
- 同一查询可见 3 张 `By主站点设备/腔室` 图和 2 张 `By过货时间` 图，证明第二图替换未移除第三图。
- 1440×1000 与 900×900 均无水平溢出；页面无 Streamlit traceback 或重复 Plotly ID。
- 控制台仅有 5 条 Streamlit 自身基础设施噪声：嵌套路由 `_stcore/health`/`host-config` 404 与 `data.streamlit.io` metrics 被环境阻止；精确豁免后无业务错误。
- M626 运行快照共 743503 行，五个追溯字段齐全；仅 9 行正式 CHAMBER 无匹配并按契约保留 `UNKNOWN/unmatched_chamber`。

# SPC Task2 主制程设备/腔室数据源规格

## 1. 目标与展示边界

本规格服务于 `SPC监控报表` 的第二幅图“Sheet 点位分布 By 主站点设备/腔室”。图表保持现有点位箱线图/点线图样式、规格线和 Sheet 颗粒度，仅把原先来自检测量测 `unit_id` 的近似腔室，替换为参数规格所指定主制程站点的正式 OUT 过站设备/腔室。

- 固定报表时间：上一个自然月 1 日至当前日期（含当天）。
- 履历追溯窗口：为覆盖量测前发生的主工序，按被加载量测窗口的开始日前推 1 个月，结束于报表结束日。
- 展示颗粒度：产品型号 → 厂别 → 检测站点 → 参数名称 → 点位测量值。
- 规格唯一键：`prod_code + step_id + param_name`。
- 第二幅图分组：`main_process_unit_id`；第三幅图仍按 `sheet_start_time`。

## 2. 展示与计算字段

| 逻辑字段 | 含义 | 数据源 | 源字段/规则 |
|---|---|---|---|
| `prod_code` | 产品型号 | `mdw.dwr_mes_productspec` + 三厂量测表 | `product_spec → productspecname → productcode` |
| `factory` | 厂别 | 三厂量测分表映射 | ARRAY/OLED/TP 常量 |
| `sheet_id` | 生产载体 | `eda.spc_tzbjx_array/oled/tsp` | ARRAY=`sheet_id`；OLED/TP=`glass_id` 统一命名 |
| `step_id` | 检测站点 | 三厂量测表 | `step_id` |
| `param_name` | 参数名称 | 三厂量测表 | `param_name` |
| `site_name` | 点位 | 三厂量测表 | `site_name` |
| `param_value` | 点位测量值 | 三厂量测表 | `param_value` 数值化后剔除无效值 |
| `sheet_start_time` | 量测时间/第三幅图排序时间 | 三厂量测表 | ARRAY=`sheet_start_time`；OLED/TP=`glass_start_time` |
| `unit_id` | 检测量测设备 | 三厂量测表 | 只允许作为 EQP 履历缺失回退，不作为 CHAMBER 回退 |
| `usl/lsl/ucl/lcl` | 规格/管控线 | `mdw.dwd_imp_dv_param_spec` | 按规格唯一键匹配 |
| `main_step_id` | 主制程站点 | `mdw.dwd_imp_dv_param_spec` | 空/空白时回退当前 `step_id` |
| `main_eqp_type` | 主站点展示路由 | `mdw.dwd_imp_dv_param_spec` | `EQP/CHAMBER`；空或非法值回退 `EQP` |
| `main_process_unit_id` | 第二幅图设备/腔室标签 | 下表列出的过站履历 | 最近先前 OUT；EQP 缺失回退 `unit_id`，CHAMBER 缺失=`UNKNOWN` |
| `main_process_event_time` | 被选中的主工序过站时间 | 履历 `event_timekey` | 20 位时间键取前 14 位按 `YYYYMMDDHHMMSS` 解析 |
| `main_process_trace_source` | 追溯来源/回退诊断 | 应用生成 | 具体履历表语义、`measurement_unit_fallback` 或 `unmatched_chamber` |

## 3. 厂别与路由表

| 厂别 | `main_eqp_type` | OUT 履历源 | 设备/腔室字段 | 来源标记与优先级 |
|---|---|---|---|---|
| ARRAY | EQP | `mdw.dwt_inout_sht` | `eqp_id` | `array_sht`, rank 1 |
| ARRAY | CHAMBER | `mdw.dwt_inout_sub_unit_sht` | `sub_unit_id` | `array_sub_unit_sht`, rank 1 |
| ARRAY | CHAMBER fallback | `mdw.dwt_inout_unit_sht` | `unit_id` | `array_unit_sht`, rank 2 |
| OLED | EQP | `mdw.dwt_inout_gls` | `eqp_id` | `oled_gls`, rank 1 |
| TP | EQP | `mdw.dwt_inout_gls` | `eqp_id` | `tp_gls`, rank 1 |
| TP | CHAMBER | `mdw.dwt_inout_sub_unit_gls` | `sub_unit_id` | `tp_sub_unit_gls`, rank 1 |
| OLED | CHAMBER | `mdw.dwt_inout_sub_unit_gls` JOIN `mdw.dwd_mes_oled_oper_layer_v` | `sub_unit_id` | `oled_sub_unit_gls`, rank 1 |

ARRAY/TP 腔室沿用北极星命名约束：

- `split_part(sub_unit_id, '-', 2) IN ('CVD','SPU','DRE','OVE')`
- `substr(sub_unit_id, 8, 6) IN ('DRE-PC','CVD-CH','OVE-CH','SPU-PM')`
- ARRAY 单元回退：`substr(unit_id, 8, 2) = 'CH'`

OLED CHAMBER 先以履历 `oper_code + sub_unit_id` 连接工序映射；当 `new_oper` 为 `21200-CVD1`～`21200-CVD4` 且映射腔室满足 CH 命名时，将主工序归一为 `21200-CVD`，否则使用 `new_oper`。

## 4. 最近 OUT 选择与行数不变量

1. 所有履历只读取 `inout_type='OUT'`。
2. SQL 下推 `date_timekey`、目标物料 ID 集合和目标 `main_step_id` 集合；OLED CHAMBER 在工序归一化后再限制目标主站点。
3. 候选按 `factory + main_eqp_type + sheet_id + main_step_id` 关联量测。
4. 只保留 `main_process_event_time <= sheet_start_time` 的候选。
5. 每个量测追溯目标先按来源优先级升序，再按履历时间降序，选第一条。
6. 多点位共享同一物料/主站点/量测时间的追溯结果；最终合回原点位明细时必须保持输入顺序和行数。
7. 没有候选时：EQP 使用检测量测 `unit_id` 并标记 `measurement_unit_fallback`；CHAMBER 使用 `UNKNOWN` 并标记 `unmatched_chamber`。

## 5. 快照与缓存契约

- 主制程追溯结果属于 SPC 原始 DataFrame payload，可进入 Parquet 快照和 `st.cache_data` 原生 payload。
- 快照必须包含：`main_step_id`、`main_eqp_type`、`main_process_unit_id`、`main_process_event_time`、`main_process_trace_source`。
- 快照策略版本为 `spc-main-process-trace-v2`；旧策略或缺列快照触发结构刷新。v2 增加 OLED/TP 共用 Glass EQP 履历的厂别隔离。
- 履历查询失败不持久化半成品；仓储继续使用既有“已有快照才降级”的失败边界。
- 自定义 ViewModel 继续在缓存外构造，遵守 ADR-0001。

## 6. 只读数据探查证据（2026-08-10）

- 所需 7 张表均在 `mdw` schema 找到，关键字段与北极星 SQL 一致。
- `dwd_imp_dv_param_spec`：1352 行，`prod_code + step_id + param_name` 也是 1352 个唯一键，无重复。
- 全库路由：CHAMBER 440 行/7 产品；EQP 904 行/14 产品；空值 8 行/8 产品。
- 当前配置产品 M626/M673/M678/Z517/Z571 均有规格；M626/M673/M678/Z571 同时具有 EQP 与 CHAMBER，Z517 以 EQP 为主。
- 履历 `event_timekey` 为 20 位数字键，例如其前 14 位覆盖 `YYYYMMDDHHMMSS`；五张履历表均以 `date_timekey` 为主键索引首列，可按窗口下推。
- M626 最近窗口去重点位抽样匹配：ARRAY/OLED/TP CHAMBER 均为 100%；ARRAY EQP 99.2%。OLED/TP EQP 存在正式历史缺口，证实需要北极星既有 `coalesce(history.eqp_id, measurement.unit_id)` 回退。

## 7. 非目标

- 不改变 CPM/CPK 公式、sigma 来源、月周日窗口或人工修饰。
- 不改变第一幅图、第三幅图、筛选器和查询门控。
- 不把检测量测 `unit_id/site_name` 当作主站点 CHAMBER。
- 不复刻北极星 SQL 的多对多放大风险，也不迁移 MQC 和其他 FineReport 逻辑。

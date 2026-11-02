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
| `prod_code` | 产品型号 | `mdw.dwr_mes_productspec` + 三厂量测表；OLED `2120A` 使用 MQC 映射表 | 常规：`product_spec → productspecname → productcode`；`2120A` 例外见[第 8 节](#8-2120a-站点特殊处理2026-11-02) |
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

- 当前共享原始量测快照在主制程追溯前写入；追溯字段保留在制备后的 DataFrame 和 `st.cache_data` 原生 payload 中，不写回共享原始快照。
- 追溯输出包含：`main_step_id`、`main_eqp_type`、`main_process_unit_id`、`main_process_event_time`、`main_process_trace_source`。
- `spc-main-process-trace-v2` 是此前独立追溯快照的历史策略标识；当前共享测量快照和 `2120A` 产品归属切换见[第 8.3 节](#83-执行位置与缓存切换)，完整快照链路见 [SPC 数据流](data-flow-spc.md#21-获取三厂测量数据与共享快照)。
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
- 不复刻北极星 SQL 的多对多放大风险；MQC 仅实现第 8 节明确的 `2120A` 产品归属例外，其他 FineReport 逻辑不在本规格范围内。

## 8. 2120A 站点特殊处理（2026-11-02）

本节维护 OLED 检测站点 `2120A` 的产品归属、参数屏蔽和相应缓存规则。`2120A` 是检测站点，不等同于主制程过站代码 `21200`。

### 8.1 按 PPID 确定产品型号

原始量测来自 `eda.spc_tzbjx_oled`。此站点的 `product_spec` 可能为 `OLED-MQC-THK`，按产品规格字典只能得到 `MQC`，无法归入所需的实际产品，因此使用以下关联：

```text
eda.spc_tzbjx_oled.ppid
    → eda.IMP_SPC_TZBJX_MQC.ppid
    → eda.IMP_SPC_TZBJX_MQC.PRODCODE
    → 输出 prod_code，并按请求产品筛选
```

- 仅 OLED 且 `step_id = '2120A'` 进入该分支；其他站点继续通过 `product_spec` 关联产品字典。
- `PRODCODE` 直接作为产品型号，不拼接 `MQC-`；例如 PPID 映射为 `Z553` 时输出 `Z553`。
- 此分支不依赖产品规格字典存在匹配项；未匹配到 PPID 产品映射时，不回退到 `product_spec` 的产品归属，该记录不进入请求产品结果。
- 映射表先按 `ppid + prodcode` 去重，避免相同产品的重复配置放大量测点；这不表示一个 PPID 在业务上被强制约束为唯一产品。
- 查询仍限定原有量测时间窗口，不因产品归属例外额外回溯；产品归属确定后，参数白名单、SPC 分类、规格匹配和主制程追溯继续执行既有规则。

### 8.2 通过工作簿屏蔽非 UNI 参数

参数屏蔽由 [spc_outlier_filters.xlsx](../../../../resources/inline_domain/spc/spc_outlier_filters.xlsx) 的首个工作表逐项配置，不在 Python 中硬编码后缀过滤。

2026-11-02 维护时，从 `eda.spc_tzbjx_oled` 查询 `step_id = '2120A'`，时间限定为既有源时间滚动窗口 `2026-08-01 00:00:00 <= glass_start_time < 2026-11-03 00:00:00`。共查得 22 个参数，其中 11 个不以 `UNI` 结尾，新增规则如下：

| step_col | param_col | lower_col | upper_col |
|---|---|---|---|
| 2120A | 4PP_TEG1_RS | 0 | 0 |
| 2120A | SE_EV2_T1 | 0 | 0 |
| 2120A | SE_EV2_T2 | 0 | 0 |
| 2120A | SE_EV3_T1 | 0 | 0 |
| 2120A | SE_EV4_T1 | 0 | 0 |
| 2120A | SE_EV5_T1 | 0 | 0 |
| 2120A | SE_EV6_T1 | 0 | 0 |
| 2120A | SE_EV7_T1 | 0 | 0 |
| 2120A | SE_EV8_T1 | 0 | 0 |
| 2120A | SE_EVC_T1 | 0 | 0 |
| 2120A | SE_EVF_T1 | 0 | 0 |

- 下界剔除 `param_value <= 0`，上界剔除 `param_value >= 0`，两者合起来屏蔽命中参数的全部有效数值。无效数值已在此前清洗步骤剔除。
- 工作簿没有 `prod_col`，因此新增规则适用于所有产品的 `2120A` 参数；原有 22 条规则和 `backup` 工作表保留。
- 另 11 个以 `UNI` 结尾的参数未新增屏蔽规则，仍需通过既有白名单、类型、日期等准入条件；不代表无条件放行。
- 规则按站点和参数全名匹配，不会自动屏蔽未来新增的非 `UNI` 参数。后续维护仍须限定时间查询实际参数，再补充工作簿。
- 过滤发生在 Sheet 特征汇总和能力计算之前，被剔除点位不进入后续图表和统计；原始测量快照不被这些规则改写。通用工作簿语义见 [SPC 数据流](data-flow-spc.md#24-应用异常点过滤规则)。

### 8.3 执行位置与缓存切换

- 产品归属由共享测量 DAO [measurement_data_loader.py](../../../../src/inline_domain/infrastructure/shared/measurement_data_loader.py) 执行；[spc_repository.py](../../../../src/inline_domain/infrastructure/spc/spc_repository.py) 保持薄投影职责。
- 工作簿通过 [outlier_filter_rules.py](../../../../src/inline_domain/infrastructure/shared/outlier_filter_rules.py) 加载，由共享测量制备管线应用。共享原始快照采用统一的产品归属；SPC、CTQ、Monitor 等消费该制备链路时使用一致的过滤规则，过滤本身不写回原始快照。
- 共享原始量测快照策略升级为 `inline-measurement-raw-v5-2120a-ppid`，版本同时进入 SPC 输入缓存签名。旧版本不再作为有效输入，下一次实际加载完整重取原有滚动窗口，避免只回刷尾部而遗漏历史归属修正。
- 成功发布新快照前保留旧文件；数据库读取失败时，不使用旧归属版本作为新规则的降级结果。
- 修改工作簿后须通过现有刷新入口触发重新计算，使规则进入当前报表结果，不以既有页面缓存判断是否已生效。

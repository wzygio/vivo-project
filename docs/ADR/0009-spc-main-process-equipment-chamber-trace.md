# ADR-0009：SPC 第二幅图的主制程设备/腔室追溯

- Status: Accepted
- Date: 2026-08-10
- Scope: `src/inline_domain/infrastructure/spc/`、`app/sections/spc/spc_dashboard.py`

## Context

`docs/dev_docs/dev_prompt/feat-SPC_CPM.md` Task2 要求把 SPC 第二幅 Sheet 点位分布图从检测站点设备/腔室改为参数对应主制程站点的设备/腔室。原实现只消费量测明细的 `unit_id` 并截取名称前缀，既没有使用规格表的主工序路由，也没有查询主工序 OUT 履历。

只读实库探查确认：

1. `mdw.dwd_imp_dv_param_spec` 的 `(prod_code, step_id, param_name)` 1352 行全部唯一，并提供 `main_step_id/main_eqp_type`；空路由需按北极星语义回退检测站点和 `EQP`。
2. ARRAY 使用 Sheet 履历，OLED/TP 使用 Glass 履历；EQP、CHAMBER 分属整机、子单元/单元来源，OLED CHAMBER 还需借助 `dwd_mes_oled_oper_layer_v` 归一化 CVD 工序。
3. 同一物料和工序可能存在多条 OUT 履历。直接复刻北极星多表连接会放大量测点位，破坏 SPC 箱线图粒度。

## Decision

1. 规格加载继续以三元组唯一匹配，同时返回并归一化 `main_step_id/main_eqp_type`。缺少主工序时使用当前 `step_id`；缺少或非法路由类型时使用 `EQP`。
2. 新增 `infrastructure/spc/main_process_trace.py` 作为追溯边界，按厂别和路由加载目标物料、主工序、时间窗内的 `INOUT_TYPE='OUT'` 履历：
   - ARRAY EQP：`dwt_inout_sht.eqp_id`；
   - ARRAY CHAMBER：优先 `dwt_inout_sub_unit_sht.sub_unit_id`，回退 `dwt_inout_unit_sht.unit_id`；
   - OLED/TP EQP：`dwt_inout_gls.eqp_id`，并显式隔离厂别；
   - TP CHAMBER：`dwt_inout_sub_unit_gls.sub_unit_id`；
   - OLED CHAMBER：`dwt_inout_sub_unit_gls` 连接 OLED 工序映射，兼容 `21200-CVD1`～`21200-CVD4` 到 `21200-CVD`。
3. 履历窗口为报表开始日前推一个月至报表结束日。每个量测追溯目标选择不晚于量测时刻的最近 OUT；ARRAY CHAMBER 先按来源优先级选子单元，再按时间选最近记录。追溯在去重目标上执行，再以 many-to-one 方式回并，保证点位行数不增加。
4. 正式 EQP 履历缺失时回退量测 `unit_id`；CHAMBER 履历缺失时使用 `UNKNOWN`，不把检测站点设备冒充主腔室。结果同时保留追溯时间与来源，便于诊断。
5. 追溯字段随原生 DataFrame 写入 SPC Parquet 快照；策略升级为 `spc-main-process-trace-v2`，缺少任一追溯字段或厂别隔离语义之前的旧快照触发结构刷新。
6. 第二幅图只消费 `main_process_unit_id`，标题为 `By主站点设备/腔室`；第一幅周期图、第三幅过货时间图、能力公式和筛选口径保持不变。

## Alternatives considered

- 直接使用检测量测 `unit_id` 或名称前缀：拒绝。它表示检测站点，不能证明主制程设备/腔室。
- 原样复刻北极星多表 LEFT JOIN：拒绝。同一物料多次过站会造成 many-to-many 行放大，改变点位统计。
- CHAMBER 缺失时回退检测设备：拒绝。该值语义错误；显式 `UNKNOWN` 更安全且可诊断。
- 只在绘图时临时查询履历：拒绝。会绕过 repository/snapshot 数据边界，并在每次重绘重复访问数据库。

## Consequences

- 正面：第二幅图展示可追溯的主制程设备/腔室；三厂路由和回退规则显式、可测试；快照命中后不增加页面数据库开销；最近 OUT 与 many-to-one 回并保持原点位粒度。
- 负面/后续约束：刷新旧快照时首次查询会增加六类履历读取成本；`event_timekey` 当前按前 14 位解析；新增厂别或设备类型时必须扩展路由表、数据源 spec 和测试。`UNKNOWN` 应被视为数据质量信号，不得在前端伪装为检测设备。

## Traceability

- Issue: `.scratch/spc-main-process-chamber/issues/01-trace-main-process-equipment-chamber.md`
- Plan: `.planning/2026-08-10-spc-main-process-chamber/`
- 数据源 spec: `references/domain/spc/spec-data_source.md`
- 关键代码: `src/inline_domain/infrastructure/spc/main_process_trace.py`、`src/inline_domain/infrastructure/spc/data_loader.py`、`src/inline_domain/infrastructure/spc/repositories/spc_repository.py`、`app/sections/spc/spc_dashboard.py`
- 自动化测试: `tests/unit/inline_domain/infrastructure/spc/test_main_process_trace.py` 等 SPC 定向用例 55 passed；`python tools/smoke.py spc` 150 passed。
- 实库/快照证据: 六条厂别/路由样本 72→72 行；M626 快照 743503 行且含全部五个追溯字段。
- E2E: `tests/e2e/spc_main_process_chamber.js` 通过；OLED/21200 渲染主站点图和时间图，宽窄视口无横向溢出，截图位于 `output/screenshots/spc_main_process_chamber_e2e.png` 与 `output/screenshots/spc_main_process_chamber_chart.png`。

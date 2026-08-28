# ADR-0017：Inline 自动预警中心（单片异常口径与异常项自动出图）

- Status: Accepted
- Date: 2026-08-25
- Scope: `src/inline_domain/core/shared/`、`src/inline_domain/core/aoi_rs/`、
  `src/yield_domain/{core,application}/`、`app/sections/inline_domain/shared/`、
  五个报表页面（SPC/CTQ/AOI_TT/AOI_RS 监控报表、入库不良率分析看板）
- Trace: Issue `.scratch/inline-alert-center/issues/01-inline-alert-center.md`、
  Plan `.planning/inline-alert-center/`、
  PRD `docs/PRD/PRD-2026-08-25-Inline自动预警中心.md`

## Context

四个 Inline 子模块的 OOS 修饰逻辑已把超规片持久化到 `resources/` 工作簿，
但除 SPC 的 CPK 预警（趋势波动）外，各页面没有产品级自动预警；Yield 看板的
预警只输出文本，用户要查看异常项图像必须手动筛选。需求方要求统一提供
"单片异常 + 趋势波动"两类预警，并自动渲染命中异常项的图像。

关键事实约束：sheet OOS 工作簿中**每一行本来就是超规片**，`flag` 三态只是
修饰决策（True=默认截断、False=用户确认释放真实值、Delete=删除）；aoi_rs
工作簿无时间列且被企业加密；aoi_tt 时间列名为 `start_time`。

## Decision

1. **单片异常口径固定为 `flag == FALSE`**（需求方确认的业务决策 D1）：
   只报"用户确认释放真实值"的超规片，而非全部超规行。已知后果是
   ctq/aoi_tt 在当前数据下预警多为空，属预期。
2. **预警范围统一为上一 ISO 周**，半开区间 `[上周一 00:00, 本周一 00:00)`，
   由共享纯函数 `core/shared/sheet_oos_alerts.py::previous_iso_week_range` /
   `build_sheet_oos_alerts` 提供；时间列经 `to_datetime(errors="coerce")`
   归一化，缺失/解析失败行不参与。
3. **预警是对修饰产物的只读消费**，不重算 OOS、不触发工作簿重写：
   spc/ctq 直接消费 ViewModel 的 `sheet_oos_decoration_result.decoration_df`；
   aoi_tt/aoi_rs 经 `load_sheet_oos_decoration`（含 COM 回退）读工作簿，
   以 `(mtime_ns, size, prod_code)` 为 `st.cache_data` 键；读取失败降级为
   页面 info 提示，不阻断报表主体。
4. **aoi_rs 明细新增 `sheet_start_time` 列**（需求方确认的决策 D2）：取值自
   图表点帧聚合的 `first_start_time`（lot 图 = lot 最早 start_time）；合并键
   不变；历史行无时间值，自然被周筛选排除。
5. **前端统一两段式**：预警中心 Expander（有警自动展开）+ "自动预警图像"
   Expander（按预警指标键精确过滤后复用各模块既有 indicator/compact 渲染）。
   共享 UI 落 `app/sections/inline_domain/shared/alert_center.py`
   （ADR-0014/0016 的 shared 提取约束）；图像渲染遵循 ADR-0004 RenderGate
   两阶段，chart key 用独立前缀（`spc_oos_alert` / `ctq_oos_alert` /
   `aoi_rs_alert` / `yield_alert`）与手动筛选区隔离，memo 签名含产品缓存
   revision（ADR-0001）。
6. **Yield 告警结构化双轨**：`AbnormalDetector.detect_system_trend_records`
   返回结构化记录（level/group/code/period/rates/rules），既有
   `List[str]` 文本接口输出逐字不变；看板按命中 Defect Code 集合
   （code 级直取、group 级展开、lot 超规并入）自动渲染紧凑组图。

## Alternatives considered

- 按全部超规行（排除 Delete）预警：能报出被修饰的真实超规，但需求方明确
  选择仅 flag=FALSE 的严格口径，拒绝。
- aoi_rs 预警时回关联 `rs_details_df` 补时间：避免改写入侧，但引入跨层
  数据关联；需求方选择改造写入补列，拒绝。
- 修改 `detect_system_trend_alerts` 改由结构化记录格式化文本：减少重复判定，
  但有任何文本回归风险；选择并行方法 + 一致性测试，拒绝改动既有接口。

## Consequences

- 正面：五个页面预警口径一致；异常项图像零筛选可见；判定逻辑零改动、
  回归风险集中在新增代码。
- 负面/约束：ctq/aoi_tt/aoi_rs 预警在 flag=FALSE 数据积累前可能长期为空；
  aoi_rs 预警自本列引入后的新数据起生效；预警工作簿读取增加一次文件 IO
  （已用 mtime 缓存控制）。
- E2E 注意：折叠 Expander 内容不在 DOM 中，自动化需先点击展开；rerun 会
  重置折叠态且可能使 locator 点击静默失败，E2E 脚本采用 eval 点击 + 重试。

## Verification

- 单元/集成：`tests/unit` 555 passed（新增 41 项），`tests/integration`
  9 passed；10 项既有失败经核实与本改动无关（HEAD 即失败）。
- E2E：`tests/e2e/{spc,ctq,aoi_tt,aoi_rs}_sheet_oos_alert.js`、
  `yield_alert_code_expanders.js` 及既有 `spc_cpk_alert.js` 回归全部通过，
  证据截图在 `output/test-results/`。

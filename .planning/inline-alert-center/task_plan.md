# Task Plan: Inline 自动预警中心（单片异常 + 趋势波动）

- Plan ID: `inline-alert-center`
- Issue: `D:/wzy/Python/vivo-project/.scratch/inline-alert-center/issues/01-inline-alert-center.md`（Status: ready-for-agent）
- PRD: `docs/PRD/PRD-2026-08-25-Inline自动预警中心.md`
- Created: 2026-08-25
- Branch: `feat/inline-alert-center`（模块 3 开始时从 master 新建；master 上现有未提交改动保持不动）

## Goal

五个页面（SPC/CTQ/AOI_TT/AOI_RS 监控报表 + 入库不良率分析看板）具备上一 ISO 周自动预警：单片异常（flag=FALSE）+ 趋势波动（SPC=CPK 复用，Yield=良率波动复用），命中异常项图像在独立 Expander 自动出图，全程 RenderGate 两阶段渲染，E2E 通过。

## 已确认决策（不得推翻）

- D1：单片异常仅报 `flag=FALSE`（明细表每行都是超规片；FALSE=用户确认释放真实值）。
- D2：aoi_rs 修饰写入新增 `sheet_start_time` 列；历史无时间行不参与预警。
- ISO 上一周 = 半开区间 `[上周一 00:00, 本周一 00:00)`，参考日为页面默认 end_date。
- 预警只读，不触发工作簿重写；读取失败降级为 info，不阻断报表。

## Phases & Checklist

### Phase 1 — 共享后端纯函数（TDD tracer bullet）

- [x] 1.1 新增 `src/inline_domain/core/shared/sheet_oos_alerts.py`：`previous_iso_week_range(reference_date)` + `build_sheet_oos_alerts(detail_df, *, time_column, reference_date)`。
  - 验证：`pytest tests/unit/inline_domain/core/shared/test_sheet_oos_alerts.py` → **12 passed**（跨年初、周一/周日参考日、flag 三态、Delete/空值排除、窗口边界、解析失败、空表/缺列、倒序、aoi_tt start_time 列名）。（对应 AC-2、AC-3）
- [x] 1.2  tracer bullet：spc 页面用 ViewModel `sheet_oos_decoration_result` 驱动 `build_sheet_oos_alerts` 产出预警 DataFrame（不接 UI）。
  - 验证：单元测试断言输出列与排序（超规时间倒序）。

### Phase 2 — aoi_rs 写入时间列（D2）

- [x] 2.1 `src/inline_domain/core/aoi_rs/aoi_rs_decoration.py` 明细新增 `sheet_start_time` 列（sheet 图取 sheet start_time；lot 图取 lot 最早 start_time）；合并键列不变。
  - 验证：单元测试断言新列存在、值来源正确、merge 键不含时间列、历史空值行被预警过滤排除。（对应 AC-7）

### Phase 3 — Yield 结构化告警记录

- [x] 3.1 `src/yield_domain/core/abnormal_detector.py` 新增结构化记录方法（level/defect_group/defect_desc/period/curr/prev/rule），既有 `List[str]` 文本接口输出逐字不变；`alert_service.py` 透传。
  - 验证：单元测试断言结构化记录与文本消息一一对应、既有文本测试无回归。（对应 AC-6 前半）

### Phase 4 — 前端 shared 预警组件

- [x] 4.1 新增 `app/sections/inline_domain/shared/alert_center.py`：`render_sheet_oos_alert_center(alerts_df, *, title, module_label)`（有警 st.error+dataframe 自动展开 / 无警 st.success / 不可用 st.info）+ `filter_report_by_alert_keys(df, alerts_df, key_map)`。
  - 验证：单元测试（过滤精确性：不多不漏）+ 渲染层用既有 dashboard 测试模式。（对应 AC-1、AC-5 过滤部分）

### Phase 5 — SPC 单片异常接线（模板验证）

- [x] 5.1 `spc_dashboard.py` + `SPC监控报表.py`：单片异常预警中心 + 自动预警图像 Expander（复用 `render_spc_indicator_sections`，`chart_key_prefix` 独立、memo 签名含产品 revision）；既有 CPK 预警不动。
  - 验证：SPC dashboard 定向测试无回归 + 新增预警测试；`uv run python tools/smoke.py spc`。（对应 AC-1、AC-4、AC-5、AC-8）

### Phase 6 — CTQ / AOI_TT / AOI_RS 推广

- [x] 6.1 CTQ：同 Phase 5 模式（ViewModel 修饰结果）。
- [x] 6.2 AOI_TT：`load_sheet_oos_decoration` 读工作簿（mtime 缓存键），`start_time` 归一化。
- [x] 6.3 AOI_RS：同上（COM 回退 + 失败降级）。
  - 验证：各 dashboard 定向测试 + 新增预警测试；读取失败降级路径测试（不阻断页面）。（对应 AC-1、AC-2、AC-5、AC-9）

### Phase 7 — Yield 自动预警缺陷图像

- [x] 7.1 `yield_dashboard.py` + `入库不良率分析看板.py`：新增 `🚨 自动预警缺陷图像（N 个 Code）` Expander，对趋势波动+Lot 超规命中 Defect Code 去重，复用 `_build_compact_render_payload` + `RenderGate.collect_memoized`，key 前缀 `yield_alert`。
  - 验证：yield dashboard 定向测试 + 新增测试；key 冲突检查（预警区与手动筛选区同页共存）。（对应 AC-5、AC-6）

### Phase 8 — 回归与 E2E

- [x] 8.1 全量单元/集成回归：`pytest tests/unit tests/integration`。
- [x] 8.2 E2E（tests/e2e/，Playwright）：五页预警 Expander 存在性与展开行为；SPC CPK 预警无回归；产物写入 `output/test-results/`。
- [x] 8.3 静态检查：触及模块编译 + `git diff --check`。

### Phase 9 — 沉淀（模块 4）

- [ ] 9.1 ADR（预警口径 D1、ISO 周定义、aoi_rs 时间列、结构化告警契约）。
- [ ] 9.2 issue 勾选 AC + 记录交付 comment；CONTEXT/ARCHITECTURE 按需更新。

## 验证命令约定

- 单元：`uv run pytest tests/unit -k <pattern>`；全量：`uv run pytest tests/unit tests/integration`
- Smoke：`uv run python tools/smoke.py spc`
- E2E：`uv run pytest tests/e2e`（按仓库既有 e2e 配置执行）

## 计划批准记录

- 2026-08-25 用户通过 AskUserQuestion 明确批准计划（含接口变化与测试优先级），进入模块 3 TDD 开发。

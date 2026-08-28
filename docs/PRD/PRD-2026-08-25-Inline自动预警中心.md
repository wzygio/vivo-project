# PRD：Inline 自动预警中心（单片异常 + 趋势波动）

- 日期：2026-08-25
- 状态：已评审（需求口径已与需求方确认，待实施）
- 适用范围：`src/inline_domain` 四个子模块（spc、ctq、aoi_tt、aoi_rs）对应页面及 `app/pages/入库不良率分析看板.py`
- 需求来源：`docs/dev_docs/dev_spec/Inline_domain/feat-alert_center.md`
- 重点资源：`resources/spc_sheet_oos_decoration.xlsx`、`resources/ctq_sheet_oos_decoration.xlsx`、`resources/aoi_tt_sheet_oos_decoration.xlsx`、`resources/aoi_rs_sheet_oos_decoration.xlsx`
- 参考实现：`app/sections/inline_domain/spc/spc_dashboard.py`（CPK 预警中心 + 自动预警指标图像）、`app/components/alert_center.py`、`app/manager/render_gate.py`、`src/yield_domain/core/abnormal_detector.py`

## 1. CAPABILITY

报表使用者打开任意 Inline 监控报表（SPC / CTQ / AOI_TT / AOI_RS）或入库不良率分析看板时，系统自动给出**上一 ISO 周**的预警摘要，并把命中异常项的图像直接渲染出来，无需用户手动筛选：

- **单片异常**：来自各模块 sheet OOS 修饰明细表中 `flag=FALSE`（用户确认释放真实值）的记录；
- **趋势波动**：SPC 复用既有 CPK 预警（`cpk < 1.33` 且未被人工修饰）；Yield 复用既有良率波动判定（`AbnormalDetector`）。

交付后，五个页面均具备"预警中心 Expander（有警自动展开）+ 自动预警图像 Expander（按预警键自动过滤出图）"两段式结构，渲染遵循 `RenderGate` 两阶段模式（先集中计算 payload，再集中渲染）。

## 2. 背景与现状

### 2.1 已验证的现状（调研结论）

1. 四个 sheet OOS 明细工作簿均存在于 `resources/`，由 `src/inline_domain/core/shared/sheet_oos_decoration.py`（spc/ctq 共享）及 `core/aoi_tt/aoi_tt_decoration.py`、`core/aoi_rs/aoi_rs_decoration.py` 写入；**工作簿中每一行本来就是超规片**，`flag` 是修饰决策三态（True=截断修饰默认 / False=用户确认释放真实值 / Delete=删除该行）。
2. 各工作簿结构差异：
   - spc / ctq：含 `sheet_start_time`（datetime64），列结构一致；
   - aoi_tt：时间列名为 `start_time`；
   - aoi_rs：**无任何时间列**，且文件已被企业加密，openpyxl 直读失败，必须走 `load_sheet_oos_decoration` 内置的 COM 回退（`src/shared_kernel/utils/excel_tools.py`）。
3. SPC 页面已有完整预警实现可作模板：`build_weekly_cpk_alerts()`（`spc_dashboard.py:67`）、`render_cpk_alert_center()`（:120）、`filter_spc_report_by_alerts()`（:153）、`render_cpk_alert_indicator_sections()`（:556，Expander + RenderGate.collect_memoized）。
4. Yield 页面已有预警中心（`app/components/alert_center.py`）与 Lot 超规扫描（`compute_lot_oos_records`）、良率波动判定（`AbnormalDetector`，阈值：环比翻倍且 >0.1%，或绝对激增 >0.2%），但**告警只返回 `List[str]` 文本**，无结构化记录，无法驱动按 Defect Code 自动出图；该页当前没有"异常项自动展示图像"能力。
5. `RenderGate`（`app/manager/render_gate.py`）提供 `stage/collect/collect_memoized` 两阶段渲染；`collect_memoized` 签名须含 `build_product_cache_signature` 的产品 revision。
6. 项目无共享"ISO 上一周"工具函数；`tools/generate_ppa_oos_weekly_summary.py:previous_calendar_week` 返回半开区间 `[上周一 00:00, 本周一 00:00)`，是现成范式。

### 2.2 需求审查结论

| 用户要求 | 审查结论 | PRD 处理 |
|---|---|---|
| 直接读明细表 flag=FALSE 记录，不重复计算 | 可行；但 flag=FALSE 只是超规片中"用户确认不修饰"的子集 | 经需求方确认：**仅报 flag=FALSE**（见 §3 决策 D1） |
| 按 sheet_start_time 筛 ISO 上一周 | spc/ctq 直接可行；aoi_tt 列名不同；aoi_rs 无时间列 | 读取侧列名归一化；aoi_rs 改造写入补时间列（D2） |
| spc 趋势波动 = cpk 超规 | 已有现成实现 | 原样复用，不改判据 |
| yield 趋势波动 = 良率波动 | 判定逻辑已有，但只输出文本 | 新增结构化告警记录，保留文本接口（D3） |
| 异常项图像放 Expander 自动展示 | SPC 已有模板 | 推广到全部五个页面（D4） |
| 渲染采用 render_gate 两阶段 | 已有机制 | 强制约束（§4.2） |

### 2.3 已确认的业务决策

- **D1（预警口径）**：单片异常仅报 `flag=FALSE` 的记录。已知影响：ctq、aoi_tt 当前明细中 FALSE 记录极少或为 0，预警可能为空，属预期行为（空态展示"暂无预警"）。
- **D2（aoi_rs 时间列）**：改造 `aoi_rs_decoration` 写入逻辑，新增时间列；历史行无时间值，不参与周筛选预警，待新数据自然积累。

## 3. CONSTRAINTS

### 3.1 固定业务规则

1. 单片异常判据固定为：`flag` 解析后为 `FALSE` 且记录时间 ∈ 上一 ISO 周；`flag=Delete` 的行永不参与。
2. 上一 ISO 周统一定义为半开区间 `[上周一 00:00, 本周一 00:00)`（本地朴素时间），以页面默认 end_date（今天）为参考日。
3. 不修改任何修饰/判定算法本身：OOS 筛选、三态 flag 解析、CPK 计算、良率波动阈值全部保持现状。
4. 预警是**只读**消费：不得因预警计算而触发工作簿重写；读取失败（含加密文件 COM 失败）降级为"预警数据不可用"提示，不得阻断报表主体渲染。
5. 每页只展示**当前产品**的预警（工作簿按 prod_code 分 sheet）。

### 3.2 渲染与缓存规则

1. 所有预警图像渲染必须经过 `RenderGate`：payload 纯计算在 `collect`/`collect_memoized` 内完成，禁止在 stage 中调用 st.*。
2. `collect_memoized` 签名必须包含产品缓存 revision（`build_product_cache_signature`），保证"刷新缓存/刷新数据"后重建。
3. 预警区图像的 chart key 必须使用独立前缀（如 `spc_alert` 已有做法），避免与手动筛选区 plotly key 冲突。
4. 有预警时预警中心 Expander 自动展开；无预警时收起并显示成功/空态文案。

### 3.3 shared 提取约束（ADR-0014/0016）

- 跨模块复用逻辑必须落在 shared 层：后端 `src/inline_domain/core/shared/`（预警过滤纯函数、ISO 周工具），前端 `app/sections/inline_domain/shared/`（通用预警中心 UI）；业务模块只保留键列映射等差异。
- 禁止跨业务模块导入私有函数。

## 4. IMPLEMENTATION CONTRACT

### 4.1 后端：共享预警过滤

新增 `src/inline_domain/core/shared/` 下的预警模块（纯函数，无 I/O 副作用）：

```python
def previous_iso_week_range(reference_date) -> tuple[pd.Timestamp, pd.Timestamp]:
    """返回 [上周一 00:00, 本周一 00:00) 半开区间。"""

def build_sheet_oos_alerts(detail_df, *, time_column, reference_date) -> pd.DataFrame:
    """筛选 flag==FALSE 且 time_column 落在上一 ISO 周的记录；
    time_column 经 pd.to_datetime(errors="coerce") 归一化，解析失败/缺失的行不参与。"""
```

各模块的时间列与指标键映射（模块差异，放各自 application/service 或页面组装层）：

| 模块 | 明细来源 | 时间列 | 指标键（用于过滤出图） |
|---|---|---|---|
| spc | ViewModel `sheet_oos_decoration_result`（内存已有，避免重读文件） | `sheet_start_time` | factory, step_id, param_name |
| ctq | ViewModel `sheet_oos_decoration_result` | `sheet_start_time` | factory, step_id, param_name |
| aoi_tt | `load_sheet_oos_decoration` 读工作簿 | `start_time`（归一化） | factory, step_id, tt_name |
| aoi_rs | `load_sheet_oos_decoration` 读工作簿（COM 回退） | 新增列（见 §4.2） | factory, step_id, rs_code |

预警展示列（中文，按模块裁剪）：厂别、站点、参数名称（或 TT 名称 / RS Code）、Sheet ID、超规时间、超规类型/实测值、规格上下限。

### 4.2 aoi_rs 修饰写入改造（D2）

- `core/aoi_rs/aoi_rs_decoration.py` 的明细构建增加时间列，列名定为 `sheet_start_time`（与其余三个工作簿对齐），取值来自写入时可获得的 sheet/lot 起始时间（sheet 图取 sheet 的 start_time，lot 图取 lot 的最早 start_time）。
- 合并/持久化键列不变（不把时间列纳入 key），历史行时间值为空，自然被 §4.1 的归一化排除。
- 既有加密工作簿的读取路径不变。

### 4.3 前端：五页统一两段式结构

模板为 SPC 既有实现（`render_cpk_alert_center` + `render_cpk_alert_indicator_sections`）。共享 UI 落 `app/sections/inline_domain/shared/alert_center.py`：

```python
def render_sheet_oos_alert_center(alerts_df, *, title, module_label) -> None:
    """Expander：有警 st.error + st.dataframe；无警 st.success；数据不可用 st.info。"""

def filter_report_by_alert_keys(df, alerts_df, key_map) -> pd.DataFrame:
    """按指标键 MultiIndex.isin 过滤（泛化自 filter_spc_report_by_alerts）。"""
```

各页面接线：

- **SPC监控报表.py**：保留既有 CPK 预警中心（趋势波动）；新增单片异常预警中心 + 复用 `render_cpk_alert_indicator_sections` 的同款图像 Expander（按 spc 预警键过滤）。
- **CTQ监控报表.py**：新增单片异常预警中心 + 图像 Expander，内部复用 `render_ctq_indicator_sections`。
- **AOI_TT监控报表.py**：同上，复用 `render_aoi_tt_indicator_sections`。
- **AOI_RS监控报表.py**：同上，复用 `render_aoi_rs_indicator_sections`。
- **入库不良率分析看板.py**（D3/D4）：
  1. `AbnormalDetector` 新增并行方法返回**结构化记录**（level=group/code、defect_group、defect_desc、period、curr、prev、rule），现有 `List[str]` 文本接口保留不改；`AlertService` 透传。
  2. 页面新增 Expander `🚨 自动预警缺陷图像（N 个 Code）`：对趋势波动与 Lot 超规命中的 Defect Code 去重集合，复用 `_build_compact_render_payload` + `RenderGate.collect_memoized` 出图，chart key 使用独立前缀（如 `yield_alert`）。

### 4.4 读取缓存

工作簿读取（aoi_tt/aoi_rs）走 `st.cache_data`，键含文件 `(mtime_ns, size)` 与 prod_code；普通 rerun 不重复启动 COM。

## 5. 接口与数据影响

### 5.1 预计修改/新增文件

| 文件 | 改动 |
|---|---|
| `src/inline_domain/core/shared/sheet_oos_alerts.py`（新增） | `previous_iso_week_range`、`build_sheet_oos_alerts` 纯函数 |
| `src/inline_domain/core/aoi_rs/aoi_rs_decoration.py` | 明细新增 `sheet_start_time` 列 |
| `src/yield_domain/core/abnormal_detector.py` | 新增结构化告警记录方法（保留文本接口） |
| `src/yield_domain/application/alert_service.py` | 透传结构化记录 |
| `app/sections/inline_domain/shared/alert_center.py`（新增） | 通用预警中心 UI + 按键过滤 helper |
| `app/sections/inline_domain/spc/spc_dashboard.py` | 单片异常预警接线（复用既有 CPK 模式） |
| `app/sections/inline_domain/ctq/ctq_dashboard.py`、`aoi_tt/aoi_tt_dashboard.py`、`aoi_rs/aoi_rs_dashboard.py` | 各自预警中心 + 图像 Expander |
| `app/sections/yield_domain/yield_dashboard.py` | 自动预警缺陷图像 Expander |
| 五个页面文件 | 预警数据装配与接线 |
| `tests/unit/`、`tests/e2e/` | 新增对应测试 |

### 5.2 向后兼容

- `AbnormalDetector` 现有文本接口与阈值不变；
- 各 ViewModel 字段不删除，仅新增；
- 三个未加密工作簿格式不变；aoi_rs 工作簿新增一列，旧读取方按列名访问不受影响；
- 预警全部失败降级为提示，不影响既有报表功能。

## 6. 风险与缓解

| 风险 | 等级 | 缓解 |
|---|---|---|
| flag=FALSE 口径下 ctq/aoi_tt 长期无预警 | 中 | 已与需求方确认（D1）；空态文案明确口径 |
| aoi_rs 加密工作簿 COM 读取失败/耗时 | 中 | 复用 `load_sheet_oos_decoration` 回退；mtime 缓存；失败降级不阻断页面 |
| aoi_rs 历史行无时间导致预警空窗 | 低 | D2 已确认；文案注明"自改造后的新数据起生效" |
| 预警区与手动筛选区 plotly key 冲突 | 中 | 独立 chart_key_prefix + sha256 label（SPC 既有做法） |
| 预警图像拖慢首屏 | 中 | RenderGate 单 spinner 集中计算 + memo 签名缓存 |
| yield 结构化记录与文本口径漂移 | 低 | 结构化方法为文本方法的真源，文本由记录格式化生成 |

## 7. NON-GOALS

- 不修改 OOS/CPK/良率波动任何判定阈值与算法；
- 不改变 `flag` 三态语义与修饰写盘时机（除 aoi_rs 新增时间列外）；
- 不为预警新增推送、邮件、持久化工单等通知能力；
- 不对 monitor 自动预警看板页（`monitor_dashboard.py`）做改造；
- 不引入用户身份与预警确认（ack）流程。

## 8. 验收标准

1. 五个页面均出现预警中心 Expander；有预警自动展开，无预警显示成功/空态，数据不可用显示 info 且不阻断报表。
2. 单片异常表内容 = 当前产品明细表中 `flag=FALSE` 且时间 ∈ 上一 ISO 周的记录；手工抽查与工作簿一致。
3. SPC 趋势波动预警与既有 CPK 预警结果一致（无回归）。
4. 命中异常项的图像出现在"自动预警图像"Expander 中，指标键过滤精确（不多图、不漏图）；预警区与手动筛选区可同时操作无 key 冲突。
5. yield 页面按命中 Defect Code 自动出图（趋势 + Mapping + Lot + Sheet 紧凑组图）。
6. 点击"刷新缓存/刷新数据"后预警与图像正确重建；普通 rerun 命中 memo 缓存、无重复 COM 读取。
7. aoi_rs 修饰写入的新记录带 `sheet_start_time`；历史无时间行不进预警。

## 9. 测试计划

- **单元测试**：`previous_iso_week_range` 边界（跨年初 ISO 周）；`build_sheet_oos_alerts` 的 flag 三态、时间解析失败、空表、时区杂质；`filter_report_by_alert_keys`；`AbnormalDetector` 结构化记录与文本一致性；aoi_rs 时间列写入与合并键不变。
- **集成测试**：临时工作簿模拟 flag=FALSE 记录落在上周/本周/上上周的筛选结果；加密样本只读回归（不覆盖真实文件）。
- **E2E 测试**（`tests/e2e/`，Playwright）：五个页面预警 Expander 存在性与展开行为；SPC 页 CPK 预警无回归。

## 10. 实施顺序

1. 共享纯函数（ISO 周 + OOS 预警过滤）+ 单测；
2. aoi_rs 写入加时间列 + 单测；
3. yield 结构化告警记录 + 单测；
4. 前端 shared 预警中心组件；
5. SPC 单片异常接线（模板验证）→ CTQ / AOI_TT / AOI_RS 推广；
6. yield 页面自动预警缺陷图像；
7. E2E 测试与整体验收。

## 11. OPEN QUESTIONS

无阻塞问题。以下采用默认值，评审有异议再调整：

- aoi_rs 新时间列名定为 `sheet_start_time`；
- 预警中心 Expander 标题统一为"预警中心（上一周 YYYY-Www）"格式；
- 单片异常默认按超规时间倒序展示。

## 12. HANDOFF

本 PRD 需求口径（D1/D2）已与需求方确认，可进入开发。按 `development-flow` 流程执行：新建分支 → 本地 issue → 计划与 checklist → TDD 实施 → E2E 验收。关键技术契约：

1. 共享纯函数签名（§4.1）；
2. aoi_rs 时间列来源与合并键不变（§4.2）；
3. yield 结构化记录 schema（§4.3）。

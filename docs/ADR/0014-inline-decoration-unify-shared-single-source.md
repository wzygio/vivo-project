# ADR-0014：Inline 数据修饰统一——shared 单一算法源与 aoi 工作簿三态对齐

- Status: Accepted
- Date: 2026-08-14
- Scope: `src/inline_domain/{core,application}/`、`app/{pages,sections}/`、`resources/`、
  `tests/e2e/`、`references/domain/Inline_domain/`
- Trace: Issue `.scratch/decoration-unify/issues/01-unify-decoration-into-shared.md`、
  PRD `.scratch/decoration-unify/PRD.md`、Plan `.planning/2026-08-14-decoration-unify/`、
  方案 `docs/dev_docs/generated/Inline_domain/decoration-unify-proposal.md`

## Context

Inline 域各子模块的数据修饰逻辑相似却分散：Sheet OOS 修饰引擎物理位于
`core/spc/` 却被 ctq/monitor 反向依赖；spc/ctq 两个应用层 wrapper 约 90% 重复；
aoi_tt/aoi_rs 只有自动截断，没有 spc/ctq 已有的"配置文件指定释放/删除"能力；
aoi_rs 的截断发生在图表组装层（section），违背分层契约；CPK 双轨（真实/修正）
移除后修饰前特征（original_*）成为仍随缓存管线透传的死代码。

已确认的决策（见 issue/PRD）：D1 同意统一方案；D2 保留"刷新缓存/snapshot_signature"
逃生通道并固化操作契约；D3 引擎泛化以现有测试为安全网；D4 是否修饰由
application 层决定、前端只消费修饰后数据（section 层不得依赖未修饰数据）；
D5 CPK 仅基于修饰后点位计算（`cpk_actual` 移除）。

## Decision

1. **修饰算法在 `core/shared/` 单一来源**：
   - 引擎迁入 `core/shared/sheet_oos_decoration.py`，load/merge/persist/剔除等
     函数增加 `key_columns` 参数（默认 = SPC/CTQ 的
     `[prod_code, step_id, param_name, sheet_id]`，行为不变）；
   - `core/shared/auto_decoration.py` 新增 `apply_tri_state_decoration`：
     Delete 剔除 / False 释放 / True（默认）截断，截断 margin（5%~15% span）与
     稳定哈希继续复用引擎常量。
2. **应用层单一入口**：`application/shared/decorated_data.py::prepare_decorated_data(
   scope=...)`（scope → 工作簿文件名映射）；`spc_data_decoration.py` /
   `ctq_data_decoration.py` 删除；`decorated_features.py` 双分支合并为单分支，
   ctq 延迟导入消除；payload 移除 original_* 键（spc 空数据判断改修饰后数据，
   已验证行为等价）。
3. **aoi 对齐工作簿三态能力**（默认行为 = 既有自动截断，向后兼容）：
   - aoi_tt：`core/aoi_tt/aoi_tt_decoration.py`，键
     `[prod_code, step_id, tt_name, sheet_id]`，工作簿
     `resources/aoi_tt_sheet_oos_decoration.xlsx`；
   - aoi_rs：`core/aoi_rs/aoi_rs_decoration.py`，键含 `chart_kind`（lot/sheet）+
     `point_id`（lot_id/sheet_id）维度以区分双规格图口径，工作簿
     `resources/aoi_rs_sheet_oos_decoration.xlsx`；
   - 均为 `resources/` 根目录、每产品一个 sheet，复用企业加密读取回退。
4. **aoi_rs 修饰下移到 service 层**（D4）：service 产出图表就绪的
   `lot_points_df`/`sheet_points_df`；section 删除全部 clip/构建调用，仅渲染
   （`attach_spec_values` 保留用于画规格线，属展示逻辑）。
5. **CPK 单轨**（D5）：`cpk_decoration.py` 改为单输入 `period_capability_df`，
   `CPK_DETAIL_COLUMNS` 移除 `cpk_actual`；默认显示修饰后点位的计算值，
   flag=True 时用修饰表 `cpk_corrected` 覆盖（opt-in 语义不变，兼容既有工作簿）。

## Alternatives considered

- **aoi 直接复用 Sheet OOS 引擎全链路**：引擎的 detail 构建绑定 Sheet 特征
  （sheet_max/min vs usl/lsl），aoi 是点级/聚合级口径，强行套用会过度泛化；
  改为"工作簿机制 + 截断语义共享、detail 构建各自实现"（决策 1/3）。
- **monitor AOI 组从 `none` 切换到 aoi 修饰口径**：属行为变更，按 PRD 约定
  不纳入本次，留作后续单独立项。

## Consequences

- 正面：修饰算法、flag 语义、截断参数全仓单点，后续优化/排障只改一处；
  aoi 获得与 spc/ctq 一致的释放/删除配置能力；前端契约清晰（只渲染修饰后数据）。
- 负面/约束：
  - **操作契约**：手工编辑任何修饰工作簿后，须点击页头「刷新缓存」生效
    （缓存命中不重读工作簿）；
  - aoi_rs 图表点帧不带 prod_code，工作簿键归一化时按查询产品补齐
    （实现于 `aoi_rs_decoration._normalized_points`）；
  - **E2E 必须在锁定环境运行**（`.venv` python -m streamlit，streamlit 1.60）：
    系统 Python 为 1.49 且缺 streamlit_echarts；1.60 的 combobox aria-label 不再
    含 "Selected X" 前缀，脚本已改为读 input.value / 多选容器文本，
    toast/spinner 等短生命周期元素不作为同步点（重渲染高峰可能错过），
    plotly 图点断言须读 `gd.data` 轨迹而非 DOM 文本（x 轴刻度跨 Code 共享）。

## Verification

- 单元/集成：`tests/unit` 417 passed / 5 failed（= 既有基线：hot_reload ×1、
  code_selector ×2、yield_global_data_policy ×2，与本改动无关）。
- E2E（.venv / streamlit 1.60，全部通过）：aoi_tt_report、aoi_rs_report、
  aoi_rs_decoration_delete（工作簿 flag=Delete 端到端生效）、ctq_report、
  spc_cpk_cpm_decoration（8 行修饰值全匹配）、spc_cpk_alert、spc_filter_layout_mt_ch、
  spc_main_process_chamber、monitor_compliance_config；截图与产物见
  `output/test-results/`、`output/screenshots/`。

# PRD：Inline_domain 数据修饰逻辑统一（decoration-unify）

- 日期：2026-08-14
- 状态：已评审（用户批准方案，授权开发至 E2E 通过）
- 需求来源：`docs/dev_docs/dev_spec/Inline_domain/feat-decoration_unify.md`
- 方案依据：`docs/dev_docs/generated/Inline_domain/decoration-unify-proposal.md`（用户已同意）
- 佐证文档：`docs/dev_docs/generated/Inline_domain/spc-ctq-decoration-analysis.md`、`docs/dev_docs/generated/Inline_domain/decorated-features-pipeline.md`

## 1. 背景与问题

企业级报表不能释放真实数据，各子模块都有"数据修饰"逻辑，但当前实现分散：

1. **应用层包装重复**：`application/spc/spc_data_decoration.py` 与
   `application/ctq/ctq_data_decoration.py` 约 90% 逐行重复，仅工作簿文件名、
   参数名不同（分析文档第 4 节）。
2. **核心引擎位置错误**：工作簿三态修饰引擎 `core/spc/spc_sheet_oos_decoration.py`
   实际被 spc/ctq/monitor 三方共用，却物理位于 spc 包内，ctq/monitor 反向依赖 spc。
3. **aoi 能力缺口**：aoi_tt/aoi_rs 只有自动截断（`core/shared/auto_decoration.py`），
   缺少 spc/ctq 已有的"配置文件指定释放或删除"能力（三态 flag 工作簿）。
4. **修饰位置不一致**：aoi_rs 的截断发生在图表组装层
   （`app/sections/aoi_rs/aoi_rs_dashboard.py:416-425`），违背"修饰由 application 层
   决定、前端只负责展示"的契约；aoi_tt 在 service 层修饰，位置正确。
5. **死代码**：CPK 双轨（真实/修正）移除后，修饰前特征（original_*）已无消费方，
   仍随共享管线透传并多算一次特征。

## 2. 目标

1. 修饰算法作为通用能力纳入 shared：核心引擎与应用层包装各只存在一份。
2. aoi_tt/aoi_rs 与 spc/ctq 对齐：超规自动修饰（已有）+ 配置文件指定释放或删除（新增）。
3. 修饰全部在 application/service 层完成；section 层只消费修饰后数据做展示，
   不存在任何依赖"未修饰数据"的前端逻辑。
4. 算法口径单一来源：截断 margin（5%~15% span）与稳定哈希只由 shared 定义，
   后续优化与排障只需改一处。

## 3. 用户已确认的决策

- **D1（方案）**：同意 `decoration-unify-proposal.md` 的全部分层归位设计。
- **D2（风险点 1，缓存一致性）**：保留"刷新缓存 / snapshot_signature 换 key"逃生通道，
  并固化"用户改工作簿 → 刷新缓存"的操作契约。
- **D3（风险点 2，引擎泛化回归面）**：`OOS_KEY_COLUMNS` 参数化前先跑绿现有测试，
  以现有测试为安全网再动引擎。
- **D4（风险点 3，section 层未修饰数据）**：如果 section 层存在需要"未修饰"数据的逻辑，
  一律修正为只处理修饰后数据。前端拿到的数据取决于后端提供什么；是否修饰由
  application 层决定，前端只负责展示。此设计约束绝对成立。
- **D5（CPK 单轨，已先行落地）**：CPK 仅基于修饰后点位计算，`cpk_actual` 已移除；
  修饰前特征透传随之成为死代码，本次一并清理。

## 4. 范围与交付物

### 4.1 应用层 wrapper 合并（纯重构，无行为变化）

- 在 `application/shared/` 提供按 scope 参数化的统一入口
  （scope → 工作簿文件名：spc→`spc_sheet_oos_decoration.xlsx`、
  ctq→`ctq_sheet_oos_decoration.xlsx`）；
- 删除 `spc_data_decoration.py` 与 `ctq_data_decoration.py`；
- `decorated_features.py` 的 spc/ctq 双分支简化为单分支，ctq 延迟导入随之消除；
- 统一后的返回结构不再携带修饰前特征（original_*），
  `fetch_decorated_features` payload 同步精简（D5）。

### 4.2 核心引擎归位与泛化

- `core/spc/spc_sheet_oos_decoration.py` 迁入 `core/shared/`，引用点全量更新；
- `_stable_fraction` 与 margin 常量保持单一来源（`auto_decoration.py` 继续复用）；
- 引擎键列（当前硬编码 `OOS_KEY_COLUMNS`）泛化为参数，spc/ctq 行为保持不变（D3）。

### 4.3 aoi_rs 截断位置归一（D4）

- aoi_rs 的双规格截断（By Lot 用 LOT_RATIO 规格、By Sheet 用 SHEET_ID/GLASS_ID 规格）
  从 section 层下移到 service 层；service 产出图表就绪的修饰后数据，
  section 只渲染；
- 同步检查并移除 section 层任何依赖未修饰值的逻辑（如报警计数）。

### 4.4 aoi 修饰工作簿（新能力）

- aoi_tt / aoi_rs 各新增修饰工作簿（`resources/` 根目录、每产品一个 sheet，
  与 spc/ctq 布局一致）；
- 复用三态 flag 语义：`True`/空 = 自动截断（与现有行为完全一致，向后兼容）、
  `False` = 释放真实值、`Delete` = 删除；
- 键列按模块数据实际可用列确定（候选：`prod_code + step_id + tt_name/rs_code + sheet_id`），
  引擎键列参数化支撑（4.2）；
- aoi_rs 双规格场景：工作簿需能区分图表口径（chart 维度），一行 flag 分别控制
  By Lot / By Sheet 两张图——实现前以数据实际列定案。

### 4.5 操作契约与文档

- 文档固化"改工作簿 → 页面点刷新缓存生效"契约（D2）；
- 同步更新 `docs/dev_docs/generated/Inline_domain/` 三份分析文档至最终状态。

## 5. 非目标

- monitor 对 AOI 组的 scope 从 `none` 切换为 aoi 口径：属行为变更，单独立项评估；
- monitor 的合规洗白（`sanitize_to_compliant`）机制：与数据修饰是两套机制，不动；
- CPK 修饰本身的 flag 语义（opt-in 覆盖）：已按 D5 落地，不再变更；
- 既有 `resources/*.xlsx` 用户工作簿中的历史 flag 数据迁移；
- aoi_tt 的 TT 公式、lot 粒度与 aoi_rs 的图表结构。

## 6. 验收标准

1. spc/ctq 修饰统一为单一应用层入口与单一引擎文件，全仓不再存在第二份 wrapper；
2. 统一后 SPC 报表、CTQ 报表、自动预警看板的数据口径与统一前完全一致（纯重构段）；
3. aoi_rs 的截断全部发生在 service 层，section 层无任何修饰逻辑、无未修饰数据依赖；
4. aoi_tt / aoi_rs 各有修饰工作簿：默认行为与现状一致（自动截断），
   手工置 `False` 的行释放真实值、置 `Delete` 的行从图表消失；
5. 截断算法（margin 区间、稳定哈希）全仓单一来源；
6. 修改工作簿后通过页面"刷新缓存"生效（缓存命中时不重读工作簿）；
7. 定向单元/集成测试全部通过，全量 pytest 不引入新失败（既有失败基线除外）；
8. SPC / CTQ / AOI_TT / AOI_RS / 自动预警 E2E 全部通过。

## 7. 风险与缓解

| 风险 | 缓解 |
|---|---|
| 用户编辑工作簿后缓存命中不重读 | 保留 snapshot_signature 换 key 与页面刷新缓存通道，文档固化操作契约（D2） |
| 引擎键列参数化触碰 spc/ctq 既有行为 | 先跑绿现有测试作为安全网，重构段与行为变更段分开提交验证（D3） |
| aoi_rs 截断下移改变 section 输入 | 按 D4 修正 section 为只消费修饰后数据；E2E 验证图表渲染 |
| aoi 明细数据缺少 sheet_id 等键列 | 计划阶段先核实数据列，缺列时按键候选降级并在 issue 留痕 |

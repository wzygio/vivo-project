# 修饰逻辑统一纳入 shared 的方案分析

> 对应需求：`docs/dev_docs/dev_spec/Inline_domain/feat-decoration_unify.md` Task1.3
>
> 议题：Inline_domain 各子模块的数据修饰逻辑极其相似却各自重复编写，应作为通用算法
> 纳入 `shared`，保证算法统一性、降低后续优化与排障成本。本文分析该想法并给出目标设计。

## 1. 结论先行

**想法成立，且方向正确。** 事实上代码库已经部分走在这条路上：

- 核心修饰引擎（三态 flag + 截断 + 工作簿持久化）已经是单份实现，被 spc/ctq/monitor 共用；
- aoi_tt/aoi_rs 的超规截断已收敛到 `src/inline_domain/core/shared/auto_decoration.py`，
  并显式仿照 SPC 的截断语义（`auto_decoration.py:1-11`）——这一做法本文同样认可，
  理由见第 4 节；
- 尚未统一的是**应用层包装**（spc/ctq 两份约 90% 重复的 wrapper）和 **aoi 侧的能力缺口**
  （只有自动截断，没有"配置文件指定释放/删除"的机制，且 aoi_rs 的截断位置与其他模块不一致）。

统一工作因此不是推倒重来，而是"把已经单份的东西归位、把复制的东西合并、把缺失的能力
按既有模式补齐"。

## 2. 现状盘点：哪些已共享、哪些在重复

| 层 | 资产 | 状态 |
|---|---|---|
| 核心算法 | `core/spc/spc_sheet_oos_decoration.py`（引擎） | 逻辑已共享，但**物理位置在 spc 包内**，ctq/monitor 都要跨包反向依赖 spc |
| 核心算法 | `core/shared/auto_decoration.py`（无工作簿自动截断） | 已正确放入 shared，复用引擎的 `_stable_fraction` |
| 核心算法 | `core/spc/cpk_decoration.py`（CPK 第二层修饰） | SPC 独有业务，保留在 spc 合理 |
| 应用包装 | `application/spc/spc_data_decoration.py` vs `application/ctq/ctq_data_decoration.py` | **约 90% 重复**：同样的特征重算、同样的资源目录解析、同样的引擎调用，仅文件名/参数名/是否保留原特征不同 |
| 应用管线 | `application/shared/decorated_features.py` | 已统一路由与缓存，是正确的复用范式 |
| 服务层调用 | aoi_tt：service 层截断（`aoi_tt_service.py:87`） | 位置正确 |
| 服务层调用 | aoi_rs：**截断在图表组装层**（`app/sections/aoi_rs/aoi_rs_dashboard.py:416-425`），service 显式不做（`aoi_rs_service.py:106-107`） | 与其他模块"修饰在 service 层完成"的契约不一致 |
| 监控路由 | monitor 对 AOI 组用 `scope='none'` 完全免修饰（`monitor_service.py:58-62`） | 与"aoi 对齐 spc/ctq"的目标存在口径缺口 |

重复的直接代价已经在显现：`_preprocess_sheet_features_by_type` 同一函数写了两遍
（ctq 那份是 spc 那份的复制），`resolve_*_product_resource_dir` 也是两份同构实现；
任何修饰口径调整（例如改 margin 区间、加一种 flag 取值）都要多处同步改，正是需求中
担心的"优化和排障复杂化"。

## 3. 目标设计

### 3.1 分层归位

```mermaid
flowchart TD
    subgraph core/shared
        A1[sheet_oos_decoration.py<br/>工作簿三态修饰引擎<br/>（现 core/spc/ 迁入）]
        A2[auto_decoration.py<br/>无工作簿自动截断（现状保留）]
        A3[clip 语义单一来源<br/>_stable_fraction / margin 常量]
    end
    subgraph application/shared
        B1[decorated_features.py<br/>缓存管线（现状保留）]
        B2[prepare_decorated_data(scope)<br/>合并 spc/ctq 两个 wrapper]
    end
    subgraph 各子模块 service
        C1[spc_service<br/>+ 独有 CPK 第二层]
        C2[ctq_service]
        C3[aoi_tt_service / aoi_rs_service<br/>修饰统一下移到 service 层]
    end
    A1 --> B2 --> B1
    A2 --> C3
    B1 --> C1
    B1 --> C2
```

三个动作：

1. **引擎归位**：`core/spc/spc_sheet_oos_decoration.py` 迁入 `core/shared/`
   （如 `core/shared/sheet_oos_decoration.py`），原位置保留 re-export 做过渡或直接
   全量改引用（模块不大，引用点可控）。`_stable_fraction` 与 margin 常量
   （5%~15% span）保持为两个模块共享的单一来源——`auto_decoration.py` 目前
   就是从引擎 import 的，归位后这个依赖方向更自然。
2. **合并应用层 wrapper**：在 `application/shared/` 提供一个按 scope 参数化的
   `prepare_decorated_data()`，内部仅"scope → 工作簿文件名"一张映射表
   （spc→`spc_sheet_oos_decoration.xlsx`，ctq→`ctq_sheet_oos_decoration.xlsx`），
   统一返回含修饰前特征的 dataclass；`spc_data_decoration.py` 与
   `ctq_data_decoration.py` 删除，`decorated_features.py` 的 spc/ctq 双分支简化为
   单分支，ctq 的延迟导入问题随之消失。
   依据：两份 wrapper 的差异已全部可参数化，见
   《SPC 与 CTQ 数据修饰逻辑分析》第 5 节。
3. **aoi 截断位置归一**：aoi_rs 的截断从 `app/sections/aoi_rs/aoi_rs_dashboard.py`
   下移到 `aoi_rs_service`，与 aoi_tt 及其他模块"service 层完成修饰、section 只渲染"
   的契约对齐。其双规格（By Lot 用 LOT_RATIO、By Sheet 用 SHEET_ID/GLASS_ID）在
   service 层分别产出两个截断后的 DataFrame 即可，不影响算法本身。

### 3.2 aoi 对齐 spc/ctq 的两个目标能力

需求目标是 aoi_tt/aoi_rs 与 spc/ctq 对齐：①超规片自动修饰（已有）②配置文件指定
释放或删除（缺失）。第②点的设计**不需要发明新机制**——直接复用工作簿三态 flag 模式：

- 新增 aoi 修饰工作簿（如 `resources/aoi_tt_sheet_oos_decoration.xlsx` /
  `aoi_rs_sheet_oos_decoration.xlsx`，延续"resources 根目录 + 每产品一个 sheet"布局）；
- 引擎按键匹配：aoi_tt 可用 `[prod_code, step_id, tt_name, sheet_id]`，
  aoi_rs 用 `[prod_code, step_id, rs_code, sheet_id/lot_id]`——
  这需要把引擎的 `OOS_KEY_COLUMNS` 从模块级常量改为参数（当前硬编码，
  `spc_sheet_oos_decoration.py:19`），这是引擎归位时唯一需要的泛化；
- flag 语义保持一致：`Delete` 删除、`False` 释放真实值、`True`/空 = 自动截断
  （对 aoi 而言"默认 True"恰好就是现有的自动截断行为，向后兼容）；
- aoi_rs 的双规格场景：工作簿行需带图表口径维度（如 `chart_kind` 列或按 type_flag
  区分规格来源），否则一行 flag 无法分别控制两张图——这是 aoi_rs 相对 spc/ctq 的
  真实设计增量，需在实现前定案。

### 3.3 明确的边界（不纳入统一的部分）

- **CPK 修饰**留在 `core/spc/`：它是 SPC 能力报表的独有业务（opt-in 语义、周期维度键），
  与 Sheet OOS 修饰层级不同；
- **monitor 的合规洗白**（`sanitize_to_compliant`）是另一套机制（按段配置规则改报警状态），
  不属于本次"数据修饰"统一范围；
- monitor 对 AOI 组的 `scope='none'` 路由：aoi 工作簿落地后，可评估将 AOI 组
  从免修饰切换为 aoi 口径，但这属于行为变更，应作为独立一步并同步更新
  `monitor_service.py:55-57` 的注释约定。

## 4. 风险与实施顺序

风险点：

1. **缓存一致性**：修饰工作簿由用户手工编辑，`st.cache_data` 命中时不会重读工作簿；
   现状靠"刷新缓存"（snapshot_signature 换 key）解决，统一后需保留同样的逃生通道，
   并在文档中固化"改工作簿 → 刷新缓存"的操作契约。
2. **引擎泛化的回归面**：`OOS_KEY_COLUMNS` 参数化会触碰 spc/ctq 既有行为，
   必须以现有测试（`tests/` 下 spc 相关用例）为安全网，先跑绿再动。
3. **aoi_rs 截断下移**会改变 section 层拿到的数据（已修饰 vs 未修饰），
   需要同步检查 dashboard 中是否有依赖未修饰值的逻辑（如报警计数）。

建议顺序（每步独立可交付）：

1. 合并 spc/ctq 应用层 wrapper 到 `application/shared`（纯重构，无行为变化）；
2. 引擎迁入 `core/shared`（纯移动 + 引用更新）；
3. aoi_rs 截断下移到 service 层（行为等价迁移）；
4. 引擎键列参数化 + aoi 修饰工作簿（新能力，实现"配置文件指定释放/删除"）；
5. （可选）monitor AOI 组口径切换评估。

## 5. 总结

| 判断 | 结论 |
|---|---|
| "修饰逻辑应作为通用算法纳入 shared" | 赞同。核心引擎已是单份，应把物理位置与应用层包装一并归位 |
| "auto_decoration 放入 core/shared 的做法" | 赞同。它复用引擎的稳定哈希与截断语义，是算法统一性的正确示范 |
| 统一的实质工作量 | 不在算法（已统一），而在：合并两个 wrapper、引擎归位与键列泛化、aoi_rs 截断位置归一、为 aoi 补齐工作簿三态机制 |
| 不应统一的部分 | CPK 修饰（SPC 独有）、合规洗白（另一机制） |

# Inline Pipeline 复用：最终设计方案（V3 定稿）

- 日期：2026-08-13（V3 定稿，吸收用户修正点 1/2 与决策 D2/D3）
- 关联 Issue：`.scratch/inline-pipeline-reuse/issues/01-shared-pipeline-to-measurement-and-monitor-reuse.md`
- 关联 ADR：`docs/ADR/0012-shared-inline-measurement-snapshot.md`
- 关联计划：`.planning/2026-08-13-inline-pipeline-reuse/`

## 用户决策与修正点（已确认）

- **D2（CTQ 修饰口径）**：切换。monitor 的 CTQ 数据改用 ctq 修饰口径。事实核查：ctq 修饰已汇总为
  `resources/ctq_sheet_oos_decoration.xlsx`（一个文件、每产品一个 sheet，
  `ctq_data_decoration.py:18,30-38`），与 SPC 共用同一引擎，仅文件名不同。
- **D3（AOI 归属）**：monitor 的 AOI 对应 aoi_tt；aoi_tt 在本次范围内，aoi_rs 不在。
- **修正点 1（段 1 归属）**：spc_repository 中的可复用逻辑**必须全部**入
  `infrastructure/measurement/`；spc 是与 ctq/aoi_tt/monitor 平行的业务模块。
- **修正点 2（段 2 结构）**：用户质疑"repository 汇总进共享 SheetFeaturesService 再供给各模块"
  的 DDD 合理性 —— 本版采纳并修正（见 §3）。

---

## 1. 段 1 设计：共享制备管线归属 measurement

### 1.1 迁移清单（spc_repository → measurement）

| 逻辑 | 现状位置 | 目标位置 | 备注 |
|---|---|---|---|
| 清洗/类型 coercion/去重 | `spc_repository.py:171-183` | `measurement/measurement_preparation.py` | 原样平移 |
| LOSS 排除参数过滤 | `:179` + `spc/measurement_preprocessor.py` | 同上（函数随迁移改名去 SPC 化） | **顺序：在 data_type 过滤之前** |
| 白名单 merge + data_type 注入 | `:184-201` | 同上 | 经 metadata port，逻辑不变 |
| 异常点过滤 `_apply_outlier_filters` | `:225-397` | 同上（独立函数） | **顺序：在 data_type 过滤之后**；COM 解密写 CSV 副作用原样保留（后续单列优化） |
| 维度/时间过滤 | `:203-213` | 同上 | 原样平移 |
| 主制程追溯纯函数 | `spc/main_process_trace.py` | `measurement/main_process_trace.py` | 纯 DataFrame 函数，直接平移 |
| 规格线 + YAML 覆盖 | `:46-152` | `measurement/`（制备仓储的方法） | 机制被 spc/ctq/monitor 共用；键名 `spc_spec_override` 保留兼容 |
| `get_scrap_data` + `_infer_factory_from_step` | `:402-524` | **`infrastructure/monitor/scrap_repository.py`** | 仅 monitor 消费，与 measurement 零耦合，不入 measurement |

### 1.2 目标结构

```text
infrastructure/
  measurement/                       # 共享测量能力（DAO + 快照 + 制备）
    measurement_data_loader.py       # 三厂 DAO（不动）
    measurement_metadata_loader.py   # 参数目录/规格 DAO（不动）
    measurement_snapshot_repository.py  # 原始快照（不动）
    main_process_history_repository.py  # 履历 DAO（不动）
    main_process_trace.py            # 【迁入】追溯纯函数
    measurement_preparation.py       # 【新增】共享制备管线 + 规格覆盖
  spc/spc_repository.py              # 薄投影：data_type_filter="SPC"，委托制备管线
  ctq/ctq_repository.py              # 薄投影：data_type_filter="CTQ"（现状已是薄投影）
  aoi_tt/aoi_tt_repository.py        # 不动（直接用快照+规格 DAO，D3 纳入结构核查）
  monitor/
    monitor_repository.py            # 门面（不动）
    scrap_repository.py              # 【新增】报废适配器
```

- 各模块 repository 直接面向本模块 application service，**1:1 对应关系保留**。
- `application/ports/measurement_snapshot.py` 增加 `MeasurementPreparationPort`
  （`get_prepared_measurements(config)` / `get_spec_limits(prod_code)`），
  spc/ctq/monitor 的 repository 组合该 port 产出各自投影。
- 顺带修正 `composition.py:18` 缺失的 `src.` 前缀。
- **保序约束**（行为不变的关键）：LOSS 过滤 → 白名单/data_type 注入与过滤 →
  异常点过滤 → 时间/维度过滤 → 主制程追溯。

---

## 2. 段 1 论证：为什么这是正确归属

制备管线的职责是"把持久化事实装配为领域就绪的测量集"——SQL/Parquet/白名单表/规则文件/
YAML，全部是出站适配器关注点，且所有消费方口径完全一致。把它留在 spc 会让 ctq/monitor
反向依赖一个兄弟业务模块；放入 measurement 后，spc 退化为与 ctq 对称的薄投影，
符合 ADR-0012 确立的"共享适配器 + 各报表派生"方向。

---

## 3. 段 2 终审：对用户修正点 2 的回答与修正后方案

### 3.1 用户的质疑成立（V2 方案的修正）

V2 的 `SheetFeaturesService` 把各模块 repository 汇总进一个共享服务、再回供各模块
service —— 这在 DDD 下确实不成立：它把"repository → 本模块 application service"的
1:1 编排关系改成了漏斗，模块 service 不再拥有自己的数据管线。

### 3.2 终审结论：共享的是"无状态计算函数"，不是"服务"

重新审视事实后，段 2 的真实结构是：

1. **修饰是同一个领域引擎**（`core/spc/spc_sheet_oos_decoration.py`），SPC/CTQ 的唯一差异是
   工作簿文件名参数；特征计算是 `core/` 纯函数。**领域层已经共享**，无需新建服务。
2. 各模块的差异化只在"用哪个口径（scope）调用引擎"：
   - spc 模块 → scope=`spc`；ctq 模块 → scope=`ctq`；
   - monitor → 按 data_type 路由：SPC 行→`spc`，CTQ 行→`ctq`（D2），AOI 行→**不修饰**（D3）。
3. 唯一真正缺失的共享点是**计算结果缓存**（消除重复执行）。

因此最终方案：**保留各模块 repository↔service 的 1:1 结构**，新增一个
**无状态、带 `st.cache_data` 的共享计算函数**（application 层的缓存边界工具），
各模块 service 用自己的 repository 数据 + 自己的 scope 调用它：

```python
# application/shared/decorated_features.py（新增，唯一新增的应用层共享点）
@st.cache_data(show_spinner=False, max_entries=12, ttl=4 * 60 * 60)
def fetch_decorated_features(
    _measurements_and_specs,   # 下划线前缀：不参与 hash（沿用 _db_manager 既有模式）
    prod_code: str,
    scope: str,                # 'spc' | 'ctq' | 'none'
    snapshot_signature: str = "",
) -> dict:                     # 只含原生 DataFrame/dict（ADR-0001）
    """按口径执行 Sheet OOS 修饰 + 特征计算；缓存 key=(prod, scope, signature)。"""
```

- spc service → `fetch_decorated_features(..., scope="spc")`
- ctq service → `fetch_decorated_features(..., scope="ctq")`
- monitor → 按 data_type 分组分别调用 `spc`/`ctq`/`none`，再合并进入判定管线
- **一致性由此达成**：SPC 页与 monitor 的 SPC 部分命中同一缓存条目（同 prod、同 scope、
  同签名），产出必然相同；CTQ 同理。这正是需求的一致性目标，且不引入服务漏斗。
- 缓存函数**只做内存计算**；spc/ctq 的审计文件落盘保留在各自模块的薄包装中
  （缓存外执行，行为不变）。

### 3.3 monitor 的最终形态

`fetch_dashboard_data_dict` 变为薄判定层：

1. 经本模块 repository（MonitorSpcDataPort）取 ALL 制备测量；
2. 按 data_type 分组，经共享缓存函数取各口径特征（CTQ 组走 ctq 口径 = D2；AOI 组 `none` = D3）；
3. 报废分支经 monitor 自己的 scrap_repository；
4. `apply_spc_rules → sanitize_to_compliant → 站点聚合 → 时间桶 → 全局/明细聚合`（不变）。
   `get_monitor_defect_details` 走同一路径，删除内联取数/修饰副本。

### 3.4 D3 的对齐边界（基于事实核查）

monitor-AOI 与 aoi_tt **不是同一份数据**：参数识别（白名单 NULL→AOI vs 规格表
param_type NULL）、粒度（sheet 特征 vs 保留 lot）、语义（报警判定 vs TT 均值）均不同。
本次对齐到：**同源快照（已成立）+ AOI 行免于 SPC 修饰文件处理（与 aoi_tt 无修饰一致）
+ aoi_tt 模块结构核查**。"参数识别集统一"属行为变更，列入 Out of scope 单独立项。
注意：AOI 行免修饰后 monitor 的 AOI 报警数可能变化（预期内的对齐结果）。

---

## 4. 迁移步骤与验证

1. **特征化测试先行**：固定 prod + 结束日期，对 SPC/CTQ/monitor 输出做快照断言（安全网）。
2. 段 1：迁 `main_process_trace.py`、`measurement_preprocessor.py`、制备管线、规格覆盖、
   异常点过滤 → `measurement/measurement_preparation.py`；spc_repository 改薄投影；
   scrap 迁 monitor；composition 重接线 + 修 import；保序。
3. 段 2：新增 `application/shared/decorated_features.py`；spc/ctq service 改走共享缓存函数；
   monitor 按类型路由修饰（D2/D3）；下钻收敛。
4. 页面登记清缓存：`自动预警看板.py:72` 等 `funcs_to_clear` 增加共享缓存函数。
5. 验证：定向单测/集成 → SPC/CTQ/AOI_TT/自动预警 E2E → 全量 pytest（基线 7 个既有失败除外）。
6. 文档：`references/domain/Inline_domain/` infrastructure 架构规范 + 本设计文档（已更新）。

### 风险与对策

| 风险 | 对策 |
|---|---|
| 制备管线迁移后顺序变化导致口径漂移 | 特征化测试逐断点比对；保序约束写入架构文档 |
| 强刷链路断裂 | 页面登记共享缓存函数 + snapshot_signature 入 key |
| 审计文件落盘语义变化 | 落盘保留在模块薄包装（缓存外），行为不变 |
| AOI 免修饰改变 monitor 报警数 | D3 预期内对齐结果，计划批准时显式确认 |

### 不做的事（YAGNI / Out of scope）

- 不统一 monitor-AOI 与 aoi_tt 的参数识别集；不迁移历史 CTQ flag 数据；
- 不重构 outlier COM 写副作用；不动 aoi_rs；不改 monitor 判定/聚合口径与 aoi_tt 公式。

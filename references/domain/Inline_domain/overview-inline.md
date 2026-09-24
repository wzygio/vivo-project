# Inline Domain · 在线监控数据域设计

> **领域代码**: `inline_domain`  
> **对应目录**: [`src/inline_domain/`](../../../src/inline_domain/)
> **最后更新**: 2026-09-16

---

## 1. 概述

Inline 数据域负责面板制造过程中的在线量测监控。应用路径：`monitor` 提供自动预警聚合，`spc` 提供 SPC 分布和 CPM/CPK 能力指数，`ctq` 提供不含能力指数的 CTQ 分布报表，`aoi_tt` 提供 TT 趋势报表，`aoi_rs` 提供 RS Code 趋势报表。前四者共享 `infrastructure/shared/` 的同一测量快照与制备管线，在各自应用服务边界固定数据类型和返回契约；`aoi_rs` 走独立取数链路（ADR-0015）。

---

## 2. 分层架构

```
┌─────────────────────────────────────────────────┐
│              Application Layer                   │
│  monitor/monitor_service.py (自动预警)             │
│  spc/spc_service.py         (SPC + CPM/CPK)       │
│  ctq/ctq_service.py         (CTQ 分布，无能力指数)   │
│  aoi_tt/aoi_tt_service.py   (TT 趋势)              │
│  aoi_rs/aoi_rs_service.py   (RS 趋势，service 层修饰) │
│  shared/decorated_features.py (共享修饰+特征缓存)    │
│  shared/decorated_data.py    (统一修饰入口，scope 路由)│
│  ports/measurement_snapshot.py (共享快照/元数据 Port) │
├─────────────────────────────────────────────────┤
│               Core Domain Layer                   │
│  shared/sheet_oos_decoration.py (工作簿三态修饰引擎)  │
│  shared/auto_decoration.py     (自动截断+三态应用)    │
│  spc/spc_calculator.py         (周期 CPM/CPK)       │
│  spc/cpk_decoration.py         (CPK 人工修饰单轨)     │
│  ctq/indicator_chart.py        (UNI 图表类型标记)     │
│  monitor/monitor_calculator.py (预警规则与特征降维)   │
│  monitor/monitor_param_classifier.py (参数类型分类)   │
│  aoi_tt/aoi_tt_calculator.py + aoi_tt_decoration.py  │
│  aoi_rs/aoi_rs_calculator.py + aoi_rs_decoration.py  │
├─────────────────────────────────────────────────┤
│            Infrastructure Layer                   │
│  shared/ (共享 DAO + 原始快照 + 制备管线 + 主制程追溯) │
│  spc/ ctq/ aoi_tt/ monitor/ (平行薄投影/门面)       │
│  aoi_rs/ (独立链路：产品级双 Parquet 快照)           │
├─────────────────────────────────────────────────┤
│  装配：src/inline_domain/composition.py（唯一组合根） │
└─────────────────────────────────────────────────┘
```

> Infrastructure 详细规范见 [`architecture-inline-infrastructure.md`](./architecture-inline-infrastructure.md)。

PNL 指标规格的版本/产品收严离线分析工具已于 2026-09-24 按维护要求移除，
不属于在线 Inline 运行链路。当前保留的运维工具见 [工具脚本说明](../overview-project-tools.md)。

---

## 3. 架构约束：shared 子模块

**对于各模块可复用的逻辑，应当提取出来并写入 shared 子模块下。** 这是本域
（以及前端 inline 报表）的核心结构约束：

| 层 | shared 子模块 | 承载内容 | 现有成员 |
|---|---|---|---|
| Application | `application/shared/` | 跨模块用例编排、修饰入口、缓存管线 | `decorated_data.py`、`decorated_features.py` |
| Core | `core/shared/` | 跨模块领域规则与算法 | `sheet_oos_decoration.py`、`auto_decoration.py` |
| Infrastructure | `infrastructure/shared/` | 跨模块取数、快照、制备、主制程追溯（原 `measurement/`） | 见 infra 规范 |
| 前端绘图 | `app/charts/inline/` | 四报表共享绘图（ADR-0016） | `chart_type`、`spec_lines`、`sheet_charts`、`aoi_charts` |
| 前端组装 | `app/sections/inline_domain/shared/` | 四报表共享筛选级联与修饰后台（ADR-0016） | `filters`、`decoration_admin` |

约束细则：

1. 业务模块（`spc` / `ctq` / `aoi_tt` / `aoi_rs` / `monitor`）只保留各自业务差异，
   不得承载跨模块共享逻辑；
2. 禁止跨业务模块导入私有函数（历史教训：ctq 曾私有导入 spc 绘图函数并产生
   签名错配缺陷，见 ADR-0016）；复用必须经由 shared 公共 API；
3. shared 成员保持可单测：纯函数不读配置、不碰 session 状态，配置与 key 前缀
   由调用方注入；
4. 新增逻辑时先判断归属：≥2 个模块复用 → shared；仅单模块使用 → 留在本模块。

---

## 4. 应用服务层 (Application)

### 4.1 `monitor/monitor_service.py` — 自动预警入口

**角色**: 旧测量链的多厂别聚合 + 重叠时间桶

核心职责：
- 接收前端查询参数（产品代码、厂别、时间范围等）
- 协调多厂别数据源（通过多态分表 UNION）
- 构建重叠时间桶（Overlapping Time Buckets）
- 按 data_type 路由修饰口径：SPC→spc、CTQ→ctq、AOI→免修饰（D2/D3）
- 调度 SPC 规则引擎
- 保留旧合规函数调用；该函数目前仅返回副本，不再修改计算结果。

新自动预警页面不再调用这条旧全量链，而由 `monitor/oos_monitor_service.py` 读取
各 scope 的 OOS/OOC Excel 产品页，只有 `flag=False` 的明细进入报警结果。
当前汇总通过工作簿基线替换本周贡献，不重算历史累计量（详见第 7 节）。
汇总表以查询截止日构建当年、截至当前季度、
最近三个月和当前 ISO 周的重叠列，行固定为过货量、OOC、SOOS=0、OOS 和 Total；
OOC 三态决策入口及产品 × scope 更新时间只在 `?admin=true` 显示。
报警率与 CPK 共用的持久化工作簿路径由 `inline_domain.yaml` 的
`resources.files.monitor_summary_workbook` 注入；工作簿使用 `<产品>报警率` 与
`<产品> CPK` 分页，基础设施读取器聚合各产品页后再交给应用服务筛选。

### 4.2 `spc/spc_service.py` — SPC 能力报表

- 在服务边界强制 `data_type_filter = "SPC"`。
- 生成 Sheet/点位分布、月/周/日 CPM/CPK、CPK 预警及 OOS/CPK 修饰结果。
- CPK/CPM 共用同一条能力计算准入规则：仅双边有效规格参与计算；`LSL` 为空或为 `0`
  均视为仅上限规格。参数名豁免列表由 `inline_domain.yaml` 的
  `spc.spc_cpk.exempt_param_name_contains` 配置，被豁免参数仍保留分布图数据。
- 应用服务不返回图表样式；SPC/CTQ Sheet 点位图由前端 `app/charts/inline/chart_type.py`
  按 `inline_config.yaml` 的 `spc.chart.line_param_name_contains` 统一选择折线或箱线（ADR-0016）。
- Sheet OOS 修饰表的 `flag` 为三态：`True` 修饰超规点、`False` 保留真实值、`Delete` 按产品/站点/参数/Sheet 四键从图表点位中排除；修改表内 `sheet_min/max/mean` 不改变计算结果。
- 修饰表支持标准或企业加密 XLSX；已有文件双重读取失败时必须中止本次报表重建并保留原文件。直接编辑文件后按 ADR-0005 通过页头“刷新缓存”手动生效。
- 缓存函数只返回原生 payload，ViewModel 在缓存外构造。

### 4.3 `ctq/ctq_service.py` — CTQ 分布报表

- 在服务边界强制 `data_type_filter = "CTQ"`，前端不参与数据类型判断。
- 返回 Sheet 特征、原始点位、指标元数据和 OOS 修饰结果；契约中不包含 CPM/CPK、CPK 预警或 CPK 修饰。
- Core 侧 `core/ctq/indicator_chart.py` 仍为 payload 标记 `chart_type` 列（参数名含 `UNI` → line）；
  前端实际渲染决策统一由 `app/charts/inline/chart_type.py` 按配置完成，与 SPC 同口径（ADR-0016）。
- OOS/OOC 决策文件位于 `resources/inline_domain/ctq/`（一个文件、每产品一个 sheet），与 SPC 决策文件隔离、共用同一持久化引擎。
- 页面缓存遵守 ADR-0001：只缓存 DataFrame/原生容器/标量，并在缓存外构造 `CtqReportViewModel`。

### 4.4 `aoi_tt/aoi_tt_service.py` — AOI TT 趋势报表

- 通过规格表（`param_type`）识别 TT 指标，趋势分母与规格口径遵循 ADR-0008。
- service 层完成超规截断（`core/shared/auto_decoration.py`）与 TT 修饰工作簿
  （`core/aoi_tt/aoi_tt_decoration.py`，键 `[prod_code, step_id, tt_name, sheet_id]`）三态应用。
- Particle Size 默认按站点比例规格稳定生成 S/M/L/H，也可切换为 ARRAY/TP 缺陷明细实表计数；
  OLED 保持 Total-only，具体约束见 ADR-0025。

### 4.5 `aoi_rs/aoi_rs_service.py` — AOI RS 趋势报表

- 不复用共享 measurement：RS Code 明细与过货分母来自独立表/视图，由
  `infrastructure/aoi_rs/` 产品级双 Parquet 快照承载（ADR-0015）。
- 截断与 RS 修饰工作簿三态（含 `chart_kind` + `point_id` 键维度）在 service 层完成，
  section 只消费修饰后数据（ADR-0014 D4）。

### 4.6 `application/shared/` — 共享修饰管线

- `decorated_data.py`：统一修饰入口 `prepare_decorated_data(scope=...)`，scope → 修饰工作簿映射。
- `decorated_features.py`：`fetch_decorated_features` 共享无状态修饰+特征计算缓存
  （scope=spc/ctq/none），缓存 key 含产品、scope、起止日期与快照签名。
- `application/ports/measurement_snapshot.py`：共享快照/元数据 Port 协议。

### 4.7 `application/spc/dtos.py` 中的 `SpcQueryConfig`

`SpcQueryConfig` DTO — 封装 SPC 查询参数（应用层契约，各模块共用）。

---

## 5. 核心领域层 (Core Domain)

### 5.1 [`monitor_calculator.py`](../../../src/inline_domain/core/monitor/monitor_calculator.py) — SPC 规则引擎

**三阶段处理流程**：

#### Phase 1: 特征降维
- 从原始 Panel 级量测数据提取统计特征
- 均值（Mean）、极值（Min/Max）、标准差（StdDev）
- 按时间桶（Time Bucket）进行分组聚合

#### Phase 2: 规则判定
| 规则 | 全称 | 判定条件 |
|------|------|----------|
| **OOS** | Out of Spec | 均值触碰规格线 USL/LSL |
| **SOOS** | Some Out of Spec | 极值触碰规格线 |
| **OOC** | Out of Control | 均值触碰管控线 UCL/LCL |

#### Phase 3: 报表聚合
- 按厂别 × 产品 × 时间维度聚合
- 生成 SPC 看板数据
- `sanitize_to_compliant` 为已停用的兼容钩子，仅返回输入副本。

### 5.2 [`monitor_param_classifier.py`](../../../src/inline_domain/core/monitor/monitor_param_classifier.py) — 参数类型分类器

纯函数，无 I/O 依赖。将 `IMP_SPC_TZBJX` 表中的原始 `data_type` 值映射为标准分类标签。

```python
def classify_param_type(raw_data_type: Optional[str]) -> str:
    # NULL / 空字符串 / 仅空白 → "AOI"
    # 其他 → 去空白后转大写 (如 "spc" → "SPC")
```

| 输入 (DB raw) | 输出 (标准标签) |
|---------------|----------------|
| `None` / `""` / `"  "` | `AOI` |
| `"SPC"` / `"spc"` | `SPC` |
| `"CTQ"` / `"ctq"` | `CTQ` |

### 5.3 `core/shared/` — 共享修饰引擎

- `sheet_oos_decoration.py`：工作簿三态修饰引擎（True 截断 / False 释放 / Delete 剔除），
  键列经 `key_columns` 参数泛化，spc/ctq/aoi_tt/aoi_rs 共用。
- `auto_decoration.py`：AOI 超规自动截断 + 三态应用；与 Sheet OOS 引擎共用稳定哈希，
  截断实现分别处理 AOI 单边上限与 SPC/CTQ 双边规格，不能混用缺失规格的行为。

---

## 6. 基础设施层 (Infrastructure)

完整规范（模块矩阵、制备管线顺序、快照契约、装配）见
[`architecture-inline-infrastructure.md`](./architecture-inline-infrastructure.md)。要点：

- `shared/`（原 `measurement/`，2026-08 按 shared 约束更名归位）拥有三厂测量 DAO、
  参数元数据 DAO、产品级原始 Parquet 快照（3 个月滚动窗口、TTL 统一配置于
  `config/global.yaml` 的 `application.cache_ttl_hours`、策略版本、原子写、
  失败降级）、共享制备管线（`measurement_preparation.py` + `measurement_preprocessor.py`）、
  主制程追溯（`main_process_history_repository.py` + `main_process_trace.py`）与
  站点描述字典（`step_description_loader.py`，纯展示用途）。
- spc / ctq / aoi_tt / monitor 为平行薄投影模块；报废适配器归 monitor
  （`monitor/scrap_repository.py`）；aoi_rs 独立。
- 制备管线顺序为行为契约：清洗 → 排除参数（LOSS）→ 去重 → 白名单 merge +
  data_type 注入/过滤 → 异常点过滤 → 时间/维度过滤 → 主制程追溯。

**涉及数据库表：**

| 表名 | Schema | 性质 | 说明 |
|------|--------|------|------|
| `spc_tzbjx_array` | eda | 时序明细 | ARRAY 厂 SPC 测量数据，主键: sheet_id + step_id + param_name + site_name |
| `spc_tzbjx_oled` | eda | 时序明细 | OLED 厂 SPC 测量数据（ID 列为 glass_id） |
| `spc_tzbjx_tsp` | eda | 时序明细 | TP 厂 SPC 测量数据（ID 列为 glass_id） |
| `IMP_SPC_TZBJX` | eda | 配置/元数据 | SPC 参数白名单，列: parmtername, data_type, productspecname |
| `dwd_imp_dv_param_spec` | - | 配置 | 管控规格基准表（USL/LSL/UCL/LCL） |
| `DWR_MES_PRODUCTSPEC` | - | 字典 | MES 产品字典表（productspecname → productcode 翻译） |

参数名包含 `LOSS` 的记录在制备层统一排除。`MT_CH_*`
不属于全局排除项，其是否进入 SPC 报表完全由白名单中的标准化 `data_type`
决定。

---

## 7. 各子模块修饰逻辑

本节描述当前代码行为。需区分三种作用：**数值修饰**生成图表/统计使用的派生数据，
**报警过滤**决定哪些 Excel 明细进入报警结果，**矩阵显示修饰**只调整页面状态。
OOC 工作簿存在或其中 `flag=True`，不代表测量值已经被拉回 UCL/LCL 内。

### 7.1 当前行为总览

| 子模块 | 数值修饰依据 | 默认行为 | OOC 处理 |
|---|---|---|---|
| SPC | 点位 `param_value` 与 USL/LSL；另有 CPK/CPM 人工覆盖 | Sheet OOS 默认 `True`；能力指数覆盖默认 `False` | 生成真实 OOC 明细供报警过滤；没有按 UCL/LCL 回调点位值的步骤 |
| CTQ | 与 SPC 共用 Sheet OOS 引擎，工作簿独立 | Sheet OOS 默认 `True`；无 CPK/CPM 覆盖 | 同 SPC |
| AOI_TT | `tt_qty` 与 UCL 比较 | 超过 UCL 默认截断；OOS 台账仍按 USL 建立 | OOC 明细按 UCL 判定并排除 OOS；OOC 工作簿不驱动数值截断 |
| AOI_RS | By Lot / By Sheet 各自匹配的单边 `spec` | 超过 `spec` 默认截断 | 尚无真实控制限来源，OOC 明细构建函数返回空表 |
| monitor | 读取各模块 OOS/OOC Excel 产品页的明细及 `flag` | 只有 `False` 进入报警结果 | 不重新截断测量值；按过滤后的报警更新本周汇总贡献 |

入口与当前配置见 [decorated_features.py](../../../src/inline_domain/application/shared/decorated_features.py)、
[inline_domain.yaml](../../../config/domain/inline_domain.yaml)。目前没有独立的 OOC 数值修饰总开关；
是否执行由各服务调用路径、规格和记录级决策决定。

### 7.2 SPC 与 CTQ：Sheet OOS 点位修饰

1. 先从未做报表修饰的测量数据计算 Sheet 特征，筛选 `sheet_max > USL` 或
   `sheet_min < LSL` 的候选 Sheet；等于规格线不算越规。
2. 按 `prod_code + step_id + param_name + sheet_id` 匹配三态决策：
   `True`（无决策时默认）修饰该 Sheet 的超规点，`False` 保留真实点位，
   `Delete` 从本次投影移除该候选 Sheet 对应的全部点位。
3. 截断要求 USL、LSL 均非空且 `USL > LSL`。令 `span = USL - LSL`，
   超上限点变为 `USL - margin`，超下限点变为 `LSL + margin`，
   `margin` 为 span 的 5%～15%，由业务键、原值等参与的稳定哈希确定。
   缺任一边界或上下限无效时保留原值；不会自动套用 AOI 的单边截断算法。
4. 修饰点位后重新计算 Sheet 特征及后续统计。编辑工作簿里的
   `sheet_min/max/mean` 不会直接覆盖测量值；这里消费的是决策 flag。

SPC/CTQ 的纯 OOC 条件为 `sheet_mean > UCL` 或 `< LCL`，并排除已有 OOS 的 Sheet。
OOC 明细从报表修饰前的 Sheet 特征生成。OOC 工作簿影响报警过滤，不参与上述点位截断；
OOS 点位修饰可能间接改变均值，但这不等于另有 OOC 自动修饰流程。

实现：[Sheet OOS 引擎](../../../src/inline_domain/core/shared/sheet_oos_decoration.py)、
[共享应用入口](../../../src/inline_domain/application/shared/decorated_data.py)、
[OOC 明细构建](../../../src/inline_domain/core/shared/sheet_ooc_decoration.py)。

### 7.3 SPC：CPK/CPM 人工覆盖

- 能力值先由修饰后的点位计算，再应用独立能力台账；CTQ 不执行此步骤。
- 新增台账候选只取**上一个完整 ISO 周、指标值小于 1.33** 的记录；保留已有历史决策。
- 匹配键为产品、厂别、站点、参数、周期类型、周期标签；覆盖应用支持月/周记录，
  不覆盖日记录。CPK 使用产品同名 sheet，CPM 使用 `<产品>_cpm`。
- `flag` 默认 `False`；只有启用为 `True` 才用 `cpk_replacement` /
  `cpm_replacement` 覆盖对应能力值。此处不支持 Sheet OOS 的 `Delete` 动作。
- 已保存且满足 `1.33 < replacement < 1.4` 的替换值继续复用；缺失或越界时随机生成
  1.331～1.399 的三位小数并由持久化流程保存。此机制不是点位截断使用的稳定哈希。
- 当前配置中参数名包含 `PPA` 的项不计算 CPK/CPM；这是能力计算准入规则，
  不表示 SPC/CTQ 的 Sheet OOS 引擎也具有同名豁免。

实现：[cpk_decoration.py](../../../src/inline_domain/core/spc/cpk_decoration.py)、
[capability_decoration_service.py](../../../src/inline_domain/application/spc/capability_decoration_service.py)。

### 7.4 AOI_TT：按 UCL 截断，按 USL 维护 OOS 台账

- OOS 台账候选为 `tt_qty > USL`，匹配键是产品、站点、TT 名、Sheet。
  数值修饰另外按站点与 TT 名匹配 UCL：`tt_qty > UCL` 时默认回调至 UCL 的
  85%～95%。同一输入使用稳定哈希，重复计算结果一致。
- 缺少有效 UCL 时不回退到 USL；无工作簿或缺少匹配决策时仍执行默认自动截断。
  因此仅超过 UCL、未超过 USL 的记录也可被截断，即使它不在 OOS 候选表中。
- 三态动作来自 **OOS** 修饰流程：匹配的 `False` 保留原值、`Delete` 剔除行，
  `True` 执行截断。当前函数在有可匹配规格处理的路径上应用这些动作；完全无 UCL
  规格时直接返回原明细，不执行该路径的三态处理。
- `auto_decoration.exempt_param_name_contains` 当前为 `PPA`，对 `tt_name` 做
  大小写不敏感的包含匹配；命中则豁免截断，Delete 在三态处理内优先于豁免。
- OOC 台账记录 `tt_qty > UCL` 且未超过 USL 的原始明细，单独用于报警过滤。
  编辑 OOC flag 不会直接改变 TT 图表数值。

实现：[aoi_tt_decoration.py](../../../src/inline_domain/core/aoi_tt/aoi_tt_decoration.py)、
[decoration_service.py](../../../src/inline_domain/application/aoi_tt/decoration_service.py)。
Particle Size 的 S/M/L/H 按比例生成属于另一个数据生成步骤，见第 4.4 节，不能视作 OOC 截断规则。

### 7.5 AOI_RS：按图表口径的单边 spec 截断

规格来自 `mdw.dwd_imp_rs_code_xishu_fo_tzsbjx`，按产品查询，再按
`factory + step_id + rs_code` 匹配；同一图表口径有多条规格时取最大 `spec`。

| 图表 | 指标 | 规格的 type_flag |
|---|---|---|
| By Lot | Lot 的 `Σcode_qty / 过货片数`，即 `value` | `LOT_RATIO` |
| By Sheet | 单片 `Σcode_qty`，即 `rs_qty` | `SHEET_ID` / `GLASS_ID` |

- `value > spec` 时默认截断为正上限 `spec` 的 85%～95%，使用稳定哈希；
  无匹配规格或上限非正时不截断。
- 决策键为产品、厂别、站点、RS Code、`chart_kind`、`point_id`；
  Lot 图的 point_id 为 lot_id，Sheet 图为 sheet_id，两个口径独立应用。
- `True` 或无匹配决策默认截断，`False` 保留真实值，`Delete` 删除图点。
  `rs_code` 包含 `PPA` 时按当前共享配置豁免截断，Delete 优先。
- 数值修饰作用于服务构造的 Lot/Sheet 图点，不回写 RS 原始明细或快照。
  `MWD_RATIO` 是月周日趋势的规格类型，不属于这里的两个图点修饰入口。
- 尚无真实 UCL/LCL 输入，`build_aoi_rs_ooc_detail()` 返回空表；
  不能因有 `aoi_rs_sheet_ooc_decoration.xlsx` 就认定已实现 OOC 数值修饰。

实现：[aoi_rs_decoration.py](../../../src/inline_domain/core/aoi_rs/aoi_rs_decoration.py)、
[规格匹配与聚合](../../../src/inline_domain/core/aoi_rs/aoi_rs_calculator.py)、
[规格加载](../../../src/inline_domain/infrastructure/aoi_rs/data_loader.py)。

### 7.6 monitor：报警过滤、汇总与矩阵显示

- 当前组合根为 OOS/OOC 装配 `ExcelAlarmReader`，读取各 scope 的产品 sheet，
  不在读报警时重算原始测量，也不重新合并 `__flags`。生成明细时无已有决策默认填 `True`。
- 读取器只保留 `flag=False` 的明细进入 `alerts_df`；`True`、`Delete` 和空 flag
  均不进入报警结果。这里是报警过滤，不是把工作簿中的观测值改写到规格以内。
- 汇总以用户工作簿为基线：本周计数由当前报警重算，年/季/月总量按
  “旧累计 − 已计入的本周贡献 + 新本周贡献”替换。缺少来源或基线时保留已有汇总并报告缺口。
  AOI_RS 只按 Sheet 图口径计片，按厂别、产品、item_id 去重。
- 矩阵另有显示开关：`compliance_config.xlsx` 中的监控行 × 产品开关启用时，
  页面显示 payload 的对应单元格设为 `ok` 并清空消息，不修改后台报警、计数或能力值。
  旧 `sanitize_to_compliant()` 只返回副本，不再承担后台修饰。

实现：[excel_alarm_reader.py](../../../src/inline_domain/application/monitor/excel_alarm_reader.py)、
[weekly_replacement.py](../../../src/inline_domain/core/monitor/weekly_replacement.py)、
[矩阵显示修饰](../../../app/manager/compliance_manager.py)。

### 7.7 工作簿、共享职责与源校正边界

资源位置由 [inline_domain.yaml](../../../config/domain/inline_domain.yaml) 注入，
下表路径均相对 `resources/inline_domain/`；OOS/OOC 文件通常每产品一个明细 sheet，
另有供生成流程维护的决策页和刷新元数据。

| 用途 | 文件 |
|---|---|
| SPC 点位 / OOC 报警 | `spc/spc_sheet_oos_decoration.xlsx` / `spc/spc_sheet_ooc_decoration.xlsx` |
| SPC 能力覆盖 | `spc/spc_cpk_cpm_decoration.xlsx` |
| CTQ 点位 / OOC 报警 | `ctq/ctq_sheet_oos_decoration.xlsx` / `ctq/ctq_sheet_ooc_decoration.xlsx` |
| AOI_TT 数值决策 / OOC 报警 | `aoi_tt/aoi_tt_sheet_oos_decoration.xlsx` / `aoi_tt/aoi_tt_sheet_ooc_decoration.xlsx` |
| AOI_RS 图点 / OOC 空契约 | `aoi_rs/aoi_rs_sheet_oos_decoration.xlsx` / `aoi_rs/aoi_rs_sheet_ooc_decoration.xlsx` |
| 报警及能力汇总 / 矩阵显示 | `monitor/北极星报警率与CPK汇总.xlsx` / `monitor/compliance_config.xlsx` |

`application/shared` 负责加载决策、调用纯规则及保存明细，`core/shared` 负责三态与截断，
`infrastructure/shared` 负责工作簿读写。数值修饰不回写数据库或原始 Parquet；
决策签名、产品 revision 和缓存刷新决定报表何时重建，不能把 Excel 编辑理解为即时改变全部缓存。

另有明确的**源校正例外**：[measurement_correction.py](../../../src/inline_domain/core/shared/measurement_correction.py)
在共享测量快照写入前修正 PPA：M673 且 site 96～114 的 PPA 值减 5，其他 PPA 值减 1，
两条规则互斥。这是源量测偏差校正，不由 OOS/OOC flag 控制，也不等于 AOI 的 PPA 截断豁免。

---

## 8. 关键数据流

```
PostgreSQL (多厂别分表)
    │
    ▼
shared/measurement_data_loader.py (UNION 查询)
    │
    ▼
shared/measurement_snapshot_repository.py (产品级原始快照)
    │
    ▼
shared/measurement_preparation.py (共享制备管线)
    │
    ▼
各模块薄投影 Repository（spc / ctq / aoi_tt / monitor）
    │
    ▼
Application boundary
    ├── monitor 旧测量链: ALL → 按 data_type 路由修饰（SPC/CTQ/AOI）
    ├── spc:   强制 SPC → 分布 + CPM/CPK
    ├── ctq:   强制 CTQ → 分布 + OOS 修饰（无 CPM/CPK）
    ├── aoi_tt: 规格表识别 TT → 趋势
    └── aoi_rs: 独立快照链路（infrastructure/aoi_rs/）→ RS 趋势
         │
         ▼
Streamlit 各独立页面（app/pages/）
    └── 组装层 app/sections/inline_domain/<module>/ + shared/
        绘图层 app/charts/inline/（ADR-0016）
```

装配统一由 `src/inline_domain/composition.py` 组合根完成：构建各模块 Repository
并注入应用服务端口。当前自动预警另走 OOS/OOC Excel 产品页 → flag 过滤 →
本周贡献替换 / 矩阵显示链路，见第 7.6 节。

---
## 9. 参数白名单过滤链路

`IMP_SPC_TZBJX` 是一张**配置表**（非时序数据），定义当前产品哪些参数受控、属于什么类型。过滤链路横跨三层：

```
前端独立页面
  ├→ SPC 页面调用 SpcReportService
  └→ CTQ 页面调用 CtqReportService
       │
Application Service
  config.data_type_filter = "SPC" / "CTQ"      ← 后端固定业务类型
       │
shared/measurement_preparation.py               ← 筛选在此层消费
  │
  ├─ ① catalog = metadata.get_parameter_catalog(prod_code)
  │     └→ DAO: SELECT parmtername, data_type FROM IMP_SPC_TZBJX（裸查询）
  │
  ├─ ② catalog["data_type"] = catalog["data_type"].apply(classify_param_type)
  │     └→ Core: NULL/空 → "AOI", else → UPPER（纯函数，无 I/O）
  │
  ├─ ③ if filter != "ALL": catalog = catalog[... == filter]
  │     └→ 制备层: 按 data_type_filter 内存筛选（不下沉到 DAO）
  │
  └─ ④ prepared.merge(catalog, on="param_name", how="inner")
        └→ 白名单过滤 + data_type 标签注入到测量数据
```

**分层职责：**

| 层级 | 文件 | 职责 | 边界 |
|------|------|------|------|
| **DAO** | `shared/measurement_metadata_loader.py` | 裸 SQL 查询，返回原始列 | 不做分类映射、不做前端筛选 |
| **Core** | `core/monitor/monitor_param_classifier.py` → `classify_param_type` | 纯函数映射 raw → 标准标签 | 不访问 DB、不感知前端 |
| **制备层** | `shared/measurement_preparation.py` | 消费 DAO + Core，按 filter 筛选 + merge | 不写 SQL、不嵌入分类规则 |

**设计决策：DAO 不做快照。** `IMP_SPC_TZBJX` 查询成本极低（单表 DISTINCT，百级数据），且变更频率极低（新产品上线才变），快照收益 < 一致性风险。测量数据（三厂时序表）仍走 Parquet 快照。

---


> **相关文件**: [`ARCHITECTURE.md`](../../../ARCHITECTURE.md) · [`data-flow-spc.md`](./data-flow-spc.md) · [`architecture-inline-infrastructure.md`](./architecture-inline-infrastructure.md) · [`overview-yield.md`](../yield_domain/overview-yield.md) · [`shared_kernel` (ARCHITECTURE.md)](../../../ARCHITECTURE.md)

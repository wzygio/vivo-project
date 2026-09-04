# Inline Domain · 在线监控数据域设计

> **领域代码**: `inline_domain`  
> **对应目录**: [`src/inline_domain/`](../../src/inline_domain/)  
> **最后更新**: 2026-08-20

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

> Infrastructure 详细规范见 [`spec-infrastructure-architecture.md`](./spec-infrastructure-architecture.md)。

PNL 指标规格的版本/产品收严分析不属于在线 Inline 运行链路。其 CLI、用例编排和
专用规格比较规则统一位于 `tools/indicator_improvement/`，只读取离线 Excel 并向
`output/` 生成可重建报告，不由 `src/inline_domain/composition.py` 装配。

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

**角色**: 多厂别聚合 + 重叠时间桶 + 合规修饰调度

核心职责：
- 接收前端查询参数（产品代码、厂别、时间范围等）
- 协调多厂别数据源（通过多态分表 UNION）
- 构建重叠时间桶（Overlapping Time Buckets）
- 按 data_type 路由修饰口径：SPC→spc、CTQ→ctq、AOI→免修饰（D2/D3）
- 调度 SPC 规则引擎
- 应用合规修饰（`compliance_config.yaml`）

### 4.2 `spc/spc_service.py` — SPC 能力报表

- 在服务边界强制 `data_type_filter = "SPC"`。
- 生成 Sheet/点位分布、月/周/日 CPM/CPK、CPK 预警及 OOS/CPK 修饰结果。
- 应用服务不返回图表样式；SPC/CTQ Sheet 点位图由前端 `app/charts/inline/chart_type.py`
  按 `inline_config.yaml` 的 `spc.chart.line_param_name_contains` 统一选择折线或箱线（ADR-0016）。
- Sheet OOS 修饰表的 `flag` 为三态：`True` 修饰超规点、`False` 保留真实值、`Delete` 按产品/站点/参数/Sheet 四键从图表点位中排除；修改表内 `sheet_min/max/mean` 不改变计算结果。
- 修饰表支持标准或企业加密 XLSX；已有文件双重读取失败时必须中止本次报表重建并保留原文件。直接编辑文件后按 ADR-0005 通过页头“刷新缓存”手动生效。
- 缓存函数只返回原生 payload，ViewModel 在缓存外构造。
- 指标改善离线工具位于 `tools/indicator_improvement/`；SPC 应用服务不导入或调用它。

### 4.3 `ctq/ctq_service.py` — CTQ 分布报表

- 在服务边界强制 `data_type_filter = "CTQ"`，前端不参与数据类型判断。
- 返回 Sheet 特征、原始点位、指标元数据和 OOS 修饰结果；契约中不包含 CPM/CPK、CPK 预警或 CPK 修饰。
- Core 侧 `core/ctq/indicator_chart.py` 仍为 payload 标记 `chart_type` 列（参数名含 `UNI` → line）；
  前端实际渲染决策统一由 `app/charts/inline/chart_type.py` 按配置完成，与 SPC 同口径（ADR-0016）。
- OOS 修饰文件为 `resources/inline_domain/ctq_sheet_oos_decoration.xlsx`（一个文件、每产品一个 sheet），与 SPC 修饰文件隔离、共用同一引擎。
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

### 5.1 [`monitor_calculator.py`](../../src/inline_domain/core/monitor/monitor_calculator.py) — SPC 规则引擎

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
- 应用合规修饰（`sanitize_to_compliant`）

### 5.2 [`monitor_param_classifier.py`](../../src/inline_domain/core/monitor/monitor_param_classifier.py) — 参数类型分类器

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
- `auto_decoration.py`：超规自动截断 + 三态应用，margin（5%~15% span）与稳定哈希
  在全仓只有单一来源。

---

## 6. 基础设施层 (Infrastructure)

完整规范（模块矩阵、制备管线顺序、快照契约、装配）见
[`spec-infrastructure-architecture.md`](./spec-infrastructure-architecture.md)。要点：

- `shared/`（原 `measurement/`，2026-08 按 shared 约束更名归位）拥有三厂测量 DAO、
  参数元数据 DAO、产品级原始 Parquet 快照（3 个月滚动窗口、TTL 统一配置于
  `config/global.yaml` 的 `data_snapshot.ttl_hours`、策略版本、原子写、
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

## 7. 合规修饰机制

通过 [`compliance_config.yaml`](../../config/compliance_config.yaml) 配置的 **"监控类型-产品型号-厂别" 三维规则**：

```yaml
# 示例规则
- monitor_type: "SPC_Mean"
  product: "M626"
  factory: "ALL"
  action: "whiten"  # 将报警数据洗白为 OK
```

此机制允许将特定场景下的 SPC 报警标记为合规（洗白），避免误报。

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
    ├── monitor: ALL → 按 data_type 路由修饰（SPC/CTQ/AOI）→ 自动预警
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
并注入应用服务端口。

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


> **相关文件**: [`ARCHITECTURE.md`](../../ARCHITECTURE.md) · [`spec-infrastructure-architecture.md`](./spec-infrastructure-architecture.md) · [`yield_domain.md`](./yield_domain.md) · [`shared_kernel.md`](./shared_kernel.md)

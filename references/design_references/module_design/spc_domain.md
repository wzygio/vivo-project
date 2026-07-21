# SPC Domain · 统计过程控制域设计

> **领域代码**: `spc_domain`  
> **对应目录**: [`src/spc_domain/`](../../src/spc_domain/)  
> **最后更新**: 2026-07-21

---

## 1. 概述

SPC 控制域（Statistical Process Control Domain）是本系统的核心业务域之一，负责对面板制造过程中的量测数据进行统计过程控制分析。核心能力包括全链路 SPC 规则引擎（特征降维 → OOS/SOOS/OOC 判定 → 报表聚合 → 合规修饰）。

---

## 2. 分层架构

```
┌─────────────────────────────────────────────────┐
│              Application Layer                   │
│  spc_service.py  (主入口/多厂别聚合)               │
│  dtos.py         (SpcQueryConfig DTO)            │
├─────────────────────────────────────────────────┤
│               Core Domain Layer                   │
│  spc_calculator.py        (SPC 规则引擎)            │
│    ├── Phase1: 特征降维                            │
│    ├── Phase2: OOS/SOOS/OOC 判定                   │
│    └── Phase3: 聚合输出                            │
│  spc_param_classifier.py  (参数类型分类器)          │
├─────────────────────────────────────────────────┤
│            Infrastructure Layer                   │
│  data_loader.py  (DAO: 多厂 UNION 查询 + 白名单裸查)  │
│  spc_repository.py (SpcRepository: 快照缓存 + 白名单过滤) │
└─────────────────────────────────────────────────┘
```

---

## 3. 应用服务层 (Application)

### 3.1 [`spc_service.py`](../../src/spc_domain/application/spc_service.py) — 主入口

**角色**: 多厂别聚合 + 重叠时间桶 + 合规修饰调度

核心职责：
- 接收前端查询参数（产品代码、厂别、时间范围等）
- 协调多厂别数据源（通过多态分表 UNION）
- 构建重叠时间桶（Overlapping Time Buckets）
- 调度 SPC 规则引擎
- 应用合规修饰（`compliance_config.yaml`）

### 3.2 [`dtos.py`](../../src/spc_domain/application/dtos.py)

`SpcQueryConfig` DTO — 封装 SPC 查询参数。

---

## 4. 核心领域层 (Core Domain)

### 4.1 [`spc_calculator.py`](../../src/spc_domain/core/spc_calculator.py) — SPC 规则引擎

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

### 4.2 [`spc_param_classifier.py`](../../src/spc_domain/core/spc_param_classifier.py) — 参数类型分类器

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


---

## 5. 基础设施层 (Infrastructure)

### 5.1 [`data_loader.py`](../../src/spc_domain/infrastructure/data_loader.py)

DAO 层职责（纯数据访问，无业务逻辑）：

**涉及数据库表：**

| 表名 | Schema | 性质 | 说明 |
|------|--------|------|------|
| `spc_tzbjx_array` | eda | 时序明细 | ARRAY 厂 SPC 测量数据，主键: sheet_id + step_id + param_name + site_name |
| `spc_tzbjx_oled` | eda | 时序明细 | OLED 厂 SPC 测量数据（ID 列为 glass_id） |
| `spc_tzbjx_tsp` | eda | 时序明细 | TP 厂 SPC 测量数据（ID 列为 glass_id） |
| `IMP_SPC_TZBJX` | eda | 配置/元数据 | SPC 参数白名单，列: parmtername, data_type, productspecname |
| `dwd_imp_dv_param_spec` | - | 配置 | 管控规格基准表（USL/LSL/UCL/LCL） |
| `DWR_MES_PRODUCTSPEC` | - | 字典 | MES 产品字典表（productspecname → productcode 翻译） |

**公开函数：**

| 函数 | 职责 | 返回 |
|------|------|------|
| `load_spc_measurements(db, start, end, prod)` | UNION ALL 三厂时序表 + JOIN 字典表去重 | 全量测量数据 |
| `load_spc_spec_limits(db, prod)` | 查询管控规格基准 | USL/LSL/UCL/LCL |
| `load_param_whitelist(db, prod)` | **裸查询** IMP_SPC_TZBJX，不做分类、不做筛选 | (param_name, raw_data_type) |

> 注意：`load_param_whitelist` 返回的是 DB 原始 `data_type` 值，分类映射由 Core 层 `spc_param_classifier` 完成。

### 5.2 [`spc_repository.py`](../../src/spc_domain/infrastructure/repositories/spc_repository.py)

`SpcRepository` 职责：
- Parquet 格式快照缓存（L1 缓存，TTL 8h）
- **参数白名单过滤**（三步：DAO 裸查 → Core 分类 → Repo 筛选 + merge）
- 异常值过滤（Outlier Filter）
- 规格覆盖（Override from YAML）
- 报废数据加载与伪装

**白名单过滤流程（详见第 8 节）：**

```
raw_whitelist = load_param_whitelist(db, prod_code)     # ① DAO: 裸查询
raw_whitelist["data_type"] = raw_whitelist["data_type"]
    .apply(classify_param_type)                          # ② Core: 分类映射
if filter != "ALL":
    raw_whitelist = raw_whitelist[raw_whitelist["data_type"] == filter]  # ③ Repo: 前端筛选
df_filtered = df_filtered.merge(raw_whitelist, ...)      # ④ 注入 data_type 标签
```

---

## 6. 合规修饰机制

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

## 7. 关键数据流

```
PostgreSQL (多厂别分表)
    │
    ▼
data_loader.py (UNION 查询)
    │
    ▼
SpcRepository (快照缓存 + 异常值过滤)
    │
    ▼
spc_service.py (多厂别聚合 + 时间桶)
    │
    ▼
spc_calculator.py (三阶段规则引擎)
    │
    ▼
合规修饰 (compliance_config.yaml)
    │
    ▼
Streamlit SPC 看板
```

---
## 8. 参数白名单过滤链路

`IMP_SPC_TZBJX` 是一张**配置表**（非时序数据），定义当前产品哪些参数受控、属于什么类型。过滤链路横跨三层：

```
前端 spc_dashboard.py
  st.selectbox("监控类型", ["SPC","CTQ","AOI","报废","ALL"])
  └→ SpcFilterState.data_type_filter
       │
Service spc_service.py
  config.data_type_filter = data_type_filter    ← 注入配置对象
       │
Repository spc_repository.py                   ← 筛选在此层消费
  │
  ├─ ① raw_whitelist = load_param_whitelist(db, prod_code)
  │     └→ DAO: SELECT parmtername, data_type FROM IMP_SPC_TZBJX（裸查询）
  │
  ├─ ② raw_whitelist["data_type"] = raw_whitelist["data_type"].apply(classify_param_type)
  │     └→ Core: NULL/空 → "AOI", else → UPPER（纯函数，无 I/O）
  │
  ├─ ③ if filter != "ALL": raw_whitelist = raw_whitelist[... == filter]
  │     └→ Repo: 按前端 data_type_filter 内存筛选（不下沉到 DAO）
  │
  └─ ④ df_filtered.merge(raw_whitelist, on="param_name", how="inner")
        └→ 白名单过滤 + data_type 标签注入到测量数据
```

**分层职责：**

| 层级 | 文件 | 职责 | 边界 |
|------|------|------|------|
| **DAO** | `data_loader.py` → `load_param_whitelist` | 裸 SQL 查询，返回原始列 | 不做分类映射、不做前端筛选 |
| **Core** | `spc_param_classifier.py` → `classify_param_type` | 纯函数映射 raw → 标准标签 | 不访问 DB、不感知前端 |
| **Repository** | `spc_repository.py` | 消费 DAO + Core，按 filter 筛选 + merge | 不写 SQL、不嵌入分类规则 |

**设计决策：DAO 不做快照。** `IMP_SPC_TZBJX` 查询成本极低（单表 DISTINCT，百级数据），且变更频率极低（新产品上线才变），快照收益 < 一致性风险。测量数据（三厂时序表）仍走 L1 Parquet 快照。

---


> **相关文件**: [`ARCHITECTURE.md`](../../ARCHITECTURE.md) · [`yield_domain.md`](./yield_domain.md) · [`shared_kernel.md`](./shared_kernel.md)

---

## 9. Station type filtering

`references/design_references/domain/step_id_data_type_mapping.json` is the
user-maintained source of truth for `Step_ID` classifications. Its
`step_id_to_data_types` values are arrays because a station can belong to more
than one report type. This supersedes the earlier use of the parameter
whitelist's `data_type` as a report-type filter in section 8.

- `SpcRepository.filter_measurements_by_step_data_type()` loads this mapping
  and applies `SpcQueryConfig.data_type_filter` after the parameter whitelist
  has been merged into the measurements.
- `ALL` leaves the measurements unchanged. For a requested type such as `SPC`,
  unmapped stations are excluded; if the mapping cannot be loaded, the result
  is empty rather than exposing unclassified stations.
- This is a **station** filter. `IMP_SPC_TZBJX.data_type` remains a
  **parameter** classification used for whitelist validation and the returned
  `data_type` label; it must not be used to decide whether a `Step_ID` belongs
  to an SPC, CTQ, AOI, or RS report.
- `data_loader.py` continues to perform database access only. The repository
  owns the mapping read and in-memory filter so future CTQ/AOI/RS reports can
  reuse the same query contract.

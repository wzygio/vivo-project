# Inline Domain · 在线监控数据域设计

> **领域代码**: `inline_domain`  
> **对应目录**: [`src/inline_domain/`](../../src/inline_domain/)  
> **最后更新**: 2026-07-21

---

## 1. 概述

Inline 数据域负责面板制造过程中的在线量测监控。目前包含三条独立应用路径：`monitor` 提供自动预警聚合，`spc` 提供 SPC 分布和 CPM/CPK 能力指数，`ctq` 提供不含能力指数的 CTQ 分布报表。三者复用同一物理量测仓储，但在应用服务边界固定各自的数据类型和返回契约。

---

## 2. 分层架构

```
┌─────────────────────────────────────────────────┐
│              Application Layer                   │
│  monitor/monitor_service.py (自动预警)             │
│  spc/spc_service.py         (SPC + CPM/CPK)       │
│  ctq/ctq_service.py         (CTQ 分布，无能力指数)   │
├─────────────────────────────────────────────────┤
│               Core Domain Layer                   │
│  monitor/monitor_calculator.py (预警规则与特征降维)   │
│  spc/spc_calculator.py         (周期 CPM/CPK)       │
│  ctq/indicator_chart.py        (UNI 图表类型规则)     │
├─────────────────────────────────────────────────┤
│            Infrastructure Layer                   │
│  data_loader.py  (DAO: 多厂 UNION 查询 + 白名单裸查)  │
│  spc_repository.py (SpcRepository: 快照缓存 + 白名单过滤) │
└─────────────────────────────────────────────────┘
```

---

## 3. 应用服务层 (Application)

### 3.1 `monitor/monitor_service.py` — 自动预警入口

**角色**: 多厂别聚合 + 重叠时间桶 + 合规修饰调度

核心职责：
- 接收前端查询参数（产品代码、厂别、时间范围等）
- 协调多厂别数据源（通过多态分表 UNION）
- 构建重叠时间桶（Overlapping Time Buckets）
- 调度 SPC 规则引擎
- 应用合规修饰（`compliance_config.yaml`）

### 3.2 `spc/spc_service.py` — SPC 能力报表

- 在服务边界强制 `data_type_filter = "SPC"`。
- 生成 Sheet/点位分布、月/周/日 CPM/CPK、CPK 预警及 OOS/CPK 修饰结果。
- 缓存函数只返回原生 payload，ViewModel 在缓存外构造。

### 3.3 `ctq/ctq_service.py` — CTQ 分布报表

- 在服务边界强制 `data_type_filter = "CTQ"`，前端不参与数据类型判断。
- 返回 Sheet 特征、原始点位、指标元数据和 OOS 修饰结果；契约中不包含 CPM/CPK、CPK 预警或 CPK 修饰。
- 参数名称包含 `UNI` 时由 Core 标记 `chart_type = "line"`，其他参数标记为 `box`。
- OOS 修饰文件位于 `resources/<product>/ctq/`，与 SPC 产品根目录资源隔离。
- 页面缓存遵守 adr-0001：只缓存 DataFrame/原生容器/标量，并在缓存外构造 `CtqReportViewModel`。

### 3.4 `infrastructure/spc/data_loader.py` 中的 `SpcQueryConfig`

`SpcQueryConfig` DTO — 封装 SPC 查询参数。

---

## 4. 核心领域层 (Core Domain)

### 4.1 [`spc_calculator.py`](../../src/inline_domain/core/monitor/monitor_calculator.py) — SPC 规则引擎

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

### 4.2 [`spc_param_classifier.py`](../../src/inline_domain/core/monitor/monitor_param_classifier.py) — 参数类型分类器

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

### 5.1 [`data_loader.py`](../../src/inline_domain/infrastructure/spc/data_loader.py)

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

### 5.2 [`spc_repository.py`](../../src/inline_domain/infrastructure/spc/repositories/spc_repository.py)

`SpcRepository` 职责：
- Parquet 格式快照缓存（L1 缓存，TTL 8h）
- 参数筛选策略版本校验；版本变化时全量刷新，数据库不可用时仍降级到旧快照
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

参数名包含 `LOSS` 的记录仍在 SQL 与历史快照兜底层统一排除。`MT_CH_*`
不属于全局排除项，其是否进入 SPC 报表完全由白名单中的标准化 `data_type`
决定。

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
SpcRepository (共享快照 + 白名单 data_type 过滤)
    │
    ▼
Application boundary
    ├── monitor: 自动预警
    ├── spc: 强制 SPC → 分布 + CPM/CPK
    └── ctq: 强制 CTQ → 分布 + OOS 修饰（无 CPM/CPK）
         │
         ▼
Streamlit 独立 Monitor / SPC / CTQ 页面
```

---
## 8. 参数白名单过滤链路

`IMP_SPC_TZBJX` 是一张**配置表**（非时序数据），定义当前产品哪些参数受控、属于什么类型。过滤链路横跨三层：

```
前端独立页面
  ├→ SPC 页面调用 SpcReportService
  └→ CTQ 页面调用 CtqReportService
       │
Application Service
  config.data_type_filter = "SPC" / "CTQ"      ← 后端固定业务类型
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

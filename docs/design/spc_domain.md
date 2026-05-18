# SPC Domain · 统计过程控制域设计

> **领域代码**: `spc_domain`  
> **对应目录**: [`src/spc_domain/`](../../src/spc_domain/)  
> **最后更新**: 2026-05-18

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
│  spc_calculator.py  (SPC 规则引擎)                │
│    ├── Phase1: 特征降维                            │
│    ├── Phase2: OOS/SOOS/OOC 判定                   │
│    └── Phase3: 聚合输出                            │
├─────────────────────────────────────────────────┤
│            Infrastructure Layer                   │
│  data_loader.py  (DAO: 多态分表 UNION)             │
│  spc_repository.py (SpcRepository: 快照缓存)       │
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

---

## 5. 基础设施层 (Infrastructure)

### 5.1 [`data_loader.py`](../../src/spc_domain/infrastructure/data_loader.py)

DAO 层职责：
- 多态厂别分表 UNION 查询
- 规格线查询（从资源文件加载）
- 参数映射

### 5.2 [`spc_repository.py`](../../src/spc_domain/infrastructure/repositories/spc_repository.py)

`SpcRepository` 职责：
- Parquet 格式快照缓存
- 异常值过滤（Outlier Filter）
- 规格覆盖（Override）

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

> **相关文件**: [`ARCHITECTURE.md`](../../ARCHITECTURE.md) · [`yield_domain.md`](./yield_domain.md) · [`shared_kernel.md`](./shared_kernel.md)

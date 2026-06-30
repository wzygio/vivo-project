# Yield Domain · 良率分析域设计

> **领域代码**: `yield_domain`  
> **对应目录**: [`src/yield_domain/`](../../src/yield_domain/)  
> **最后更新**: 2026-05-18

---

## 1. 概述

良率分析域（Yield Domain）是本系统的核心业务域之一，负责从 PostgreSQL 数据仓库抽取 Panel 级生产和量测数据，执行多级不良率模拟、Mapping 空间集中性分析、MWD 趋势聚合等计算，最终以热力图、趋势图、明细表等形式呈现。

---

## 2. 分层架构

```
┌─────────────────────────────────────────────────┐
│              Application Layer                   │
│  yield_service.py  (主入口/缓存编排)               │
│  alert_service.py  (预警服务)                     │
│  excel_service.py  (Excel 导出)                   │
│  pdf_service.py    (PDF 导出 - 骨架)              │
│  ppt_service.py    (PPT 导出 - 骨架)              │
│  file_manager_service.py (文件管理)               │
├─────────────────────────────────────────────────┤
│               Core Domain Layer                   │
│  sheet_lot_processor.py  (Lot→Sheet 不良率散布)    │
│  mapping_processor.py    (Mapping 集中性分析)      │
│  mwd_trend_processor.py  (月/周/日 EMA 聚合)       │
│  batch_statistics.py     (批次统计)                │
│  abnormal_detector.py    (异常检测)               │
│  defect_modifier.py      (缺陷倍数修饰)            │
│  trend_regulator.py      (趋势调节器)              │
├─────────────────────────────────────────────────┤
│            Infrastructure Layer                   │
│  data_loader.py     (DAO: SQL + 本地文件)          │
│  yield_repository.py (PanelRepository: 快照缓存)   │
└─────────────────────────────────────────────────┘
```

---

## 3. 应用服务层 (Application)

### 3.1 [`yield_service.py`](../../src/yield_domain/application/yield_service.py) — 主入口

**角色**: 缓存编排 + L1/L2 数据管道 + 计算调度

核心方法：
| 方法 | 职责 |
|------|------|
| `get_raw_panel_details` | 原始 Panel 数据（L1 快照 + L2 缓存） |
| `get_modified_panel_details` | 修饰后 Panel 数据 |
| `get_mwd_trend_data` | MWD 趋势数据 |
| `get_sheet_defect_rate` | Sheet 级不良率 |
| `get_lot_defect_rate` | Lot 级不良率 |
| `get_mapping_data` | Mapping 集中性数据 |

### 3.2 [`dtos.py`](../../src/yield_domain/application/dtos.py)

`YieldQueryConfig` DTO — 封装查询参数（产品代码、时间范围、缺陷组等）。

### 3.3 辅助服务

| 服务 | 职责 |
|------|------|
| [`alert_service.py`](../../src/yield_domain/application/alert_service.py) | 基于预警线的自动报警逻辑 |
| [`excel_service.py`](../../src/yield_domain/application/excel_service.py) | 将分析结果导出为 Excel 格式 |
| [`file_manager_service.py`](../../src/yield_domain/application/file_manager_service.py) | 文件资源管理 |
| [`pdf_service.py`](../../src/yield_domain/application/pdf_service.py) | PDF 导出（骨架，待实现） |
| [`ppt_service.py`](../../src/yield_domain/application/ppt_service.py) | PPT 导出（骨架，待实现） |

---

## 4. 核心领域层 (Core Domain)

### 4.1 [`sheet_lot_processor.py`](../../src/yield_domain/core/sheet_lot_processor.py) — Sheet/Lot 不良率

**核心算法**：泊松散布 + 多项式分配 + 软熔断

- 输入：Panel 级缺陷数据
- 计算：`_simulate_concentration` → `_distribute_sheet_from_lot`
- 输出：Lot 级 / Sheet 级不良率
- **红线**：禁止静态重构 `_simulate_concentration` 和 `_distribute_sheet_from_lot`

### 4.2 [`mapping_processor.py`](../../src/yield_domain/core/mapping_processor.py) — Mapping 集中性

**核心算法**：级联衰减 + 热点修饰脚本

- 输入：Sheet 级坐标数据
- 计算：基于坐标解析 `_parse_panel_id_to_coords`，Rate-Based 级联衰减
- 输出：Mapping 热力图数据
- **红线**：禁止静态重构级联衰减逻辑

### 4.3 [`mwd_trend_processor.py`](../../src/yield_domain/core/mwd_trend_processor.py) — MWD 趋势

**核心算法**：月/周/日 EMA 聚合

- 支持 Code 级和 Group 级双轨趋势
- 使用指数移动平均（EMA）进行趋势平滑

### 4.4 其他核心模块

| 模块 | 职责 |
|------|------|
| [`batch_statistics.py`](../../src/yield_domain/core/batch_statistics.py) | 批次级别统计分析 |
| [`abnormal_detector.py`](../../src/yield_domain/core/abnormal_detector.py) | 异常值检测逻辑 |
| [`defect_modifier.py`](../../src/yield_domain/core/defect_modifier.py) | 缺陷数据倍数修饰 |
| [`trend_regulator.py`](../../src/yield_domain/core/trend_regulator.py) | 趋势数据的调节器 |

---

## 5. 基础设施层 (Infrastructure)

### 5.1 [`data_loader.py`](../../src/yield_domain/infrastructure/data_loader.py)

DAO 层职责：
- PostgreSQL SQL 查询（Panel 级明细）
- Excel/CSV 本地文件读取（Override 数据）
- 多数据源统一接口

### 5.2 [`yield_repository.py`](../../src/yield_domain/infrastructure/repositories/yield_repository.py)

`PanelRepository` 职责：
- Parquet 格式本地快照缓存
- 增量更新（2 天缓冲窗口）
- 三防线容灾降级

---

## 6. 关键数据流

```
PostgreSQL
    │
    ▼
data_loader.py  (SQL 查询)
    │
    ▼
PanelRepository  (Parquet 快照缓存 / L1)
    │
    ▼
yield_service.py  (L2: @st.cache_data)
    │
    ├──→ sheet_lot_processor.py  (不良率散布)
    ├──→ mapping_processor.py    (集中性分析)
    ├──→ mwd_trend_processor.py  (趋势聚合)
    └──→ ... (其他核心模块)
    │
    ▼
Streamlit Pages  (可视化呈现)
```

---

> **相关文件**: [`ARCHITECTURE.md`](../../ARCHITECTURE.md) · [`spc_domain.md`](./spc_domain.md) · [`shared_kernel.md`](./shared_kernel.md)

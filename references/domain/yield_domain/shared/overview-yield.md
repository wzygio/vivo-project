# Yield Domain · 良率分析域设计

> **领域代码**: `yield_domain`  
> **对应目录**: [`src/yield_domain/`](../../../../src/yield_domain/)  
> **最后更新**: 2026-11-03

---

## 1. 概述

良率分析域（Yield Domain）是本系统的核心业务域之一，负责从 PostgreSQL 数据仓库抽取 Panel 级生产和量测数据，执行多级不良率模拟、Mapping 空间集中性分析、MWD 趋势聚合等计算，最终以热力图、趋势图、明细表等形式呈现。

报表生成的 Lot 超规与周度趋势报警分别发布为本地结果工作簿，自动预警矩阵和预计算仅读取这些结果。当前暂禁用月度趋势预警，Lot 当期预警仅上一完整 ISO 周。发布与有效性见 [Yield 预警结果规则](rules-yield-alert-results.md)。最新的共享输入、缓存、Excel 读取、宏观先显示和按需详情见 [报表数据流](data-flow-yield-report.md)；原始 L1、数值分配与 Mapping 级联保持原职责。

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
│  mwd_trend_processor.py  (目标日度与月/周聚合)     │
│  batch_statistics.py     (批次统计)                │
│  abnormal_detector.py    (异常检测)               │
│  defect_modifier.py      (缺陷倍数修饰)            │
├─────────────────────────────────────────────────┤
│            Infrastructure Layer                   │
│  data_loader.py     (DAO: SQL + 本地文件)          │
│  yield_repository.py (PanelRepository: 快照缓存)   │
└─────────────────────────────────────────────────┘
```

---

## 3. 应用服务层 (Application)

### 3.1 [`yield_service.py`](../../../../src/yield_domain/application/yield_service.py) — 主入口

**角色**: 缓存编排 + L1/L2 数据管道 + 计算调度

核心方法：
| 方法 | 职责 |
|------|------|
| `get_raw_panel_details` | 原始 Panel 数据（L1 快照 + L2 缓存） |
| `get_modified_panel_details` | 修饰后 Panel 数据 |
| `get_mwd_trend_data` | MWD 趋势数据 |
| `get_sheet_defect_rates` | Sheet 级不良率（详情请求后加载） |
| `get_lot_defect_rates` | Lot 级不良率 |
| `get_mapping_data` | Mapping 集中性数据 |

### 3.2 [`dtos.py`](../../../../src/yield_domain/application/dtos.py)

`YieldQueryConfig` DTO — 只封装动态查询参数（产品代码、时间范围）。

`YieldDataPolicy` — 从已校验 `AppConfig` 一次构造的不可变数据策略，封装
全局 `work_order_types` 与 `target_defect_groups`，并提供稳定签名参与缓存和
快照隔离。页面和告警层不再拆读这两个配置字段。

### 3.3 辅助服务

| 服务 | 职责 |
|------|------|
| [`alert_service.py`](../../../../src/yield_domain/application/alert_service.py) | 基于预警线的自动报警逻辑 |
| [`excel_service.py`](../../../../src/yield_domain/application/excel_service.py) | 将分析结果导出为 Excel 格式 |
| [`file_manager_service.py`](../../../../src/yield_domain/application/file_manager_service.py) | 文件资源管理 |
| [`pdf_service.py`](../../../../src/yield_domain/application/pdf_service.py) | PDF 导出（骨架，待实现） |
| [`ppt_service.py`](../../../../src/yield_domain/application/ppt_service.py) | PPT 导出（骨架，待实现） |

---

## 4. 核心领域层 (Core Domain)

### 4.1 [`sheet_lot_processor.py`](../../../../src/yield_domain/core/sheet_lot/sheet_lot_processor.py) — Sheet/Lot 不良率

**核心算法**：泊松散布 + 多项式分配 + 软熔断

- 输入：Panel 级缺陷数据
- MWD 周度契约：`weekly_full` 保存三个月计算窗口，供 Lot 等下游逻辑使用；
  `weekly` 仅保留最近三周供界面展示
- Lot 基准：按入库日期映射所属 ISO 周，读取同 Code 的 MWD `weekly_full` 良损；
  先按 Code × 周汇总并四舍五入整数不良总数，再按 Lot 面积及固定种子的
  ±20% 权重分配，避免稀有不良在每个 Lot 独立取整后全部归零
- 完整周度中未命中的周：按 0 生成，不使用原始 Lot 或 `daily_full` 兜底
- 外部 Lot 覆盖：启发式公式的 Base 同样使用所属周良损
- 计算：`_simulate_concentration` → 规格截断/覆盖 → `_distribute_sheet_from_lot`
- 输出：Lot 级 / Sheet 级不良率
- **红线**：禁止静态重构 `_simulate_concentration` 和 `_distribute_sheet_from_lot`

### 4.2 [`mapping_processor.py`](../../../../src/yield_domain/core/mapping/mapping_processor.py) — Mapping 集中性

**核心算法**：级联衰减 + 热点修饰脚本

- 输入：Sheet 级坐标数据
- 布局：产品可通过 `processing.mapping_layout.row_labels/column_labels`
  声明物理膜位；未配置时使用标准 10 × 19
- 计算：布局感知的坐标解析、确定性偏移与热点修饰，随后执行 Rate-Based 级联衰减
- 输出：Mapping 热力图数据
- **红线**：禁止静态重构级联衰减逻辑

### 4.3 [`mwd_trend_processor.py`](../../../../src/yield_domain/core/mwd_trend/mwd_trend_processor.py) — MWD 趋势

**核心算法**：指定月度目标驱动日度整数生成，再聚合月／ISO 周

- 支持 Code 级和 Group 级双轨趋势
- Group 月度先由最终 Code 日度按 Group 聚合；修饰表仅以同一月份的
  `指定良损` 覆写该月结果（指定为 0 也生效）。当月未指定或缺少该月行时，
  保留日度聚合结果，不沿用历史指定良损，也不使用表中 `当月良损` 覆写。
  Group 指定值不改变日度和周度；Code 级的历史指定值回退规则保持不变。
- 使用相邻月中目标锚点插值和稳定扰动准备逐日权重，再按月度目标进行容量内整数分配；旧 EMA 链路不再消费。

### 4.4 其他核心模块

| 模块 | 职责 |
|------|------|
| [`batch_statistics.py`](../../../../src/yield_domain/core/batch_statistics.py) | 批次级别统计分析 |
| [`abnormal_detector.py`](../../../../src/yield_domain/core/abnormal_detector.py) | 异常值检测逻辑 |
| [`defect_modifier.py`](../../../../src/yield_domain/core/defect_modifier.py) | 缺陷数据倍数修饰 |

---

## 5. 基础设施层 (Infrastructure)

### 5.1 [`data_loader.py`](../../../../src/yield_domain/infrastructure/data_loader.py)

DAO 层职责：
- PostgreSQL SQL 查询（Panel 级明细）
- Excel/CSV 本地文件读取（Override 数据）
- 多数据源统一接口

Panel 明细的蒸镀批次取数规则（2026-11-02）：

- 从 `mdw.spot_glass_batch_info` 读取，`lot` 前 8 位作为 `batch_no`，例如 `26/11/01`；不再从工单描述中提取日期。
- 关联键为产品型号 `sgbi.productcode = dmp.productcode`，以及 `glass_id` 前 11 位等于 `panel_id` 前 11 位；批次表的 `productcode` 不能与入库表的 `prod_id` 直接匹配。
- 必须直接过滤分区字段 `sgbi.timekey`：下界为本次 `load_panel_details` 查询开始日（含），上界为结束日下一日（不含），格式为 `YYYYMMDD`。沿用仓储传入的源时间窗口，不额外回溯；仓储分片或增量查询时使用对应分片窗口。
- 同产品、同 Sheet 在窗口内存在多条记录时，按 `timekey` 降序选一条；相同 `timekey` 冲突按完整 `lot` 降序、空值最后确定顺序，避免批次关联放大 Panel/缺陷明细。
- 未匹配到批次时保留 Panel 和缺陷记录，`batch_no` 为空；产品字典、入库日期、工单类型及缺陷关联保持各自原有职责。
- 此规则作用于重新取数。已有快照和增量窗口外的记录不会被自动改写，切换来源后需通过现有完整刷新入口重建所需窗口，才能统一批次格式。

### 5.2 [`yield_repository.py`](../../../../src/yield_domain/infrastructure/repositories/yield_repository.py)

`PanelRepository` 职责：
- Parquet 格式本地快照缓存
- 增量更新（回读天数来自全局 `application.incremental_refresh_days`，当前配置为 7 天）
- 三防线容灾降级
- 构造时接收 `YieldDataPolicy`
- Work Order Type 仅在数据库查询边界应用
- 原始 Defect Group 数据写入带策略签名的快照
- 新查、快照、增量与降级数据在返回上层前统一应用一次 Defect Group 策略

---

## 6. 关键数据流

```
global.yaml + product YAML
    │
    ▼
AppConfig → YieldDataPolicy（一次注入）
    │
    ▼
PostgreSQL  ← Work Order 查询约束
    │
    ▼
data_loader.py  (只提取原始 Panel 数据)
    │
    ▼
PanelRepository  (原始 Parquet 快照 / L1)
    │
    ▼
统一应用 Defect Group 数据策略
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

> **相关文件**: [`ARCHITECTURE.md`](../../../../ARCHITECTURE.md) · [`overview-inline.md`](../../inline_domain/shared/overview-inline.md) · [`overview-shared-kernel.md`](../../../design/shared_kernel/shared/overview-shared-kernel.md)

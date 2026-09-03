# Project Context

## Purpose

天柱专项报表系统面向 OLED/Array 显示制造质量分析，提供入库不良率（Yield）、
SPC/CTQ、自动预警、Q-Time 过货监控和关键备件寿命管理报表。

## Operating Model

- `app/` 是 Streamlit 展示层，包含门户、页面、区块、组件和图表适配器。
- `src/` 按领域划分业务代码；每个领域遵循 Application → Core →
  Infrastructure 分层。运行流和模块职责见 `ARCHITECTURE.md`。
- `config/` 保存全局、产品、SPC、设备与合规配置；全局配置与产品配置由
  `ConfigLoader` 深度合并为 `AppConfig`。
- `resources/` 保存受版本控制的业务输入、产品规格和人工修饰文件；`data/`
  保存本地运行数据和 Parquet 快照；`output/` 仅存放可重建产物。

## Directory Guide

| Path | Purpose |
|---|---|
| `app/` | Streamlit 门户、页面、UI 组件、展示区块和图表。 |
| `src/yield_domain/` | Yield 趋势、Lot/Sheet、Mapping、告警和导出。 |
| `src/inline_domain/` | SPC、CTQ 和自动预警。 |
| `src/equipment_domain/` | 关键备件规格、寿命计算和快照匹配。 |
| `src/indicator_domain/` | 指标监控领域；Q-Time 与 IJP 作为同级子模块，分别承担过货时长及溢流监控。 |
| `src/shared_kernel/` | 配置、数据库单例、输出路径和跨领域工具。 |
| `config/` | 应用与产品配置。 |
| `resources/` | 版本控制的产品资源、规格、人工覆盖和前端静态文件。 |
| `data/` | 本地运行数据和领域 Parquet 快照。 |
| `output/` | 可重建的报告、下载、日志、截图、测试结果和临时文件。 |
| `tools/` | 分域 smoke、离线分析及设备仿造快照维护命令。 |
| `tests/` | 单元、集成和端到端测试。 |
| `docs/ADR/` | 已接受的架构决策。 |
| `docs/agents/` | 本项目的 issue、领域和 triage 协作规则。 |
| `references/` | 项目自有领域知识与 Harness 演进记录的索引入口。 |
| `projects/` | 独立交付项目的源文件、分析和导出物。 |

## Hard Boundaries

- 报表日期前推只改变仓储输出的显示时间轴；数据库事实与原始 Parquet 保持源时间，
  直接查询窗口需在仓储边界反向换算，相关缓存签名需包含前推策略。
- 未获得具体任务与回归证明前，不重构已验证的 Yield 浓度和 Mapping 算法。
- 不随意修改 `DatabaseManager` 的单例与重试语义。
- 不移除页面数据流中的 `st.cache_data`；缓存只跨越原生 payload，ViewModel
  在缓存外构建。
- 未经批准，不简化 Parquet 快照刷新或数据库失败时的降级策略。
- `output/` 中的内容可清理和重建；业务源数据、规格和人工修饰应保留在
  `resources/` 中。

## Fast Routing

- 项目目标、目录职责和硬约束：`CONTEXT.md`（本文件）
- 运行调用流、领域边界、缓存和验证入口：`ARCHITECTURE.md`
- 架构决策：`docs/ADR/`
- 领域术语（涉及制造数据时必读）：
  `references/domain/GLOSSARY.md`
- 项目自有领域资料：`references/domain/`
- 共享工程规范：`$ecc-production-rules`（默认 `common + python`）
- Harness 创建与修正：`$manage-harness`
- 项目参考资料入口：`references/index.md`
- 需求与 issue 工作流：`docs/agents/issue-tracker.md`
- 运行产物分类规则：`output/README.md`
- 产品配置与产品资源：`config/products/`、`resources/<product>/`

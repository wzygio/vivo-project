# 天柱专项报表系统 · AGENTS.md

> 项目代号: 天柱专项报表系统 | 领域: OLED/Array 半导体显示良率分析与 SPC 控制
> 技术栈: Python 3.11+ | Streamlit | PostgreSQL | Parquet | uv 包管理
> 最后更新: 2026-05-22

---

## 项目概述

面板制造业良率分析系统，覆盖四大业务域：
- **Yield 良率分析** — 入库不良率、Mapping 热力图、Sheet/Lot 明细
- **SPC 统计过程控制** — OOS/SOOS/OOC 检测、EMA 趋势、合规修饰
- **关键备件报表** — 备件寿命管控、台账周报
- **自动预警** — 异常检测、自动告警看板

---

## Harness 入口

本仓库采用轻量 Codex Harness。根文件只做路由，详细规则进入对应索引：

- `CONTEXT.md` — 项目目标、边界和快速路由。
- `docs/design/index.md` — 架构、领域设计、共享内核和历史快照入口。
- `docs/plans/index.md` — 活跃/历史计划入口。
- `specs/README.md` — 用户可维护规则、任务契约和验收标准入口。
- `docs/references/README.md` — 外部框架、供应商和设计参考入口。
- `docs/generated/README.md` — 可重建审计和生成事实入口。

---

## 目录结构

`
vivo-project/
├── app/                          # Streamlit 前端（展示层）
│   ├── Home.py                   #   入口：全屏 iframe + 静态资源加载
│   ├── pages/                    #   多页面路由
│   │   ├── 入库不良率分析看板.py
│   │   ├── 入库不良率ByLot明细表.py
│   │   ├── 入库不良率BySheet明细表.py
│   │   ├── 专项资料-台账周报.py
│   │   ├── 专项资料-解析报告.py
│   │   ├── 关键备件报表.py
│   │   └── 自动预警看板.py
│   ├── charts/                   #   ECharts 图表组件
│   ├── components/               #   Streamlit UI 组件
│   └── utils/                    #   日志、热重载、Session 管理
├── src/                          # 核心业务逻辑（领域层，DDD 分层）
│   ├── shared_kernel/            #   共享内核：config、db_handler、excel_tools
│   ├── yield_domain/             #   良率分析域
│   │   ├── application/          #     服务编排（YieldService）
│   │   ├── core/                 #     业务逻辑（良率计算、Mapping 算法）
│   │   └── infrastructure/       #     数据访问（PanelRepository）
│   ├── spc_domain/               #   SPC 控制域
│   │   ├── application/          #     服务编排（SPCService）
│   │   ├── core/                 #     业务逻辑（规则引擎、异常检测）
│   │   └── infrastructure/       #     数据访问（SPCRepository）
│   └── equipment_domain/         #   设备/备件域
│       ├── application/
│       └── infrastructure/
├── config/                       # YAML 配置文件
│   ├── global.yaml               #   全局配置
│   ├── compliance_config.yaml    #   合规修饰规则
│   ├── scrap_factory_mapping.yaml#   报废站点→厂别映射
│   └── products/                 #   产品级配置（M626.yaml, M678.yaml）
├── resources/                    # 静态资源（HTML/CSS/JS）+ Excel 基线数据
├── tests/                        # 测试
│   ├── unit/                     #   单元测试
│   ├── integration/              #   集成测试
│   ├── conftest.py               #   pytest fixtures
│   └── factories.py              #   测试数据工厂
├── docs/                         # 设计文档与计划
│   ├── design/                   #   领域设计文档
│   ├── plans/                    #   计划与知识提案
│   └── prompt/                   #   AI Agent Prompt 模板
├── skills/                       # 专项解决方案（加密 Excel 读取等）
├── ARCHITECTURE.md               # 系统架构全貌（技术栈、数据流、缓存、容灾）
├── pyproject.toml                # uv 项目元数据与依赖声明
└── start_streamlit.bat           # Windows 启动脚本
`

---

## 编码约定

### 类型与异常
- **所有函数必须有完整 Type Hints**（参数 + 返回值）。使用 Optional[X] 而非 X | None。
- **所有 I/O 操作必须包含 try-except**：数据库查询、文件读写、外部 API 调用。异常必须记录日志并向上传播或降级处理。
- 优先使用 Optional / Union，避免裸 None 作为默认参数。

### 配置
- 配置通过 ConfigLoader 链式加载：.env → global.yaml → products/{code}.yaml，使用 _deep_merge 深度合并。
- 最终结果经 AppConfig.model_validate()（Pydantic V2）校验。
- 配置访问示例：config.application.cache_ttl_hours、config.data_source.product_code。

### 日志
- 使用 TimedRotatingFileHandler（按天轮转），日志文件位于 logs/ 目录。
- 日志级别按域 × 级别二维隔离：pp.log_yield.log、pp.log_spc.log、pp.log_error.log 等。

### DDD 分层约束
- pplication/ — 服务编排，不包含业务逻辑，只调用 core/ 和 infrastructure/。
- core/ — 纯业务逻辑与算法，不依赖数据库或文件 I/O。
- infrastructure/ — 数据访问（Repository）、外部集成，可依赖 core/ 的接口。
- 跨域调用必须通过 shared_kernel/ 或依赖注入，禁止域间直接耦合。

---

## 运行与测试

`ash
# 启动应用（Windows 推荐）
start_streamlit.bat

# 或手动启动
cd d:\wzy\Python\vivo-project
set PYTHONPATH=%cd%\src;%PYTHONPATH%
uv run streamlit run app/Home.py --server.headless true --server.port 8503

# 运行全部测试（必须 100% PASS）
uv run pytest tests/ -v --tb=short

# 仅单元测试
uv run pytest tests/unit/ -v --tb=short

# 仅集成测试
uv run pytest tests/integration/ -v --tb=short

# 类型检查
uv run pyright
`

---

## 全局红线纪律

以下为硬性约束，**任何时候不得违反**：

1. **禁止静态重构核心算法** — 良率模拟（_simulate_concentration、_distribute_sheet_from_lot）和 Mapping 级联衰减经过多次业务验证，静态重构会破坏已修复的边界条件。
2. **禁止修改数据库连接单例** — DatabaseManager（__new__ 单例 + 失败重试 + .env 幂等加载）是容灾基石，任何修改会破坏断线重连和延迟加载。
3. **禁止移除 @st.cache_data** — L2 缓存移除将导致每次刷新触发全量数据库查询，造成性能灾难。
4. **禁止简化 Parquet 快照增量更新** — TTL 保护 + 2 天缓冲 + 三防线容灾是精心设计的降级策略。

> 详细红线说明参见 [docs/design/development_framework.md](docs/design/development_framework.md#4-红线纪律负面清单)

---

## 渐进式披露

根据当前任务，**仅加载相关文档**。不要一次性加载全部内容。

| 文档 | 用途 | 何时阅读 |
|------|------|----------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | 系统架构：技术栈、目录结构、数据流、缓存体系 | ✅ **首次进入项目 / 需要了解技术选型时** |
| [docs/design/yield_domain.md](docs/design/yield_domain.md) | 良率分析域设计：Yield 业务逻辑、核心算法 | 需要修改 Yield 相关代码时 |
| [docs/design/spc_domain.md](docs/design/spc_domain.md) | SPC 控制域设计：SPC 规则引擎、合规修饰 | 需要修改 SPC 相关代码时 |
| [docs/design/shared_kernel.md](docs/design/shared_kernel.md) | 共享内核设计：配置、数据库、日志、文件处理 | 需要修改基础设施层时 |
| [docs/design/development_framework.md](docs/design/development_framework.md) | 开发框架：EPCC Flow、TDD 纪律、红线约束 | ✅ **每次修改前必读** |
| [docs/design/index.md](docs/design/index.md) | 设计文档入口：领域设计、共享内核、历史快照 | 需要判断该读哪个设计文档时 |
| [docs/plans/index.md](docs/plans/index.md) | 计划入口：当前/历史计划与计划模板 | ✅ **开始新任务前，先查看是否已有计划** |
| [CONTEXT.md](CONTEXT.md) | 项目目标、边界和快速路由 | 需要快速恢复项目上下文时 |
| [specs/README.md](specs/README.md) | 可维护规则和任务契约入口 | 需要沉淀稳定规则或验收契约时 |
| [skills/README.md](skills/README.md) | 技能库索引：加密 Excel、类型标准化等专项方案 | 遇到 skills/ 中记录的特性问题时 |

---

## 缓存体系

| 层级 | 类型 | 作用域 | TTL | 失效机制 |
|------|------|--------|-----|----------|
| **L1** | Parquet 磁盘快照 | 全局共享 | 8h | 超时触发增量更新 |
| **L2** | @st.cache_data 内存 | Streamlit session | 依赖快照签名 | MD5(mtime+size) 变更 |

### 降级路径
`
正常路径：PostgreSQL → DataLoader → Parquet(L1) → @st.cache_data(L2) → UI
降级路径1（DB 假死）：Parquet(陈旧快照) → @st.cache_data(L2) → UI
降级路径2（快照过期）：增量更新 → Parquet(新快照) → @st.cache_data → UI
`

### 三防线容灾（PanelRepository）
`
第一防线：正常查询 → PostgreSQL 在线
第二防线：降级查询 → Parquet 陈旧快照（DB 假死）
第三防线：硬编码默认值 → 极端情况保底
`

---

## 领域词汇表

| 术语 | 全称 / 说明 |
|------|-------------|
| **Panel** | 面板（最小分析单元），带有 ID、坐标、缺陷信息 |
| **Sheet** | 玻璃基板（Panel 的物理载体） |
| **Lot** | 生产批次，由多个 Sheet 组成 |
| **OOS** | Out of Spec（均值触碰规格线 USL/LSL） |
| **SOOS** | Some Out of Spec（极值触碰规格线） |
| **OOC** | Out of Control（均值触碰控制线 UCL/LCL） |
| **MWD** | Monthly/Weekly/Daily（月/周/日趋势数据） |
| **Mapping** | 不良在 Sheet 上的空间集中性分布热力图 |
| **Snapshot** | Parquet 格式本地磁盘快照 |
| **COM** | 通过 comtypes 调用本地 Excel.Application 读取加密 Excel |
| **TTL** | Time-To-Live，缓存有效期 |
| **EMA** | Exponential Moving Average（指数移动平均） |

---

> **信息冲突时，以更具体的文档为准。** 例如：docs/design/yield_domain.md 中的详细设计优先于 ARCHITECTURE.md 中的概述。
> **本文件（AGENTS.md）为根级指令，scope 覆盖整个仓库。** 子目录可放置更具体的 AGENTS.md 覆盖本文件的对应部分。

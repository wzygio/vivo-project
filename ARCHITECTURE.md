# 天柱专项报表系统 · 系统架构

> **项目代号**: 天柱专项报表系统  
> **领域**: 面板制造业（OLED/Array 半导体显示）良率分析与 SPC 统计过程控制  
> **技术栈**: Python + Streamlit + PostgreSQL + Parquet 快照  
> **最后更新**: 2026-05-18

---

## 目录

- [1. 技术栈](#1-技术栈)
- [2. 项目结构](#2-项目结构)
- [3. 领域模块划分](#3-领域模块划分)
- [4. 数据流架构](#4-数据流架构)
- [5. 缓存体系](#5-缓存体系)
- [6. 容灾与降级策略](#6-容灾与降级策略)
- [7. 配置体系](#7-配置体系)
- [8. 系统环境与命令](#8-系统环境与命令)
- [9. 词汇表](#9-词汇表)

---

## 1. 技术栈

| 组件 | 技术选型 |
|------|----------|
| **Web 门户** | [Streamlit](app/Home.py:48)（Python 数据应用框架） |
| **数据库** | [PostgreSQL](src/shared_kernel/infrastructure/db_handler.py:64)（Panel 级生产和量测数据） |
| **包管理** | [uv](pyproject.toml:31)（依赖声明在 `[project] dependencies`） |
| **配置管理** | Pydantic V2 + YAML（链式加载 + 深度合并） |
| **缓存** | Parquet 快照（L1）+ `@st.cache_data`（L2） |
| **加密文件读取** | COM（`win32com`/`comtypes`）调用本地 Excel.Application |
| **图表** | ECharts（前端 Mapping 热力图 / 趋势图 / SPC 看板） |
| **测试** | pytest（`conftest.py` + `factories.py`） |
| **日志** | `TimedRotatingFileHandler`（按天轮转，领域 × 级别二维隔离） |

**依赖镜像源**: [`https://pypi.tuna.tsinghua.edu.cn/simple`](pyproject.toml:40)  
**Python 版本**: 参见 [`.python-version`](.python-version)

---

## 2. 项目结构

```
vivo-project/
├── .env                          # 环境变量（数据库凭证，被 .gitignore 排除）
├── pyproject.toml                # 项目元数据 & uv 依赖声明
├── uv.lock                       # 依赖锁定文件
├── start_streamlit.bat           # Windows 启动脚本
├── ARCHITECTURE.md               # 系统架构（本文）
│
├── app/                          # [展示层/Presentation] Streamlit 前端应用
│   ├── Home.py                   #   门户入口：全屏 iframe 加载静态资源
│   ├── charts/                   #   图表组件（ECharts 封装）
│   ├── components/               #   Streamlit UI 组件
│   ├── pages/                    #   多页面路由
│   │   ├── 入库不良率分析看板.py
│   │   ├── 入库不良率ByLot明细表.py
│   │   ├── 入库不良率BySheet明细表.py
│   │   ├── 专项资料-台账周报.py
│   │   ├── 专项资料-解析报告.py
│   │   ├── 关键备件报表.py
│   │   └── 自动预警看板.py
│   └── utils/                    #   前端工具
│       ├── app_setup.py          #     应用初始化
│       ├── logger_setup.py       #     企业级日志架构
│       ├── reloader.py           #     代码热重载
│       └── session_manager.py    #     Session 状态管理
│
├── config/                       # [配置层] YAML 配置仓库
│   ├── global.yaml               #   全局配置
│   ├── compliance_config.yaml    #   合规修饰规则
│   ├── scrap_factory_mapping.yaml#   报废站点→厂别映射
│   └── products/                 #   产品级配置
│       ├── M626.yaml
│       └── M678.yaml
│
├── src/                          # [领域层/Domain] 核心业务逻辑
│   ├── shared_kernel/            # [共享内核] 通用基础设施
│   ├── yield_domain/             # [良率分析域] Yield Analysis
│   └── spc_domain/               # [SPC控制域] Statistical Process Control
│
├── resources/                    # [资源层] 静态文件与Excel基线数据
│   ├── static/                   #   HTML/CSS/JS 前端门户源码
│   ├── xlsx_to_csv/              #   加密Excel的CSV备用文件
│   ├── M626/ M678/               #   产品级资源
│   └── scrap_sheets.xlsx ...     #   SPC规则文件
│
├── tests/                        # [测试层]
│   ├── conftest.py               #   Pytest Fixtures
│   ├── factories.py              #   测试数据工厂
│   ├── unit/                     #   单元测试
│   └── integration/              #   集成测试
│
├── docs/                         # [文档层] 架构与设计文档
│   ├── design/                   #   业务与功能设计（按模块）
│   │   ├── yield_domain.md
│   │   ├── spc_domain.md
│   │   ├── shared_kernel.md
│   │   ├── development_framework.md
│   │   └── business_boundary.md
│   ├── plans/                    #   执行计划
│   │   ├── PLANS.md              #     计划总览
│   │   ├── spec_architecture_plan.md
│   │   └── spec_关键备件报表.md
│   └── prompt/                   #   AI Agent 辅助指令
│       ├── spec_extractor.md
│       ├── spec_generator.md
│       └── spec_template_generator.md
│
├── skills/                       # [技能库] 专项问题解决方案
│   └── README.md                 #   技能库索引
│
├── logs/                         # [运行时日志] 按天轮转
│   ├── app_info.log / app_error.log / app_trace.log
│   ├── app_spc.log / app_yield.log / app_shared.log
│
└── scripts/                      # [脚本] 辅助工具脚本
    ├── fix_imports.py
    ├── generate_tree.py
    └── verify_export.py
```

---

## 3. 领域模块划分

### 3.1 三层架构模式

每个领域域（Domain）遵循 DDD 分层架构：

| 层级 | 职责 | 依赖方向 |
|------|------|----------|
| **Application** | 服务编排、缓存调度、DTO | → Core |
| **Core (Domain)** | 纯业务逻辑、规则引擎 | ← Application |
| **Infrastructure** | DAO、外部系统集成、存储 | → Core |

### 3.2 领域清单

| 领域 | 目录 | 核心职责 |
|------|------|----------|
| **Yield Domain** | [`src/yield_domain/`](src/yield_domain/) | 良率分析、不良率散布、Mapping 热力图、MWD 趋势 |
| **SPC Domain** | [`src/spc_domain/`](src/spc_domain/) | 统计过程控制、OOS/SOOS/OOC 判定 |
| **Shared Kernel** | [`src/shared_kernel/`](src/shared_kernel/) | 配置管理、数据库连接、日志、文件处理 |

> **详细设计请参考**: [`docs/design/yield_domain.md`](docs/design/yield_domain.md) · [`docs/design/spc_domain.md`](docs/design/spc_domain.md) · [`docs/design/shared_kernel.md`](docs/design/shared_kernel.md)

---

## 4. 数据流架构

### 4.1 全量数据管道

```
┌──────────┐   ┌──────────────┐   ┌────────────────┐   ┌──────────────┐
│PostgreSQL│──▶│Data Loader   │──▶│Repository      │──▶│Service Layer │──▶ Streamlit UI
│(源数据)   │   │(DAO/SQL查询) │   │(快照缓存/L1)   │   │(缓存编排/L2) │
└──────────┘   └──────────────┘   └────────────────┘   └──────────────┘
                                                              │
                                                  ┌───────────┴───────────┐
                                                  ▼                       ▼
                                          ┌──────────────┐       ┌──────────────┐
                                          │Core Domain   │       │ Charts/Pages │
                                          │(业务计算)     │       │ (可视化)      │
                                          └──────────────┘       └──────────────┘
```

### 4.2 缓存降级路径

```
正常路径： PostgreSQL → DataLoader → Parquet(L1) → @st.cache_data(L2) → UI
                                                 │
降级路径1（DB假死）： Parquet(陈旧快照) → @st.cache_data(L2) → UI
降级路径2（快照过期）： 增量更新 → Parquet(新快照) → @st.cache_data → UI
```

---

## 5. 缓存体系

| 层级 | 类型 | 作用域 | TTL | 失效机制 |
|------|------|--------|-----|----------|
| **L1** | Parquet 磁盘快照 | 全局共享 | 8h | 超时触发增量更新 |
| **L2** | `@st.cache_data` 内存 | Streamlit session | 依赖快照签名 | MD5(mtime+size) 变更 |

### 代码热重载
- `deep_reload_modules()` — 强制卸载 `src/`、`app/` 模块
- `get_project_revision()` — MD5 指纹用于 cache key

---

## 6. 容灾与降级策略

### 三防线容灾（PanelRepository）

```
第一防线：正常查询 → PostgreSQL 在线
第二防线：降级查询 → Parquet 陈旧快照（DB 假死）
第三防线：硬编码默认值 → 极端情况保底
```

### Agent 熔断
同一个 Bug 连续修复 3 次失败，必须立即停止并要求人类介入。

---

## 7. 配置体系

```
load_config(product_code)
  → 加载 .env
  → 加载 global.yaml
  → 加载 products/{product_code}.yaml
  → _deep_merge 深度合并
  → AppConfig.model_validate() Pydantic 校验
```

配置模型链式访问：
```python
config.application.cache_ttl_hours        # 缓存TTL
config.data_source.product_code           # 产品代码
config.paths['static_warning_lines'].file_name  # 文件路径
config.processing['defect_capping']       # 处理参数
```

---

## 8. 系统环境与命令

### 运行命令

```bash
# 方式1：批处理脚本（Windows 推荐）
start_streamlit.bat

# 方式2：手动启动
cd d:\wzy\Python\vivo-project
set PYTHONPATH=%cd%\src;%PYTHONPATH%
uv run streamlit run app/Home.py --server.headless true --server.port 8503
```

### 测试命令

```bash
# 运行全部测试
uv run pytest tests/ -v --tb=short

# 仅运行单元测试
uv run pytest tests/unit/ -v --tb=short

# 仅运行集成测试
uv run pytest tests/integration/ -v --tb=short
```

---

## 9. 词汇表

| 术语 | 全称 / 说明 |
|------|-------------|
| **Panel** | 面板（最小分析单元），带有 ID、坐标、缺陷信息 |
| **Sheet** | 玻璃基板（Panel 的物理载体） |
| **Lot** | 生产批次，由多个 Sheet 组成 |
| **OOS** | Out of Spec（均值触碰规格线 USL/LSL） |
| **SOOS** | Some Out of Spec（极值触碰规格线） |
| **OOC** | Out of Control（均值触碰管控线 UCL/LCL） |
| **MWD** | Monthly/Weekly/Daily（月/周/日 趋势数据） |
| **Mapping** | 不良在 Sheet 上的空间集中性分布热力图 |
| **Snapshot** | Parquet 格式本地磁盘快照 |
| **COM** | 通过 `comtypes` 调用本地 Excel.Application 读取加密 Excel |
| **TTL** | Time-To-Live，缓存有效期 |
| **EMA** | Exponential Moving Average（指数移动平均） |

---

> **相关设计文档**: [`docs/design/yield_domain.md`](docs/design/yield_domain.md) · [`docs/design/spc_domain.md`](docs/design/spc_domain.md) · [`docs/design/shared_kernel.md`](docs/design/shared_kernel.md)  
> **开发规范**: [`docs/design/development_framework.md`](docs/design/development_framework.md)  
> **业务边界**: [`docs/design/business_boundary.md`](docs/design/business_boundary.md)

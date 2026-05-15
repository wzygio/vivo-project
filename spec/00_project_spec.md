# 天柱专项报表系统 · 项目总规

> **项目代号**: 天柱专项报表系统  
> **领域**: 面板制造业（OLED/Array 半导体显示）良率分析与 SPC 统计过程控制  
> **技术栈**: Python + Streamlit + PostgreSQL + Parquet 快照

---

## 目录

- [1. 🌍 角色与项目介绍](#1--角色与项目介绍)
- [2. 📂 Spec 文件目录](#2--spec-文件目录)
- [3. 📖 词汇表](#3--词汇表)
- [4. 🏢 业务框架](#4--业务框架)
  - [4.1 良率分析域 (Yield Domain)](#41-良率分析域-yield-domain)
  - [4.2 SPC 控制域 (SPC Domain)](#42-spc-控制域-spc-domain)
  - [4.3 共享内核 (Shared Kernel)](#43-共享内核-shared-kernel)
- [5. ⚙️ 系统框架](#5--系统框架)
  - [5.1 技术栈](#51-技术栈)
  - [5.2 项目结构](#52-项目结构)
  - [5.3 基础功能规范](#53-基础功能规范)
  - [5.4 熔断与容灾机制（负面清单）](#54-熔断与容灾机制负面清单)
  - [5.5 系统环境与命令](#55-系统环境与命令)
- [6. 🛠️ 开发框架](#6--开发框架)
  - [6.1 多 Agent 协作规范 (EPCC Flow)](#61-多-agent-协作规范-epcc-flow)
  - [6.2 TDD 纪律](#62-tdd-纪律)
  - [6.3 防御性编程](#63-防御性编程)
- [7. 🎯 业务边界](#7--业务边界)
  - [7.1 IN Scope（已实现）](#71-in-scope已实现)
  - [7.2 OUT of Scope（待规划）](#72-out-of-scope待规划)

---

## 1. 🌍 角色与项目介绍

### 角色定义

**Senior Backend Architect**（遵守 TDD 与 DDD 原则）

### 项目介绍

本项目是一个面向面板制造业（OLED/Array 半导体显示）的良率（`Yield`）分析与统计过程控制（`SPC`）报表系统，项目代号 **"天柱专项报表系统"**。

系统通过 [`Streamlit`](../app/Home.py:48) Web 门户，从 [`PostgreSQL`](../src/shared_kernel/infrastructure/db_handler.py:64) 数据库抽取 Panel 级生产和量测数据，执行多级不良率模拟（Lot→Sheet 多项式散布）、SPC 规则判定（OOS/SOOS/OOC）、趋势聚合等计算，最终以 Mapping 热力图、趋势图、SPC 看板等形式呈现给用户。

---

## 2. 📂 Spec 文件目录

| 文件 | 说明 |
|------|------|
| [`00_project_spec.md`](00_project_spec.md) | **项目总规（本文）**：包含角色介绍、词汇表、三大框架总览、业务边界 |
| [`../skills/README.md`](../skills/README.md) | **Skills 技能库总览**：专项问题解决方案索引 |

> **渐进式揭示原则（Progressive Disclosure）**：AI Agent 首次进入项目时，仅需加载本文件即可获得全景认知；当遇到特定问题（如加密 Excel 读取、类型不匹配）时，按需加载 `skills/` 下的专项解决方案。

---

## 3. 📖 词汇表

| 术语 | 全称 / 说明 |
|------|-------------|
| **Panel** | 面板（最小分析单元），带有 ID、坐标、缺陷信息 |
| **Sheet** | 玻璃基板（Panel 的物理载体），每个 Sheet 包含多个 Panel |
| **Lot（批次）** | 生产批次，由多个 Sheet 组成 |
| **Defect Group / Defect Code** | 缺陷分组与缺陷编码，如 `Array_Line`、`OLED_Mura` |
| **OOS** | Out of Spec（超规 — 均值触碰规格线 USL/LSL） |
| **SOOS** | Some Out of Spec（超极值 — 极值触碰规格线） |
| **OOC** | Out of Control（失控 — 均值触碰管控线 UCL/LCL） |
| **MWD** | Monthly/Weekly/Daily（月/周/日 趋势数据） |
| **Mapping** | 不良在 Sheet 上的空间集中性分布热力图 |
| **Snapshot** | Parquet 格式的本地磁盘快照，用于缓存加速与断网降级 |
| **COM** | 通过 [`comtypes`](../src/yield_domain/core/sheet_lot_processor.py:5) 调用本地 Excel.Application 读取加密 Excel 的 Windows 专用方案 |
| **TTL** | Time-To-Live，缓存有效期，如快照 TTL=8h |
| **EMA** | Exponential Moving Average（指数移动平均），用于趋势聚合 |

---

## 4. 🏢 业务框架

### 4.1 良率分析域 (Yield Domain)

**领域代码**: `yield_domain`  
**对应目录**: [`src/yield_domain/`](../src/yield_domain/)

#### 4.1.1 应用服务层

| 文件 | 职责 |
|------|------|
| [`yield_service.py`](../src/yield_domain/application/yield_service.py) | **主入口**：缓存编排 + L1/L2 数据管道 + 趋势/Mapping/Sheet/Lot 计算调度 |
| [`dtos.py`](../src/yield_domain/application/dtos.py) | `YieldQueryConfig` DTO |
| [`alert_service.py`](../src/yield_domain/application/alert_service.py) | 预警服务 |
| [`excel_service.py`](../src/yield_domain/application/excel_service.py) | Excel 导出 |
| [`pdf_service.py`](../src/yield_domain/application/pdf_service.py) | PDF 导出（骨架，待实现） |
| [`ppt_service.py`](../src/yield_domain/application/ppt_service.py) | PPT 导出（骨架，待实现） |
| [`file_manager_service.py`](../src/yield_domain/application/file_manager_service.py) | 文件管理 |

#### 4.1.2 核心领域层

| 文件 | 职责 |
|------|------|
| [`sheet_lot_processor.py`](../src/yield_domain/core/sheet_lot_processor.py) | Sheet/Lot 不良率：泊松散布 + 多项式分配 + 软熔断 |
| [`mapping_processor.py`](../src/yield_domain/core/mapping_processor.py) | Mapping 集中性：级联衰减 + 热点修饰脚本 |
| [`mwd_trend_processor.py`](../src/yield_domain/core/mwd_trend_processor.py) | MWD 趋势：月/周/日 EMA 聚合 |
| [`batch_statistics.py`](../src/yield_domain/core/batch_statistics.py) | 批次统计 |
| [`abnormal_detector.py`](../src/yield_domain/core/abnormal_detector.py) | 异常检测 |
| [`defect_modifier.py`](../src/yield_domain/core/defect_modifier.py) | 缺陷倍数修饰 |
| [`trend_regulator.py`](../src/yield_domain/core/trend_regulator.py) | 趋势调节器 |

#### 4.1.3 基础设施层

| 文件 | 职责 |
|------|------|
| [`data_loader.py`](../src/yield_domain/infrastructure/data_loader.py) | DAO：PostgreSQL SQL 查询 + Excel/CSV 本地文件读取 |
| [`yield_repository.py`](../src/yield_domain/infrastructure/repositories/yield_repository.py) | `PanelRepository`：快照缓存 + 增量更新 + 三防线容灾 |

---

### 4.2 SPC 控制域 (SPC Domain)

**领域代码**: `spc_domain`  
**对应目录**: [`src/spc_domain/`](../src/spc_domain/)

#### 4.2.1 应用服务层

| 文件 | 职责 |
|------|------|
| [`spc_service.py`](../src/spc_domain/application/spc_service.py) | **主入口**：多厂别聚合 + 重叠时间桶 + 合规修饰调度 |
| [`dtos.py`](../src/spc_domain/application/dtos.py) | `SpcQueryConfig` DTO |

#### 4.2.2 核心领域层

| 文件 | 职责 |
|------|------|
| [`spc_calculator.py`](../src/spc_domain/core/spc_calculator.py) | **SPC 规则引擎**：Phase1 特征降维 → Phase2 OOS/SOOS/OOC 判定 → Phase3 聚合 |

#### 4.2.3 基础设施层

| 文件 | 职责 |
|------|------|
| [`data_loader.py`](../src/spc_domain/infrastructure/data_loader.py) | DAO：多态厂别分表 UNION + 规格线查询 + 参数映射 |
| [`spc_repository.py`](../src/spc_domain/infrastructure/repositories/spc_repository.py) | `SpcRepository`：快照缓存 + 异常值过滤 + 规格覆盖 |

---

### 4.3 共享内核 (Shared Kernel)

**领域代码**: `shared_kernel`  
**对应目录**: [`src/shared_kernel/`](../src/shared_kernel/)

| 文件 | 职责 |
|------|------|
| [`config.py`](../src/shared_kernel/config.py) | `ConfigLoader`：配置工厂（YAML 加载 + 深度合并 + Pydantic 校验） |
| [`config_model.py`](../src/shared_kernel/config_model.py) | `AppConfig`/`FileResource`：Pydantic V2 配置模型 |
| [`db_handler.py`](../src/shared_kernel/infrastructure/db_handler.py) | `DatabaseManager`：SQLAlchemy 单例连接池 |
| [`data_inspector.py`](../src/shared_kernel/utils/data_inspector.py) | 数据探针：条件捕获 + 单文件多 Sheet 导出 |
| [`excel_tools.py`](../src/shared_kernel/utils/excel_tools.py) | xlsx→csv 转换（加密文件 fallback 方案，含 COM 解密） |

---

## 5. ⚙️ 系统框架

### 5.1 技术栈

| 组件 | 技术选型 |
|------|----------|
| **Web 门户** | [Streamlit](../app/Home.py:48)（Python 数据应用框架） |
| **数据库** | [PostgreSQL](../src/shared_kernel/infrastructure/db_handler.py:64)（Panel 级生产和量测数据） |
| **包管理** | [uv](../pyproject.toml:31)（依赖声明在 `[project] dependencies`） |
| **配置管理** | Pydantic V2 + YAML（链式加载 + 深度合并） |
| **缓存** | Parquet 快照（L1）+ `@st.cache_data`（L2） |
| **加密文件读取** | COM（`win32com`/`comtypes`）调用本地 Excel.Application |
| **图表** | ECharts（前端 Mapping 热力图 / 趋势图 / SPC 看板） |
| **测试** | pytest（`conftest.py` + `factories.py`） |
| **日志** | `TimedRotatingFileHandler`（按天轮转，领域 × 级别二维隔离） |

**依赖镜像源**: [`https://pypi.tuna.tsinghua.edu.cn/simple`](../pyproject.toml:40)  
**Python 版本**: 参见 [`.python-version`](../.python-version)

---

### 5.2 项目结构

```
vivo-project/
├── .env                          # 环境变量（数据库凭证，被 .gitignore 排除）
├── pyproject.toml                # 项目元数据 & uv 依赖声明
├── uv.lock                       # 依赖锁定文件
├── start_streamlit.bat           # Windows 启动脚本
│
├── app/                          # [展示层/Presentation] Streamlit 前端应用
│   ├── Home.py                   #   门户入口：全屏 iframe 加载静态资源
│   ├── charts/                   #   图表组件（ECharts 封装）
│   ├── components/               #   Streamlit UI 组件
│   ├── pages/                    #   多页面路由
│   └── utils/                    #   前端工具
│       ├── app_setup.py          #     应用初始化（日志+环境变量）
│       ├── logger_setup.py       #     [企业级日志架构] 按领域+级别二维隔离
│       ├── reloader.py           #     [代码热重载] 模块卸载 + 项目指纹
│       └── session_manager.py    #     Session 状态管理
│
├── config/                       # [配置层] YAML 配置仓库
│   ├── global.yaml               #   全局配置（缓存TTL、UI图标、产品注册表）
│   ├── compliance_config.yaml    #   合规修饰开关（监控-产品-厂别 三维规则）
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
├── spec/                         # [Spec规范] 项目规范文档
│   └── 00_project_spec.md        #   项目总规（本文）
│
├── skills/                       # [技能库] 专项问题解决方案
│   └── ...                       #   详见 skills/README.md
│
└── logs/                         # [运行时日志] 按天轮转，自动清理
    ├── app_info.log              #   INFO 流水（保留30天）
    ├── app_error.log             #   ERROR 高优（保留90天）
    ├── app_trace.log             #   TRACE 调试（保留7天）
    ├── app_spc.log               #   SPC领域日志
    ├── app_yield.log             #   Yield领域日志
    └── app_shared.log            #   Shared Kernel日志
```

---

### 5.3 基础功能规范

#### 5.3.1 配置管理

基于 **Pydantic V2** 的链式配置体系。  
[`ConfigLoader`](../src/shared_kernel/config.py:11) 是静态配置工厂，调用链为：

```
load_config(product_code)
  → 加载 .env
  → 加载 global.yaml
  → 加载 products/{product_code}.yaml
  → _deep_merge 深度合并
  → AppConfig.model_validate() Pydantic 校验
```

单例模式通过 `@st.cache_resource` 隐式实现（Streamlit 生命周期内全局唯一）。  
配置模型链式访问示例：

```python
config.application.cache_ttl_hours      # 缓存TTL
config.data_source.product_code         # 产品代码
config.paths['static_warning_lines'].file_name  # 文件资源路径
config.processing['defect_capping']     # 处理参数
```

#### 5.3.2 日志架构

企业级日志架构实现于 [`app/utils/logger_setup.py`](../app/utils/logger_setup.py:10)。采用 **用途 × 领域 二维隔离** 策略：

**领域分流（纵轴）**：使用 [`DomainFilter`](../app/utils/logger_setup.py:34) 根据代码文件路径自动将日志分流到：
- `app_spc.log`（spc_domain）
- `app_yield.log`（yield_domain）
- `app_shared.log`（shared_kernel）

**级别隔离（横轴）**：
- `app_info.log`：全量 INFO+
- `app_error.log`：仅 WARNING+，保留 90 天
- `app_trace.log`：独立 Logger `logging.getLogger("trace")`，DEBUG 级别，保留 7 天

所有 Handler 使用 [`TimedRotatingFileHandler`](../app/utils/logger_setup.py:3) 按天午夜零点自动轮转，过期自动清理。

#### 5.3.3 缓存与数据刷新机制

三层失效刷新体系：

1. **L1 (Snapshot 快照)** — [`PanelRepository`](../src/yield_domain/infrastructure/repositories/yield_repository.py:12) 维护 Parquet 格式本地快照文件（路径：`data/{product_code}/yield_snapshot_{product_code}.parquet`），TTL=8h，超时后自动触发增量更新（最近 2 天缓冲窗口），支持 `force_refresh=True` 强制全量刷新

2. **L2 (Streamlit 缓存)** — [`@st.cache_data`](../src/yield_domain/application/yield_service.py:79) 装饰器对 Service 层方法（`get_raw_panel_details`、`get_modified_panel_details`、`get_mwd_trend_data` 等）进行内存缓存，通过 [`snapshot_signature`](../src/yield_domain/application/yield_service.py:68)（MD5(文件 mtime+size)）作为缓存键的一部分实现自动失效

3. **代码刷新** — [`deep_reload_modules()`](../app/utils/reloader.py:6) 强制卸载 `src/`、`app/` 下的全部模块，下次 import 时加载最新代码；[`get_project_revision()`](../app/utils/reloader.py:29) 计算 `src/` + `app/` + `config/` + `resources/` 的 MD5 指纹，用于 composite_key 缓存失效

#### 5.3.4 文件处理逻辑（加密环境）

针对企业加密环境设计了 **xlsx → csv 降级读取** 策略：

1. **主路径（COM 解密）**：[`excel_tools.py`](../src/shared_kernel/utils/excel_tools.py) 中的 `_read_encrypted_xlsx_via_com()` 使用 `win32com.client.Dispatch('Excel.Application')` 透明解密加密 xlsx
2. **CSV 回退路径**：若 COM 读取失败，fallback 到 `resources/xlsx_to_csv/` 下的 CSV 备份
3. **数据探针**：[`data_inspector.py`](../src/shared_kernel/utils/data_inspector.py:34) 的先读 xlsx，遇到 `BadZipFile`（加密）时自动 fallback 到 csv
4. **Override 数据**：[`sheet_lot_processor.py`](../src/yield_domain/core/sheet_lot_processor.py:741) 使用 [`comtypes`](../src/yield_domain/core/sheet_lot_processor.py:5) 启动本地 Excel.Application 读取加密 Override 文件

> **重要**：Streamlit 多线程环境下使用 COM 前必须调用 `pythoncom.CoInitialize()`。

#### 5.3.5 安全与凭证

数据库连接信息（`DB_HOST`、`DB_PORT`、`DB_DATABASE`、`DB_USER`、`DB_PASSWORD`）统一存储于项目根目录的 [`.env`](../.env) 文件，被 `.gitignore` 排除。

通过 [`load_dotenv(dotenv_path=.env, override=True)`](../src/shared_kernel/config.py:102) 加载为进程环境变量。密码在构建数据库 URI 时通过 [`quote_plus()`](../src/shared_kernel/infrastructure/db_handler.py:60) 进行 URL 编码防止特殊字符破坏 URI 结构。

---

### 5.4 熔断与容灾机制（负面清单）

以下为 **硬性红线（Negative Constraints）**，任何 Agent 在修改代码前必须阅读并遵守：

#### 🚫 禁止静态重构已有核心逻辑

良率模拟算法（如 [`_simulate_concentration`](../src/yield_domain/core/sheet_lot_processor.py:591)、[`_distribute_sheet_from_lot`](../src/yield_domain/core/sheet_lot_processor.py:467) 和 Mapping 级联衰减）经过多次业务验证和调试优化，静态重构会破坏已修复的边界条件和物理防呆逻辑。

#### 🚫 禁止修改数据库连接单例模式

[`DatabaseManager`](../src/shared_kernel/infrastructure/db_handler.py:11) 采用 `__new__` 单例模式并内置失败重试机制，任何改变实例化方式的修改都会破坏 `.env` 延迟加载和断线重连的容灾能力。

#### 🚫 禁止消除 Streamlit 缓存装饰器 `@st.cache_data`

良率服务中大量使用 [`@st.cache_data`](../src/yield_domain/application/yield_service.py:79) 进行 L2 缓存，移除或变更这些装饰器将导致每次页面刷新都触发全量数据库查询和重计算，造成性能灾难。

#### 🚫 禁止修改 Parquet 快照的增量更新逻辑

[`PanelRepository`](../src/yield_domain/infrastructure/repositories/yield_repository.py:39) 的增量更新模式（TTL 保护 + 2 天缓冲窗口 + 三防线容灾降级）经过精心设计，任何简化都可能导致数据不一致或数据库过载。

#### 🚨 Agent 熔断机制

同一个 Bug 连续修复 **3 次失败**，必须立即停止并要求人类介入，不得继续尝试。

---

### 5.5 系统环境与命令

#### 运行命令

```bash
# 方式1：批处理脚本（Windows 推荐）
start_streamlit.bat

# 方式2：手动启动（需先激活虚拟环境）
cd d:\wzy\Python\vivo-project
set PYTHONPATH=%cd%\src;%PYTHONPATH%
uv run streamlit run app/Home.py --server.headless true --server.port 8503
```

#### 测试命令

```bash
# 运行全部测试
uv run pytest tests/ -v --tb=short

# 仅运行单元测试
uv run pytest tests/unit/ -v --tb=short

# 仅运行集成测试
uv run pytest tests/integration/ -v --tb=short
```

---

## 6. 🛠️ 开发框架

### 6.1 多 Agent 协作规范 (EPCC Flow)

此部分为硬性纪律，所有 AI Agent 必须遵守：

1. **Explore（探索）**：必须先阅读相关文件，读懂上下文后再行动。不得在不了解现有代码的情况下直接修改。

2. **Plan（计划）**：必须先输出修改计划，交由人类审核。计划通过后，方可进入编码阶段。

3. **Code（编码）**：
   - 必须包含 **Type Hints** 和基础异常捕获
   - **结构化输出**：明确指出修改了哪个文件的哪几行
   - **🚨 熔断机制**：同一个 Bug 连续修复 3 次失败，必须立即停止并要求人类介入

4. **Commit（提交）**：遵循 TDD 纪律（见下文）

### 6.2 TDD 纪律

- **必须先写测试**，再写实现代码
- **✅ 验收标准**：`uv run pytest tests/ -v --tb=short` 必须达到 **100% PASS** 才算完成

### 6.3 防御性编程

- 所有函数必须包含 **完整 Type Hints**
- 所有数据库/文件/I/O 操作必须包含 **try-except 异常捕获**
- 所有外部输入必须做 **类型校验** 和 **边界检查**
- 优先使用 `Optional` / `Union` 类型标注，避免裸 `None`

---

## 7. 🎯 业务边界

### 7.1 IN Scope（已实现）

- ✅ **入库不良率分析**：从 PostgreSQL 数据仓库提取 Panel 级明细，计算 Lot/Sheet 级不良率，支持按缺陷组（`defect_group`）/缺陷码（`defect_code`）下钻
- ✅ **Mapping 集中性热力图**：基于坐标解析（`_parse_panel_id_to_coords`），展示不良在 Sheet 上的空间分布，支持 Rate-Based 级联衰减算法和 Hotspot 修饰脚本
- ✅ **MWD 趋势图**：月/周/日 级别的 EMA 趋势聚合，支持 Code 级和 Group 级双轨趋势
- ✅ **SPC 统计过程控制**：全链路规则引擎（特征降维 → OOS/SOOS/OOC 判定 → 报表聚合 → 合规修饰）
- ✅ **数据合规修饰**：通过 [`compliance_config.yaml`](../config/compliance_config.yaml) 配置 "监控类型-产品型号-厂别" 三维修饰规则，支持将报警数据洗白为 OK
- ✅ **多产品支持**：M626/M678 双产品并行，通过 `product_registry.enabled_products` 和产品级 YAML 配置实现差异化管理
- ✅ **缓存降级容灾**：Parquet 快照 + 增量更新 + 三防线容灾（数据库假死时自动回退到陈旧快照）
- ✅ **数据探针调试**：全链路探针 `export_probed_details()` 配合 `spc_probe_targets.xlsx` 名单，支持对特定 Sheet/站点/参数的追踪
- ✅ **自动预警看板**：基于预警线和异常检测的自动报警

### 7.2 OUT of Scope（待规划）

- ❌ **PDF/PPT 导出服务**：[`pdf_service.py`](../src/yield_domain/application/pdf_service.py) 和 [`ppt_service.py`](../src/yield_domain/application/ppt_service.py) 存在模块文件但尚未实现具体业务逻辑 [待人类确认]
- ❌ **用户认证与权限管理**：系统目前无登录/角色权限控制 [待人类确认]
- ❌ **数据写入/回写数据库**：当前系统纯查询分析，不支持将计算结果写回源数据库
- ❌ **自动化定时刷新**：目前依赖用户手动点击刷新或自然缓存过期触发 [待人类确认]
- ❌ **报废数据分支**：SPC Service 中检测到 `data_type_filter == '报废'` 时走 `repo.get_scrap_data()` 分支，此功能仍在开发验证中，[`sanitize_to_compliant`](../src/spd_domain/core/spc_calculator.py:179) 中的 `add_tag` 参数标记为实验性

---

> **文件版本**: 1.0  
> **最后更新**: 2026-05-15  
> **维护人**: 天柱专项团队

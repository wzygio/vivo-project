# Vivo SPC & Yield Analysis Platform

![Python](https://img.shields.io/badge/Python-3.11+-blue.svg?logo=python)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B.svg?logo=streamlit&logoColor=white)
![DDD](https://img.shields.io/badge/Architecture-DDD-512BD4.svg)
![TDD](https://img.shields.io/badge/Tests-TDD-green.svg)
![License](https://img.shields.io/badge/License-Internal-lightgrey.svg)

---

## 项目简介 (Project Overview)

本项目是面向 vivo 制造品质管理的 **数据分析与可视化看板平台**，核心业务聚焦于 **SPC（统计过程控制）** 与 **Yield（良率/入库不良率）** 两大领域。平台通过自动化的数据清洗、异常预警、多维度下钻分析（ByLot / BySheet）以及周报/解析报告的自动化生成，帮助品质工程师快速定位制程风险、降低入库不良率，实现从被动响应到主动预警的数字化品质管理。

---

## 架构设计 (Architecture Design)

项目严格遵循 **DDD (Domain-Driven Design, 领域驱动设计)** 的分层架构，将 UI 展示与核心业务逻辑彻底解耦，确保代码的可测试性、可维护性和长期演进能力。

```text
┌─────────────────────────────────────────────────────────────┐
│  Presentation Layer  (展示层)                                │
│  app/  ── Streamlit 页面、可复用组件、图表渲染                │
├─────────────────────────────────────────────────────────────┤
│  Domain Layer  (领域层)                                      │
│  src/spc_domain/    ── SPC 统计过程控制领域                 │
│  src/yield_domain/  ── 入库不良率 & 自动预警领域            │
│    ├── application/     应用服务 (编排用例、DTO 转换)        │
│    ├── core/            领域核心 (算法、规则、状态计算)      │
│    └── infrastructure/  基础设施 (数据加载、仓储实现)        │
├─────────────────────────────────────────────────────────────┤
│  Shared Kernel  (共享内核)                                   │
│  src/shared_kernel/  ── 跨领域的通用配置、数据库连接、工具库  │
└─────────────────────────────────────────────────────────────┘
```

### 各层职责说明

| 目录 | 层级 | 职责 |
|------|------|------|
| `app/` | **Presentation Layer** | 仅负责页面渲染与用户交互。包含 Streamlit 的 `pages/` 页面脚本、`components/` 可复用组件（如 `spc_sections.py`、`yield_sections.py`）以及 `charts/` 图表封装。禁止直接调用数据库或包含业务规则。 |
| `src/spc_domain/` | **Domain Layer** | SPC 领域逻辑。`application/spc_service.py` 暴露用例接口；`core/spc_calculator.py` 封装 SPC 算法；`infrastructure/` 负责 SPC 原始数据的加载与仓储。 |
| `src/yield_domain/` | **Domain Layer** | Yield 领域逻辑。`application/` 提供良率分析、异常预警、Excel/PDF/PPT 导出等应用服务；`core/` 包含 `abnormal_detector.py`、`batch_statistics.py` 等核心算法；`infrastructure/` 负责 Yield 数据持久化。 |
| `src/shared_kernel/` | **Shared Kernel** | 被多个领域共享的通用能力。包括 `config.py` 全局配置、`db_handler.py` 统一数据库连接、`excel_tools.py` 等通用工具。 |

---

## 目录结构 (Directory Structure)

以下是经精简后的核心目录树，帮助新成员快速定位代码：

```text
vivo-project/
├── app/                              # 展示层 (Streamlit UI)
│   ├── Home.py                       # 入口主页
│   ├── pages/                        # 多页面脚本
│   │   ├── 自动预警看板.py
│   │   ├── 入库不良率分析看板.py
│   │   ├── 入库不良率ByLot明细表.py
│   │   ├── 入库不良率BySheet明细表.py
│   │   ├── 专项资料-台账周报.py
│   │   └── 专项资料-解析报告.py
│   ├── components/                   # 可复用 UI 组件
│   │   ├── spc_sections.py
│   │   ├── yield_sections.py
│   │   └── table_sections.py
│   └── charts/                       # 图表封装
│       ├── spc_chart.py
│       ├── mwd_chart.py
│       └── sheet_lot_chart.py
│
├── src/                              # 领域层 (核心业务逻辑)
│   ├── spc_domain/                   # SPC 领域
│   │   ├── application/
│   │   ├── core/
│   │   └── infrastructure/
│   ├── yield_domain/                 # Yield 领域
│   │   ├── application/
│   │   ├── core/
│   │   └── infrastructure/
│   └── shared_kernel/                # 共享内核
│       ├── infrastructure/
│       └── utils/
│
├── tests/                            # 测试套件 (TDD)
│   ├── unit/                         # 单元测试
│   └── integration/                  # 集成测试
│
├── config/                           # 配置文件
│   ├── products/                     # 各机型的产品配置 (M626.yaml, M678.yaml)
│   ├── compliance_config.yaml
│   └── global.yaml
│
├── docs/                             # 项目文档 & 报表模板
├── resources/                        # 静态资源 & 分析报告附件
├── scripts/                          # 开发辅助脚本
├── data/                             # 数据缓存与原始数据
├── start_streamlit.bat               # 本地启动脚本
├── run_hidden.vbs                    # 后台静默启动脚本
├── pyproject.toml
├── requirements_locked.txt
└── uv.lock
```

---

## 快速开始 (Getting Started)

### 1. 环境准备

推荐使用 [`uv`](https://github.com/astral-sh/uv) 进行依赖管理（项目中已锁定 `uv.lock`）：

```bash
# 方式一：使用 uv (推荐)
uv sync

# 方式二：使用 pip
pip install -r requirements_locked.txt
```

> 注意：项目已包含 `.python-version` 文件，请确保本地 Python 版本一致。

### 2. 启动项目

```bash
# 方式一：直接运行批处理脚本（会打开浏览器窗口）
start_streamlit.bat

# 方式二：后台静默启动（不显示命令行窗口）
run_hidden.vbs
```

启动成功后，默认访问地址：`http://localhost:8501`

---

## 开发规范 (Development Guidelines)

### 1. 严格遵循 DDD 分层
- **`app/` 禁止出现业务规则**：页面和组件只负责调用 `src/*/application/` 中的应用服务，并渲染返回结果。
- **领域层内禁止反向依赖**：`core/` 不依赖 `infrastructure/`；`application/` 通过接口或 DTO 与展示层交互。
- **共享内核保持轻量**：`src/shared_kernel/` 只存放真正跨领域复用的代码，避免成为“大杂烩”。

### 2. 测试驱动开发 (TDD)
- **新增功能必须先写测试**：所有测试代码统一放在 `tests/` 目录下。
  - `tests/unit/` — 针对 `core/` 中算法、规则类的单元测试。
  - `tests/integration/` — 针对数据流、应用服务、数据库查询的集成测试。
- 提交代码前请确保本地测试全部通过：
  ```bash
  pytest
  ```

### 3. 代码提交建议
- 保持单次提交聚焦于一个领域变更（SPC 或 Yield），避免跨层大改。
- 配置文件（`config/products/*.yaml`）修改需同步验证对应机型的集成测试。

---

*本项目为 vivo 内部品质数据分析平台，仅供授权人员使用。*

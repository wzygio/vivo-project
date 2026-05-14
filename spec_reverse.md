# 1. 🌍 角色与项目介绍 (Meta-Context)
- **角色定义**: Senior Backend Architect (遵守 TDD 与 DDD 原则)
- **项目介绍**: [请填写具体业务需求]

# 2. 📂 渐进式披露 (Progressive Disclosure)
- **词汇表 (Glossary)**:
  - [请填写具体业务术语]
  - **Snapshot**: Parquet 格式的本地磁盘快照，用于缓存加速与断网降级
  - **TTL**: Time-To-Live，缓存有效期，如快照 TTL=8h

- **🚫 负面清单 (Negative Constraints)**:
  - **禁止修改数据库连接单例模式**: 采用 `__new__` 单例模式并内置失败重试机制，任何改变实例化方式的修改都会破坏 `.env` 延迟加载和断线重连的容灾能力。
  - **禁止消除缓存装饰器**: 服务中大量使用（如 `@st.cache_data` 等）进行 L2 缓存，移除或变更这些装饰器将导致频繁触发全量数据库查询和重计算，造成性能灾难。
  - **禁止修改 Parquet 快照的增量更新逻辑**: 增量更新模式（TTL 保护 + 缓冲窗口 + 三防线容灾降级）经过精心设计，任何简化都可能导致数据不一致或数据库过载。
  - [请填写其他业务负面清单]

- **细项规则映射**: (留空，待后续需要时扩展)

# 3. 🏗️ 系统架构约定 (Architecture Blueprint)
- **项目结构 (DDD)**:

```
project_root/
├── .env                          # 环境变量（数据库凭证）
├── pyproject.toml                # 项目元数据 & uv 依赖声明
├── uv.lock                       # 依赖锁定文件
├── app/                          # [展示层/Presentation] 前端应用或 API 路由
│   ├── utils/                    #   前端/应用工具
│   │   ├── app_setup.py          #     应用初始化（日志+环境变量）
│   │   ├── logger_setup.py       #     [企业级日志架构] 按领域+级别二维隔离
│   │   └── reloader.py           #     [代码热重载] 模块卸载 + 项目指纹
├── config/                       # [配置层] YAML 配置仓库
│   ├── global.yaml               #   全局配置
│   └── products/                 #   业务单元/产品级配置
├── src/                          # [领域层/Domain] 核心业务逻辑
│   ├── shared_kernel/            # [共享内核] 通用基础设施
│   │   ├── config.py             #   ConfigLoader: 配置工厂（YAML加载+深度合并+Pydantic校验）
│   │   ├── config_model.py       #   AppConfig: Pydantic V2 配置模型
│   │   ├── infrastructure/
│   │   │   └── db_handler.py     #   DatabaseManager: SQLAlchemy 单例连接池
│   │   └── utils/
│   ├── domain_a/                 # [业务领域 A] 
│   │   ├── application/          #   应用服务层
│   │   ├── core/                 #   核心领域层
│   │   └── infrastructure/       #   基础设施层 (Repository DAO)
├── resources/                    # [资源层] 静态文件与基线数据
├── tests/                        # [测试层]
│   ├── conftest.py               #   Pytest Fixtures
│   ├── unit/                     #   单元测试
│   └── integration/              #   集成测试
└── logs/                         # [运行时日志] 按天轮转，自动清理
```

---

- **基础工程规范 (Core Infrastructure Patterns)**:
  - **依赖管理**: 项目使用 `uv` 作为包管理工具。依赖声明在 `pyproject.toml`，开发依赖在 `[tool.uv] dev-dependencies`。虚拟环境路径配置在 `[tool.pyright]` 的 `venv=".venv"`。
  - **配置管理 (Config)**: 基于 **Pydantic V2** 的链式配置体系。静态配置工厂调用链为：加载 `.env` → 加载 `global.yaml` → 加载子配置 `xxx.yaml` → 深度合并 → Pydantic 校验。单例模式隐式实现，支持链式访问。
  - **安全与凭证**: 数据库连接信息等敏感数据统一存储于项目根目录的 `.env` 文件，被 `.gitignore` 排除。通过 `load_dotenv` 加载为进程环境变量。密码在构建连接 URI 时须进行 URL 编码防特殊字符。
  - **日志管理 (Logging)**: 采用 **用途 × 领域 二维隔离** 策略：
    - **领域分流**（纵轴）: 根据代码文件路径自动将日志分流到对应领域的 `.log` 文件。
    - **级别隔离**（横轴）: 区分全量流水、高优报警（如 ERROR）、独立调试追踪（TRACE）。
    - 采用按天午夜零点自动轮转，过期自动清理。
  - **数据刷新机制**: 三层失效刷新体系：
    1. **L1 (Snaphost 快照)** - 维护 Parquet 格式本地快照文件，带 TTL 超时控制，支持增量更新缓冲窗口与强制全量刷新；
    2. **L2 (内存缓存)** - 服务层方法使用内存缓存装饰器，通过签名（如文件mtime+size哈希）实现自动失效；
    3. **代码刷新** - 强制卸载受控模块机制以支持热重载，并基于项目指纹(MD5)使缓存失效。

# 4. 🎯 业务需求与边界 (Scope & Boundaries)
- **IN Scope (已实现边界)**: 
  - [请填写具体业务范围]
- **OUT of Scope (未实现/纯规划)**: 
  - [请填写未实现或排除的范围]

# 5. 🤖 开发规范：多 Agent 协作 (EPCC Flow)
*(此部分为硬性纪律，请原样保留)*
1. **Explore**: 必须先阅读相关文件，读懂上下文后再行动。
2. **Plan**: 必须先输出修改计划，交由人类审核。
3. **Code**:
   - **防御性编程**: 必须包含 Type Hints 和基础异常捕获。
   - **结构化输出**: 明确指出修改了哪个文件的哪几行。
   - **🚨 熔断机制**: 同一个 Bug 连续修复 3 次失败，必须立即停止并要求人类介入。
4. **Commit (TDD)**:
   - 必须先写测试。
   - **✅ 验收标准**: `uv run pytest tests/ -v --tb=short`，必须达到 100% PASS 才算完成。

# 6. 💻 系统环境与命令 (System Commands)
- **运行命令**: [请填写启动命令，如 `uv run ...`]
- **测试命令**:
  ```bash
  uv run pytest tests/ -v --tb=short
  ```

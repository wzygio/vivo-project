# 测试程序编写与 TDD 流程标准

> 适用项目：`vivo-project`
> 适用范围：新增功能、缺陷修复、重构、测试维护与测试退役
> 核心方法：行为驱动 TDD；一次完成一个 RED→GREEN→REFACTOR 垂直切片

规范级别：

- **必须**：合并门禁；偏离需要书面豁免、原因和负责人。
- **应该**：默认规则；偏离需要在交付证据中说明。
- **可以**：由风险、成本和任务范围决定。

本文件是 Agent 执行标准，不承担 pytest 语法教学。pytest 概念与运行机制说明见 `D:/wzy/Visionox-Docs_Backup/dev-docs/dev-project_dev/Pytest实战教程：收集、测试编写与本项目落地.md`。

## 模块一：pytest 程序编写

### 1. 测试目标

测试必须证明调用方或用户可观察的行为，不证明内部实现步骤。

必须：

- 从公开函数、服务、CLI、页面或 adapter 进入。
- 测试名描述条件、动作和可观察结果。
- 内部重命名、拆函数或替换实现但行为不变时，测试继续通过。
- 失败信息能定位被破坏的业务规则。
- 独立运行、重复运行、改变顺序时结果一致。

禁止：

- 只为覆盖率直接测试私有函数。
- 断言内部调用次数、临时数据结构或实现顺序，除非它们是公开契约。
- 为当前实现反向拼凑无业务含义的断言。
- 已能通过公开接口证明结果时，再绕过接口读取 DB 内部状态。
- 用 mock 复制自有实现，再断言 mock 行为。

### 2. 目录结构与所有权

目录规则：

```text
tests/
  <layer>/
    <domain>/
      test_<capability>.py
```

- 一级目录必须表示测试层级。
- 二级目录必须表示业务或系统 domain。
- 三级及以下目录可以按 capability、adapter 或页面边界继续拆分。
- 同一 domain 在不同层级重复出现是正常的；层级回答“验证深度”，domain 回答“行为所有权”。

本项目推荐形状：

```text
tests/
  unit/
    yield_domain/
    inline_domain/
    equipment_domain/
    shared_kernel/
    app/
  contract/
    <domain>/
  integration/
    <domain>/
  e2e/
    <domain>/
```

测试层级：

| 一级目录 | 目的 | 允许依赖 |
| --- | --- | --- |
| `unit` | 领域规则、纯计算、单模块公开行为 | 无真实网络、DB、浏览器 |
| `contract` | 跨层接口、数据形状、ADR 契约 | fake、固定样本、受控 adapter |
| `integration` | 多组件协作 | 本地受控资源；真实外部系统需 marker |
| `e2e` | 少量关键用户路径 | 运行中的应用、浏览器、受控环境 |

`external`、`slow`、`serial` 是运行条件，不是测试层级；使用已注册 marker 表达，不得占用二级 domain 目录。

人工诊断、数据探针和 Streamlit 检查页不属于自动化测试，必须放入 `references/test_references/tools/diagnostics/`，不得使用 `test_*.py` 命名。

历史目录不符合新结构时：

- 新测试遵守本标准。
- 只在任务需要或有独立迁移计划时移动旧测试。
- 移动文件后同步更新 smoke、CI、文档和固定 nodeid。

### 3. 命名与路由规范

发现命名：

- 文件：`test_<capability>.py`。
- 函数：`test_<condition_or_action>_<observable_result>`。
- 测试类：仅用于表达共享行为语境；类名必须是有意义的 `Test...`。
- 参数化用例：复杂或业务关键场景必须有稳定、可读的 case ID。

领域词汇必须与 `CONTEXT.md`、`references/design_references/domain/GLOSSARY.md`、公开接口和 ADR 一致。

标准 nodeid 形状：

```text
tests/<layer>/<domain>/test_<capability>.py::test_<behavior>
tests/<layer>/<domain>/test_<capability>.py::Test<Context>::test_<behavior>[<case-id>]
```

路由选择规则：

| 目的 | 首选路由 |
| --- | --- |
| RED/GREEN 精确反馈 | 完整 nodeid |
| 单 capability 回归 | 测试文件 |
| 单 domain、单层级回归 | `tests/<layer>/<domain>/` |
| 运行条件筛选 | 已注册 marker 表达式 |
| 临时探索 | `-k` 名称表达式 |
| 发布或共享边界验证 | 项目 smoke 或广泛套件 |

必须：

- 新测试先用 `--collect-only` 确认 nodeid，再记录和使用该 nodeid。
- nodeid 必须通过真实收集结果取得，不得根据函数名猜测参数化后缀。
- 稳定自动化路由优先使用路径、nodeid 或已注册 marker。
- `-k` 只用于本地探索，不作为长期 CI 契约。
- 文件移动、函数重命名、参数 ID 变化都视为路由变更；同步更新引用方。
- 不根据 Git diff 自动推断唯一测试范围；共享依赖可能产生跨 domain 回归。

路由只决定“运行哪些测试”，不表达测试间业务依赖。测试不得依赖固定执行顺序。

### 4. pytest 各运行步骤的编写约束

| pytest 步骤 | Agent 编写时必须注意 |
| --- | --- |
| 定位项目和配置 | 从仓库根运行项目标准命令；以 `pyproject.toml` 为配置源；不得在测试中修改 `sys.path` |
| 发现测试文件 | 遵守层级/domain 目录和文件命名；诊断脚本移出 `tests/`；显式控制测试入口 |
| 导入并收集测试项 | 模块顶层无副作用；名称和参数 ID 形成稳定 nodeid；`--collect-only` 必须成功 |
| 执行测试项 | 每项独立；fixture 失败与断言失败分开诊断；不得依赖执行顺序或前一测试状态 |

本项目由 `pyproject.toml` 的 `pythonpath = ["src", "."]` 统一处理导入路径。测试文件不得自行追加路径。

测试模块顶层只允许：

- import；
- 常量；
- fixture、factory 和测试声明；
- 轻量、确定的参数数据。

测试模块顶层禁止：

- `sys.exit()`；
- 连接 DB 或网络；
- 启动 Streamlit、浏览器、子进程或服务；
- 写文件、生成报表或修改环境；
- 加载大型真实数据；
- 临时修改 `sys.path`。

`0 tests collected`、collection error、`SystemExit` 或 pytest internal error 均为门禁失败。

### 5. 单条测试结构

- 使用 Arrange–Act–Assert；用空行分隔，不写无信息量阶段注释。
- 一条测试表达一个行为规则，不机械限制为一个 `assert`。
- 同一行为的状态、值和事件可以一起断言。
- 多个互不相关结果必须拆成多条测试。
- 断言最终结果、公开状态转换、输出事件或稳定错误。
- 异常测试必须验证具体异常类型；错误文本只有在属于稳定契约时才验证。
- 测试设置明显长于行为与断言时，先检查接口是否过深、fixture 是否过宽。

### 6. 测试数据、fixture 与资源边界

- 一次使用的小型数据保留在测试内。
- 多测试复用的具名前提或需要清理的资源使用 fixture。
- 多变体数据生成优先使用 factory；fixture 只提供常用默认场景。
- 可变数据默认使用 `function` scope。
- `module` 或 `session` scope 只用于昂贵且共享安全的资源；交给测试前提供只读对象或隔离副本。
- 需要清理的资源使用 `yield` fixture。
- 文件副作用写入 `tmp_path`；不得污染 `resources/`、`data/`、`logs/` 或 `output/`。
- `autouse` 只用于该 scope 内所有测试都必须执行的可控边界动作。
- 单文件 fixture 放在测试文件；domain 内共享 fixture 放在对应目录的 `conftest.py`；全套件 fixture 放在 `tests/conftest.py`。
- 不得手工导入 `conftest.py`。
- fixture 不得删除不受控生产数据或读取真实凭据。

### 7. 参数化与边界覆盖

同一行为的多组输入使用参数化。每个业务关键参数组合必须有稳定 case ID，以便生成可读、可精确执行的 nodeid。

至少按风险考虑：

- 正常代表值；
- 空输入、零值、缺失值；
- 边界值及边界两侧；
- 非法输入和公开错误；
- 已知缺陷的最小回归样例。

禁止为覆盖率穷举无意义组合。可变参数不得在用例间泄漏状态。

### 8. marker、skip 与 xfail

静态测试层级和 domain 由目录表达。marker 表达跨目录运行条件。

建议 marker：

| marker | 语义 |
| --- | --- |
| `external` | 需要真实 DB、网络或获批外部系统 |
| `slow` | 有效但耗时明显较长 |
| `serial` | 不能并发执行 |

约束：

- `unit`、`contract`、`integration`、`e2e` 等层级默认只由目录表达；只有 CI 明确使用 `-m` 选择时才注册同名 marker，并保证两者一致。
- 自定义 marker 使用前必须在 pytest 配置中注册。
- 当前项目未注册自定义 marker 时，Agent 不得直接添加；先提交配置变更或使用现有路径/smoke 路由。
- marker 只提供元数据；CI、命令或插件必须实际执行选择和调度约束。
- `skip`/`skipif` 只表示明确、可复现的运行前提不满足，必须写原因。
- `xfail` 只表示已知且暂时接受的失败，必须关联 Issue、owner、到期或清理条件。
- 不得用无条件 skip、宽泛 xfail 或异常吞噬让 CI 变绿。
- 外部测试不得泄露连接信息、凭据或生产数据。

### 9. 确定性、隔离与 mock

- 时间：注入 clock 或固定时间。
- 随机数：注入随机源或固定种子，并断言业务属性。
- 环境变量：隔离修改，不依赖开发机环境。
- 文件：使用临时目录或仓库内脱敏固定样本。
- 配置：每条测试使用独立副本。
- DB：unit 使用 fake/adapter；真实 DB 进入带明确运行条件的 integration。
- 顺序：测试不得依赖另一测试先运行。

mock 只用于系统边界：外部 API、时间、随机源、文件系统、消息系统、DB adapter。

默认不得 mock 自有领域对象或内部模块。若必须 patch 内部位置，交付证据必须说明为什么不能通过依赖注入、公开接口或小型 fake 完成。

优先级：

```text
真实值对象和领域代码
→ 小型 fake
→ 边界 mock/monkeypatch
→ 受控真实外部集成
```

### 10. 测试文件生命周期管理

pytest 只决定测试文件在本次运行中是否被发现、收集和执行；不会自动保存、归档或删除测试源码。测试文件是版本化资产，由 Git、PR 和产品契约治理。

| 状态 | 管理规则 |
| --- | --- |
| 新建 | 与功能或缺陷修复一起提交；证明能被收集且 RED 原因正确 |
| 活跃 | 只要对应行为仍受支持，就持续保留用于回归 |
| 条件运行 | 行为有效但依赖特定环境；使用已注册 marker 或条件 skip |
| 已知失败隔离 | 临时 xfail；必须有 Issue、owner 和清理期限 |
| 替换 | 先建立并验证新契约测试，再删除旧测试 |
| 退役 | 仅在能力下线、契约消失或更强稳定覆盖替代时删除；PR 记录依据 |
| 人工诊断 | 移出 pytest 收集路径；不得伪装成回归测试 |

禁止：

- 因测试长期通过而删除。
- 因当前失败而直接修改期望值或删除。
- 用“长期未修改”“执行较慢”或“偶发失败”作为自动删除依据。
- 静默删除 flaky 测试。

flaky 测试必须诊断、修复，或带治理信息临时隔离。缺陷回归测试原则上长期保留。

### 11. pytest 程序评审门禁

- [ ] 文件位于正确的 `tests/<layer>/<domain>/` 路由。
- [ ] 测试名和 case ID 形成稳定、可读 nodeid。
- [ ] 测试描述公开行为，通过公开接口进入。
- [ ] 数据确定、隔离、无顺序依赖。
- [ ] fixture scope 最小，资源清理边界明确。
- [ ] mock 只位于系统边界。
- [ ] 无模块顶层副作用或本地 import-path hack。
- [ ] marker 已注册，skip/xfail 有治理信息。
- [ ] 人工诊断未进入 pytest 收集路径。
- [ ] 新增、替换、隔离或退役的测试资产有交付证据。

## 模块二：TDD 流程规范

### 1. 开始前：确定行为队列

实现前必须：

1. 阅读公开接口、调用方、现有测试、配置、领域 glossary 和适用 ADR。
2. 从任务、spec 或用户目标中提取可观察行为，不写实现步骤。
3. 按关键路径、风险和边界对行为排序。
4. 确定预期接口和所属 layer/domain。
5. 识别需要的系统边界、测试数据和最小验证路线。

若关键契约不明确且不同选择会改变公开行为，停止实现并请求决策。非关键细节按现有项目约定推进。

### 2. 每次只完成一个垂直切片

```text
RED
  → 新增一条行为测试
  → 确认文件被收集并取得真实 nodeid
  → 单独运行该 nodeid
  → 确认失败来自目标行为缺失

GREEN
  → 编写满足当前测试的最小实现
  → 单独运行同一 nodeid
  → 确认通过

REFACTOR
  → 仅在 GREEN 状态消除重复、加深模块、收紧接口
  → 每个小步后重跑当前 nodeid 和相关文件
```

RED 有效条件：

- 不是 collection error。
- 不是 fixture/setup 环境故障。
- 不是 import-path 或依赖安装问题。
- 不是测试断言无效或走错代码路径。
- 失败信息指向预期缺失行为。

新测试首次运行即通过时，必须判断：

- 行为已经存在；
- 断言无效；
- 测试未被收集；
- 测试走错接口或数据路径；
- 该测试没有增加有效保护。

### 3. 禁止水平切片和推测性实现

禁止：

```text
先批量写完 test1、test2、test3
再批量实现 behavior1、behavior2、behavior3
```

必须：

```text
test1 RED → behavior1 GREEN
→ 根据证据决定 test2
→ test2 RED → behavior2 GREEN
```

每个周期只为当前测试写足够实现。不提前添加未来行为，不在 RED 状态重构，不顺带修改无关业务。

### 4. 验证路线：由精确到广泛

每个周期按影响范围扩展：

```text
精确 nodeid
→ 当前测试文件
→ 当前 layer/domain
→ 对应 domain smoke
→ 完整 unit
→ 任务要求的 integration/e2e 或全部测试
```

本项目标准命令：

```powershell
# 收集并取得 nodeid
uv run pytest tests/<layer>/<domain>/test_<capability>.py --collect-only -q

# RED/GREEN
uv run pytest <nodeid> -q --tb=short

# capability 与 layer/domain
uv run pytest tests/<layer>/<domain>/test_<capability>.py -q --tb=short
uv run pytest tests/<layer>/<domain>/ -q --tb=short

# domain smoke
uv run --no-sync python tools/smoke.py <spc|yield|equipment>

# 完整 unit
uv run --no-sync python tools/smoke.py all
uv run pytest tests/unit -q --tb=short
```

共享接口、全局配置、持久化、并发、UI 或跨 domain 行为变化时必须扩大验证范围。真实 external/e2e 仅在对应受控环境执行。

### 5. 基线故障与测试失败处理

- 先区分产品回归、测试过时、测试数据错误、环境故障和已知基线失败。
- 不得为了全绿修改与当前契约无关的业务行为。
- 已知基线失败必须与本次新增失败分开记录。
- 收集失败先修复测试入口，再判断业务 RED/GREEN。
- flaky 不是可接受 GREEN；重复运行或顺序变化必须稳定。

### 6. 每周期证据

必须记录：

- RED 命令、真实 nodeid、核心失败原因。
- RED 为何对应目标行为，而不是环境或收集故障。
- GREEN 命令和结果。
- 扩大验证的命令、退出状态和结果。
- 未执行检查及原因。
- 已知基线失败与本次新增回归的区分。

### 7. 完成条件

- [ ] 所有接受行为均由公开接口测试覆盖。
- [ ] 每个行为按 RED→GREEN 垂直推进。
- [ ] REFACTOR 全程保持 GREEN。
- [ ] 精确、domain 和所需广泛验证均已完成。
- [ ] 测试 nodeid、marker 和 smoke 路由已同步。
- [ ] 测试文件新增、替换、隔离或退役已按生命周期规则处理。
- [ ] 验证证据足以让下一位 Agent 重现结果。

## 参考

- pytest 教程：`D:/wzy/Visionox-Docs_Backup/dev-docs/dev-project_dev/Pytest实战教程：收集、测试编写与本项目落地.md`
- 项目验证规则：`references/test_references/methods/validation.md`
- 项目命令：`references/test_references/tools/project_commands.md`
- 项目可观测性：`references/test_references/observability.md`

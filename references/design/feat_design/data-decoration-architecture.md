# 数据修饰业务逻辑与架构范式

## 1. 目的与适用范围

本文定义报表系统中“数据修饰”的统一语义和分层方式，适用于 Q-Time、Inline
SPC/CTQ/AOI、Yield 以及后续新增的人工决策型修饰。目标是同时保证：原始事实可追溯、
业务规则可单测、人工决策可持久化、展示结果可复现。

数据修饰是根据规格、配置或人工决策生成报表投影的业务过程。Excel、Parquet、数据库、
COM 和文件路径只是存储机制，不属于修饰规则。

## 2. 统一业务模型

一条完整链路包含四类对象：

| 对象 | 含义 | 示例 |
|---|---|---|
| 源事实 | 数据库或原始快照中的可追溯事实 | Q-Time `wait_time`、Inline 点位值 |
| 候选明细 | Core 根据规格识别出的待决策记录 | `wait_time > q_spec`、Sheet OOS |
| 决策台账 | 用户维护的稳定业务键与动作 | `True`、`False`、`Delete` |
| 修饰投影 | Core 将决策应用于源事实后生成的报表数据 | 截回规格内、保留、剔除 |

三态动作的统一含义为：

- `True`：执行领域定义的确定性修饰；
- `False`：保留真实值，并允许其进入对应预警口径；
- `Delete`：仅从当前报表投影中排除，不删除数据库事实或原始快照；
- 无决策：由具体领域定义默认值。Q-Time 与 Inline OOS 当前默认为 `True`，
  CPK/CPM 人工覆盖当前默认为 `False`。

确定性修饰必须满足相同业务键与输入得到相同结果，不使用进程随机数。

## 3. 分层职责

```text
Presentation
    │ 上传/下载、筛选、渲染
    ▼
Application
    │ 加载源数据与决策 → 调用 Core → 保存台账/审计明细 → 组装 DTO
    ├──────────────────────────────┐
    ▼                              ▼
Core                         Infrastructure
候选识别、键归一化、          Excel/COM、数据库、Parquet、
决策合并、三态规则、           文件锁、原子写、错误转换
确定性数值变换
```

### Core

- 只接收 DataFrame、值对象或领域 DTO，返回新对象，不原地污染调用方输入；
- 拥有超规判定、业务键、动作解析、默认决策、截断/倍率/删除等业务规则；
- 不导入 Infrastructure，不读取或写入 Excel/Parquet/数据库/sidecar；
- 不知道文件路径、sheet 枚举、COM、锁或原子替换。

### Application

- 编排一次用例的顺序，协调源数据端口与决策仓储；
- 将 Infrastructure 读出的决策交给 Core，再保存生成明细或用户台账；
- 负责上传校验、下载工作簿 payload、缓存键和 ViewModel/结果 DTO；
- 不复制领域算法，不直接实现数据库查询或文件格式降级。

### Infrastructure

- 只负责外部表示与领域 DataFrame 之间的转换以及安全持久化；
- 处理 Excel/COM、缺 sheet、加密文件、锁、原子写和 sidecar；
- 可以调用 Core 的规范化/校验规则，但不得自行决定何时截断、删除或报警；
- 错误必须保留因果链，并向 Application 暴露稳定、无敏感信息的异常。

## 4. 快照与时序

报表修饰必须发生在源快照生成和读取之后：

```text
数据库事实 → 原始快照（源时间） → 仓储输出 → Application → Core 修饰 → 报表/预警
```

修饰结果不得回写原始 Parquet；缓存修饰结果时，键必须包含决策签名以及影响结果的
业务策略。日期前推同样只发生在仓储输出边界，不改写源快照。

“源校正”是单独概念：当规则用于纠正采集系统的已确认系统性偏差，并明确把校正值定义为
领域的规范事实时，可以在快照写入前执行。此类规则仍须是 Core 纯函数，由组合根注入
Infrastructure；必须有独立策略版本和回归测试，不能借“源校正”绕过报表修饰边界。
当前 Inline PPA 数值修正属于这一显式例外。

## 5. 当前模块映射

| 领域 | Core 规则 | Application 编排 | Infrastructure 适配 |
|---|---|---|---|
| Q-Time | `core/qtime/decoration.py` | `application/qtime/service.py`、`decoration_service.py` | `infrastructure/qtime/decoration_repository.py` |
| Inline Sheet OOS | `core/shared/sheet_oos_decoration.py` | `application/shared/sheet_oos_decoration_service.py` | `infrastructure/shared/sheet_oos_decoration_repository.py` |
| AOI-TT/RS | `core/aoi_tt`、`core/aoi_rs` 纯规则 | 各自 `application/*/decoration_service.py` | 复用 Inline 共享工作簿适配器 |
| CPK/CPM | `core/spc/cpk_decoration.py` | `application/spc/capability_decoration_service.py` | `infrastructure/spc/capability_decoration_repository.py` |
| Yield MWD | `core/mwd_trend/modifier_table.py` | `application/modifier_table_service.py` | `infrastructure/modifier_table_repository.py` |
| Yield Sheet/Lot | `core/sheet_lot/overrides.py` | `YieldAnalysisService` | `infrastructure/rate_override_repository.py` |
| Inline 源校正 | `core/shared/measurement_correction.py` | 组合根注入 | 测量快照仓储调用注入的纯函数 |

## 6. 新增或评审检查表

- 业务动作、默认值和匹配键是否只定义在 Core？
- Core 是否完全没有 Excel、COM、数据库、Parquet、文件路径和 Infrastructure 依赖？
- Application 是否清楚表达“加载决策—调用规则—持久化—组装结果”的顺序？
- 原始快照是否保持事实与源时间，修饰是否只影响派生投影？
- `False`、`Delete`、缺失和非法 flag 是否有明确测试？
- 人工决策是否与系统生成明细分离，写失败是否显式可见？
- 缓存键是否包含决策签名、产品 revision 和相关策略？
- 是否同时具备 Core 单元测试、Infrastructure 适配器测试和至少一条编排集成测试？

仓库通过 `tests/architecture/test_decoration_boundaries.py` 持续阻止 Core 重新引入
Excel/文件持久化或反向依赖 Infrastructure。

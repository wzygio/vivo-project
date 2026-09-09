# Architecture

## 用途与定位顺序

本项目是采用标准 `src` 布局的 Streamlit 制造质量报表系统。本文件记录**领域归属、分层职责、子模块路径和依赖约束**，供人和 AI 定位代码；不维护逐文件或逐函数说明。

定位顺序：**业务问题 → domain → DDD 分层 → 子模块 → 文件/符号 → 调用方与测试**。算法、SQL 字段、产品特例和操作步骤按需查阅 [项目知识路由](references/index.md) 与 `docs/ADR/`。新增文件通常不需要更新本文；新增领域、子模块、职责迁移或依赖规则变化时才更新。

## 三级结构规范

```text
src/<domain>/<layer>/<submodule>/
     领域      DDD 分层   子模块
```

- **领域**按业务责任划分，不按页面名称或存储类型划分。
- **分层**使用 `application`（用例）、`core`（领域规则）、`infrastructure`（外部适配）。`core` 就是 DDD 领域层；展示层统一在 `app/`。
- **子模块**按稳定业务能力或层内共同职责划分。同一业务跨层优先同名，如 `qtime`；层内通用能力可使用 `shared`，技术适配可按实际职责命名。
- **小模块允许扁平化**：尚未拆子包时，文件直接位于分层根目录；下文“层根”只是这一现状的标记，不是真实目录名。不为形式对称创建空目录或机械搬移文件。
- 领域根部的 `composition.py` 是跨层装配入口，不是第四个业务层。`shared_kernel` 是跨域公共能力区，也不强套业务领域三层结构。

### 当前目录骨架

仅列领域、分层和子模块，省略文件、缓存目录及更深的内部拆分。`(层根)` 表示该层还有直接承载职责的模块文件。

```text
src/
├─ yield_domain/
│  ├─ application/       (层根)
│  ├─ core/              (层根)
│  │  ├─ mapping/
│  │  ├─ mwd_trend/
│  │  └─ sheet_lot/
│  └─ infrastructure/    (层根)
│     └─ repositories/
├─ inline_domain/
│  ├─ application/
│  │  ├─ spc/
│  │  ├─ ctq/
│  │  ├─ aoi_tt/
│  │  ├─ aoi_rs/
│  │  ├─ monitor/
│  │  ├─ shared/
│  │  └─ ports/
│  ├─ core/
│  │  ├─ spc/
│  │  ├─ ctq/
│  │  ├─ aoi_tt/
│  │  ├─ aoi_rs/
│  │  ├─ monitor/
│  │  └─ shared/
│  └─ infrastructure/
│     ├─ spc/
│     ├─ ctq/
│     ├─ aoi_tt/
│     ├─ aoi_rs/
│     ├─ monitor/
│     └─ shared/
├─ indicator_domain/
│  ├─ application/
│  │  ├─ qtime/
│  │  └─ ijp/
│  ├─ core/
│  │  ├─ qtime/
│  │  └─ ijp/
│  └─ infrastructure/
│     ├─ qtime/
│     └─ ijp/
├─ equipment_domain/
│  ├─ application/       (层根)
│  ├─ core/              (层根)
│  └─ infrastructure/    (层根)
├─ iqc_domain/
│  ├─ application/       (层根)
│  └─ infrastructure/    (层根；当前为示例数据适配)
└─ shared_kernel/        (根部公共契约与配置)
   ├─ infrastructure/
   └─ utils/
```

### 按领域、分层、子模块定位职责

子模块路径相对于对应行的 `src/<domain>/<layer>/`。这张表说明“到哪里查”，具体实现继续在目标目录内检索。

| Domain / 业务关键词 | DDD 分层 | 子模块 → 职责 |
|---|---|---|
| `yield_domain`：入库不良率、Code/Group、Lot/Sheet、Mapping | `application` | 层根 → 报表与告警用例、数据读取端口、修饰表管理和 Office 导出编排 |
| 同上 | `core` | `mapping/` → 坐标与缺陷分布；`mwd_trend/` → 月周日聚合与人工覆盖；`sheet_lot/` → Lot/Sheet 分配及封顶；层根 → 共用缺陷处理、批次统计与异常判断 |
| 同上 | `infrastructure` | `repositories/` → Panel 仓储与快照；层根 → 数据加载、修饰表持久化及应用端口适配 |
| `inline_domain`：SPC、CTQ、AOI、Inline 预警 | `application` | `spc/`、`ctq/`、`aoi_tt/`、`aoi_rs/` → 各报表用例与消费方端口；`monitor/` → 预警、历史与汇总用例；`shared/` → 共用修饰、特征、决策签名及过货编排；`ports/` → 共用测量快照契约 |
| 同上 | `core` | `spc/` → 能力计算与修饰；`ctq/` → 指标展示类型规则；`aoi_tt/`、`aoi_rs/` → 各自统计与修饰规则；`monitor/` → 周期汇总与替换规则；`shared/` → OOS/OOC、测量校正、过货事实等复用规则 |
| 同上 | `infrastructure` | `spc/`、`ctq/`、`aoi_tt/` → 业务数据投影与专属读取/持久化；`aoi_rs/` → 独立 RS 事实与快照；`monitor/` → 预警输入、汇总工作簿和历史适配；`shared/` → 共享测量读取/制备/快照、主制程追溯、修饰与资源路径 |
| `indicator_domain`：Q-Time、IJP | `application` | `qtime/` → 过货监控、修饰与缓存用例；`ijp/` → 溢流监控查询、筛选与报表编排；各模块拥有自身 DTO、端口和错误契约 |
| 同上 | `core` | `qtime/` → 厂别、超规与修饰规则；`ijp/` → 溢流、周期与机台聚合规则 |
| 同上 | `infrastructure` | `qtime/` → 数据查询、厂别源快照和决策工作簿；`ijp/` → IJP 数据查询适配 |
| `equipment_domain`：关键备件、寿命、真实/仿造匹配 | `application` | 层根 → 备件报表、数据端口、刷新与缓存编排 |
| 同上 | `core` | 层根 → 备件身份、测量匹配、寿命与状态计算 |
| 同上 | `infrastructure` | 层根 → 规格基线、真实/仿造数据加载及快照维护 |
| `iqc_domain`：来料检验、寿命测试示例 | `application` | 层根 → 示例报表读取与筛选 |
| 同上 | `infrastructure` | 层根 → 示例资源适配；当前没有独立 `core/`，不代表已实现生产质量判定 |

`shared_kernel` 根部承载配置、源/显示时间、数据健康、缓存及路径公共契约；`infrastructure/` 承载共享数据库连接，`utils/` 承载 Excel/CSV 等工具。它不是业务逻辑的默认归属：仅在单个领域复用的代码优先留在该领域对应层的 `shared/`。

## 依赖方向与运行装配

下图表示代码依赖，不是数据逐层流过的流水线。**Core 不依赖 Infrastructure。**

```text
app/ ──调用──> application/ ──使用──> core/
                    │
                    └──定义/消费──> application 中的出站端口
                                           ↑ 实现
                                    infrastructure/

领域 composition ──装配──> 应用服务 + 基础设施适配器
```

- `application` 组织规则与出站调用，端口由消费方拥有；`infrastructure` 实现端口并管理 SQL、Excel、快照和外部资源。适配器可复用纯 Core 规则，不能要求 Core 导入适配器。
- 跨业务模块复用优先使用本领域同层 `shared/` 公共入口，禁止导入其他业务模块的私有实现；跨领域协作使用公开应用契约，不直连对方仓储。
- 端口可能位于子模块的 `ports.py`、专用协议文件、公共 `ports/` 或层根，按消费范围定位；不要仅凭命名假定所有端口都在一个目录。
- 既有静态服务通过受控默认 resolver 接入组合根，这是渐进迁移的兼容边界。存量例外以 [依赖守卫](tests/architecture/test_backend_dependencies.py) 为准，不作为新代码直接依赖基础设施的范例。守卫检查静态 import，不覆盖所有动态调用或隐式 I/O。
- 配置和数据库生命周期由既有配置入口、组合根与共享基础设施管理。不要在页面新增 SQL、工作簿持久化或独立数据库实例。

### 展示入口与其他路径

| 路径 | 所属责任与定位规则 |
|---|---|
| `app/Home.py`、`app/pages/` | 门户初始化与薄页面入口；先查调用了哪个 section，再进入业务路径 |
| `app/sections/<domain>/[<submodule>/]` | 查询门控、会话状态、页面区块与展示编排；小领域直接位于 domain 下 |
| `app/charts/<domain>/[<submodule>/]` | 图表适配；Inline 共用图表在 `app/charts/inline_domain/`，并非每个后端子模块都有同名图表目录 |
| `app/components/` | 跨页面组件、筛选及刷新协作；全指标矩阵目前在 `app/sections/inline_domain/monitor/`，按指标消费各领域结果 |
| `config/` | 全局、领域、产品及其他配置；沿配置加载或装配入口查实际来源，不猜测文件名 |
| `resources/` | 规格、人工维护台账、基线与静态输入；部分资源仍在根部，不假设全部按领域迁移 |
| `data/`、`output/` | 前者为运行快照；后者为可重建报告、日志、下载与测试产物。不得将人工维护数据当临时产物清理 |
| `tools/` | 刷新、诊断和离线分析；独立离线能力不因使用同类业务数据就必须归入报表领域 |
| `tests/` | 单元、架构、集成与浏览器证据；兼有镜像目录和扁平命名，需检索确认 |

## AI 按任务定位代码

1. **确定业务归属**：用职责表选择 domain/子模块。跨指标看板从展示聚合入口进入，再追到相关领域；不因页面放在 Inline 就把所有业务规则放进 Inline。
2. **确定修改责任**：计算与不变量查 `core`；业务步骤、健康状态传递、缓存契约查 `application`；SQL、文件读写、源快照查 `infrastructure`；依赖选择查组合根；交互与渲染查 `app`。
3. **确认路径并找入口**：存在 `.codegraph/` 时先按 `AGENTS.md` 使用 CodeGraph 查询符号/调用路径；没有索引时用 `rg --files` 列目标目录，按业务词与职责词筛选，再用 `rg -n` 查定义和引用。文件名只是线索，不能替代源码核验。
4. **沿一条用例追踪**：阅读入口、消费端口、组合根绑定和对应适配器；涉及算法再进入 Core。能指出输入/输出、依赖归属及消费方后，停止无关目录扫描。
5. **补充文档与测试证据**：遇到业务口径、快照/修饰或缓存边界时，查下节资料；从 `tests/` 找同业务词及目标符号。没有镜像目录不代表没有测试。路径不匹配时先核实实际代码，再修正过时路由，不推测新路径。

无 CodeGraph 索引时，可在仓库根目录执行：

```powershell
# Q-Time 查询/降级：先列子模块，再查健康状态的生产者和消费者
rg --files src/indicator_domain/application/qtime src/indicator_domain/infrastructure/qtime
rg -n 'data_health|QTimeDataPort' src/indicator_domain app/sections/indicator_domain/qtime

# Yield 月周日算法：限定 Core 子模块，不先遍历全部页面
rg --files src/yield_domain/core/mwd_trend

# Inline 共享测量：当前实际位于 infrastructure/shared，并核对装配
rg --files src/inline_domain/infrastructure/shared -g '*measurement*'
rg -n 'measurement|snapshot' src/inline_domain/composition.py

# 测试兼有目录与扁平命名，先列候选再读相关断言
rg --files tests -g '*qtime*' -g '*yield*' -g '*dependencies*'
rg -n 'QTimeReportService|data_health' tests/unit tests/integration tests/architecture
```

## 架构约束与按需资料

| 触发条件 | 必须保留的边界 / 进一步资料 |
|---|---|
| 缓存、热重载、端口注入 | 缓存只保存 DataFrame、标量和原生容器，项目结果类型在缓存外重建；显式端口不能串用默认共享缓存。见 [缓存边界 ADR](docs/ADR/0001-streamlit-cache-native-payload-boundary.md) 与 [健康/端口契约](references/design/feat_design/architecture-data-health-and-outbound-ports.md) |
| 数据失败、空结果、降级显示 | Yield/Q-Time 区分成功空结果、不可用、陈旧与未知；Yield 分片失败不发布部分新窗口；旧数据不得支撑当前正常结论，状态须穿过缓存、筛选与展示。见 [健康/端口契约](references/design/feat_design/architecture-data-health-and-outbound-ports.md)；不假定所有领域已采用同一健康契约 |
| 时间、源快照 | 数据库和原始快照保留源时间，查询窗口与显示时间在仓储边界转换；缓存签名包含时间策略。见 [源/显示时间 ADR](docs/ADR/0022-source-and-display-time-boundary.md)。窗口、增量和回退按各领域契约查阅，不统一猜测 |
| TTL、刷新 | 项目自有数据缓存与领域快照 TTL 读取 `config/global.yaml` 的 `application.cache_ttl_hours`；保留产品/指标定向失效，不用全局清缓存替代。见 [缓存语义 ADR](docs/ADR/0006-rerun-slimming-cache-semantics.md)、[矩阵缓存 ADR](docs/ADR/0022-alert-matrix-board-and-qtime-cache.md) |
| 修饰、共享测量、OOS/OOC、汇总工作簿 | 区分原始事实、人工决策、报表投影与历史维护值，保护其他产品/sheet。见 [修饰架构](references/design/feat_design/architecture-data-decoration.md)，再由 [知识路由](references/index.md) 进入 Inline 架构、数据流或规则资料 |
| Yield、备件、Q-Time、IJP 业务口径 | 从 [知识路由](references/index.md) 查领域规则；未命中则检索 `docs/ADR/` 的对应业务词。不得随带重写 Mapping、浓度、快照降级或数据库生命周期 |
| 验证范围 | [架构测试](tests/architecture/)保护依赖边界，单元与集成验证业务链路，浏览器验证展示；`tools/smoke.py` 不覆盖全部测试类别。见 [个人测试与发布检查](docs/dev_docs/generated/others/solo-developer-testing-and-release-explained.md)。浏览器产物仅写入 `output/test-results/` 或 `output/tmp/` |

## 扩展与维护规则

- 新增文件优先放入既有职责模块；形成独立、稳定的业务能力或复用边界后才新增子模块。目录层级不意味着需要微服务、事件总线或额外框架。
- 同一业务跨层同名有助于定位，但各层目录无需完全对称；技术职责目录应注明所属层，避免把 `repositories` 或 `ports` 误认成业务子域。
- 新增子模块时更新骨架和职责表；端口或跨域协作变化时更新依赖说明。逐程序说明、算法细节和资源格式留在代码附近或专题文档，经 `references/index.md` 路由。
- 修改后核对目录、链接、职责归属；代码边界变化时运行相关架构测试。本文只描述已存在的结构，待实施方案另放设计文档，禁止将规划目录写成现状。

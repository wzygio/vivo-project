# Architecture

## 项目定位

`vivo-project` 是面向显示制造质量分析的 Streamlit 报表系统，覆盖四类
业务能力：入库不良率（Yield）、SPC/CTQ 监控、自动预警和关键备件寿命管理。

系统采用标准 `src` 布局：`app/` 负责交互和展示，`src/` 承载领域逻辑，
`src/shared_kernel/` 提供跨领域配置、数据库和通用工具。

## 运行入口与调用流

```text
Streamlit
  ├─ app/Home.py                 门户、应用初始化、热重载
  └─ app/pages/*.py              各业务页面
       ├─ app/components/        页头、筛选、上传、告警与缓存失效
       ├─ app/sections/          页面区块组装
       └─ app/charts/            Plotly / ECharts 图表适配
            ↓
src/<domain>/application/        用例编排、缓存 payload、ViewModel
            ↓
src/<domain>/core/               领域计算与规则
            ↓
src/<domain>/infrastructure/     PostgreSQL、Parquet 快照、Excel 资源
            ↓
src/shared_kernel/               配置、数据库单例、输出与 Excel 工具
```

- `app/Home.py` 将项目根目录和 `src/` 加入模块搜索路径，初始化日志与
  Streamlit 门户资源；各页面使用 `SessionManager` 获取当前产品配置。
- `ConfigLoader` 深度合并 `config/global.yaml` 与
  `config/products/<product>.yaml`，并校验为单个 `AppConfig`。全局 Yield
  数据策略由全局配置拥有，产品配置只声明产品差异和资源路径。
- `DatabaseManager` 是共享 PostgreSQL 引擎单例。领域基础设施负责查询、
  快照、刷新和降级；页面不应自行实现数据读取或业务计算。
- 页面通过共享页头提供缓存刷新和热重载。产品页将产品级 revision 写入
  缓存签名，因此刷新一个产品不会清空其他产品的缓存。

## 领域边界

| 区域 | 职责 | 主要入口 |
|---|---|---|
| `yield_domain` | 入库不良率趋势、Code/Group MWD、Lot/Sheet 明细、缺陷 Mapping、告警和 Office 导出。 | `YieldAnalysisService`、`AlertService`、`PanelRepository` |
| `inline_domain` | SPC、CTQ 与自动预警的测量查询、能力计算、OOS 修饰和监控汇总。 | `SpcReportService`、`CtqReportService`、`MonitorAnalysisService`、`SpcRepository` |
| `equipment_domain` | 关键备件规格基线、寿命计算、状态预警，以及真实与仿造快照的匹配。 | `PartsReportService`、`PartsRepository` |
| `shared_kernel` | 配置模型与加载、数据库连接、输出目录、合规配置和 Excel/CSV 工具。 | `ConfigLoader`、`DatabaseManager` |

### Yield 数据流

1. `PanelRepository` 根据 `YieldQueryConfig` 查询 Panel 明细，或读取产品的
   Parquet 快照；数据库不可用时可退化到已有快照。
2. 工作单类型和 Defect Group 过滤在仓储边界按已验证的 `YieldDataPolicy`
   执行。快照保留原始 Defect Group，避免重复或不可逆过滤。
3. `YieldAnalysisService` 调用核心算法生成 MWD、Code 趋势、Lot/Sheet
   缺陷率与 Mapping 数据；`AlertService` 和展示层消费这些结果。
4. Code 级 MWD 以 `defect_multipliers` 后的 Panel 明细作为月度整数计数
   权威。EMA 决定日度形状，之后按月回补、再执行月/周/日人工覆盖，最终从
   日度数据重建周/月输出。`weekly_full` 保留完整三自然月窗口，`weekly`
   仅供近期展示。
5. Lot 模拟按 Code-week 的 `weekly_full` 速率分配整数缺陷 token；聚合后
   才取整，再按稳定加权噪声分配到 Lot，最后执行封顶与显式覆盖。
6. Mapping 与 MWD 独立。它先解析单一有效的产品/Code/batch 修改计划，再
   处理坐标；无匹配配置时采用确定性位置偏移。布局来自
   `processing.mapping_layout`，未配置产品使用默认 10 × 19 布局。

### SPC、CTQ 与自动预警

- `SpcRepository` 是 SPC/CTQ/监控共用的数据边界：读取规格与测量数据、
  应用产品规格覆盖和异常值过滤，并维护带策略标记的快照。
- `SpcReportService` 固定使用 `SPC` 数据类型，提供 CPM/CPK 能力结果和
  图表类型；CPK 人工修饰文件
  `resources/<product>/spc_cpk_decoration.xlsx` 是用户维护状态，只按周期键
  合并到新结果，刷新时不会重建既有文件。
- `CtqReportService` 固定使用 `CTQ` 数据类型，只返回 Sheet/点位分布和后端
  选定的图表类型；CTQ OOS 文件隔离在 `resources/<product>/ctq/`。
- `MonitorAnalysisService` 基于同一 SPC 数据源完成时间桶映射、规则判定和
  汇总，自动预警页面再叠加合规配置的可见性规则。

### 关键备件

- 规格基线来自 `resources/critical_parts_baseline.csv`；数据库快照和仿造
  快照分别以规格签名命名，互不覆盖。
- 报表按每条规格先匹配真实数据库快照；无真实匹配时才使用仿造快照。报表
  读取不会生成或更新仿造数据。
- `tools/fabricate_equipment_data.py` 负责首次生成，
  `tools/update_fabricated_equipment_data.py` 负责显式、受 24 小时新鲜度约束的
  后续更新。该语义见
  `docs/ADR/0003-equipment-real-first-fabricated-fallback.md`。

## 缓存、快照与可变资源

- 页面可缓存的应用服务只返回 DataFrame、标量或原生容器；缓存外再构造
  dataclass/Pydantic ViewModel，避免热重载期间的 pickle 类身份冲突。
- 数据库快照使用 Parquet；页面刷新显式触发快照刷新。数据库失败时，仅在
  已有快照可用时才降级读取。
- `resources/` 存放产品规格、人工覆盖、OOS/CPK 修饰及静态资源；`data/`
  保存领域快照；`output/` 是可重建的报告、日志、下载、测试和临时产物根目录。
- Excel 文件既可能是输入契约，也可能是用户维护状态。需要读取企业加密
  工作簿时，项目的 Excel COM 回退逻辑仍由本仓库维护；普通 Excel-to-CSV
  通过 `fr-common-utils[excel]` 提供。

缓存边界和产品级失效规则见
`docs/ADR/0001-streamlit-cache-native-payload-boundary.md`。

## 目录地图

| 路径 | 内容 |
|---|---|
| `app/` | Streamlit 门户、页面、UI 组件、页面区块、图表与应用辅助代码。 |
| `src/yield_domain/` | Yield 的 application/core/infrastructure 分层实现。 |
| `src/inline_domain/` | SPC、CTQ、自动预警的 application/core/infrastructure 分层实现。 |
| `src/equipment_domain/` | 关键备件的 application/core/infrastructure 分层实现。 |
| `src/shared_kernel/` | 共享配置、数据库、输出路径和工具。 |
| `config/` | 全局、产品、SPC、设备和合规配置。 |
| `resources/` | 受版本控制的产品资源、基线、人工修饰与前端静态文件。 |
| `data/` | 本地运行时数据与领域 Parquet 快照。 |
| `tools/` | 分域 smoke 测试和设备仿造数据运维命令。 |
| `tests/` | 单元、集成和浏览器端到端测试。 |
| `docs/ADR/` | 已接受的架构决策记录。 |
| `references/` | 项目自有领域知识与 Harness 演进记录。 |
| `specs/` | 用户维护的规格契约、运行追踪与模板。 |
| `output/` | 可重建的报告、下载、日志、截图、测试结果与临时文件。 |

## 约束与验证

- `app/` 负责交互、筛选、调用应用服务和渲染；新的业务规则应落在领域
  `core/` 或 `application/`，数据访问和快照语义应落在 `infrastructure/`。
- 不要在没有专门任务和回归证明的情况下重构 Yield 的浓度/Mapping 算法、
  `DatabaseManager` 生命周期或 Parquet 刷新与降级策略。
- 不要移除页面数据流中的 `st.cache_data`；若修改缓存边界，必须遵守 ADR-0001
  的原生 payload 规则，并覆盖模块热重载场景。
- 默认单元测试入口为 `pytest`。快速、显式的领域烟测通过：

  ```powershell
  uv run python tools/smoke.py spc
  uv run python tools/smoke.py yield
  uv run python tools/smoke.py equipment
  ```

- 性能敏感的 SPC 周期能力聚合和 smoke 范围约束见
  `docs/ADR/0002-performance-safe-smoke-and-cpm-aggregation.md`。

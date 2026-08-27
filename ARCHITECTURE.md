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
       ├─ app/sections/          页面区块组装层（对齐后端 application 层；
       │                         inline 各报表共享 inline_domain/shared/
       │                         级联筛选与修饰后台组装）
       └─ app/charts/            Plotly / ECharts 图表适配
            └─ app/charts/inline/   inline 四报表共享绘图：图表类型决策、
                                   规格线、月周天分布、Sheet 点位图、AOI 趋势/点线图
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
4. Code MWD 以 `defect_multipliers` 后的 Panel 明细作为月度投入权威，由“入库良率
   修饰表”的 Code Sheet 指定良损驱动；月中锚点插值、稳定哈希噪声和月内整数分配
   生成 Code 日度。Group 日度严格由 Code 最终日度按 Group 汇总，Group 周度再由
   该日度聚合；Group Sheet 只覆写 Group 月度结果，不反向生成日度。
   `weekly_full` 保留完整三自然月窗口，`weekly` 仅供近期展示。
5. Lot 模拟按 Code-week 的 `weekly_full` 速率分配整数缺陷 token；聚合后
   才取整，再按稳定加权噪声分配到 Lot，最后执行封顶与显式覆盖。
6. Mapping 与 Code MWD 共享 Code Sheet 的月度目标水准，但 Mapping 随后仍执行
   批次级联衰减；非有限、负值或超过 10 倍的月度倍率按 1.0 防御性回退。它先
   解析单一有效的产品/Code/batch 修改计划，再处理坐标；无匹配配置时采用确定性位置偏移。布局来自
   `processing.mapping_layout`，未配置产品使用默认 10 × 19 布局。

### SPC、CTQ 与自动预警

- `infrastructure/measurement/` 拥有三厂 Inline 测量事实的数据库读取和产品级
  Parquet 快照。共享快照只保存预处理前的稳定字段超集；TTL、策略版本、强制
  刷新、原子写入和数据库失败降级均在这一适配器内完成。快照写入前可通过
  `measurement_corrector` 钩子应用数值修正（当前规则见
  `infrastructure/spc/spc_data_correction.py`：M673 且 param_name 含 PPA 且
  site_name∈[96,114] 的记录 param_value 减 5，属真实量测偏差修正；其余
  所有 PPA 记录统一减 0.5），由 `composition.py` 注入。
- `application/*/ports.py` 定义消费方拥有的出站端口；`composition.py` 是显式
  组合根。SPC、CTQ、AOI_TT、AOI_RS 应用服务只依赖端口，不读取 Parquet，也不构造
  基础设施仓储。
- `infrastructure/measurement/measurement_preparation.py` 拥有共享制备管线：
  清洗去重、LOSS 排除、白名单参数分类、异常值过滤、查询维度过滤、规格
  YAML 覆盖和主制程追溯。`infrastructure/spc/`、`infrastructure/ctq/` 和
  `infrastructure/aoi_tt/` 分别是共享测量事实的薄业务投影。SPC 透传制备
  投影；CTQ 固定选择 CTQ 分类；AOI_TT 按规格表中的
  `(step_id, param_name)` 识别 TT 并映射 lot/sheet 字段。派生规则不会写回共享快照。
- `application/shared/decorated_features.py` 是 SPC/CTQ/monitor 共享的无状态
  修饰+特征计算缓存函数 `fetch_decorated_features`（缓存 key 含产品、scope、
  起止日期与快照签名）：scope='spc'/'ctq' 分别使用对应 OOS 修饰工作簿，
  scope='none' 跳过修饰；monitor 按 data_type 分组路由 scope（SPC→spc、
  CTQ→ctq、AOI→none）。缓存 miss 时修饰工作簿落盘一次，命中不重写。
- 主制程 OUT 履历查询归 `infrastructure/shared/main_process_history_repository.py`
  所有；`infrastructure/shared/main_process_trace.py` 仅执行规格路由和 DataFrame
  匹配，补充主制程设备/腔室字段。
- `SpcReportService` 固定使用 `SPC` 数据类型并提供 CPM/CPK 能力结果；SPC/CTQ Sheet
  点位图类型统一由 `app/charts/inline/chart_type.py` 根据
  `config/inline_config.yaml` 的前端样式配置决定，不进入应用服务 payload。CPK 人工修饰文件
  `resources/spc_cpk_decoration.xlsx` 的产品 sheet 是用户维护状态，只按周期键
  合并到新结果，刷新时不会重建既有 sheet。
- PNL 指标规格的版本/产品收严分析是独立离线工具，归
  `tools/indicator_improvement/` 所有，不进入 `src/inline_domain` 的应用服务、
  Core 或组合根；其输入为离线 Excel，输出为 `output/` 下的可重建报告。
- `CtqReportService` 固定使用 `CTQ` 数据类型，只返回 Sheet/点位分布和后端
  选定的图表类型；CTQ OOS 修饰保存在共享工作簿 `resources/ctq_sheet_oos_decoration.xlsx` 的产品 sheet 中。
- `AoiTtReportService` 通过 AOI_TT 数据端口读取共享事实的 TT 投影，趋势分母
  和规格口径仍遵循 ADR-0008。
- `AoiRsReportService` 不复用共享 measurement：RS Code 明细和过货分母来自独立
  表/视图与事实契约，由 `infrastructure/aoi_rs/` 的产品级双 Parquet 仓储负责
  三个月滚动提取、8 小时 TTL、覆盖版本、原子写入和数据库失败降级；规格保持
  独立元数据查询。页面通过 `composition.py` 注入端口，并分别提供底层快照刷新
  与产品级 Streamlit 缓存失效。边界见 ADR-0015。
- `MonitorAnalysisService` 基于同一 SPC 数据源完成时间桶映射、规则判定和
  汇总，自动预警页面再叠加合规配置的可见性规则；报废数据由
  `infrastructure/monitor/scrap_repository.py` 适配。该服务只依赖
  `MonitorSpcRepositoryFactory`；页面通过 `composition.py` 注入
  `infrastructure/monitor/monitor_repository.py` 中的 monitor 专属仓储门面，
  application 层不得直接导入 infrastructure。

### 关键备件

- 规格基线来自 `resources/critical_parts_baseline.csv`；数据库快照和仿造
  快照分别以规格签名命名，互不覆盖。
- 报表按每条规格先匹配真实数据库快照；无真实匹配时才使用仿造快照。报表
  载荷每小时重新进入快照层；仿造快照缺失时自动生成，超过 24 小时 TTL 时
  自动按完整过期周期推进。
- 报表只允许三天新鲜度窗口内的真实测量参与优先匹配；陈旧真实记录被排除，
  再由新鲜仿造快照补缺，确保最终展示时间满足当前性约束。该语义见
  `docs/ADR/0011-equipment-measurement-freshness-fallback.md`。
- 仿造测量值采用确定性的近似等差日推进，越过寿命规格后保留溢出量作为新
  备件起点；运维生成/更新命令仍保留用于诊断和人工维护。来源优先级见
  `docs/ADR/0003-equipment-real-first-fabricated-fallback.md`，自动维护语义见
  `docs/ADR/0010-equipment-fabricated-snapshot-auto-maintenance.md`。

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
共享 Inline 原始快照和派生适配器边界见
`docs/ADR/0012-shared-inline-measurement-snapshot.md`。
AOI_RS 专属产品级快照边界见
`docs/ADR/0015-aoi-rs-product-local-snapshot.md`。
Inline/Yield 页面级自动预警（单片异常 flag=FALSE 口径、上一 ISO 周、
异常项自动出图）见 `docs/ADR/0017-inline-alert-center.md`。
MWD 指定良损、Group/Code Sheet 优先级与 Mapping 边界见
`docs/ADR/0018-yield-modifier-specified-rate-driven.md`。

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
| `tools/` | 分域 smoke、离线分析和设备仿造数据运维命令。 |
| `tests/` | 单元、集成和浏览器端到端测试。 |
| `docs/ADR/` | 已接受的架构决策记录。 |
| `references/` | 项目自有领域知识与 Harness 演进记录。 |
| `specs/` | 用户维护的规格契约、运行追踪与模板。 |
| `output/` | 可重建的报告、下载、日志、截图、测试结果与临时文件。 |

## 约束与验证

- **shared 提取约束**：对于各模块可复用的逻辑，应当提取出来并写入对应层的
  `shared/` 子模块下（`application/shared/`、`core/shared/`、
  `infrastructure/shared/`；前端对应 `app/charts/inline/` 与
  `app/sections/inline_domain/shared/`）。业务模块只保留业务差异，禁止跨
  业务模块导入私有函数；复用必须经由 shared 公共 API（ADR-0014、ADR-0016）。
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

### MWD 趋势模块职责

`src/yield_domain/core/mwd_trend/mwd_trend_processor.py` 仅作为 MWD 趋势
门面，负责调用顺序和对外兼容入口。数据准备、聚合与整数分配、EMA、人工
覆盖与日度重建、结果格式化分别由同目录下的专用模块承担。人工覆盖的顺序
固定为：周度覆盖先重建日度再聚合月度；月度覆盖只修改最终月度结果，不回写
日度；日度覆盖完成后重新聚合周度和月度。

# 数据修饰业务逻辑与架构范式

## 1. 目的与适用范围

本文定义报表系统中“数据修饰”的统一语义和分层方式，适用于 Q-Time、Inline
SPC/CTQ/AOI、Yield、IJP、关键备件、IQC 蒸镀材料、寿命测试、自动预警看板及后续新增的人工决策型修饰。目标是同时保证：原始事实可追溯、
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
- 新键按来源填写默认值。Q-Time、Yield Lot 和 Inline 单片超规/失控通常为 `True`，Inline 指定免修饰参数按豁免规则初始化。SPC CPK/CPM、AOI 周期趋势、Yield 趋势、IJP、备件、IQC 蒸镀、寿命和矩阵显示决定为 `False`。

### 2.1 台账生命周期

每个有预警来源的子域在 `resources/<domain>/<sub_domain>/` 保存本地 Excel 台账，在 `config/global.yaml` 注册资源路径。同一工作簿可分别保存候选、决定及公开明细。按产品或事项分表时，工作表作用域也是业务键的一部分。

| 场景 | 自动处理 | 公开结果 |
|---|---|---|
| 首次遇到业务键 | 保存领域默认 Flag 和本次测量 | 依据当前资格和 Flag 生成 |
| 同键再次计算 | 更新测量、规格、当前判定和计算时间，沿用已存 Flag | 使用最新事实与已存决定 |
| 达标、缺测、退出本次查询范围 | 保留历史键与决定；区分本次达标、未判定和未参与计算 | 本次无有效预警 |
| 再次超规 | 取原键的人工决定 | 重新按本次候选和原决定发布 |
| 人工维护 | 校验所选业务键及动作，保存所改决定；未提交的键继续保留 | 更新后重新读取或投影 |

业务键须能跨巡检匹配到同一对象。测量值、行号、预测值、动态规格和本次计算时间不能用作记录身份。有周期语义的预警应包含其业务周期，以免本周决定覆盖下周另一条记录。公开读取以本地发布数据为准；HTTP 请求保持只读。报表既有的自动发布用例及 Q-Time 空发布表头初始化可以写入其负责的工作簿。自动发布与人工编辑共享写入锁，在锁内重读已存决定并原子更新；失败时保留上一个完整版本。

纯预警项的 `True` 表示修饰并抑制发布。有替代数值的指标按本域规则生成报表数值，预警发布还应遵守该指标的放行条件，例如 SPC 能力预警要求明确的 False。

`Delete` 隐去相应的公开记录，台账中的决定与源数据均保留。旧工作簿中有效的布尔值继续沿用；新增字段的初值须保持旧有发布行为。有 Flag 的人工提交必须是明确动作，不能把无法识别的文字当作布尔真值。

### 2.2 管理员入口与消费端

在报表的“预警中心”通过 `?admin=ture` 或 `?admin=true` 维护所属台账。普通模式仅展示公开结果，不展示 Flag、原始候选、维护提示和上传控件。矩阵的指标 × 产品显示台账单独在自动预警看板维护。

报表预警表、矩阵状态与明细、现有 HTTP 接口和通过接口取数的通知均应遵守其来源的行级决定。HTTP 和矩阵还共用指标 × 产品的总体显示决定：`False` 使用已发布结果，`True` 令矩阵显示达标并抑制接口记录，`Delete` 隐去矩阵单元格及其下钻并抑制接口记录。计算快照可复用，读取公开状态时须识别决定变化。公开条数以最终可发布的当前记录为准；历史台账行数单独统计。

同一业务键的修饰投影须可复现。SPC 能力台账在首次建键时生成并保存替代值，已有键的重算和读取均复用已保存的替代值。

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

### 5.1 预警台账覆盖范围

| 所属子域 | 台账或发布对象 | 主要消费者 |
|---|---|---|
| Inline SPC、CTQ、AOI_TT、AOI_RS | Sheet/Lot OOS 与 OOC 各自的稳定对象键；AOI 两域另有周期趋势决定 | 报表、巡检；矩阵和 API 按各自已支持的事件取数 |
| SPC 能力 | CPK 与 CPM 分项，以产品、参数、站点及统计周期识别 | SPC 报表、矩阵、巡检、能力事件 API |
| Yield | Lot 超规，Group/Code 的周期趋势波动 | Yield 报表、矩阵、巡检；Lot 同时供 API 使用 |
| Q-Time | 产品、站点段、批次及时间键对应的超时记录 | Q-Time 报表、矩阵、巡检、API |
| IJP | AA 溢流、孔区，各按对应测量维度与业务周期 | IJP 预警中心、矩阵、巡检 |
| 关键备件 | 设备、机台腔室和备件维度的寿命预警 | 备件预警中心、矩阵、巡检 |
| IQC | 蒸镀材料的检验项目，寿命的产品/批次/测试项目及通道等业务维度 | 对应 IQC 报表、矩阵、巡检 |
| Inline monitor | 指标 × 产品公共显示决策 | 自动预警看板与 HTTP 公开过滤 |

有独立预警事项的报表提供预警中心。密集点、专项资料、历史汇总和 Yield BySheet/ByLot 明细没有独立事项，维护入口归所属预警来源。Yield 上游指定良损、Sheet 不良率覆写和 Mapping 热点剧本继续采用原有数值算法。

站点字典、规格表、过货及原始测点、成员清单、发布回执、矩阵预热缓存和文档归档各自保留原有用途。若一张表既有源值又有人工决定，应明确区分两类列的维护者。

## 6. AOI_RS 顺序修饰（2026-09-24）

- 特殊规则独立位于 `core/aoi_rs/aoi_rs_special_decoration.py`；厂别范围由
  `config/domain/inline_domain/aoi_rs.yaml` 的 `aoi_rs.special_decoration.factories` 配置，
  默认 `["OLED"]`，`[]` 表示全部关闭，可列出多个厂别。名称去除首尾空格并忽略大小写。
  以下顺序规则仅适用于名单内厂别；其余厂别继续在 Sheet/Lot 图各自执行原有三态
  截断（月周天仍用源明细，不受 Sheet/Lot 回写和 1.3 倍限幅影响）。
- 按产品、厂别、站点和 RS Code 隔离修饰。先按 Sheet 汇总，超过
  `SHEET_ID/GLASS_ID` 规格的点截断至 `[0, 0.5 × spec]`，随机值由稳定业务键生成。
- 将 Sheet 总量按原始明细数量占比分配到报表投影，保留时间分布；以该投影重算 Lot
  分子，过货 distinct Sheet 分母不变。仍超过 `LOT_RATIO` 的 Lot 及其全部 Sheet
  归零，同步影响月周天汇总。
- 月度密度按上述投影计算；周/日密度超过对应月度的 1.3 倍时截断至上限。
  跨月周取周末所属月份，未结束周取报表截止日所属月份。缺失或无有效分母的月度
  基准不触发限幅。此步只改变趋势投影（同步其展示分子），不反向修改 Sheet 明细。
- Sheet/Lot 继续使用三态 flag 与参数豁免。`False` 跳过本阶段自动修饰；Lot 的
  `Delete` 仅删除 Lot 图点，Sheet 的 `Delete` 同时从后续分子中排除该片；分母不变。
  上游保留的 Sheet 仍可能随下游超规 Lot 归零。
- 源明细仍用于 OOS 台账与追溯，源快照不变；应用服务只向页面和 PDF 共用的绘图
  入口提供明细投影及最终 Sheet/Lot 点帧。缓存键包含修饰策略版本和厂别名单，
  修改配置后下次查询即使用新范围。

## 7. Inline 全域厂别日期过滤（2026-10-06）

- 全域共用 `config/domain/inline_domain/shared.yaml` 的 `data_exclusion`：`factories`
  为厂别列表，`start_date` 为 ISO 日期，`end_date` 为 ISO 日期或 `today`
  （服务器当天）。空厂别列表或缺配置关闭过滤；兼容旧 `aoi_data_exclusion`，
  新字段存在时优先使用新字段。厂别忽略大小写和首尾空格，按仓储输出的报表事件时间
  判断，首尾日期均包含。厂别/日期以当前配置为准，不在 Core 中硬编码。
- 规则统一位于 `core/shared/date_exclusion.py`，由 SPC、CTQ、AOI_RS、AOI_TT、
  监控（含报废/下钻）、预警与矩阵共用；Application 负责读配置、
  解析当天和传参。非法配置显式报错，配置值及解析后的结束日期进入报表缓存键。
- AOI_RS 在聚合前过滤明细和过货分母，再生成 Sheet、Lot、月周日投影；跨日期
  Sheet/Lot 只统计保留明细。AOI_TT 先过滤再执行原有三态/UCL 修饰和 Particle Size
  分解。日期过滤不受 `False` 或参数豁免影响，过滤后为空时不生成指标/图点。
- SPC/CTQ 通过共享修饰特征管线过滤点位，再计算 Sheet 特征、历史分布与能力指标；
  监控的 AOI 免修饰口径同样过滤。源 OOS/OOC/过货台账不因过滤重写，预警读取及
  实时计算在内存中同步过滤事件与过货分母。各层缓存和矩阵快照签名包含解析后的策略。
- 对于仅有维护周期结果、没有逐日事实可拆分的汇总，若厂别及周期与排除区间相交，
  当前投影省略该记录，展示为无数据；不以零值替代，也不反向删除维护记录。
  支持年/季/月/周/日和 `ALL`、逗号分隔厂别，处理截至查询日的周期范围。
- 过滤只影响报表投影，不修改数据库、原始快照、源 OOS/OOC 台账或过货历史；
  原有人工决策保留。缺失/无法解析的时间无法匹配日期区间，保留源值；启用过滤时
  非空输入缺少厂别/时间列直接报错，避免悄悄返回未过滤报表。

## 8. 新增或评审检查表

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

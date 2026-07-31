# `yield_domain` 六边形架构评审

- 评审日期：2026-07-30
- 评审范围：`src/yield_domain/`
- 评审方法：`hexagonal-architecture` Skill + 项目 `common + python` ECC 评审规则
- 评审性质：只读架构评审；未修改业务代码或算法

## 1. 结论

`yield_domain` 已经具有 `application / core / infrastructure` 的分层外观，也有相当数量的
纯计算模块和回归测试，但当前仍属于**传统分层架构向六边形架构迁移中的状态**，尚未形成
完整的 Ports & Adapters 依赖关系。

最关键的判断依据是：

```text
当前：
Application → concrete Infrastructure
Core → AppConfig / Excel COM / filesystem
Application → Streamlit / Excel / filesystem

六边形目标：
Inbound Adapter → Application → Domain
                         ↓
                   Outbound Port
                         ↑
                Infrastructure Adapter
```

目前没有显式 outbound ports，也没有统一 composition root。应用服务直接构造具体仓储，
核心计算中仍存在文件、COM 和全局配置依赖，Streamlit 缓存与 UI 提示进入了 application。

建议采用渐进式迁移，而不是重写：

1. 先统一包导入路径并建立行为基线；
2. 为 Panel 数据访问定义最小 outbound port；
3. 将具体仓储构造集中到 composition root；
4. 将 Streamlit 缓存移到 inbound adapter；
5. 再逐步抽离 Excel、COM、基线文件和时间等外围能力。

不得在架构迁移时同时改写已经验证的 MWD、Lot/Sheet 或 Mapping 算法。

## 2. 做得较好的部分

### 2.1 已有明确的领域目录

源码按 `application / core / infrastructure` 分类，至少表达了业务编排、核心计算和数据访问
应承担不同职责的意图。

### 2.2 多数分析算法已经可以独立测试

以下模块主要围绕 DataFrame 和数值规则工作：

- `core/mapping/layout.py`
- `core/mapping/panel_position.py`
- `core/mapping/hotspot_modification.py`
- `core/defect_modifier.py`
- `core/abnormal_detector.py`
- `core/sheet_lot/aggregation.py`
- `core/sheet_lot/capping.py`

现有测试直接覆盖 Mapping、Shadow EMA、Sheet/Lot、缺陷计数对齐和异常检测等规则，说明
项目已经具备进一步明确领域边界的基础。

### 2.3 查询契约和数据策略已经显式化

`application/dtos.py` 定义了：

- `YieldQueryConfig`
- `YieldDataPolicy`

相比直接传递无结构字典，这些 DTO 已经能够表达查询条件和静态数据策略，并对快照签名提供
稳定序列化能力。

### 2.4 基础设施目录承载了主要数据库和快照逻辑

`infrastructure/repositories/yield_repository.py` 负责数据库、Parquet 快照、增量刷新和降级；
`infrastructure/data_loader.py` 负责 SQL 和外部报表读取。虽然端口尚未反转，但物理位置大体正确。

### 2.5 已存在重要架构约束

ADR 0001 已明确 Streamlit 缓存只应跨越稳定原生载荷。当前 Yield 缓存主要返回 DataFrame、
dict、tuple 或标量，与该约束基本一致。后续迁移应保留这个行为。

## 3. 主要发现

### F-01 HIGH：Application 直接依赖并构造具体 Infrastructure

证据：

- `application/yield_service.py:13-16` 直接导入 `PanelRepository` 和快照路径函数；
- `application/yield_service.py:100-113` 构建路径、创建目录、实例化 Repository；
- `application/yield_service.py:353-364` 再次构造 Repository 获取 Array 时间；
- `application/yield_service.py:486-501` 在安全刷新流程中再次构造 Repository。

影响：

- Application 无法脱离具体 Repository 实现进行用例级测试；
- PostgreSQL、Parquet、快照路径和降级策略会向用例层传播；
- 实例化逻辑分散，没有可审计的 composition root；
- “传入 db_manager”只是具体类的参数注入，不是对端口的依赖注入。

建议：

在 `application/ports/outbound/` 定义由用例拥有的最小端口，例如：

```python
class PanelDataPort(Protocol):
    def get_panel_details(
        self,
        query: YieldQueryConfig,
        policy: YieldDataPolicy,
        *,
        force_refresh: bool = False,
    ) -> pd.DataFrame: ...

    def get_array_input_times(
        self,
        lot_ids: tuple[str, ...],
        custom_times: Mapping[str, str],
    ) -> pd.DataFrame: ...
```

让现有 `PanelRepository` 实现该协议，并在单一装配入口注入 `YieldAnalysisService`。第一阶段可以
继续以 DataFrame 作为应用边界载荷，避免迁移时改写算法。

### F-02 HIGH：Streamlit 交付机制泄漏到 Application

证据：

- `application/yield_service.py:4` 导入 Streamlit；
- `application/yield_service.py:82,129,170,201,239,283,321,338,367` 在应用服务方法上
  使用 `st.cache_data`；
- `application/yield_service.py:434` 直接调用 `st.error`；
- `application/excel_service.py:3`、`pdf_service.py:7`、`ppt_service.py:7` 也导入 Streamlit。

影响：

- 应用服务只有在安装并初始化 Streamlit 的环境中才容易使用；
- CLI、定时任务和测试无法复用同一用例而不承担 UI 框架依赖；
- 用户提示、缓存策略和业务编排混在一起；
- Application 不再是与交付协议无关的用例层。

建议：

- 把 `st.cache_data` 包装放入 `app/` 下的 Streamlit inbound adapter；
- 缓存函数调用无框架的 application use case；
- 保持 ADR 0001 的原生 payload 边界；
- Application 返回结构化错误或结果，由页面决定 `st.error` 如何展示。

### F-03 HIGH：Core 中存在文件、Excel COM 和调试输出副作用

证据：

- `core/sheet_lot/overrides.py:7-8` 导入 `comtypes`；
- `core/sheet_lot/overrides.py:33-110` 初始化 COM、启动 Excel、打开工作簿；
- `core/mwd_trend/code_baseline.py:25-26` 自行决定资源路径；
- `core/mwd_trend/code_baseline.py:141,174-177` 直接读取和写入 Excel；
- `core/sheet_lot/simulation.py:453-455` 在计算过程中创建目录并写出调试 CSV。

影响：

- Domain/Core 无法作为纯业务计算独立运行；
- 单元测试被 Windows、Excel、文件权限和当前工作目录影响；
- 计算函数产生隐式副作用，调用方无法从签名获知；
- 领域规则与资源格式、路径策略和诊断策略绑定。

建议：

逐步提取以下 outbound ports：

- `LotOverrideSource`
- `CodeBaselineStore`
- `DiagnosticSink`

适配器负责 COM、Excel 和文件系统，core 只接收已解析的数据并返回计算结果。迁移时先为当前
文件行为建立 characterization tests，再移动 I/O，不修改计算公式。

### F-04 HIGH：同一包存在 `yield_domain` 与 `src.yield_domain` 双重导入身份

证据：

- `pyproject.toml:35` 同时将 `src` 和项目根目录加入测试 `pythonpath`；
- `app/Home.py:30-36` 同时把项目根目录和 `src` 写入 `sys.path`；
- `application/yield_service.py:13-17` 使用 `src.yield_domain...`；
- 同一文件 `application/yield_service.py:24-29` 又使用 `yield_domain...`；
- core、app 和 tests 中两种导入方式并存。

影响：

同一个源文件可能以两个模块名加载，例如：

```text
yield_domain.application.dtos
src.yield_domain.application.dtos
```

这会造成：

- 同名类具有不同的 Python 身份；
- `isinstance`、异常类型判断和单例状态可能失效；
- 模块缓存和热重载行为变得不可预测；
- 与 ADR 0001 所描述的类身份/模块重载风险叠加。

建议：

将 `yield_domain` 作为标准 src-layout 顶级包，统一使用：

```python
from yield_domain.application.dtos import YieldQueryConfig
```

逐步移除 `from src.yield_domain...`，并让运行入口只需要将 `src` 作为包根。这个迁移应单独进行，
配套导入一致性和 Streamlit 热重载回归测试，不应与算法修改合并。

### F-05 HIGH：基础设施 SQL 使用字符串插值

证据：

- `infrastructure/data_loader.py:34-64` 把产品、工单类型和日期拼入 SQL；
- `infrastructure/data_loader.py:106-118` 把 Lot ID 列表拼入 SQL；
- 虽然使用了 `sqlalchemy.text`，但变量已经在生成字符串时完成插值，并非参数绑定。

影响：

- 如果任何输入可受外部用户或非可信配置影响，会形成 SQL 注入风险；
- 引号和特殊字符会造成查询语义错误；
- 数据适配器契约难以独立验证。

建议：

- 使用 SQLAlchemy bind parameters；
- 列表参数使用 expanding bind parameter 或受控临时表/批量策略；
- 在 adapter integration tests 中覆盖特殊字符、空列表和日期边界。

这是基础设施安全问题，不需要改变领域模型，但应在端口迁移前后都保留行为验证。

### F-06 MEDIUM：Core 和 Application 广泛依赖完整 `AppConfig`

证据：

- `core/mwd_trend/mwd_trend_processor.py:10` 导入 `AppConfig`；
- `core/mwd_trend/code_baseline.py:9` 导入 `AppConfig`；
- `core/sheet_lot/sheet_lot_processor.py:8` 导入 `AppConfig`；
- 多个 application 方法也直接接收完整 `AppConfig`。

影响：

- 实际输入依赖隐藏在 `.processing.get(...)` 等动态读取中；
- 全局配置模型的修改可能迫使核心模块一起变化；
- 测试需要构造远大于用例需求的对象；
- Core 难以区分业务策略与部署配置。

建议：

在 application 边界把 `AppConfig` 映射为窄而不可变的用例输入或领域策略，例如：

- `YieldAnalysisPolicy`
- `MappingLayout`
- `DefectMultiplierPolicy`
- `LotSimulationPolicy`

并非每个字典都要创建类；只为具有稳定业务含义和校验规则的数据建模。

### F-07 MEDIUM：错误被折叠为空数据或布尔值

证据：

- `infrastructure/data_loader.py:67-88` 捕获全部异常并返回空 DataFrame；
- `infrastructure/data_loader.py:121-136` 对 Array 时间查询采取相同策略；
- `infrastructure/repositories/yield_repository.py:240-242` 查询失败返回空 DataFrame；
- `application/yield_service.py:157-162` 缺陷修饰失败后继续返回未修饰数据；
- `application/yield_service.py:468-470` 警戒线读取失败返回空 dict；
- `application/yield_service.py:505-507` 快照刷新失败返回 `False`。

影响：

- “确实没有数据”与“系统故障”无法区分；
- 调用方无法决定是否降级、重试或阻止报表；
- 日志是唯一诊断来源，错误契约无法被测试；
- 某些情况下可能生成看似正常但口径不完整的报表。

建议：

为端口定义少量有意义的应用错误，例如：

- `PanelDataUnavailable`
- `SnapshotUnavailable`
- `WarningLineInvalid`

基础设施异常在 adapter 边界转换，application 决定降级策略，inbound adapter 决定用户提示。
保留原异常作为 cause，并区分空数据与失败。

### F-08 MEDIUM：Application 中混入了多个 Outbound Adapter

证据：

- `application/excel_service.py` 直接读写 Excel、创建锁文件、修改配置；
- `application/pdf_service.py` 使用 PyMuPDF、文件删除和图片输出；
- `application/ppt_service.py` 使用 PowerPoint COM、临时目录和图片导出；
- `application/file_manager_service.py` 直接操作目录。

影响：

这些类以 `Service` 命名，但职责实际是文件、Office 和媒体适配器。目录名称让依赖关系显得比
实际情况更干净，也使 application 层逐渐成为杂项工具集合。

建议：

按能力迁移到：

```text
adapters/outbound/excel/
adapters/outbound/pdf/
adapters/outbound/powerpoint/
adapters/outbound/filesystem/
```

如果它们仅供特定页面使用，也可以归入 `app/adapters/`，不必强行成为 Yield 领域的一部分。

### F-09 MEDIUM：时间和运行状态是隐式全局依赖

证据：

- `application/yield_service.py:47-68` 使用类级 `_custom_end_date`；
- `application/yield_service.py:62` 直接调用 `datetime.now()`；
- Repository 的 TTL 和目标日期也直接读取系统时间。

影响：

- 测试必须修改类级全局状态并在结束时恢复；
- 并发会话可能共享分析截止日期；
- 时间窗口和缓存新鲜度规则难以独立测试。

建议：

引入很小的 `ClockPort`，或直接注入 `Callable[[], datetime]`。分析截止日期应作为用例输入；
Repository 的 TTL 时钟由基础设施装配注入。

### F-10 MEDIUM：核心与服务文件过大，职责边界难以审计

当前较大的文件包括：

- `core/mwd_trend/mwd_trend_processor.py`：约 1200 行；
- `application/yield_service.py`：约 500 行；
- `application/excel_service.py`：约 480 行；
- `core/mapping/hotspot_modification.py`：约 445 行；
- `core/mwd_trend/code_baseline.py`：约 420 行。

文件大本身不是缺陷，但这些文件同时承担数据准备、计算、I/O、配置解析、缓存或格式化时，会让
端口边界不清晰。

建议：

先按副作用和用例边界拆分，不要为了满足行数指标机械拆函数。优先抽离 I/O，再判断纯计算内部
是否需要继续拆分。

## 4. 当前依赖矩阵

| 调用方 | 当前依赖 | 六边形判断 |
|---|---|---|
| Streamlit 页面 | Application Service | 方向基本正确 |
| Streamlit 页面/图表 | 部分直接依赖 Core | 绕过用例边界 |
| Application | Core | 正确 |
| Application | concrete Infrastructure | 方向错误，应依赖 Port |
| Application | Streamlit / filesystem / Excel | 交付与适配器泄漏 |
| Core | `AppConfig` | 部署配置泄漏 |
| Core | COM / Excel / filesystem | 严重外围依赖泄漏 |
| Infrastructure | Application DTO | 可接受，但最好只依赖稳定 Port 契约 |
| Infrastructure | shared DB manager | 外围依赖，位置合理 |

## 5. 建议的目标结构

不建议立即把整个项目重排。可以让新结构与旧结构短期共存：

```text
src/yield_domain/
  domain/
    mapping/
    mwd_trend/
    sheet_lot/
    abnormal_detection/
  application/
    commands/
      analyze_yield.py
      refresh_yield_snapshot.py
    ports/
      inbound/
        analyze_yield.py
      outbound/
        panel_data.py
        warning_lines.py
        code_baseline_store.py
        lot_override_source.py
        clock.py
    services/
      analyze_yield_service.py
  adapters/
    inbound/
      # 如果 Streamlit 适配器由 app/ 拥有，这里可以为空
    outbound/
      postgres/
        panel_query.py
      parquet/
        yield_snapshot.py
      excel/
        warning_lines.py
        code_baseline.py
        lot_overrides.py
      diagnostics/
        csv_sink.py
  composition/
    yield_container.py
```

对当前分析型系统而言，DataFrame 可以暂时作为 application 与 domain 之间的实用载荷。
除非已经证明列契约频繁失控，否则无需一次性把每一行都转换为实体对象。

## 6. 推荐端口清单

只建议从真实副作用中提取端口，不要为每个类创建接口。

| Port | 使用方 | 适配器实现 |
|---|---|---|
| `PanelDataPort` | Yield 分析用例 | PostgreSQL + Parquet Repository |
| `WarningLinePort` | Code 趋势用例 | Excel Warning Line Adapter |
| `CodeBaselineStore` | Code 基线流程 | Excel/加密 Excel Adapter |
| `LotOverrideSource` | Lot/Sheet 计算 | Excel COM Adapter |
| `ClockPort` 或 callable | 时间窗口、TTL | System Clock / Fixed Clock |
| `DiagnosticSink` | 可选诊断输出 | CSV Sink / No-op Sink |
| `ReportExporterPort` | 导出用例 | Excel/PDF/PPT Adapters |

首个迁移切片只需要 `PanelDataPort` 和 Clock，不应一次引入所有端口。

## 7. 渐进迁移计划

### Phase 0：建立安全基线

1. 记录当前 Yield smoke 和单元测试结果；
2. 为关键页面数据流增加 characterization tests；
3. 单独统一 `yield_domain` 包导入路径；
4. 验证 Streamlit 热重载和缓存清理契约。

完成条件：

- 不再出现 `src.yield_domain` 与 `yield_domain` 混用；
- 现有业务结果没有变化；
- 缓存原生 payload 约束继续通过。

### Phase 1：反转 Panel 数据依赖

1. 在 application 定义 `PanelDataPort`；
2. 让现有 `PanelRepository` 满足该协议；
3. `YieldAnalysisService` 构造时接收 Port；
4. 建立单一 `yield_container.py` 装配入口；
5. 用内存 fake 测试应用编排。

完成条件：

- Application 不再导入或构造 `PanelRepository`；
- 用例测试无需 DatabaseManager、Parquet 或 Streamlit。

### Phase 2：移出 Streamlit 缓存

1. 在 `app/` 创建 Yield inbound adapter；
2. 将 `st.cache_data` 包裹在 adapter 的 payload 函数上；
3. Application 保持无 Streamlit 导入；
4. 页面负责错误到 `st.error` 的映射。

完成条件：

- `src/yield_domain/application/` 不导入 Streamlit；
- ADR 0001 模块重载竞态测试继续通过。

### Phase 3：清理 Core I/O

依次抽离：

1. Lot override Excel COM；
2. Code baseline Excel store；
3. simulation debug CSV；
4. warning line Excel reader。

每次只迁移一个副作用，并保留当前算法函数和数据形状。

完成条件：

- `core/` 不导入 `comtypes`、Streamlit 或文件格式库；
- 纯领域测试不访问文件系统。

### Phase 4：收窄配置和错误契约

1. 将 `AppConfig` 映射为用例输入和领域策略；
2. 区分空数据、依赖不可用和快照损坏；
3. 明确数据库失败时的降级决策；
4. 为适配器增加 port contract tests。

## 8. 测试策略

### Domain

- MWD、Mapping、Lot/Sheet 和异常检测保持纯数据测试；
- 固定随机种子、时钟和输入数据；
- 不 mock 内部纯函数。

### Application

- 使用 `InMemoryPanelDataPort`；
- 验证调用顺序、输入映射、降级决策和结果；
- 不启动 Streamlit 或数据库。

### Adapter Contract

对 `PanelDataPort` 的每个实现复用同一套契约：

- 日期范围；
- Defect Group 策略；
- 去重；
- 空数据；
- 快照回退；
- 错误类型。

### Adapter Integration

- SQL 参数绑定；
- Parquet 读写和损坏快照；
- 企业加密 Excel/COM；
- 文件权限和不存在的资源；
- PostgreSQL 暂时不可用时的明确降级。

### Inbound Adapter

- `st.cache_data` 只返回原生稳定 payload；
- 产品级缓存签名隔离；
- 模块热重载期间不会缓存项目类实例；
- 应用错误被正确映射为用户提示。

## 9. 本次验证结果

执行了与 Yield、Mapping、Sheet/Lot、基线、缺陷、异常检测和数据加载相关的单元测试：

```text
79 passed, 4 failed, 31 warnings
```

失败项：

1. `test_shadow_ema.py::TestShadowEMA::test_spike_rejection_logic`
2. `test_shadow_ema.py::TestShadowEMA::test_zero_denominator`
3. `test_yield_global_data_policy.py::test_yield_data_policy_is_defined_once_in_global_config`
4. `test_yield_global_data_policy.py::test_yield_data_policy_is_built_once_from_validated_app_config`

前两项是 Shadow EMA 当前实现与测试预期不一致；后两项是全局配置比测试预期多出
`TP_Short NG` 和 `TP 容值NG`。此外存在 pandas 3.0 chained-assignment 及 `M` frequency
弃用警告。

这些失败在本次只读评审和文档新增之前已经存在。本次没有修改源码，不能把失败归因于评审工作；
但在任何架构迁移前，应先由业务所有者判断是实现回归还是测试/配置契约已经过期。

## 10. 最终建议

`yield_domain` 不需要进行“大爆炸式 DDD 重写”。最有价值的第一步不是创建大量实体和接口，
而是解决三个具体边界：

1. **Application 不再构造具体 Repository；**
2. **Streamlit 不再进入 Application；**
3. **COM、Excel 和文件系统不再进入 Core。**

如果只完成一个改造，优先选择 `PanelDataPort + composition root`。它能建立正确的依赖方向，
同时保留现有 DataFrame、缓存、快照和核心算法，为后续迁移提供稳定落点。

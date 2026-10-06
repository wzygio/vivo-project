# `yield_domain` API Design 评审

- 评审日期：2026-07-30
- 评审范围：`src/yield_domain/` 及其 `app/` 消费方
- 使用 Skill：`api-design`
- API 类型：当前为 Python/Application API，不是 HTTP REST API
- 评审结果：**需要重构公开契约；REST 专属检查当前为 N/A**

## 1. 结论

`yield_domain` 当前没有 FastAPI、Flask 或 Django REST Framework 路由，也没有 OpenAPI。
真实消费者是 Streamlit 页面，主要通过以下类的 Python 方法调用：

- `YieldAnalysisService`
- `AlertService`
- `ExcelService`
- `FileManagerService`
- `PDFService`
- `PPTService`

因此本次 `$api-design` 执行分为：

1. 对真实存在的 Python/Application API 进行契约评审；
2. 对未来可能的 REST 暴露给出 Proposed Design；
3. 对认证、HTTP 状态码、rate limit 等尚不存在的能力标记为 N/A，不假装其已经实现。

总体判断：

> 当前 API 可以支撑现有 Streamlit 页面，但它更像“页面可调用的内部实现集合”，还不是稳定、
> 独立、可版本化的领域应用契约。

主要问题：

- 公共 API 泄漏 `AppConfig`、`DatabaseManager`、`Path` 和缓存签名；
- 返回 DataFrame 与任意 dict，schema 没有显式定义；
- `None`、空 DataFrame、空 dict 和 `False` 混合表达无数据与失败；
- 包级公开入口为空，消费者依赖深层模块路径；
- 方法名称不能完整表达读取、写入、刷新和降级副作用；
- 两种包导入路径造成 API 身份不稳定；
- 数据访问层动态拼接 SQL，不适合直接置于未来 HTTP 输入之后。

## 2. 当前 API Inventory

### 2.1 `YieldAnalysisService`

主要公开方法：

| 方法 | 主要输入 | 返回 |
|---|---|---|
| `set_analysis_end_date` | `datetime` | 隐式 `None` |
| `get_time_window` | 无 | `(datetime, datetime)` |
| `compute_snapshot_signature` | `Path` | `str` |
| `get_raw_panel_details` | JSON 字符串、DB manager、签名 | `DataFrame` |
| `get_modified_panel_details` | `AppConfig`、DB manager、签名 | `DataFrame` |
| `get_mwd_trend_data` | config、product path、DB、签名、EMA | `dict[str, DataFrame] \| None` |
| `get_code_level_trend_data` | config、product path、DB、签名、EMA | `dict[str, DataFrame] \| None` |
| `get_lot_defect_rates` | config、product path、DB、签名、EMA | `dict[str, Any] \| None` |
| `get_sheet_defect_rates` | config、product path、DB、签名 | `dict[str, Any] \| None` |
| `get_mapping_data` | config、scale、DB、签名 | `DataFrame` |
| `load_static_warning_lines` | config、product path、签名 | `dict[str, Any]` |
| `safe_refresh_snapshots` | DB manager、config | `bool` |

### 2.2 其他 Service

`AlertService.get_dashboard_alerts` 返回字符串列表。

`ExcelService` 同时提供：

- 读取和清洗；
- timestamp；
- 带锁写入；
- COM 解密回退；
- 配置注入；
- Mapping 配置解析；
- 路径定位。

`PDFService` 和 `PPTService` 提供媒体转图片以及图片枚举。

`FileManagerService` 提供目录扫描和文件分类。

这些 API 可以工作，但把多个协议、I/O 和业务概念暴露为一组静态/实例方法，没有形成稳定 facade。

## 3. 当前消费者

主要消费流：

```text
Streamlit page
  ├─ 构造 DatabaseManager
  ├─ 获取 AppConfig / product_dir / cache signature
  ├─ 调用 YieldAnalysisService 多个静态缓存方法
  ├─ 根据 None/空 dict/空 DataFrame 判断是否停止
  └─ 将结果交给 section/chart 渲染
```

例如 `app/pages/入库不良率分析看板.py:77-109` 连续调用：

- Group MWD
- Code MWD
- Lot
- Sheet
- Mapping
- Warning Lines

页面必须了解：

- config；
- product directory；
- DB manager；
- snapshot/cache signature；
- 每种返回值的真实形状。

这说明调用边界过度暴露内部装配细节。

## 4. 正面发现

### 4.1 API 使用了领域语言

方法名称基本体现真实业务：

- MWD trend
- Code level trend
- Lot defect rates
- Sheet defect rates
- Mapping
- Warning lines

比 `process_data` 或 `run_report` 更容易理解。

### 4.2 查询与数据策略已有 Pydantic DTO

`application/dtos.py` 定义：

- `YieldQueryConfig`
- `YieldDataPolicy`

它们具有类型、校验和稳定序列化能力，是构建明确 API 契约的良好起点。

### 4.3 页面多数使用关键字参数传递基础设施依赖

页面调用 `_db_manager=` 和 `snapshot_signature=` 时采用关键字，降低了部分位置参数错位风险。

### 4.4 业务入口与 UI 渲染大体分离

页面主要消费计算结果，核心趋势、Lot/Sheet 和 Mapping 计算仍位于 `src/yield_domain/`，没有完全
散落在页面。

### 4.5 返回值遵守当前 Streamlit 原生缓存载荷约束

DataFrame、dict、tuple 和标量适合当前 ADR 0001 的缓存边界。未来 API 重构不能简单地用项目
dataclass 替换缓存返回值，而应继续在缓存 adapter 中映射原生 payload。

## 5. 主要发现

### A-01 HIGH：没有稳定的包级公共 API

证据：

- `src/yield_domain/__init__.py` 为 0 字节；
- `src/yield_domain/application/__init__.py` 为 0 字节；
- 页面直接导入 `yield_domain.application.yield_service` 等深层路径；
- 其他模块和测试又使用 `src.yield_domain...`。

影响：

- 目录移动会直接破坏消费者；
- 无法明确哪些类型和函数是 public；
- 内部 helper 容易被测试或页面长期依赖；
- 同一类可能通过两个模块名加载，造成类型和缓存身份问题。

建议：

选择唯一 canonical package：

```python
from yield_domain.application import (
    YieldAnalysis,
    YieldTrendQuery,
    YieldTrendResult,
)
```

通过 `__all__` 声明公开契约，内部模块路径不承诺兼容。包导入统一应作为独立迁移完成，并覆盖
Streamlit 热重载。

### A-02 HIGH：公共 API 泄漏装配和基础设施参数

证据：

- 多个方法接收完整 `AppConfig`；
- 页面需要构造并传入 `DatabaseManager`；
- 页面需要传入 `product_dir: Path`；
- 页面需要构造并传入 `snapshot_signature`；
- `get_raw_panel_details` 接收两个 JSON 字符串而不是查询对象。

影响：

- 消费者必须理解数据库、文件布局和缓存机制；
- 无法在 CLI、HTTP 或测试中自然复用；
- 参数变化会影响所有页面；
- “业务查询”与“框架缓存键”成为同一 API。

建议：

建立用例输入：

```python
@dataclass(frozen=True)
class YieldAnalysisQuery:
    product_code: str
    start_date: date
    end_date: date
```

Repository、paths 和 cache revision 在 composition root/inbound adapter 处理，不进入应用 API。

### A-03 HIGH：失败语义不一致且与空数据混淆

证据：

- 趋势、Lot、Sheet 返回 `None` 表示无法产生结果；
- Mapping 返回空 DataFrame；
- warning lines 返回空 dict；
- refresh 返回 `False`；
- data loader 捕获异常并返回空 DataFrame；
- 页面 `if not all(...)` 把无数据与错误统一显示为“暂无足够数据”。

影响：

- 消费者无法区分合法空结果、验证失败、数据库故障、快照故障和配置错误；
- 无法建立可靠 HTTP status mapping；
- 告警和重试策略依赖日志文本；
- 可能把系统错误展示成“暂无数据”。

建议：

定义：

```python
class YieldApplicationError(Exception): ...
class InvalidYieldQuery(YieldApplicationError): ...
class PanelDataUnavailable(YieldApplicationError): ...
class YieldSnapshotUnavailable(YieldApplicationError): ...
class WarningLineInvalid(YieldApplicationError): ...
```

合法无数据返回空的强类型结果；技术错误在 adapter 转为应用错误；页面或 HTTP adapter 决定展示和
status code。

### A-04 HIGH：返回 schema 依赖 DataFrame 和任意 dict 的偶然形状

证据：

- `dict[str, pd.DataFrame]` 没有约束允许的 key；
- `dict[str, Any]` 无法说明 Lot/Sheet 结果字段；
- DataFrame 列、dtype、索引、时间格式和单位未在 API 层声明；
- 页面和 section 通过隐式知识消费这些结构。

影响：

- 内部列重命名可能静默破坏 UI；
- pandas/numpy 类型不适合直接 JSON 序列化；
- 无法自动生成文档和客户端；
- contract tests 难以覆盖。

建议：

Python 内部先定义稳定结果：

```python
@dataclass(frozen=True)
class MwdTrendResult:
    monthly: pd.DataFrame
    weekly: pd.DataFrame
    weekly_full: pd.DataFrame
    daily: pd.DataFrame
```

并为每个 DataFrame 记录 column contract。未来 REST adapter 再映射为 Pydantic response models，
不要直接 `df.to_dict()` 后把所有字段暴露出去。

### A-05 HIGH：数据库适配器不适合接受未来外部 API 输入

证据：

- `infrastructure/data_loader.py:34-64` 使用 f-string 拼接产品、日期和工单类型；
- `infrastructure/data_loader.py:106-118` 拼接 Lot ID 列表。

影响：

当前输入主要来自 config 和数据库结果，但如果未来 REST path/query 参数进入相同链路，会扩大 SQL
注入风险。即使输入可信，引号和特殊字符也可能破坏查询。

建议：

在提供任何 HTTP API 前完成：

- scalar bind parameters；
- expanding list parameters；
- filter/sort 白名单；
- integration tests；
- 最大日期窗口和列表大小限制。

### A-06 MEDIUM：方法名称不能完整表达副作用

示例：

- `get_raw_panel_details` 可能触发数据库读取和 Parquet 快照写入；
- `get_modified_panel_details` 在缺陷修饰失败时继续返回原始结果；
- `load_static_warning_lines` 同时读取 Excel、解析和触发 `st.error`；
- `safe_refresh_snapshots` 执行强制刷新，但只返回 bool；
- `inject_excel_overrides_to_config` 原地修改 config。

影响：

消费者难以从 API 判断：

- 是否 safe；
- 是否幂等；
- 是否修改状态；
- 是否可能耗时；
- 失败后是否有部分结果。

建议：

把 query 与 command 分开：

```text
query_yield_trends()
query_lot_report()
refresh_yield_snapshot()
apply_yield_overrides()
```

Command 返回明确结果或 job，而不是简单 bool。

### A-07 MEDIUM：全局可变时间状态进入公共 API

证据：

- `YieldAnalysisService._custom_end_date` 是类级状态；
- `set_analysis_end_date` 修改所有调用者共享状态；
- 测试需要保存并恢复该字段。

影响：

- 多会话可能互相影响；
- 同一服务调用结果取决于调用历史；
- API 难以声明线程安全和可重入性；
- 缓存键可能不能完整体现隐藏状态。

建议：

把 `analysis_end_date` 放入 query；系统当前时间通过 Clock 注入。查询应尽量满足：

```text
相同显式输入 + 相同数据版本 → 相同结果
```

### A-08 MEDIUM：方法参数顺序和可见性不一致

示例：

- `safe_refresh_snapshots(_db_manager, config)` 先 DB 后 config；
- 其他方法通常先 config 再 product path/DB/signature；
- `_db_manager` 使用下划线暗示内部参数，却要求页面传入；
- 有些调用把 `snapshot_signature` 作为位置参数，有些使用关键字；
- scale、EMA 和 signature 与业务参数混在同一签名。

建议：

- 对多参数 API 使用 keyword-only；
- 不把 `_` 前缀参数作为公开必需参数；
- 使用单一 query/command 对象；
- 通过实例构造注入稳定依赖。

### A-09 MEDIUM：多个领域能力通过一个超大静态 Service 暴露

`YieldAnalysisService` 同时负责：

- 时间窗口；
- 缓存签名；
- Panel 数据；
- 缺陷修饰；
- MWD；
- Lot；
- Sheet；
- Mapping；
- warning line；
- snapshot refresh。

影响：

- public surface 持续增长；
- 页面容易调用不应直接暴露的低层方法；
- 无法按用例单独版本和测试；
- 静态方法不利于依赖注入。

建议按用例形成 facade：

- `QueryYieldTrends`
- `QueryLotDefectReport`
- `QuerySheetDefectReport`
- `QueryYieldMapping`
- `ListYieldAlerts`
- `RefreshYieldSnapshot`

不需要为每个方法创建类，但 public contract 应按用例组织。

### A-10 MEDIUM：页面重复调用形成粗粒度 API 编排和计算耦合

分析看板依次调用 Group、Code、Lot、Sheet、Mapping 和 warning lines。部分方法内部又调用其他
方法，例如 Group 依赖 Code，Sheet 依赖 Lot。

风险：

- 调用图和缓存命中关系复杂；
- API 消费者不知道哪个调用已经包含其他结果；
- 无 HTTP 场景下的成本模型；
- 未来 REST 客户端若照搬会产生多个昂贵请求。

建议：

对页面首屏提供一个明确 dashboard query：

```python
dashboard = query_yield_dashboard(query)
```

但不要返回无限膨胀的万能 payload。可把耗时明细按需加载，并在 REST 中使用独立资源。

### A-11 LOW：部分写 API 返回 `(bool, str)`

`ExcelService.save_data_with_lock` 返回：

```python
tuple[bool, str]
```

问题：

- 消费者依赖中文 message；
- 无稳定错误 code；
- 成功与冲突/锁定/未知错误只能解析 bool 和文本；
- 不便映射 409、412、500。

建议返回：

```python
SaveResult(status=SaveStatus.CONFLICT, version=...)
```

或抛出明确的 `ConcurrentModification` / `FileBusy`。

## 6. REST 专属检查

当前项目没有 REST 端点，因此以下不是 FAIL，而是 N/A：

| 项目 | 当前状态 |
|---|---|
| Resource URL naming | N/A |
| HTTP methods | N/A |
| HTTP status codes | N/A |
| Response envelope | N/A |
| Pagination | N/A |
| Authentication | N/A |
| Authorization | N/A |
| Rate limiting | N/A |
| URL versioning | N/A |
| OpenAPI | N/A |
| HTTP cache headers | N/A |

若未来引入 FastAPI，应在确认实际应用后再加载项目的 FastAPI 专用工程规则。本次没有把
`yield_domain` 误判为 FastAPI 应用。

## 7. 推荐的 Python Application API

### 7.1 Query

```python
@dataclass(frozen=True)
class YieldQuery:
    product_code: str
    start_date: date
    end_date: date


@dataclass(frozen=True)
class TrendQuery:
    yield_query: YieldQuery
    level: Literal["group", "code"]
    granularity: tuple[Literal["monthly", "weekly", "daily"], ...]
```

### 7.2 Result

```python
@dataclass(frozen=True)
class TrendSeries:
    monthly: pd.DataFrame
    weekly: pd.DataFrame
    daily: pd.DataFrame


@dataclass(frozen=True)
class SnapshotRefreshResult:
    product_code: str
    refreshed_at: datetime
    row_count: int
    snapshot_version: str
```

### 7.3 Facade

```python
class YieldApplication:
    def query_trends(self, query: TrendQuery) -> TrendSeries:
        ...

    def query_lots(self, query: YieldQuery) -> LotReport:
        ...

    def query_sheets(self, query: YieldQuery) -> SheetReport:
        ...

    def query_mapping(self, query: MappingQuery) -> MappingResult:
        ...

    def refresh_snapshot(
        self,
        command: RefreshYieldSnapshot,
    ) -> SnapshotRefreshResult:
        ...
```

Application 构造时接收 ports，页面不传 DatabaseManager 和 cache signature。

### 7.4 Cache Adapter

Streamlit adapter 保留 ADR 0001：

```python
@st.cache_data(show_spinner=False)
def fetch_trend_payload(query_json: str, revision: str) -> dict:
    result = build_yield_application().query_trends(
        TrendQuery.from_json(query_json)
    )
    return result.to_native_payload()
```

缓存外再构造项目 ViewModel。

## 8. Proposed REST API

以下全部是建议设计，不是当前实现。

### 8.1 Resource Map

| Use case | Method | Path |
|---|---|---|
| 查询趋势 | GET | `/api/v1/products/{product_code}/yield-trends` |
| 查询 Lot | GET | `/api/v1/products/{product_code}/yield-lots` |
| 查询 Sheet | GET | `/api/v1/products/{product_code}/yield-sheets` |
| 查询 Mapping | GET | `/api/v1/products/{product_code}/yield-mapping` |
| 查询 Alerts | GET | `/api/v1/products/{product_code}/yield-alerts` |
| 创建刷新任务 | POST | `/api/v1/products/{product_code}/yield-snapshot-refreshes` |
| 查询刷新任务 | GET | `/api/v1/yield-snapshot-refreshes/{job_id}` |

### 8.2 Trend Query

```http
GET /api/v1/products/M626/yield-trends
    ?level=code
    &granularity=weekly
    &start_date=2026-05-01
    &end_date=2026-07-30
```

约束：

- `level`: `group | code`
- `granularity`: `monthly | weekly | daily`
- 日期包含边界必须明确；
- 最大窗口必须限制；
- 所有时间使用明确时区。

### 8.3 Lot/Sheet Pagination

推荐 cursor：

```http
GET /api/v1/products/M626/yield-lots
    ?start_date=2026-05-01
    &end_date=2026-07-30
    &sort=-defect_rate,lot_id
    &limit=100
    &cursor=opaque-token
```

限制：

- `limit` 默认 100、最大值由容量测试决定；
- cursor 包含稳定排序键；
- filter/sort 使用白名单；
- 不允许客户端指定任意列名进入 SQL。

### 8.4 Snapshot Refresh

推荐异步 job：

```http
POST /api/v1/products/M626/yield-snapshot-refreshes
Idempotency-Key: 15a1...
```

响应：

```http
HTTP/1.1 202 Accepted
Location: /api/v1/yield-snapshot-refreshes/job-123
```

```json
{
  "data": {
    "id": "job-123",
    "product_code": "M626",
    "status": "queued",
    "created_at": "2026-07-30T15:20:00+08:00"
  }
}
```

不要把内部 `snapshot_signature` 暴露为客户端参数。

## 9. Proposed Error Mapping

| Application error | HTTP |
|---|---|
| `InvalidYieldQuery` | 422 |
| `ProductNotFound` | 404 |
| `YieldDataNotFound`（指定唯一资源） | 404 |
| 空集合查询 | 200 + `[]` |
| `SnapshotRefreshConflict` | 409 |
| `ConcurrentModification` | 412 或 409 |
| `PanelDataUnavailable` | 503 |
| `UpstreamTimeout` | 504 |
| 未预期错误 | 500 |
| 未认证 | 401 |
| 无产品权限 | 403 |
| 超配额 | 429 |

统一 envelope：

```json
{
  "error": {
    "code": "panel_data_unavailable",
    "message": "Yield data is temporarily unavailable.",
    "request_id": "req_..."
  }
}
```

## 10. 版本与字段约定

### Naming

JSON 使用 `snake_case`，与 Python/Pydantic 和当前数据字段更一致。

### Dates

- 日期：`YYYY-MM-DD`
- 时间：ISO 8601，带时区

### Rates

- 统一使用 0 到 1 的 fraction；
- 字段名为 `defect_rate`；
- schema description 明确单位；
- 展示层负责格式化为百分数。

### IDs

- Lot ID、Sheet ID、Panel ID 都按字符串；
- 不转换为数字；
- 保留前导零；
- 记录格式约束但避免过早硬编码无法确认的全局正则。

### Null

区分：

- `null`：字段适用但未知；
- 缺少字段：该版本不提供；
- `0`：真实零值；
- `""`：通常不用于未知值。

## 11. Security Requirements for Future REST

在任何 REST 暴露前：

- [ ] SQL 全部参数化；
- [ ] 每个 product resource 做资源级授权；
- [ ] admin UI 参数不作为授权；
- [ ] 日期窗口、limit、筛选数量设上限；
- [ ] 导出和刷新使用独立权限；
- [ ] 错误不返回 SQL、路径和 stack trace；
- [ ] 日志不记录 token 和敏感明细；
- [ ] 重量端点设置 rate/concurrency limit；
- [ ] 上传 Excel 校验类型、大小、内容和路径；
- [ ] OpenAPI 明确 auth scheme。

## 12. 迁移建议

### Phase 0：锁定当前契约

1. 列出页面实际读取的 DataFrame 列和 dict keys；
2. 为它们建立 contract tests；
3. 统一 canonical import path；
4. 不修改核心算法。

### Phase 1：建立 Python Facade

1. 定义 query/command；
2. 定义明确 result；
3. 使用实例依赖注入；
4. 包级导出稳定 public API；
5. 旧静态方法暂时委托给新 facade。

### Phase 2：分离框架与缓存

1. Streamlit cache 参数留在 adapter；
2. Application 不接收 `_db_manager` 和 signature；
3. UI 错误从应用结果映射；
4. 保留 ADR 0001 原生 payload。

### Phase 3：规范错误和 SQL

1. 区分 empty 与 unavailable；
2. 定义应用错误；
3. 参数化 SQL；
4. 增加 adapter integration tests。

### Phase 4：仅在真实需求出现时增加 REST

1. OpenAPI contract first；
2. 先实现只读趋势或 Lot slice；
3. 加认证、授权、分页和 rate limit；
4. 快照刷新使用 202 job；
5. 执行 contract/security/performance tests。

不要仅为“架构更现代”而引入 HTTP 层。

## 13. 测试建议

### Python Public API

- canonical imports；
- query validation；
- result field contract；
- empty result；
- typed errors；
- dependency injection；
- deprecation warnings。

### Data Contract

- DataFrame required columns；
- dtype/nullable；
- fraction units；
- timezone；
- stable dict keys；
- JSON native conversion。

### REST Contract（未来）

- OpenAPI schema；
- 200/201/202/204；
- 400/401/403/404/409/422/429/503；
- pagination cursor；
- max limit；
- invalid sort/filter；
- idempotency key；
- resource authorization。

## 14. 与当前测试基线的关系

本轮 Yield smoke：

```text
88 passed, 6 failed, 31 warnings
```

其中两个失败直接体现 API contract drift：

```text
build_batch_code_options_by_group()
missing required positional argument: count_threshold
```

它说明函数签名变化和消费者/测试尚未同步，是内部 API 也需要版本纪律的具体例子。

其他四项失败涉及 Shadow EMA 与全局 Yield 数据策略，需要业务契约确认，不能仅按 API 风格修改。

## 15. 最终建议

最优先的 API 改进不是增加 REST 路由，而是建立稳定的 Python Application API：

1. 统一 `yield_domain` 导入路径；
2. 用 query/command 替代 config、DB、Path、signature 参数组合；
3. 用明确 result 替代任意 dict；
4. 用 typed errors 区分空结果和故障；
5. 将 Streamlit cache 与 application contract 分离。

完成这些后，Streamlit、CLI 或未来 REST 都可以成为同一应用契约的适配器，而不需要复制业务逻辑。

```text
Current:
Streamlit → static methods + config + DB + paths + cache signature

Target:
Streamlit/REST → explicit application API → domain + ports
```

如果未来确有跨系统访问需求，再增加版本化 REST adapter。当前阶段直接把现有 DataFrame 和静态方法
包装成 HTTP，只会把内部不稳定性永久公开。

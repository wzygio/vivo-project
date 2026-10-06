# `yield_domain` Error Handling 评审

- 评审日期：2026-07-30
- 评审范围：`src/yield_domain/` 及主要 Streamlit 消费页面
- 使用 Skill：`error-handling`
- 工程规则：ECC `common + python`
- 评审性质：只读评审；未修改业务源码
- 总体结论：**存在 1 项 CRITICAL、数项 HIGH；不建议按当前错误契约直接交付**

## 1. 执行结论

`yield_domain` 已经有大量日志、部分 stack trace、数据库快照降级和 Excel COM fallback，说明项目
充分意识到了生产环境中的外部故障。

但当前主要问题是：

> 很多错误在下层被转换为 `None`、空 DataFrame、空 dict、`False` 或原始数据，上层因此无法区分
> 合法无数据、依赖故障、计算失败和降级成功。

最严重的问题出现在 Excel 文件锁：

- 未原子获取锁；
- 未记录本次调用是否真正持有锁；
- `finally` 只要看到锁文件存在就删除；
- 等待其他写者超时并返回后，仍会进入 `finally` 删除别人的锁。

这可能破坏并发保护并造成覆盖或数据丢失风险，按 ECC 分级为 CRITICAL。

错误处理改进不能与 MWD、Lot/Sheet、Mapping 算法重写绑定。建议先通过故障注入测试固定现有行为，
再逐层增加 typed errors 和 explicit degraded outcome。

## 2. 当前错误流

```text
SQL / Parquet / Excel / COM
  ↓
catch Exception
  ↓
empty DataFrame / None / {} / False
  ↓
Application 继续计算或再次转换为空值
  ↓
Streamlit 显示“暂无数据”或原始异常文本
```

理想错误流：

```text
Concrete dependency error
  ↓
Infrastructure translates and preserves cause
  ↓
Typed application error
  ↓
Application decides fail / retry / explicit fallback
  ↓
UI maps code to safe message

Degraded success:
  payload + source + stale flag + warning code
```

## 3. 正面发现

### 3.1 已有明确的数据库快照降级意图

`PanelRepository` 明确区分：

- fresh snapshot；
- incremental update；
- full refresh；
- stale snapshot fallback。

这种降级策略符合报表系统对可用性的需求，不应被简单删除。

### 3.2 部分关键 catch 保留 stack trace

例如：

- `yield_service.py:468-469`
- `mapping_processor.py:196-197`
- `sheet_lot/aggregation.py`
- `sheet_lot/overrides.py:96-97`

使用了 `exc_info=True`，比只记录异常字符串更有诊断价值。

### 3.3 Excel 标准读取到 COM fallback 的边界明确

`code_baseline.py:138-149` 在 openpyxl 读取失败后记录 warning，再使用企业加密 Excel COM
回退。这是一个有明确触发条件和替代路径的 fallback。

### 3.4 资源清理大多放在 `finally`

PDF、PPT 和 Excel COM 都尝试在 `finally` 中关闭文档或进程。设计方向正确，主要问题在于部分
cleanup 使用裸 `except`，以及锁 ownership 没有建模。

### 3.5 Pydantic DTO 提供输入校验

`YieldQueryConfig` 与 `YieldDataPolicy` 能在部分无效输入进入数据访问前产生明确
`ValidationError`，相关测试已有覆盖。

## 4. 主要发现

### EH-01 CRITICAL：文件锁可能删除其他写者的锁

证据：

- `application/excel_service.py:103` 使用固定的 `file_path + ".lock"`；
- `application/excel_service.py:113-122` 先检查存在，再普通 `open(..., "w")`，获取不是原子的；
- `application/excel_service.py:123-124` 等待失败后返回“系统繁忙”；
- Python 返回仍会执行 `finally`；
- `application/excel_service.py:143-149` 只要锁文件存在就删除，没有确认本次调用是否成功获取。

具体失败时序：

```text
Writer A 持有 lock
Writer B 等待 5 次仍看到 lock
Writer B return False
Writer B 进入 finally
Writer B 删除 Writer A 的 lock
Writer C 进入并开始写
Writer A 也仍在写
```

此外，“检查不存在 → 创建文件”不是原子操作，两个写者可能同时通过检查。

影响：

- 并发写保护失效；
- 两个进程可能同时覆盖 Excel；
- 当前写入会完整重写目标文件，注释已说明其他 Sheet 可能丢失；
- 存在数据覆盖或损坏风险。

建议：

1. 使用原子 exclusive create 或成熟 file-lock library；
2. 记录 `lock_acquired`；
3. lock 包含 owner token；
4. `finally` 只释放本调用持有且 token 匹配的锁；
5. 写入临时文件并原子 replace；
6. 为锁超时、非 owner cleanup、并发 writer 建立测试；
7. 在确认 Excel 多 Sheet 契约前，不扩大当前覆盖写逻辑。

### EH-02 HIGH：数据库错误先被转换为空数据，Repository 无法判断故障

证据：

- `infrastructure/data_loader.py:67-88` 捕获所有 Panel 查询异常并返回空 DataFrame；
- `infrastructure/data_loader.py:121-136` 对 Array 时间查询同样处理；
- `infrastructure/repositories/yield_repository.py:229-238` 只看到空 chunk；
- `_fetch_from_db_in_chunks` 没有得到异常，只能返回空 DataFrame；
- Repository 中 `except` 分支因此无法可靠区分依赖失败。

影响：

- 数据库成功返回 0 行与 SQL/连接失败具有相同值；
- Repository 日志可能写“增量查询为空”，而实际是查询错误；
- 上层无法选择准确的 fallback；
- 没有快照时，UI 可能显示“暂无数据”而不是数据源故障；
- 失败类型和重试决策丢失。

建议：

`data_loader` 捕获具体 SQLAlchemy 异常并抛出：

```python
raise PanelQueryFailed(...) from exc
```

Repository 统一决定是否回退快照。成功空数据仍正常返回空 DataFrame。

### EH-03 HIGH：数据库降级成功没有显式 metadata

证据：

- `yield_repository.py:142-145` 异常时使用陈旧快照；
- `yield_repository.py:160-169` 全量空或异常时也可能回退；
- 最终只返回 DataFrame；
- 页面无法知道结果来自数据库还是旧快照。

影响：

- 用户可能把陈旧数据视为实时数据；
- 无法展示快照时间；
- 无法统计降级频率；
- 告警、趋势和导出可能基于旧数据却没有显式标识。

建议：

保持现有 fallback，但返回原生 outcome payload：

```python
{
    "data": dataframe,
    "source": "snapshot",
    "is_stale": True,
    "snapshot_time": "...",
    "warnings": ["panel_data_degraded"],
}
```

遵循 ADR 0001：缓存边界内使用原生 payload，缓存外构造强类型 ViewModel。

### EH-04 HIGH：缺陷修饰失败后静默返回未修饰数据

证据：

- `application/yield_service.py:157-161` 捕获所有 `apply_defect_multipliers` 异常；
- 只记录一条不含 stack trace 的日志；
- `yield_service.py:163` 返回原始 `processed_df`。

影响：

- 配置要求的缺陷衰减没有生效；
- 后续 MWD、Lot/Sheet 和 Mapping 继续运行；
- 报表看起来成功，但业务口径可能错误；
- 页面和用户不知道结果已降级；
- cache 可能保存这一结果。

建议：

由业务所有者明确二选一：

1. 修饰是强制口径：抛出 `YieldComputationError` 并阻止报表；
2. 修饰允许跳过：返回 explicit warning metadata，并在 UI 显示。

不能只记录日志后返回正常形状。

### EH-05 HIGH：核心计算异常被转换为空结果，页面误判为无数据

证据：

- `core/mapping/mapping_processor.py:196-198` 任意异常后返回空 DataFrame；
- `core/mwd_trend/mwd_trend_processor.py:294-296` Group 异常返回 `None`；
- `mwd_trend_processor.py:357-359` Code 异常返回 `None`；
- 多个 Sheet/Lot aggregation catch 也返回空或 `None`；
- `app/pages/入库不良率分析看板.py:112-115` 将 falsy 结果统一展示为“暂无足够数据”。

影响：

- 算法缺陷、schema 漂移和无数据无法区分；
- 故障可能被用户当成正常业务状态；
- 自动监控无法按 error code 统计计算失败；
- 回归测试可能只看到空结果而没有预期异常。

建议：

- 纯计算输入不满足 schema：`YieldValidationError`；
- 计算中不可能状态：`YieldComputationError`；
- 合法过滤后为空：正常空结果；
- 页面集中映射错误，不从空值推断故障。

### EH-06 HIGH：警戒线缺失、损坏和合法空配置都变成 `{}`

证据：

- `yield_service.py:380-382` 配置不存在返回 `{}`；
- `yield_service.py:388-390` 文件不存在返回 `{}`；
- `yield_service.py:431-435` 必需表头缺失返回 `{}`；
- `yield_service.py:468-470` 任意读取异常返回 `{}`；
- `trend_regulator.py:27` 没有 warning lines 时跳过 Code 级截断。

影响：

- 可选配置缺失与配置损坏无法区分；
- 如果警戒线是必需业务控制，系统会静默降低告警/截断能力；
- UI 不知道当前监控是否完整；
- 错误只存在日志或 `st.error` 副作用中。

建议：

首先在业务契约中声明警戒线是：

- required；
- optional；
- optional but degraded。

然后分别使用：

- 正常空配置；
- `YieldResourceMissing`；
- `YieldResourceInvalid`；
- explicit warning metadata。

### EH-07 HIGH：快照写入失败后仍返回数据库结果，持久化失败不可见

证据：

- `yield_repository.py:186-191` Parquet 写入异常只记录日志；
- `yield_repository.py:193-200` 继续返回查询数据。

这可能是合理的“当前请求成功、缓存刷新失败”，但当前 API 没有表达这种 partial success。

影响：

- 用户当前看到新数据，但下一次数据库故障时可能回退到更旧快照；
- 运维无法从调用结果知道 snapshot 未更新；
- refresh handler 可能根据非空 DataFrame 返回成功。

建议：

返回：

```text
data_source=database
snapshot_persisted=false
warnings=[snapshot_write_failed]
```

如果调用是显式“刷新快照”命令，则 snapshot 写失败应令命令失败，而不是只看 DataFrame 非空。

### EH-08 MEDIUM：用户消息直接包含内部异常或绝对路径

证据：

- `excel_service.py:78` 把原始 Excel 异常传给 `st.error`；
- `excel_service.py:141` 把原始异常放入返回消息；
- `pdf_service.py:36-38` 用户消息包含完整文件路径；
- `pdf_service.py:77-79` 显示原始 PyMuPDF 异常；
- `ppt_service.py:36-38,72-75` 显示路径和 COM 异常。

影响：

- 泄漏本地路径、库实现和 COM 细节；
- 用户消息依赖技术文本，难以本地化和稳定测试；
- 可能暴露环境信息。

建议：

UI 使用稳定 error code：

```text
document_not_found
document_conversion_failed
excel_sheet_missing
```

服务端日志记录路径和 stack；用户只看到安全提示和 request ID。

### EH-09 MEDIUM：裸 `except: pass` 隐藏 cleanup 和数据问题

证据包括：

- `application/pdf_service.py:87`
- `application/ppt_service.py:83,87,91-92`
- `core/sheet_lot/overrides.py:34-35,104,108,111`
- `core/mwd_trend/mwd_trend_processor.py` 多处裸 except；
- `core/defect_modifier.py:118-119` 捕获所有错误后返回原始 ID，完全不记录。

cleanup 失败不一定要覆盖主异常，但至少应：

- 捕获具体类型；
- 低级别记录；
- 不吞 `KeyboardInterrupt/SystemExit`；
- 在需要时把 cleanup 错误挂到主错误上下文。

`defect_modifier` 的静默回退更危险，因为它改变业务结果，应显式统计或警告。

### EH-10 MEDIUM：错误类型和返回协议碎片化

当前错误表达包括：

- `None`
- 空 DataFrame
- 空 dict
- `False`
- `(bool, str)`
- 日志后继续
- `st.error`
- Pydantic `ValidationError`

影响：

- 调用方需要知道每个方法的特殊约定；
- 无法统一 UI、CLI 或未来 HTTP 映射；
- 错误 code 不稳定；
- 测试依赖中文字符串。

建议的最小类型：

```text
YieldValidationError
PanelDataUnavailable
YieldSnapshotUnavailable
YieldResourceInvalid
YieldComputationError
YieldConcurrencyConflict
YieldExportError
```

不建议一次创建过深继承树。

### EH-11 MEDIUM：日志上下文和异常链不一致

部分路径使用 `exc_info=True`，但大量路径只执行：

```python
logging.error(f"...: {e}")
```

当前普遍缺少：

- stable error code；
- product code；
- query range；
- data source；
- snapshot timestamp；
- pipeline stage；
- correlation ID。

建议在最终处理边界使用 `logger.exception`，中间层使用 `raise ... from exc`，避免每层重复 stack。

### EH-12 MEDIUM：故障分支测试覆盖不足

本次执行错误处理相关测试：

```text
9 passed
```

覆盖：

- DTO validation；
- 数据策略；
- snapshot identity；
- policy 应用；
- refresh 基本路径；
- encrypted Excel COM fallback；
- Mapping 原始模式。

未发现对以下关键行为的直接测试：

- DB exception 与成功空数据的区分；
- DB failure + stale snapshot；
- DB failure + no snapshot；
- corrupt Parquet；
- snapshot write failure；
- defect multiplier failure；
- warning file missing vs malformed；
- MWD/Mapping calculation exception；
- lock ownership；
- 非 owner 不得删除 lock；
- 原始异常不得进入用户消息。

错误处理代码只有在故障时运行，缺少这些测试会让 fallback 本身成为风险。

## 5. 错误语义矩阵

| 场景 | 当前表现 | 推荐表现 |
|---|---|---|
| 数据库成功无数据 | 空 DataFrame | 成功空结果 |
| 数据库异常 | 空 DataFrame | `PanelDataUnavailable` |
| DB 异常且快照可用 | 普通 DataFrame | degraded outcome |
| DB 异常且无快照 | 空 DataFrame | typed failure |
| 快照损坏 | 尝试 DB，结果无原因 | `SnapshotReadError` + 明确 fallback |
| 快照写失败 | 返回数据 | partial success warning；刷新命令失败 |
| 缺陷修饰失败 | 返回未修饰数据 | fail 或 explicit degraded |
| Mapping 计算失败 | 空 DataFrame | `YieldComputationError` |
| MWD 计算失败 | `None` | `YieldComputationError` |
| 警戒线缺失 | `{}` | 按 required/optional 契约 |
| Excel 写冲突 | `(False, 中文文本)` | `YieldConcurrencyConflict` |
| 文档转换失败 | `False` + 原异常 UI | `YieldExportError` + safe message |

## 6. 推荐错误层次

```python
class YieldError(Exception):
    code: str
    context: Mapping[str, object]


class YieldValidationError(YieldError):
    pass


class PanelDataUnavailable(YieldError):
    pass


class YieldSnapshotUnavailable(YieldError):
    pass


class YieldResourceInvalid(YieldError):
    pass


class YieldComputationError(YieldError):
    pass


class YieldConcurrencyConflict(YieldError):
    pass


class YieldExportError(YieldError):
    pass
```

原则：

- concrete SQL/COM/OSError 留在 adapter；
- 使用 `raise ... from exc`；
- application error 不包含 Streamlit 或 HTTP status；
- UI/REST adapter 将 `code` 映射为协议；
- 不把内部 exception text 暴露给用户。

## 7. 推荐 Data Outcome

为了保留现有快照降级行为：

```python
@dataclass(frozen=True)
class PanelDataOutcome:
    data: pd.DataFrame
    source: Literal["database", "snapshot"]
    is_stale: bool
    snapshot_time: datetime | None
    snapshot_persisted: bool | None
    warnings: tuple[str, ...]
```

由于 ADR 0001 禁止项目 dataclass 直接跨越 `st.cache_data`，缓存 payload 应为：

```python
{
    "data": dataframe,
    "source": "snapshot",
    "is_stale": True,
    "snapshot_time": "...",
    "snapshot_persisted": None,
    "warnings": ["panel_data_degraded"],
}
```

缓存外再构造 `PanelDataOutcome`。

## 8. 推荐用户消息

| Error code | 用户消息示例 |
|---|---|
| `panel_data_unavailable` | 数据源暂时不可用，请稍后重试。 |
| `panel_data_degraded` | 当前展示历史快照，数据可能不是最新。 |
| `snapshot_refresh_failed` | 快照刷新失败，旧快照未被覆盖。 |
| `warning_line_invalid` | 警戒线配置无效，请联系维护人员。 |
| `yield_computation_failed` | 报表计算失败，请使用错误编号联系支持人员。 |
| `concurrent_modification` | 文件已被其他用户更新，请刷新后重试。 |
| `document_conversion_failed` | 文档转换失败，请确认文件格式后重试。 |

开发者日志另外记录：

- stack trace；
- product；
- query range；
- path；
- adapter；
- snapshot version；
- correlation ID。

## 9. 迁移计划

### Phase 0：修复锁 ownership

这是唯一 CRITICAL，优先级最高。

先写并发 characterization tests，再实现：

- 原子获取；
- owner token；
- `lock_acquired`；
- owner-only release；
- 临时文件 + atomic replace。

### Phase 1：让数据库异常重新可见

1. `data_loader` 不再把技术异常变为空；
2. Repository 捕获 typed adapter error；
3. 在 Repository 统一执行快照 fallback；
4. 明确 DB empty 的业务语义。

不得同时修改 DatabaseManager 单例和重试语义。

### Phase 2：增加 degraded outcome

保留现有 DataFrame 和算法，只增加：

- source；
- stale；
- snapshot time；
- warnings；
- persist status。

### Phase 3：处理静默计算降级

由业务所有者确认：

- defect multiplier；
- warning line；
- Mapping；
- MWD；
- manual override

哪些必须 fail，哪些允许 degraded。

### Phase 4：UI 错误映射

- Application 移除 `st.error`；
- 页面集中映射 error code；
- 用户消息不包含 exception 和绝对路径；
- 加 request ID。

### Phase 5：故障注入与可观测性

- DB unavailable；
- corrupted snapshot；
- disk full；
- COM failure；
- concurrent writer；
- partial chunk；
- calculation failure。

统计：

- fallback count；
- stale data age；
- computation failure；
- snapshot write failure；
- resource parse failure。

## 10. 推荐测试清单

### CRITICAL

- [ ] B 未获取锁时不得删除 A 的锁。
- [ ] 两个 writer 不能同时进入写区。
- [ ] 锁 owner token 不匹配时拒绝释放。
- [ ] 写失败不破坏原文件。

### Data access

- [ ] DB 空结果保持成功空。
- [ ] DB 异常产生 typed error。
- [ ] DB 异常 + 快照产生 degraded outcome。
- [ ] DB 异常 + 无快照显式失败。
- [ ] 单分片失败不伪装完整成功。
- [ ] 快照损坏与不存在区分。
- [ ] 快照写失败进入 warnings。

### Calculation

- [ ] defect multiplier 异常按业务策略 fail/degrade。
- [ ] Mapping 异常不是普通空数据。
- [ ] MWD 异常不是普通 `None`。
- [ ] warning line malformed 不等于 optional missing。

### UI

- [ ] 用户消息不含绝对路径。
- [ ] 用户消息不含 SQL/stack/COM error。
- [ ] degraded 数据有明确标识。
- [ ] error code 对应正确提示。

## 11. 验证结果

执行：

```powershell
uv run pytest -q `
  tests/unit/test_data_loader_batch_sql.py `
  tests/unit/test_yield_repository_data_policy.py `
  tests/unit/test_yield_service_policy_injection.py `
  tests/unit/test_yield_alert_service_policy.py `
  tests/unit/test_excel_override_decryption.py `
  tests/unit/test_mapping_original_pipeline.py
```

结果：

```text
9 passed in 8.63s
```

同时出现 `tool.uv.dev-dependencies` 弃用 warning。

这个结果证明已选现有行为通过，不证明错误处理完善，因为上文列出的关键故障路径没有对应测试。

## 12. 最终判定

```text
ERROR HANDLING REVIEW: NOT APPROVED

CRITICAL:
  Excel lock ownership / non-owner deletion risk.

HIGH:
  DB failures collapse into empty data.
  Degraded snapshot use is invisible.
  Defect modifier failure returns unmodified data.
  Core computation failures look like no data.
  Warning-line failures disable behavior without stable status.
  Snapshot persistence failure is not reflected in refresh outcome.

TESTS:
  Selected existing tests: 9 passed.
  Critical failure-path coverage: missing.
```

最优先动作是修复文件锁 ownership 并增加并发测试。随后应让数据库错误重新传播到 Repository，
在不改变现有降级策略的前提下增加显式 degraded metadata。核心算法和 DatabaseManager 重试语义
不应在同一改造中被重写。

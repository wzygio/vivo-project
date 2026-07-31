# adr-0001：Streamlit 数据缓存只跨越稳定载荷

- Status: Accepted
- Date: 2026-07-14
- Scope: `app/pages/` 可达的页面数据缓存与应用服务 ViewModel

## Context

项目使用 `st.cache_data` 缓存页面报表数据，并通过页头“刷新缓存”功能清除缓存、从 `sys.modules` 卸载项目模块，再 rerun 页面。

`st.cache_data` 会 pickle 缓存函数的返回值。当缓存函数直接返回项目自定义 dataclass/Pydantic 对象时，缓存计算与模块重载并发会产生以下竞态：

1. 缓存计算开始，并持有重载前的类对象；
2. 另一个会话或刷新操作卸载并重新导入该模块；
3. 缓存函数返回旧类的实例；
4. pickle 通过模块路径解析到重载后的同名新类；
5. 因两个类对象身份不同而抛出 `UnserializableReturnValueError`。

该问题已在 CPM 监控报表和关键备件报表中通过“缓存填充期间重导入服务模块”的线程回归测试稳定复现。普通 rerun 能恢复，是因为新的计算与新的类身份重新对齐，但不能消除并发窗口。

自动预警服务已经采用缓存原生 dict、缓存外组装 `SpcDashboardViewModel` 的安全模式。

## Decision

所有 `app/pages/` 可达的 `st.cache_data` 边界遵循以下规则：

1. 缓存函数只返回跨项目模块重载仍稳定的载荷：
   - pandas DataFrame；
   - dict、list、tuple 等原生容器；
   - 字符串、数字、布尔值和 `None`；
   - 路径以字符串形式存入缓存载荷。
2. 项目定义的 dataclass、Pydantic 模型及其他 ViewModel 不得直接作为 `st.cache_data` 返回值，也不得嵌套在缓存载荷中。
3. 应用服务采用两层接口：
   - 可被 `st.cache_data` 修饰的 payload 函数执行昂贵查询和计算；
   - 不带缓存装饰器的公开 facade 读取 payload，并使用当前模块中的类实时构造 ViewModel。
4. payload 缓存函数需要保持为可被 `extract_cached_funcs` 发现的公开方法，或者由页面显式注册，以保证“刷新缓存”仍能清除实际 L2 缓存。
5. DataFrame 报表继续使用 `st.cache_data`。不得为规避 pickle 而改用 `st.cache_resource`，因为后者会把可变对象作为跨会话共享实例。
6. 新增或修改此类服务时，必须有模块重载发生在缓存填充期间的回归测试，而不只测试冷态 `pickle.dumps`。
7. 产品级页面必须把共享产品缓存版本写入 `snapshot_signature`。页头“刷新缓存”
   只推进当前产品版本，不调用无参数 `func.clear()`，从而避免清除其他产品的缓存条目；
   `ALL` 聚合页面和无产品分区页面继续使用全量清理。

当前实现：

- CPM：`fetch_cpm_report_payload()` 缓存原生载荷，`get_cpm_report_data()` 在缓存外构造 `CpmReportViewModel` 和 `SheetOosDecorationResult`。
- 关键备件：`fetch_report_payload()` 缓存 DataFrame 与统计标量，`get_report_data()` 在缓存外构造 `PartsReportViewModel`。
- 自动预警：继续沿用既有的 dict 缓存与缓存外 ViewModel 组装模式。
- 良率及其他页面：当前缓存返回 DataFrame、原生 dict、tuple 或标量，无需迁移。
- SPC、CTQ 与良率产品页面：使用 `output/tmp/product_cache_revisions/`
  下的共享版本文件生成产品级缓存签名；版本提升后仅该产品产生缓存 miss。

## Consequences

### Positive

- 消除项目类身份与模块热重载之间的 pickle 竞态。
- 保留 `st.cache_data` 的缓存键、数据副本和跨 rerun 性能收益。
- 页面继续消费强类型 ViewModel，不需要解析缓存内部 dict。
- 业务计算、CPM/CPK 口径、关键备件预警逻辑和页面渲染契约保持不变。
- 原生 payload 更容易单独审计、序列化和版本兼容。

### Negative

- 每个强类型报表服务需要维护 payload 与 ViewModel 之间的映射。
- payload 字段增删时必须同步更新 facade 和测试。
- 缓存清理针对 payload 方法，而不是 ViewModel facade；方法命名或注册错误会造成刷新按钮遗漏缓存。
- 缓存命中后会进行一次轻量 ViewModel 构造，但其成本相对于查询和 DataFrame 计算可以忽略。

## Alternatives considered

### 使用 `st.cache_resource`

Rejected。它避免 pickle，但返回跨用户共享的可变实例，改变 DataFrame 报表的复制与隔离语义，并引入线程安全和会话串扰风险。

### 禁用项目模块卸载

Rejected as the primary fix。它会削弱现有热重载和缓存刷新机制，也不能防御部署更新、测试重载或其他模块替换场景。稳定缓存边界应独立于模块生命周期。

### 让页面直接消费 dict

Rejected。虽然可序列化，但会把服务内部 payload 结构泄漏给页面，扩大页面与计算服务的耦合。

### 修改 dataclass 选项或配置 `hash_funcs`

Rejected。`frozen`、`slots` 等选项不能解决模块重载后的类身份差异；`hash_funcs` 控制缓存参数哈希，不控制返回值 pickle。

## Verification

- CPM 模块重载竞态回归：通过。
- 关键备件模块重载竞态回归：通过。
- 页头缓存函数发现契约：通过。
- 页头产品版本隔离测试：刷新一个产品不会改变另一产品的缓存签名，也不会执行
  整函数缓存清理。
- 页面相关集中回归：`60 passed`。
- 扩展 unit 回归：`124 passed, 2 failed`；两个失败均为本变更前已存在、与缓存边界无关的 Shadow EMA 测试。

## References

- [Streamlit `st.cache_data`](https://docs.streamlit.io/develop/api-reference/caching-and-state/st.cache_data)
- [Streamlit `st.cache_resource`](https://docs.streamlit.io/develop/api-reference/caching-and-state/st.cache_resource)

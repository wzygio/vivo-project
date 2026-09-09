# 数据健康状态与应用出站端口

适用范围：Yield、Q-Time 数据健康传递，以及 Inline 修饰、Yield/Equipment 数据读取的应用边界。核验日期：2026-09-09。

## 数据契约

`src/shared_kernel/data_health.py` 定义原生字典契约：

| 字段 | 含义 |
|---|---|
| `status` | `fresh` 已成功读取或在有效快照复用范围内；`stale` 本次失败后沿用旧事实；`unavailable` 无可用来源；`unknown` 无可验证健康元数据；`partial` 为显式部分数据状态，当前整窗口读取不发布该结果 |
| `source_start` / `source_end` | 原始数据覆盖时间，保持源时间；各仓储原有闭/半开窗口语义不被统一字典改变 |
| `refreshed_at` | 最后一次成功读取的时间；降级不会改写为当前时间 |
| `error_code` | 不包含 SQL、凭据或驱动消息的稳定错误标识 |

仓储兼容返回 DataFrame 时将字典放入 `attrs['data_health']`。应用在会改变 DataFrame 结构的处理前提取元数据，在显式结果字段和缓存 payload 中保存；不能依赖 `concat`、merge、修饰函数自动保留 attrs。没有 metadata 的旧适配器结果是 unknown，不自动推断 fresh。

合法空窗口仍可 fresh；不可用不是空成功。Yield 的读取异常为安全 `YieldSourceReadError`，一个分片失败就中止整窗口；仓储可返回带 stale 的旧快照，或 unavailable。应用 raw facade 对 unavailable 抛安全错误，健康 facade 转为原生 unavailable 描述，使失败不进入成功数据缓存。新 Yield 快照保存完整窗口元数据，合法空快照可复用；旧版快照命中 TTL 时仍可显示，但健康状态为 unknown，成功重取后才具备完整健康信息。

数据库与源快照保留源时间，日期前推在仓储输出时应用。Q-Time 普通查询明确区分有效快照复用与失败回退；即使筛选后没有记录，降级 metadata 仍保留。

## 展示与预警

Yield 三个页面显示状态；主看板预警中心接收 `source_current`，只有相关来源全部 fresh 才允许“系统监测正常”。旧记录中已发现的异常可继续展示，但标为历史结果。

Q-Time 普通查询的健康信息经过修饰、厂别级 L2 缓存和内存产品/站点过滤。旧快照没有异常时不生成当前正常结论；旧快照为空时不显示“当前没有数据”的成功提示。手动刷新失败会把已展示的会话结果降级；重新查询失败会清除旧成功结果；跨日保留的会话必须重新查询。

Q-Time 三厂合并保留 `data_health_by_shop`，并生成合并状态。任一厂陈旧、未确认或因无站点被跳过时，聚合预警保守地显示未知，不假设缺失厂别没有异常。此规则可能让有完整局部数据的产品也显示未知，取舍是避免在缺少完整厂别覆盖时给出绿色正常灯。普通页面继续展示可用旧明细。

## 端口与装配

- Yield `YieldDataPort` 拥有 Panel、Array 时间与 rate override 读取；`report_data_adapter` 接入现有仓储。
- Equipment `PartsDataPort` 拥有规格、真实/仿造快照及刷新，不改变真实优先、TTL或仿造策略。
- Inline 消费方分别定义 Sheet 决策读写、能力修饰、决策签名和资源路径协议；生产可用一个无状态 Excel 适配器实现组合协议。Throughput 持久化也经单独消费方端口。
- 修饰错误与跨用例结果类型由 application 拥有，基础设施可再导出旧名称维持兼容。
- 新用例显式注入端口。旧静态/自由函数使用受控默认 resolver 调用 composition，不在多个应用函数中构造具体仓储。

默认 resolver 是过渡兼容措施，不表示严格消除所有应用外向依赖。精确例外由 `tests/architecture/test_backend_dependencies.py` 登记，并检查新增越界和已迁移却未删除的过期例外；Core 出向导入及跨域私有访问有负向样例。该测试是静态 import 守卫，不声称检测任意动态导入或文件 I/O 调用。

## 缓存与验证

遵守 ADR-0001：缓存只保存 DataFrame、原生容器及标量；Q-Time 每次调用缓存后读取当前模块的结果类，在缓存外重建，包括 cache hit。健康 metadata 不嵌套 dataclass。

`shared_kernel/cache_ports.py` 仅在显式传入 `_data_port` 时绕过共享 Streamlit 数据缓存，默认生产调用继续缓存并保留 `clear` 与缓存发现元数据。这样两个内存端口使用相同业务参数时不会串用结果。Inline 显式签名端口采用同样隔离原则。

重点证据入口：`tests/unit/test_yield_data_health.py`、`test_report_data_ports.py`、`test_yield_alert_health.py`、`tests/unit/indicator_domain/application/qtime/test_cached_monitoring.py`、Q-Time dashboard AppTest、Inline fake 端口测试及 `tests/architecture/`。首次导入的循环风险使用独立 Python 进程验证，不能只依赖预加载模块后的整套 pytest。

## 有意排除

本次不增加台账审计/版本冲突、身份权限、原子文件发布升级、CI 平台或业务算法重写。以上是用户暂缓的 H2/H3 或仅解释的 H5，不能因本契约自行扩大范围。

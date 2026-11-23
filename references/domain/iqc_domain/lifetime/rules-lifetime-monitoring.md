# IQC 寿命监控与取数规则

核验日期：2026-11-23。适用范围：IQC 寿命测试报表的监控维度与数据新鲜度。

## 监控维度

| 监控名称 | M3 字段 | 展示口径 |
| --- | --- | --- |
| 效率衰减 | `eff_decay` | 保留源比例 |
| 亮度衰减 | `lumi_decay` | 保留源比例 |
| JNCD | `jncd` | 数值转换后直接展示源值，不进行衰减换算或合格判定 |

每个产品/批次下依次展示三个默认展开的监控折叠区。效率衰减和亮度衰减按 W/R、G/B 顺序排成 2×2，每行两个测试画面；JNCD 仅绘制测试画面为 W 的曲线，沿用相同网格中的首个位置。没有 W 记录时，JNCD 区显示“该组暂无 W 画面测点。”；W 的 JNCD 全空时沿用无有效测点提示。三类复用同一个绘图函数，样品颜色、图例、数值测试时间轴、100 间隔刻度及悬浮信息样式一致；纵轴和悬浮信息标明当前指标。

JNCD 同时进入公开明细表和原生 CSV 导出。空值、缺测标记、非数值及无穷值保留为空白；曲线不跨越缺测点连线。单个测试画面的某指标全空时，仅该指标显示无有效测点提示，其它指标继续展示。不添加未经确认的单位和判定阈值。

匿名编号、分组、空批次与冲突测点规则沿用 [ADR-0030](../../../../docs/ADR/0030-iqc-lifetime-anonymous-m3-boundary.md)。原始样品标识不进入页面、图表、明细或导出。

## 1200H 预测与规格点评估

每个产品、批次、画面内按匿名样品和指标分别计算。100、200、300、400H 各需要一个有限实测值。以这四个时刻和实测值做带截距的普通最小二乘线性回归，取斜率 `k`。虚线从 400H 实测值出发，1200H 端点为 `y400 + 800 × k`。缺少任一回归点时跳过该预测。原始测点及明细保留源值。

规格工作簿取自《北极星寿命测试规格》右侧 800nit 表的 W-Spec、R-Spec、G-Spec、B-Spec，单位为小时。产品型号按全局产品注释映射，含 CPD2571、CPD2559 的屏体复用。展示和巡检均使用启用产品范围。只对具有对应规格的效率衰减与亮度衰减评估，JNCD 单独展示。

每个规格时间优先使用同一样品、同一指标的精确实测值。落在有效实测点之间时做线性插值，超出实测范围的后续时刻用四点回归斜率从 400H 实测值外推。规格时间线上以菱形标记样品点，悬浮时显示数值和取值方式。阈值判断使用数值本身，严格小于 0.95 产生一条预警，等于 0.95 不触发。没有对应规格或无法估值时跳过该项。

每日 `api.inspection` 计算并发布完整启用产品范围的预警。报表预警中心与自动预警看板使用同一份已发布结果。规格文件与结果文件路径由 `config/global.yaml` 中 IQC 资源注册表管理。

## 取数与缓存

首次打开页面展示预警中心、三个联动筛选框和“查询”按钮，不自动应用条件或展示曲线、明细。筛选框均留空表示全部；首次修改选择也不展示结果，点击“查询”后才应用当前条件并展示结果。已有查询结果时，后续选择作为草稿，直到再次点击“查询”才应用。

会话只将显式查询条件保存为 `iqc_lifetime_submitted_filters`，不采用旧版本首次自动生成的 `iqc_lifetime_applied_filters`。页面仍读取公开载荷以生成产品状态和批次号选项；该显示门控不改变数据缓存与刷新策略。

- `LifetimeRepository.read_report()` 在 M3 只读事务内先核对基础表容量和采集时间字段，再分批读取截至查询时刻的测点，并保留历史采集时间为空的记录。`ext_time` 是采集时间，独立于测试持续时间。超过保护上限时整次读取失败，避免发布截断样本。
- `LifetimeReportService.get_report()` 读取仓储并生成公开投影，服务层没有结果缓存；M3 engine 的 `lru_cache` 只复用连接池对象，不缓存查询结果。
- 默认 composition 从 `ConfigLoader.get_enabled_products(strict=True)` 注入启用产品范围；Core 在字段完整性检查后、身份/时间/冲突校验及匿名编号前过滤范围外记录。空产品和未启用产品不参与校验，也不进入公开缓存；启用产品的完整组仍执行原有严格校验。显式空范围返回完整列结构的空表；直接构造且不传范围的服务保留全源校验，供受控调用使用。
- 默认页面入口经过 `fetch_report_payload()` 的 `st.cache_data`。它只缓存匿名公开 DataFrame，`max_entries=1`，没有启用磁盘持久化；有效期来自 `config/global.yaml` 的 `application.cache_ttl_hours`（核验时为 12 小时）。这是内存结果缓存，因此页面并非每次交互实时读库。
- 页面将启用产品元组作为缓存参数传入，并将相同范围传给服务；配置变化重新生成对应公开载荷，避免复用先前范围的缓存导致新启用产品缺失。页面仍在渲染前复核启用范围。
- “查询”按钮应用筛选条件，不清除数据缓存。缓存过期后下一次页面运行，或页头“刷新缓存”清理本页注册函数后，才重新读库。页头刷新入口遵循既有管理展示模式；本页不注册快照刷新任务。
- 服务注入路径绕过默认页面缓存，用于受控验证。读取失败不缓存失败结果；页面显示安全业务提示，不用旧快照或示例替代。

项目部分刷新代码将 Streamlit 结果缓存称为 L2。确认数据新鲜度时应区分“文件快照”和“内存结果缓存”：寿命原始测点使用上述内存结果缓存，预警结果另行发布为 Excel 快照。

## JNCD 初次接入验证

2026-10-13 聚焦验证：79 项通过，包括 JNCD 数值投影与缺测、三类曲线、空集/失败恢复、筛选与匿名边界、缓存命中与清除后重读、M3 实库逐测点对账、页头缓存刷新及架构检查。

```powershell
$env:IQC_LIFETIME_LIVE_DB='1'
.venv/Scripts/python.exe -m pytest tests/unit/test_iqc_lifetime.py tests/unit/test_iqc_lifetime_repository.py tests/e2e/test_iqc_lifetime_apptest.py tests/integration/test_iqc_lifetime_live.py tests/unit/app/components/test_page_header_product_cache.py tests/architecture -q --tb=short
```

受控浏览器脚本 `tests/e2e/iqc_lifetime_states.js` 通过三类曲线、同样品同色、四图同排、折叠、缺测断线、筛选、空集及失败恢复验证。原生 CSV 验证包含 109 条公开记录、16 列、正确 JNCD 数值与 36 个缺测空白。1365、768、390px 宽度无页面横向溢出；截图、导出和日志位于 `output/test-results/iqc-lifetime-jncd/`。

本次未运行完整业务回归及实库浏览器脚本；实库字段对账由上述只读集成测试验证。

## JNCD 仅绘制 W 画面的追加验证

2026-10-13 后续需求仅限制 JNCD 的绘图范围；公开明细和 CSV 仍保留全部测试画面的公开测点。聚焦测试 35 项通过，覆盖 W 专属曲线数据、无 W 记录、W 的 JNCD 全空、两类衰减曲线、明细完整性及已有筛选、空集/失败恢复规则。

验证命令：`.venv/Scripts/python.exe -m pytest tests/e2e/test_iqc_lifetime_apptest.py tests/unit/test_iqc_lifetime.py tests/unit/test_iqc_lifetime_repository.py -q --tb=short`。

已同步浏览器脚本的画面范围和图数预期；本轮未重跑浏览器、实库或完整业务回归。

## 2026-11-03 空产品记录导致整页失败的修复

服务端报错为 `LifetimeDataInvalid / LIFETIME_IDENTITY_MISSING`。先经只读元数据探查确认源表规模较小，再读取核验：1,871 条源记录中有 1 条产品型号为空；其余 1,870 条可通过原有投影校验。原流程在页面过滤启用产品之前校验全部源记录，导致范围外空产品记录阻断整页。现按上述启用范围先过滤再校验，保留启用产品的身份、时间及冲突测点失败规则，源表不写入。

修复前，composition 回归用例对 NULL、空字符串、纯空白和禁用产品四类范围外异常记录均失败；修复后实库服务成功返回 1,870 条、16 列公开数据。以下聚焦验证共 95 项通过，包含实库逐测点对账、默认 composition 到页面查询的 AppTest、空范围、范围内异常拦截、配置变化缓存失效、页头刷新及架构检查：

```powershell
$env:IQC_LIFETIME_LIVE_DB='1'
.venv/Scripts/python.exe -m pytest tests/unit/test_iqc_lifetime.py tests/unit/test_iqc_lifetime_repository.py tests/e2e/test_iqc_lifetime_apptest.py tests/integration/test_iqc_lifetime_live.py tests/unit/app/components/test_page_header_product_cache.py tests/architecture -q --tb=short
```

实库对账测试新增只读元数据规模门控，拒绝未知或超出上限的全量核验。本轮未启动临时服务，也未重跑浏览器脚本或完整业务回归。

## 首次展示由查询触发的追加验证

2026-10-13 移除首次自动应用空筛选条件的逻辑。聚焦测试 37 项通过，覆盖首次无结果、修改筛选仍无结果、显式查询、空条件查询全部启用产品、后续筛选按查询应用、旧会话自动条件不触发展示及现有 JNCD/明细/失败恢复规则。

验证命令：`.venv/Scripts/python.exe -m pytest tests/e2e/test_iqc_lifetime_apptest.py tests/unit/test_iqc_lifetime.py tests/unit/test_iqc_lifetime_repository.py -q --tb=short`。本轮使用受控内存数据，未启动临时服务、查询实库或运行浏览器/完整业务回归；浏览器脚本已同步查询前后的预期。

# Task2：蒸镀材料产品关联与权限检查

- 日期：2026-10-13。
- 状态：Task2 已实现并通过聚焦测试、独立实库对账与浏览器验收。
- 需求：[Task2：报表优化](task-蒸镀材料报表开发.md#task2报表优化)。
- 参考：[V3坩埚加料与回收 SQL](../../../../project_files/iqc_domain/eva_materials/V3坩埚加料&回收报表-sql语句.md)、[Mapping 数据源](../../../../../src/yield_domain/infrastructure/data_loader.py)。

## 1. 已确认的关联来源

### 物料号 → 蒸镀批次

提供的坩埚报表从 `mdw.dwr_mes_ct_cruciblelothistory` 读取：

- `PLANORGANICSPEC`：物料规格名，对应需求中的物料号。
- `CYCLETIME`：蒸镀周期，对应需求中的蒸镀批次。
- `CRUCIBLELOTNAME`：与物料履历、回收记录关联的坩埚批次标识。

原 SQL 只保留 `l.EVENTNAME = 'KitBatchCrucible'`、`l.MACHINENAME != '3CEE03'`、非空 `l.CYCLETIME`，并要求物料履历 `d.EVENTNAME = 'KitBatchCrucible'`。虽然物料履历写为 LEFT JOIN，后续 WHERE 条件实际要求存在符合事件条件的履历。

`mdw.dwr_mes_ct_recoverymaterial` 通过 `CRUCIBLELOTNAME` 与 `MATERIALSPECNAME = PLANORGANICSPEC` 提供回收重量；它不提供物料号或蒸镀批次字段。当前筛选查询省略该 LEFT JOIN；实库验收使用完整原坩埚 SQL 作为独立物料／批次集合的参考，确认省略辅助重量关联没有改变所选物料集合。

### Mapping 查询参考与当前蒸镀批次 → 产品型号

Mapping 的 `load_panel_details()` 使用以下链路：

1. `mdw.dwt_yield_result_pnl` 的 `SUB_PROD_ID` 关联 `mdw.dwr_mes_productrequest.PRODUCTREQUESTNAME`。
2. 从 `DESCRIPTION` 开头提取日期，转换为 `YYYY/MM/DD`，形成 `batch_no`；只使用 `oper_group = 'CT'`，并按产品及工单类型限定。
3. 通过 `panel_id` 与 `mdw.dwt_warehousing_pnl` 关联。
4. `dwt_warehousing_pnl.PROD_ID` 关联 `mdw.dwr_mes_productspec.PRODUCTSPECNAME`，取 `PRODUCTCODE` 作为产品型号；入库数据限定 `last_flag = 'Y'` 及配置的工单类型。

这是 Mapping 以已入库 Panel 为主体的查询口径。实库发现坩埚 `CYCLETIME` 为 `YYYYMMDD`，需把工单描述日期也归一化为 `YYYYMMDD`；直接拿 Mapping 的 `YYYY/MM/DD` 与它相等匹配会失败。

用户进一步确认产品范围采用**工单主数据，包含尚未入库工单**。当前产品关系直接使用 `dwr_mes_productrequest.PRODUCTSPECNAME → dwr_mes_productspec.PRODUCTSPECNAME → PRODUCTCODE`，并按 `SUBPRODUCTIONTYPE` 限定全局工单类型。保留 Mapping 的日期前缀提取规则，不增加 CT／入库 Panel 存在性限制，也不套用入库日期窗口。该选择是明确的业务范围决定，并非声称两种范围等价的性能重写。

批次、物料、产品均可多对多，物料关联任意所选启用产品即可进入结果；按物料集合过滤原检验明细，不展开关联组合。

## 2. 数据表职责与权限实测

使用既有 `DatabaseManager`，在只读事务内核查元数据和 `SELECT <相关字段> ... LIMIT 0`。各表检查使用独立保存点；没有写入数据库，也没有输出账号、连接信息、检验数据或 Panel 标识。

| 表（均为 `mdw` schema） | 作用 | 结果 |
|---|---|---|
| `dwr_mes_ct_cruciblelothistory` | 蒸镀周期、物料规格及上机条件 | 相关字段 SELECT 通过 |
| `dwr_mes_ct_recoverymaterial` | 回收重量及回收记录关联 | 首轮 `42501`；用户开通后复查 SELECT 通过，完整原报表对账通过 |
| `dwr_mes_consumablespec` | 物料分类和规格描述 | 相关字段 SELECT 通过 |
| `dwr_mes_consumablehistory` | 上机物料履历、物料批号 | 相关字段 SELECT 通过 |
| `dwr_wms_tblreclabel` | 物料批号到供应商批号 | 相关字段 SELECT 通过 |
| `dwt_yield_result_pnl` | CT Panel 与工单、产品关系 | 相关字段 SELECT 通过 |
| `dwr_mes_productrequest` | 工单描述中的蒸镀批次日期 | 相关字段 SELECT 通过 |
| `dwt_warehousing_pnl` | Panel 到产品规格的对应关系 | 相关字段 SELECT 通过 |
| `dwr_mes_productspec` | 产品规格到产品型号 | 相关字段 SELECT 通过 |

首轮发现回收表缺 SELECT 权限，按需求中断；用户开通后 9 张表相关字段复查均通过。`LIMIT 0` 仅证明被检查字段的访问；后续完整原坩埚 SQL 的真实执行另验证了其所需字段可读取。

复查命令：`.venv\Scripts\python.exe output/tmp/probe_evaporation_product_sources.py`。
本轮权限证据：`output/tmp/evaporation-product-source-access.json`。两者属于本地临时诊断产物。

## 3. 当前实现契约

- 需求明确改变当前“V3／有机、产品型号为通用、无产品筛选”的约定。现有规则见 [ADR-0029](../../../../ADR/0029-iqc-evaporation-source-contract.md)；实现完成后应同步更新该规则、项目约束及对应知识路由。
- 新产品范围来自蒸镀批次关系，不能对 WMS 源 `EATTRIBUTE10` 的“通用”直接执行 enabled-products 过滤，否则会错误删除有机材料。
- 产品选项及默认全部范围由 `ConfigLoader.get_enabled_products()` 控制，顺序保持配置顺序；空选表示全部启用产品。标签注释复用 `app/components/product_labels.py`，查询和缓存使用原始产品代码。
- 在进入公开表格、缓存与原生 CSV 导出前限定合法产品对应的物料；不能仅限制下拉选项。
- `CYCLETIME` 和工单日期统一为 `YYYYMMDD`。物料／产品成员关系使用 DISTINCT，事实按物料号 `.isin` 过滤；原明细的目录展开和重复保留，关联的重复不能新增检验行。
- 保持 V3／有机范围、测点 CASE 判定、缺失测点不填值、检验日期窗口、前推及当日截止语义。更改产品范围必须参与缓存身份，并清理已停用产品选择及旧结果。
- 关联权限或读取失败应明确失败，不回退到未过滤的旧全量报表、示例数据或成功空集。

## 4. 实现与验证（2026-10-13）

### 实现

- application 对所选产品与启用范围取交集，空选择代表全部启用产品；空有效范围不读取数据库。
- infrastructure 在同一只读事务内读取 WMS 检验事实和当前物料的工单主数据关联，再按物料并集筛选；全局工单策略由 composition 注入。产品、物料、工单集合参数均绑定，不导入 Yield 私有实现。
- 页面增加带注释产品多选，保留日期与查询门控、原生 27 列表格／CSV；更换产品或配置后隐藏旧结果。WMS 产品属性“通用”仍原样展示。
- 缓存包含有效产品、启用范围、工单策略、时间策略与产品范围规则版本；配置与读取异常用安全业务提示。严格启用配置为空时没有产品范围，不回退默认 M678。其它既有 ConfigLoader 调用的兼容行为未改变。

### 实库证据

- 全产品 CT 明细计数在 45 秒超时；完整 Panel 关联查询以及仅按 7,050 个相关工单绑定的 CT 查询在 60 秒超时，未作为生产降级路径保留。
- 工单主数据关联原型约 1.42 秒；当时 WMS 740 条、48 个物料，启用产品关联范围为 722 条、42 个物料。
- 正式应用服务冷读取实测约 5.69 秒（包括连接、事实读取、关联、投影）；当前窗口 722 条、27 列。耗时为单次观察，不是性能保证。
- `IQC_LIVE_DB=1` 对账测试：1 passed，47.07 秒。独立执行用户完整原坩埚 SQL，再关联工单／产品主数据，核验物料集合；同时执行原 WMS SQL 与 PostgreSQL 测点 CASE，对全部启用产品及单产品的 26 个业务字段逐行多重集比对。
- 真实浏览器 CSV：全部启用范围 722 行，M626 710 行，均为 27 列；查询切换隐藏旧结果、标签注释、结果列及完整导出通过，1365／768 视口无横向溢出。

### 自动化与受控浏览器

- 聚焦 pytest：62 passed，覆盖测点 CASE、NULL、严格配置、源端口产品范围、只读时间窗口、关联失败、物料集合去重、缓存版本、AppTest 和架构边界。
- 共享配置接口兼容回归另有 52 passed：产品标签、共享页头缓存及增量刷新配置；未改变其它页面非严格加载的既有默认行为。
- 关系型夹具：同周期多个产品和物料、重复坩埚／履历、两种日期位数、无日期工单、非上机事件、排除设备、未入库工单及未启用型号；不创建任何 CT／入库表，证明最终查询不依赖它们。
- 受控浏览器：205 条全集、M626 65 条（包含 5 条共用物料），产品切换／CSV、失败／恢复／空集通过；1365／768／375 视口通过。原生保存 API 仅由测试接管输出流，应用导出逻辑未变。
- 数据库与安全评审复审通过，无未关闭的高风险问题；配置失败与空配置默认型号两项发现已修复并加入回归。

命令：

```powershell
.venv\Scripts\python.exe -m pytest tests/unit/test_iqc_evaporation.py tests/unit/test_iqc_evaporation_repository.py tests/unit/test_iqc_evaporation_cache.py tests/unit/test_iqc_evaporation_product_scope.py tests/unit/test_iqc_evaporation_product_config.py tests/integration/test_iqc_evaporation_sql.py tests/integration/test_iqc_evaporation_product_sql.py tests/e2e/test_iqc_evaporation_apptest.py tests/architecture/test_backend_dependencies.py -q
$env:IQC_LIVE_DB = '1'
.venv\Scripts\python.exe -m pytest tests/integration/test_iqc_evaporation_live.py -q
playwright-cli -s=iqc-task2 run-code --filename=D:/wzy/Python/vivo-project/tests/e2e/iqc_evaporation_states.js
playwright-cli -s=iqc-task2-live run-code --filename=D:/wzy/Python/vivo-project/tests/e2e/iqc_evaporation_report.js
```

浏览器工作目录／截图：`output/test-results/iqc-evaporation-task2/`。当前规则见 [产品范围规则](../../../../../references/domain/iqc_domain/eva_materials/rules-evaporation-product-scope.md)；规格及任务状态：`.scratch/iqc-evaporation-product-scope/`。

未运行其它领域全量业务回归；无数据库写入、来料批号到实际消耗的归属推断、回收重量新增列或关联异常的未过滤回退。没有提交／合并本工作区其它用户改动。

## 5. 产品型号与蒸镀周期展示优化（2026-10-13）

用户后续要求“产品型号”与筛选框对应，并在其后增加蒸镀批次（蒸镀周期）。本节替代第 4 节的“通用”展示及 27 列约定；历史实库结果保留为当时的证据。

关联 SQL 增加输出 `batch_no`，adapter 按当前所选产品聚合产品代码及周期，映射回物料检验事实；不扩大检验行数。公开字段顺序为“序号、产品型号、蒸镀批次、量产/非量产、…”（28 列），表格与 CSV 使用相同投影。产品范围缓存版本更新为 `request-master-cycle-v2`。当前字段语义与多值规则见 [产品范围规则](../../../../../references/domain/iqc_domain/eva_materials/rules-evaporation-product-scope.md)。

验证命令沿用第 4 节聚焦 pytest，并加入 `tests/integration/test_iqc_evaporation_live.py`：**68 passed，1 skipped**；实库测试未设置 `IQC_LIVE_DB=1`，本次没有重新查询数据库。覆盖所选产品对应、多个周期去重及排序、重复检验事实保留、空源／空关联／空物料、批次缺失失败、SQL 关联、结果规则、缓存、AppTest 与架构边界。

受控浏览器运行 `tests/e2e/iqc_evaporation_states.js`：205 条全集、M626 单选 65 条，公开产品均为 M626，周期均为 `20260802`；验证 28 列 CSV、失败／恢复／空集及 1365／768／375 视口通过。截图和执行结果位于 `output/test-results/iqc-evaporation-cycle/`。本次临时服务进程身份及子进程已记录，测试后核验进程退出和 8518 监听释放；既有正式监听保持不变。修改文件的 `git diff --check` 通过；全工作区检查发现既有任务文档的空格问题，未更改该无关内容。

## 6. 删除批次字段与默认日期调整（2026-10-13）

用户进一步要求删除“蒸镀批次”字段，并将“检验日期范围”默认值改为上个月 1 日至今天。本节替代第 5 节的批次展示约定。公开表格、缓存及原生 CSV 均恢复 27 列；产品代码对应所选范围的规则继续保留。蒸镀周期仍用于内部关联，缓存版本更新为 `request-master-cycle-v3`。

默认日期按日历月份计算，例如 2026-10-13 的初始范围为 2026-09-01 至 2026-10-13。聚焦 pytest **70 passed**，包括跨年、闰年、手动改期后的查询门控、空结果和失败提示；本次未运行实库对账。受控浏览器验证 27 列 CSV 不含批次字段、产品切换、失败／恢复／空集及 1365／768／375 视口通过。产品切换的浏览器检查等待表格移除后再断言，避免 Streamlit 渲染期间的竞态。

证据位于 `output/test-results/iqc-evaporation-default-range/`；临时服务全部所属进程退出及 8518 监听释放已独立核验，既有正式服务保留。

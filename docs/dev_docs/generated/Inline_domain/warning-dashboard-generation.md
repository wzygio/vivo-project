# 自动预警看板数据处理逻辑

更新日期：2026-09-08。入口：`app/pages/自动预警看板.py`。

## 查询与刷新

Inline 超规片和 CPK 各有独立筛选及“查询”按钮。首次打开不生成图表，筛选变化后需重新查询。
Inline 查询不访问数据库、不读取原始 Parquet、不触发规格判定或全年量测补载。
Q-Time 独立运行，仅其查询需要时初始化数据库。

`st.cache_data` 缓存 Excel 解析和展示 payload，TTL 读取全局配置。
文件路径、大小、修改时间参与输入签名；管理员“刷新缓存”清除解析与展示缓存并收起查询状态。
再次查询即可读取最新文件，无需删除 OOS/OOC 快照。

只改规格而未重新生成超规明细，读到的仍是旧明细。规格判定由对应报表负责，不由看板隐式重算。
直接修改明细产品 sheet 的 `flag` / `Flag` 后刷新缓存则直接生效。
各产品、scope 的明细生成时间仅在 `?admin=true` 时显示。页面不再逐条展示汇总数据缺失的黄色 Warning；
缺失数据保留原值的保护规则不变，诊断信息仍保留在状态数据中，真正的查询异常仍显示错误。
超规时间趋势使用柱状图，直接复用汇总表的周期数值：当年1列、当年Q1至当季、最近3个月、最近4个ISO周（含当前月/周），按此顺序展示，不按天展开，也不从近期明细推算历史。
趋势图缺失周期按0展示并保留横轴位置，仅在绘图副本中补零，不回写汇总Excel；即使明细为空，全部固定周期仍可展示。Top10站点按数量降序沿横轴排列、柱体向上，固定保留10个位置，不足部分留空。
趋势柱体按周期层级着色：年为蓝色、季为橙色、月为绿色、周为紫色；悬停显示报警类型与数量。
横轴显式保留完整周期序列（例如2026-09-08为W34、W35、W36、W37四周）。
页面payload缓存包含周期布局版本，避免下层周期规则变化后继续复用旧的一周布局。

## Excel-only 数据来源

```text
其他 Inline 报表的超规明细 Excel ── 当前周 flag=False ─┐
                                                     ├─ 周替换、当前月/季/年差额更新
用户汇总 Excel“报警率”sheet ── 历史、当前基线、过货量 ─┘
                                                     ↓ 保存并回读
                                              年/季/月/周报警汇总

用户汇总 Excel“CPK”sheet ───────────────────── 年/季/月/周 CPK 汇总
```

超规明细按 `resources/inline_domain/<scope>/` 的配置路径读取：

- OOS：`*_sheet_oos_decoration.xlsx`。
- OOC：`*_sheet_ooc_decoration.xlsx`；缺文件不代表零报警，不自动运行对应模块补建。
- 按产品名称读取 sheet；以产品 sheet 的 `flag=False` 为准，不再合并 `__flags`。
- 文件或产品 sheet 缺失：提示并保留对应类型汇总值；合法空 sheet 可以作为零条结果。
- 字段、产品、日期不合法：显示查询错误，不回退到原始快照分析。

汇总文件：`resources/inline_domain/monitor/北极星报警率与CPK汇总.xlsx`。
过货量完全由此文件提供，不读取原始过货事实或派生 throughput 快照。

## 超规片：当周更新，历史不变

周为 ISO 周，时间标签带完整年份。仅重算当前周；关闭的周、月、季、年不回填。
缺失历史由用户补充，不从现有几周数据重新累加全年历史。

1. 取当前周未修饰超规明细，按 `factory + prod_code + item_id` 去重。
   同一 Sheet 多参数不重复计片；Total 为 OOS/OOC 集合去重，不是直接相加。
   AOI_RS 片数只计 `chart_kind=sheet`，Lot 点不算 Sheet。
2. 读取所选产品、scope、厂别当前年／季／月／周基线。
   所选 scope 某类明细不完整时不更新该类型；OOS/OOC 均完整才更新 Total。
3. 当周片数替换为最新值。当前月、季、年使用：
   `新总数 = 原总数 − 原已计入本周片数 + 新本周贡献`。
   例如月原值100、周原值10，最新周为12，则月变为102，不重新汇总历史周。
4. 使用用户维护的过货量更新受影响周期报警率，保存后回读生成表格。
   SOOS 暂无当前事实源，保留用户维护值，不强制改为0。

持久保存 `已计入周`、`本周OOS已计入片数`、`本周OOC已计入片数`、
`本周Total已计入片数`，重复查询不重复累加。手工调整当前基线时需保持这些字段一致。
这是维护汇总基线的差额更新，不是凭历史 Sheet 身份重建全年唯一集合。

跨月／季／年的周，仅取周与当前周期交集内 Sheet 计算新贡献，关闭周期仍不改。
首次接入跨界周时，若周原值非零而缺少该周期“本周已计入片数”，无法安全拆分旧贡献，
会保留该类型四个当前周期值并提示补充，不猜测分摊。
缺少任一当前周期基线也不自动创建零基线。

## 汇总表字段约定

共同业务键：`产品 + 监控类型 + 厂别 + 周期类型 + 时间标签`。
`显示标签` 用于 Y/Q/M/W 展示。完整范围用 ALL，筛选子集用排序后的组合键。
部分查询不覆盖 ALL，子集历史也须用户提供。

“报警率”保存过货量、OOC/SOOS/OOS/Total 的片数与率，旧表默认 ALL/ALL。
缺失片数允许按授权使用 `floor(过货量 × 报警率 + 0.5)` 初始化；已有片数不覆盖，
缺失率或过货量保留未知。Total 无可用率或片数时，不从 OOS+OOC 猜测。
率使用数值比例，例如0.01表示1%。

工作簿级线程／进程锁内重新读取基线，仅更新“报警率”sheet，验证成功后提交并保留其他 sheet。
企业保护文件走既有 Excel COM 安全写入流程，不将生产文件解密为普通文件。
读取、锁或写入失败显式报错，不用空表覆盖。
趋势、站点和明细展示来自明细 Excel 的可用时间范围，不表示补齐了全年明细。

## CPK 预警看板

历史数据读取汇总工作簿的 `CPK` sheet；最新数据读取
`resources/inline_domain/spc/spc_cpk_cpm_decoration.xlsx` 的产品同名 sheet，排除 `_cpm` sheet。
查询后仅更新**上一完整 ISO 周和当月**：按产品、厂别、站点、参数和周期去重，
计数 `flag=False` 且 `cpk_corrected < 1.33` 的项目作为预警项目数；
以用户维护的 `CPK总项目数` 为分母，计算达标项目数及达标率，再写回并读取汇总表展示。
年、季及其余历史保持不变，周 CPK 不累加或平均为年／季 CPK。

缺少对应周期明细、汇总基线、有效分母或未修饰项目的 CPK 值时，保留维护值。
已修饰项目不计入预警；仅预填 replacement 的空白行忽略。冲突重复项或预警数超过总项目数拒绝更新。
汇总项目数为小数或无穷值时明确报错，不写入工作簿；零、负数或空分母保留维护值。
无变化不重写；更新使用与 Inline 共用的工作簿锁，保护其他 sheet 和闭合历史。
页面不访问数据库或原始快照；Excel 解析使用 `st.cache_data`，点击“刷新缓存”后可重新查询。
仅 `?admin=true` 展示更新诊断，普通页面不展示黄色 Warning。

表格和柱状趋势固定显示当年、Q1 至当季、近三个月及含当周的近四周，按年／季／月／周配色。
图表缺失值只在展示层按 0 占位，不写回历史数据。

首版监控类型 SPC，字段为 `CPK总项目数`、`Cpk≥1.33达标率`、`达标项目数`、`预警项目数`。
旧表默认 SPC/ALL，缺指标显示 `—`；无项目明细时不展示虚假的0个有效项目。
历史项目数不臆造；跨产品达标率沿用项目数加权口径。

## 原始快照与看板解耦

其他 Inline 报表仍通过仓储端口使用原始快照，看板不消费它们。
不再生成 `data/inline_domain/oos_history/`、`ooc_history/` 判定快照。

目标目录：

```text
data/
  inline_domain/
    shared/inline_measurements_<产品>.parquet
    aoi_rs/aoi_rs_details_<产品>.parquet
    aoi_rs/aoi_rs_pass_through_<产品>.parquet
    <scope>/throughput_<产品>.parquet  # 兼容存储，非看板数据源
  yield_domain/yield/yield_snapshot_<产品>_<签名>.parquet
  equipment_domain/parts/part_life_*.parquet
  indicator_domain/qtime/...
```

Inline 延续既有原始窗口：截至日所在月月初往前3个月至当天（9月8日从6月1日起），
不再为年度看板扩展到1月，换月自动裁剪最早月份。
首次或旧策略初始化完整滚动窗口；后续重拉覆盖尾部前2日重叠区间并替换该区间。
旧量测不重复数值修正；TTL依据成功刷新时间和覆盖元数据，不以最后事实日期判断。
合法空查询也记录覆盖；失败不发布成功元数据、不覆盖有效旧快照。

迁移、重建是独立维护操作，执行前须停止应用及刷新任务写入：

- `tools/migrate_snapshot_layout.py` 默认只预览；`--apply --preserve-conflicts` 按清单迁移，
  冲突旧版本以 `legacy_` 文件名保留，未知文件不删除。
- `tools/refresh_inline_snapshots.py --products M626 M678 Z571 M673 Z517 Z553 Z576 --end-date 2026-09-08`
  默认只预览；日期换为执行当天。加 `--apply --writers-stopped` 才删除识别的 Inline 原始快照并通过仓储重建。
  按用户要求不备份；失败报告产品与模块，不修改人工 Excel，Yield/Equipment/QTime 不在删除范围。

## 实现与验证入口

- Excel解析：`application/monitor/excel_alarm_reader.py`、`infrastructure/monitor/excel_alarm_store.py`。
- 汇总编排：`application/monitor/oos_monitor_service.py`、`summary_workbook_service.py`。
- 周贡献替换：`core/monitor/weekly_replacement.py`。
- 工作簿：`infrastructure/monitor/summary_workbook_store.py`。
- CPK只读：`application/monitor/cpk_workbook_service.py`。
- 滚动快照：`infrastructure/shared/rolling_snapshot.py` 与两个原始仓储。
- 单测：`tests/unit/inline_domain/`；浏览器：`tests/e2e/fixtures/oos_monitor_app.py`、
  `tests/e2e/oos_monitor_dashboard.js`。

E2E使用隔离 Excel，禁止数据库／原始分析调用，验证门控、周替换幂等、修改Flag后刷新生效、
关闭周期保护、CPK只读及管理员可见性。隔离测试通过不等同生产快照已迁移重建，维护结果另行记录。

## 生产维护执行记录（2026-09-08）

用户确认停止 Inline 查询与刷新任务后，已完成75个文件迁移，迁移前后内容哈希全部一致。
按明确清单删除58个 Inline 原始快照及元数据文件（含冲突旧版本，不备份），
7个产品的共享量测与AOI_RS共14项仓储刷新全部成功，生成21份Parquet、共3,025,495条源记录。
覆盖元数据及实际记录日期逐份验证为2026-06-01至2026-09-08；无残留旧全年原始快照。
再次读取21份新快照未访问数据库。其他领域文件在重建前后哈希不变。
旧空目录已清理，data一级仅保留equipment_domain、indicator_domain、inline_domain、yield_domain。
本次维护相关101项回归测试通过；不修改人工Excel、不包含Git合并。

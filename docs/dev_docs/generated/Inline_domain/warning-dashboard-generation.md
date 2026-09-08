# 超规片与 CPK 预警看板生成逻辑

## 使用方式

入口为 `app/pages/自动预警看板.py`。超规片、CPK 有独立筛选和“查询”按钮，
首次打开不运行看板查询。筛选条件改变后需重新查询。

修改 `resources/inline_domain/` 下的规格、异常值或人工修饰配置后，
在管理员页头点击“刷新缓存”，再点击对应看板的“查询”。无需删除任何 OOS/OOC 快照。
“刷新缓存”清除计算和展示缓存、重载代码与配置，并收起已查询状态；它不等价于强制更新数据库原始事实。
原始事实按仓储的过期策略加载，强制拉取最新源数据仍使用对应报表的“刷新数据”。
管理员入口为 URL 参数 `?admin=true`；各产品/scope 最后计算时间和 OOC 修饰后台只在该模式出现。

## 两层持久化，一个中间计算缓存

```text
数据库事实 → 产品级原始 Parquet 快照
                     ↓ 当前规格、过滤和人工修饰
             st.cache_data 中间计算
              ├─ OOS / OOC / 过货事实
              └─ SPC CPK 周期指标
                     ↓ 仅写当前周期
       北极星报警率与CPK汇总.xlsx
              ├─ 报警率 sheet
              └─ CPK sheet
                     ↓ 回读
               年／季／月／周汇总表
```

原始事实路径保持不变：SPC/CTQ/AOI_TT 共用 `data/<产品>/inline_measurements_<产品>.parquet`，
AOI_RS 使用既有产品级 RS 明细与过货源快照。不是新增一份判定后的 L1。
年度计算要求原始快照至少覆盖当年 1 月 1 日，同时包含当前 ISO 周的跨年部分；
原有近三个月窗口比这更早时也保留。旧短窗口快照需一次补齐，补齐失败不得覆盖年度汇总。

生产组合根不再使用 `data/inline_domain/oos_history/` 和 `ooc_history/`。
对应应用服务的持久化通知与历史 bootstrap 工具已移除。
旧 store 类仅供遗留读模型回归测试，不在生产计算链中装配。
人工维护的 OOS/OOC `__flags` 工作表不是要删除的快照，继续保留。
其他报表的兼容性 throughput 存储不参与新看板分母计算。

## 超规片计算

`LiveMonitorSource` 通过现有仓储端口读取原始事实，在同一产品、scope、时间窗口、
配置签名下缓存一个原生 DataFrame/dict payload。OOS、OOC、过货量三种消费者共享一次计算。
缓存签名包含相关资源与配置文件、原始快照的路径/大小/修改时间；TTL 读取全局配置。
显式“刷新缓存”会清除中间缓存，因此即使签名未变化也会重新计算。

- SPC/CTQ：复用原始 Sheet 特征与当前规格；OOS 使用规格上下限，OOC 使用控制上下限。
- AOI_TT：按既有 TT 明细、规格构造 OOS/OOC；过货取同源 Sheet。
- AOI_RS：按 Lot/Sheet/Code 既有规则构造 OOS；没有真实控制界限时不伪造 OOC。
- 人工决策按现有业务键重新合并；`flag=False` 的记录进入预警，沿用原有修饰语义。
- 周期报警片数按产品、厂别、物理 Sheet 去重，不是参数明细行数；Total 为报警集合去重。
- SOOS 暂无事实源，沿用固定 0。过货分母来自同次计算的原始过货事实，不读取旧派生快照。

规格读取、配置解析或计算失败时显示查询错误，不用旧 OOS/OOC 文件静默兜底。

## CPK 计算

首版监控类型为 SPC，复用 OOS 修饰后的 Sheet 特征、CPK 参数排除配置和现有 CPK 人工修饰表。
每个产品 × 厂别 × 站点 × 参数为一个项目。分别取当前年度、季度、月份、ISO 周的
全部 Sheet 均值，使用样本标准差（`ddof=1`）及既有 `calculate_cpk` 函数计算。
年度/季度 CPK 不由月度 CPK 平均得到。本页固定此口径，不跟随 SPC 页临时的 sigma 选项。

CPK ≥ 1.33 为达标；非空有效 CPK 项目构成达标率分母；无法计算的项目单列展示，
不作为 0 混入达标率。常量分布的无穷大结果沿用既有核心算法定义。
CPK 中间结果只做 `st.cache_data`，人工修饰配置只读，不回写重建用户决策。

## 汇总工作簿与增量更新

文件：`resources/inline_domain/monitor/北极星报警率与CPK汇总.xlsx`。

共同业务键为 `产品 + 监控类型 + 厂别 + 周期类型 + 时间标签`；
时间标签含完整年份，周使用 ISO 周年，显示标签仅用于 Y/Q/M/W 展示。
ALL 表示完整范围；筛选子集使用标准化组合键，避免部分查询覆盖全量汇总。

“报警率”保存过货量、OOC/SOOS/OOS/Total 的片数和率。
“CPK”保存总项目数、达标率、达标项目数、预警项目数。
旧报警率数据默认 ALL/ALL；旧 CPK 数据默认 SPC/ALL。旧表缺失精确片数或项目数时
保留未知值，页面显示 `—`，不会用四舍五入后的历史率反推数量。
跨产品历史 CPK 达标率按项目数加权，不直接平均百分比。

例如 2026-09-07 的一次查询仅允许更新 2026 年、Q3、9 月、当前 ISO 周对应行；
已过去的 Q1/Q2、7 月/8 月及已关闭周保持原值。换月/换年也不回填已关闭周期，
迟到数据不会自动更改已结历史。历史缺行或筛选子集无对应历史记录时显示 `—`。

两个 sheet 共享工作簿级线程锁与进程锁；锁内重新读取最新文件，修改目标 sheet，
验证后原子替换。普通 xlsx 保留其他 sheet 的公式和格式；企业保护文件走 Excel COM 临时副本路径，
校验后提交，不把工作簿整体转换为未保护文件。错误不会以空表覆盖原文件。
周期汇总以提交后回读的工作簿为准；趋势、站点和明细展示仍来自本次计算事实。

## 实现入口与验证

- 组合：`src/inline_domain/composition.py`
- 计算缓存：`application/monitor/live_source.py`、`live_history.py`
- 超规汇总：`application/monitor/oos_monitor_service.py`、`summary_workbook_service.py`
- CPK：`application/monitor/cpk_monitor_service.py`、`core/monitor/cpk_summary.py`
- 持久化：`infrastructure/monitor/summary_workbook_store.py`、`cpk_summary_workbook_store.py`
- 测试：`tests/unit/inline_domain/{application,core,infrastructure}/monitor/`，
  `tests/e2e/fixtures/oos_monitor_app.py` 与 `tests/e2e/oos_monitor_dashboard.js`。

E2E 使用隔离原始事实和测试工作簿，不修改生产汇总文件，也不代表生产数据库已成功补齐全年数据。

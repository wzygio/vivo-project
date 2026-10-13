# Task2 蒸镀腔室数据库版交付与验证

日期：2026-10-13。development-flow general；分支 `feat/qtime-chamber-database`。
范围：[Task2](../../../dev_spec/indicator_domain/qtime/task-Q_Time单腔停留时间.md)。
需求与执行记录：[PRD/票据](../../../../../.scratch/qtime-chamber-database/PRD.md)。
数据源：[实际权限、字段和样本分析](datasource-qtime-chamber-database.md)。

## 实现

生产腔室模块使用数据库事件适配器，LL1.prod_id经MES主数据映射产品，不再依赖手工Excel
和SPC抽样身份。保留11区间、线体名称、SVG点位、1000秒过滤、配置规格和独立fragment。
新增进片显示日期范围，默认当月、最大31日；日期进入缓存身份，修改日期立即移除旧图，
重新点击查询后读取该窗口。产品/线体/腔室多选继续在提交结果内过滤，空选取全部启用范围。

Infrastructure按设备/玻璃/进片批次配对，应用前推、当日截止和Indicator日期排除。
参数化、只读、60秒超时，超过500,000条拒绝发布；缺配对不填零、失败不回退旧Excel。
工作簿适配器保留显式注入，并支持可选日期。工作簿、原始快照、其它用户改动和正式服务保留。

## 已执行测试

1. 腔室直接相关回归：

   ```powershell
   .venv/Scripts/python.exe -m pytest tests/integration/indicator_domain/test_chamber_database.py tests/integration/indicator_domain/test_chamber_qtime.py tests/unit/indicator_domain/application/qtime/test_chamber_settings.py tests/unit/app/sections/indicator_domain/qtime/test_chamber_dashboard.py tests/unit/app/charts/indicator_domain/qtime/test_chamber_chart.py tests/unit/app/pages/test_qtime_page.py tests/e2e/test_chamber_database_apptest.py -q --tb=short --junitxml=output/test-results/qtime-chamber-db/chamber-final.xml
   ```

   **35 passed**。覆盖11路径、秒数、重复事件、复过货、设备隔离、跨午夜、产品归属和冲突、
   前推/截止/排除、日期/cache、结果上限拒绝、实际SQL失败、安全空态和日期交互。
   SQLfixture不创建SPC表，验证生产适配器独立于SPC采样来源。

2. 扩大回归：

   ```powershell
   .venv/Scripts/python.exe -m pytest tests/integration/indicator_domain tests/unit/indicator_domain tests/unit/app/sections/indicator_domain/qtime tests/unit/app/sections/indicator_domain/test_date_exclusion_signatures.py tests/unit/app/charts/indicator_domain/qtime tests/unit/app/pages/test_qtime_page.py tests/e2e/test_qtime_snapshot_flow.py tests/e2e/test_chamber_database_apptest.py tests/architecture -q --tb=short --junitxml=output/test-results/qtime-chamber-db/final-regression.xml
   ```

   **214 passed、1 failed**。唯一失败为既有站间模块标题断言：测试期待“北极星QTime监控”，
   实际为“重点站点QTime监控”。用`git show HEAD:app/sections/indicator_domain/qtime/dashboard.py`
   核实基线已是实际标题；本次没有修改该标题或更改旧断言以宣称全部通过。
   一项IJP异常测试产生已有pandas连接类型警告。未运行无关全库测试。

3. 数据库浏览器E2E：`tests/e2e/chamber_database.js` **通过**。
   使用真实Service/Core/cache/section和SQL适配器的隔离fixture；覆盖1000秒边界、
   产品/线体/腔室筛选、日期变更/缓存、非法跨度、刷新、空态、故障恢复、普通模式内容和390px窄屏。
   浏览器真实数据库场景使用生产组合根，选2026-09-05：10个有效腔室组别、7,080个点，
   SVG、1200秒当前配置纵轴、全部测量≤1000；缺少事实的第11路径不伪造。

4. 真实默认当月窗口：2026-10-01～13，应用输出策略后150,581区间、15,058片，
   启用型号44,720区间，两线均有数据；未匹配0、冲突0，读数及Core计算约9.2秒。
   显示时间2026-10-07 00:00:18～13日11:58:33，符合排除和当天截止。

5. 浏览器证据目录：`output/test-results/qtime-chamber-db/`，包括
   `database-fixture.png`、`database-mobile.png`、`database-live.png`、`database-result.json`及测试XML。
   浏览器命令运行于此输出目录，快照和日志不写入根目录或src。

6. 既有双模块浏览器脚本 `tests/e2e/chamber_qtime.js` **通过**：本次仅在输出目录生成副本，
   将测试端口映射为8537、截图改至本次目录。22张SVG图、OC2→OC3、窄屏、筛选/刷新/安全状态通过。
   页面/站间/腔室计数：初始1/1/1，操作下区1/1/3，再操作上区1/5/3，双向fragment隔离有效。
   fixture规格6000，生产真实场景规格1200；两种数值来源不同，均按各自配置验收。

7. 最终静态核验：受影响Python文件compileall、限定文件git diff --check通过；7份新增
   文档的相对链接全部有效。最终身份检查无本次存活服务进程，8537端口已释放，记录为
   `output/test-results/qtime-chamber-db/final-checks.json`。未运行Ruff/Pyright或覆盖率统计。

## 服务清理

运行器为每次服务创建独立Windows job，在恢复进程主线程前分配，记录创建时间/PID、
子进程和8537端口。job设置KILL_ON_JOB_CLOSE，finally关闭后核实身份对应进程退出和端口释放。
先用不连接数据库的最小Streamlit页覆盖正常、失败、KeyboardInterrupt三场景，均通过。
首次CLI管道阻塞时，仅中断本任务确切运行器；kernel job清理后实际端口探针确认释放，
再关闭本任务命名浏览器会话。后续成功与失败E2E运行均确认清理。
未停止、重启或改变任何正式服务。

## 评审与边界

Standards自检：分层、原生缓存、资源/秘密保护、只读绑定查询和普通模式安全内容符合项目约束。
Spec自检：权限、来源分析、11路径、日期和真实数据库/E2E均有证据；产品归属改用过货身份
解决初步SPC匹配缺失。测试中SQLite日期相减的浮点误差曾影响1000边界，按SQLite毫秒精度
归一，PostgreSQL仍用EPOCH原精度；修正后通过。

业务限制：前后1日补齐以覆盖跨午夜，跨出扫描窗口的长流程不保证全部配对；无配对不补值。
当前真实样本OC9→OC10无配对，不能解释为该腔不存在或没有异常；本次不扩展末尾两条路径。
样本产品完备不保证所有历史日期主数据完备。

用户已授权直接完成实现和验证，需求与计划按此适配执行；合并、提交、部署没有自动授权。
未提交、合并或发布；不会将既有大量未提交改动纳入任务提交。正式ADR审批与合并阶段未宣称完成，
实际取舍保存在[ADR草案](../../../../../.scratch/qtime-chamber-database/ADR-draft.md)，
已验证规则归[腔室数据库规则](../../../../../references/domain/indicator_domain/qtime/rules-chamber-database.md)。

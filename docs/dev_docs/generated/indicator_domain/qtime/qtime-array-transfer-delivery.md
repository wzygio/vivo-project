# Q-Time 固定路径补充取数交付记录

日期：2026-11-17。运行模式：development-flow general。

## 范围与实现

依照 [用户 PRD](../../../../PRD/PRD-2026-11-17-QTime-1A200-to-21100.md) 和后续范围澄清，只补充 `1A200 → 21100` 的 TrackIn 取数，标准输入之后使用原服务、修饰、图表和预警。来源仅在生产组合根显式装配，不向数据库写入，不使用临时表，不重启正式服务。

实现包括固定来源及产品映射、原仓储直接/快照读取的输入合并、ARRAY 来源策略后缀、全滚动补充路径刷新，以及隔离的真实样本/合成样本浏览器验证。

## 真实只读核验

系统目录确认履历及主数据对象、字段和只读权限后，使用有限时间/站点/返回行数采样。标准取数回放为 589 条、6 个启用产品；原监控服务应用已有显示时间政策后，实际只读查询为 523 条：M626 166、M678 136、Z553 98、Z576 87、Z517 24、Z571 12。规格为空，没有杜撰标准或实际超规结论。

测试抓取位于 `output/test-results/qtime-route/`，不作为提交的业务输入。浏览器只使用这些已保存的只读样本与内存合成样本，服务不连接生产数据库。

## 验证进度

- 新增来源单元测试：11 passed，包含 CPT 最早目标、同终点多个起点、产品版本/厂别、缺失/歧义、负时长、容量和秒级键冲突。
- 仓储/应用/Core/页面/刷新/CLI 快照 E2E/架构相关回归：178 passed。
- Python 编译、Node 浏览器脚本语法检查通过。
- 既有临时进程树 runner 的无数据库最小页正常、失败、中断三场景均通过；进程与全部已记录监听已释放。
- 浏览器 E2E 通过：合成数据时长/产品映射、缺规格、原 True/False/Delete、真实样本回放、空结果、读取失败、恢复、390 像素移动端与滚动。结果和截图位于 `output/test-results/qtime-route/browser/`。
- Standards/Spec 双轴评审完成。Standards 发现外部环境 URL 可能绕过本次测试服务，已改为使用本次分配端口的 URL，并在继承错误 URL `http://127.0.0.1:1` 的环境下重新通过完整 E2E；每次使用独立快照目录。
- 最终浏览器记录 `cleaned=true`、`alive=[]`、`listening_ports=[]`，正式服务未重启。

主要命令：

```text
.venv/Scripts/python.exe -m pytest tests/unit/indicator_domain/infrastructure/qtime tests/unit/indicator_domain/application/qtime tests/unit/indicator_domain/core/qtime tests/unit/app/sections/indicator_domain/qtime tests/unit/app/pages/test_qtime_page.py tests/unit/tools/test_refresh_qtime_snapshots.py tests/e2e/test_qtime_snapshot_flow.py tests/architecture -q
.venv/Scripts/python.exe tests/e2e/run_z622_spc_browser.py --lifecycle-check
.venv/Scripts/python.exe tests/e2e/run_qtime_array_transfer_browser.py
```

未执行全仓库业务测试套件，也未安装额外类型检查工具；本次执行相关回归、编译、语法、文档链接和浏览器检查。没有把浏览器只读样本回放冒称为浏览器直接访问生产数据库；真实 SQL 通过独立只读监控查询验证。

## 分支、授权与限制

开发分支：`feat/qtime-1a200-21100`。开始点：`d0ee4c1aa947df80f80c2e5002eda9722576f801`。

已有无关工作区改动保持原状，不计入本任务提交。用户在实现提交 `7e9b6f6d` 及评审/E2E 结果呈现后，批准合并并补充全部产品统一 200 小时规格。ADR 见 [ADR-0047](../../../../ADR/0047-qtime-array-transfer-source.md)。通用站点能力与正式服务重启不在本次交付内。

## 200 小时规格补充

使用既有 `qtime.spec_overrides`，没有修改修饰算法。新增生产组合根测试先失败后通过，覆盖原始时长 250、200、100 小时以及默认/False/Delete 三态：规格线均为 200，达到规格的展示值沿用限幅落在 170–190 小时，False 预警仍保留真实 250 小时及超规证据。

针对正式 200 小时配置的浏览器 E2E 已再次通过，真实样本回放的 6 张产品图均具有 200 小时规格线，所有展示值小于 200；缺规格与原三态测试也继续通过。相关完整范围回归现为 181 passed；测试进程和监听均已清理。浏览器标题定位兼容既有标题的连字符写法，不依赖其他未提交的页面文案变更。

## 重复 Lot 柱顶异常修复

用户截图及浏览器重放复现了渲染问题：M626 单条最大时长为 199.725 小时，但同类别累计后的柱顶达到约 296.22 小时；M678 单条最大约 197.30，累计柱顶约 223.80。先前 E2E 只检查了单条值，没有检查最终 SVG 柱顶，因此漏检。

修复只涉及图表坐标：同一路径重复 Lot 的每条过站记录独立成柱，标签保留 Lot ID，悬停补充过货时间。原始行、等待时长、规格和下游预警不做修改。新增单位回归先红后绿，并将 E2E 检查扩展为实际 SVG 边界转换后的柱顶检查。

同一抓取样本修复后的渲染柱顶：M626 约 199.73、M678 约 197.30，均低于 200。相关图表、页面及正式规格回归 41 passed；双轴评审无阻断。浏览器 E2E 通过，并确认 SVG 柱数覆盖全部记录，避免空选择器使检查假通过。证据为 `output/test-results/qtime-route/browser/rendered-boundaries.json` 及 `captured-real.png`；服务、浏览器进程及监听均已清理。SVG 像素量化容差仅用于测试几何边界，不改变任何业务值。

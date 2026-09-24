# 蒸镀单腔停留时间监控：开发与验证记录

- 日期：2026-09-24
- 模式：development-flow general
- 分支：`feat/qtime-chamber-residence`
- 基线：`d9304b7802031e26c898b53baace69d5e0255a7f`
- 原始需求：`docs/dev_docs/dev_spec/indicator_domain/task-Q_Time单腔停留时间.md`
- 本地PRD/票据：`.scratch/qtime-chamber-residence/`

## 已实现

现有 Q-Time 模块下方增加“蒸镀单腔停留时间监控”。产品/线体/腔室多选，空选表示全部；按线体－腔室分组显示等距柱状图，过货时间精确到小时，颜色区分产品。提供超限和缺失汇总，超过1000秒的测量从展示及汇总剔除，保留1000秒及缺失值；逐片明细表和下载已按后续优化要求移除。非启用产品不进入图表、表格或选项。上下两个模块分别为独立fragment，普通页面不包含内部字段或原始异常信息。

代码沿用 `qtime` 命名。领域资源配置通过 `qtime_chamber_3cee001/3cee002` 指向用户提供的两份文件；后续换月应更新这两个配置值。未移动或提交业务工作簿，未恢复用户删除的资料。

源表第9行是表头，明细从第10行开始，E～O为11个区间，D列排除。阈值读取第5行，超过才判超限。完全重复记录去重，数值冲突拒绝读取，复过货保留。缺失/非法/负数值按缺失展示，不填零。原始导出文件声明工作表范围为A1，读取器重置尺寸以避免漏行。

数据库查询限定源月份，对 `glass_id/productcode` 去重后精确匹配，不猜测型号；多型号归属隔离。展示遵循现有日期前推和当天中午截止，缓存包含文件状态、日期、策略、启用产品和来源身份。页头刷新清理新区域缓存及结果。显式注入源默认隔离。

## 真实数据验收

两份表共有43,016个有效GlassID，另3条无有效标识/时间的记录。完整当月映射仅5,382片：线1 Z576=100；线2 M626=2248、M678=1200、Z517=340、Z553=1494。现有数据不足以解释未匹配原因，页面提示覆盖不完整。

初版原始匹配结果，应用当前配置（前推4天、2026-09-24 12:00截止）后、尚未剔除大于1000秒测量时：

- 3,705片玻璃、40,755条腔室记录，未匹配31,220片、型号冲突0。
- M626=1405、M678=900、Z517=340、Z553=960（均线2）；Z576=100（线1）。
- 时间范围：2026-09-05 06:07:31～2026-09-24 11:59:28。
- 正常测量36,923条、缺失3,832条、超限0条；不能据此推断未匹配玻璃无异常。
- 真实页面查询、M626筛选及M626+线1空态已通过浏览器验证。

单位：文件可见文字未明确单位，当前按向用户说明的默认“秒”实现，源目标值为6000；尚未收到业务单位确认。

## 初版自动化验证

1. 相关回归：
   `.venv/Scripts/python.exe -m pytest tests/integration/indicator_domain tests/unit/indicator_domain tests/unit/app/sections/indicator_domain/qtime tests/unit/app/charts/indicator_domain/qtime tests/unit/app/pages/test_qtime_page.py tests/e2e/test_qtime_snapshot_flow.py tests/architecture -q`
   **177 passed**；XML：`output/test-results/qtime-chamber/targeted.xml`。
2. 浏览器脚本：`tests/e2e/chamber_qtime.js`（隔离fixture，8521），覆盖初始门控、顺序、三个筛选、空态、刷新、CSV内容、窄屏、脱敏失败及恢复。
3. 真实页面：`tests/e2e/chamber_qtime_live.js`（生产页面，8522）；计数是本次2026-09-24数据快照的验收值。
4. CSV内容实读：6行8个业务字段、2腔室、仅M626/M678、2个空测量；另验收M678+线2筛选下载。
5. 所有浏览器快照、截图、下载、XML位于 `output/test-results/qtime-chamber/`。
6. 最后一次交互调整后5项页面/AppTest复测通过；隔离浏览器脚本含两次下载内容断言再次通过。Python compileall及git diff --check通过。环境未安装Ruff/Pyright，本次未运行这两项静态工具。

## 全量回归限制

执行 `.venv/Scripts/python.exe -m pytest tests -q --tb=short` 得到 **1730 passed、12 failed、2 skipped**，不能称全套通过。报告 `regression.xml`。

在基线独立worktree（`output/tmp/qtime-baseline`）运行涉及失败的7个测试文件，复现8个相同失败：Inline页面按钮缺失、设备报表2项、IQC示例JSON缺失3项、SPC缓存版本期望、Delete规则期望。基线该子集最终20通过9失败，额外1项是另一加密文件的明文头断言。余下当前失败为IQC AppTest 3秒超时和3个依赖透明加密可见状态的诊断断言，基线未稳定复现；相关业务代码未被本次修改。基线COM诊断还输出0x80010108异常，测试进程继续完成并生成XML。没有为了使全套变绿而修改无关模块、资源或断言。

## 本轮展示与 Fragment 优化验证

优化基线为 `8513487`。沿用上文相关回归命令，新增柱形排序、小时显示、稳定颜色和
1000边界测试后，**181 passed**，报告为 `output/test-results/qtime-chamber/optimization-regression.xml`。

旧图复现：相同三条有效数据同时绘制11张 `Scattergl`，浏览器出现6次
`Too many active WebGL contexts`，`chart0/1/2` 被回收；第三张正是 OC2→OC3。
修复为 SVG `go.Bar`，超出纵轴范围的6000秒规格显示文字，避免压扁实际柱形。

浏览器验收：

- `tests/e2e/chamber_qtime.js`：22张分组图均绘制柱形，无WebGL画布；覆盖OC2→OC3、
  数值过滤、明细移除、三个筛选、刷新、窄屏及失败恢复。
- 双向隔离计数（页面/站间/单腔）：初始 `1/1/1`，操作下方后 `1/1/3`，
  再操作并查询上方后 `1/5/3`。查询会额外计算缓存签名，故单腔计数包含签名调用次数。
- 真实页面 `tests/e2e/chamber_qtime_live.js`：默认PT→OC1显示3698片、3672条有效测量；
  全选22个组别中20个有有效柱形，2个仅缺失。OC2→OC3线2绘制3584根柱，最大974秒。
  产品筛选及M626+线1空态通过。截图 `live-oc2-oc3.png` 位于上述输出目录。
- 管理员保存使用 `st.rerun(scope="fragment")` 重建下载及图表，避免旧下载内容。
  `tests/e2e/chamber_qtime_admin.js` 保存前后计数 `1/5/3 → 1/9/3`，下区和页面主体保持不变。

管理员测试准备：运行 `tests/e2e/fixtures/prepare_chamber_admin.py`，以8523端口本地HTTP
服务提供 `output/test-results/qtime-chamber/`。8521运行隔离页面，执行admin浏览器脚本后，
用同一准备脚本的 `--verify-downloads` 验证两产品对应决策由False改为True。
上传通过内存字节绕开测试机透明加密对Node读取文件的影响，下载实读使用既有解密包。
这些操作只作用于隔离fixture，不保存生产台账。

设计范式已写入 [多模块页面的 Fragment 隔离范式](../../../references/design/feat_design/interaction-multi-module-fragments.md)，
并加入 `references/index.md` 知识路由。本轮未重跑无关全套用例，上述初版全量限制仍保留。

## 评审与交付状态

- Standards轴发现注入源缓存隔离问题，已修复并经独立复核关闭。
- Spec轴要求的逐列值/阈值、跨月源时间、文件变更、启用范围变化、旧选择清理及CSV证据已补齐。
- ADR草案：`.scratch/qtime-chamber-residence/ADR-0033-draft.md`，待确认后采用正式编号落库。
- 功能开发与相关E2E完成；未宣称整个四阶段流程已完成。按development-flow的独立授权边界，尚未合并master或发布。

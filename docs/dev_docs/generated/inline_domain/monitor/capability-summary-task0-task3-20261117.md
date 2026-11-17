# Task0–Task3 执行核验（2026-11-17）

需求：[task-cpk_cpm_summary.md](../../../dev_spec/inline_domain/monitor/task-cpk_cpm_summary.md)。Task0–Task3 已完成正式数据发布与核验。本记录分别列出维护文件、历史统计、真实后端和隔离浏览器证据。

| 要求 | 当前证据与状态 |
|---|---|
| 三种汇总分文件、报警率保持原内容 | 正式文件已发布为北极星报警率汇总.xlsx、北极星CPK汇总.xlsx、北极星CPM汇总.xlsx，分别包含7、8、8个产品Sheet。正式报警率数据与源文件一致，迁移保留原工作簿和带日期备份 |
| 独立资源注册 | config/global.yaml 已注册 monitor_summary_workbook、cpk_summary_workbook、cpm_summary_workbook；原合并工作簿登记为 archived_monitor_summary_workbook，运行时各看板消费独立路径 |
| 去除显示标签、补缺、季度/年度重算 | 正式CPK/CPM工作簿不再持久化“显示标签”，读取时派生。267条CPK、177条CPM历史记录的结构与计数匹配候选计划；计数补缺、季度/年度汇总及零总数100%规则已核验 |
| CPK近五个完整周 | 2026-W42–W46，8产品，共40条周记录已重算并写入正式汇总及祖先周期。另按用户授权修复Z576历史异常及其月份覆盖，共3条附加周记录，见下文 |
| CPM近三个月完整周 | 2026-09-01至2026-11-17范围内，2026-W36–W46，8产品，共88条周记录已重算并写入正式汇总及祖先周期。另按用户授权修复4个产品8月异常及必要覆盖，共20条附加周记录 |
| 总数复用统计逻辑，预警数按flag=False | 离线快照适配器复用共享最终点位与能力计算器；按业务键去重的显式False预警记录计数，不重复应用阈值或跨度门槛。参数/规格使用经过schema、时间和规模检查的只读捕获，不写数据库 |
| 仅目标周及祖先更新，排除未完成周 | 原定窗口与获准历史修复均按明确产品/周键执行；健康的既有历史周保留。缺失的受影响月度周覆盖经真实输入计算，未按0猜测。未完成/未来周保留存储但不进入完整月汇总 |
| 柱线趋势、颜色与断线、无项目明细 | 页面及图表改造完成；CPM/AOI_RS仍仅管理员模式显示；默认折叠、独立产品查询 |
| E2E及文档 | 浏览器实际页面隔离E2E通过，1440/768/375宽度、普通/管理员、空/失败、零分母、fragment互不重跑、滚轮不缩放均有证据。正式文件副本的实际composition首次与重复查询均无写回；规则与ADR0046已更新 |

## 已批准的历史扩展范围

用户明确回复“允许，仅补算这些异常历史周（推荐）”，授权纠正原窗口之外的异常历史周及其所属月、季、年。执行范围由原维护表的异常产品/周键确定，未扩大为所有产品的整段历史重算：

- CPK：Z576 / 2026-W39为异常周；补算同一受影响月份原表缺失的W36、W37，以建立完整月度覆盖。既有健康W38保留。
- CPM：M626的W32–W35、M678和Z517的W31–W35、Z571的W31–W34为异常周；另核算同一受影响月份缺失的M626/W31、Z571/W35。最终为上述4产品各自W31–W35，共20条产品/周结果。

缺失的早期原始输入通过有时间、产品和规模边界的只读查询补齐到隔离历史输入目录，没有覆盖共享原始快照。普通页面继续只读本地已验证输入，不访问原始数据库。

Task0 同时按已知计数统一派生达标项目数和达标率：20个既有CPK比率得到纠正；Z571的2026-W37达标项目数由82改为81，总项目数82及预警数1保持不变。“保留健康历史周”指范围外的总项目数与预警数不重算，不表示保留与计数关系矛盾的派生值。

## 验证

- 最终组合回归521项通过，页面及本地工作簿AppTest 24项通过，架构检查15项通过，共560项。对应日志和JUnit结果位于 `output/test-results/capability-summary/delivery-final/`。覆盖范围包括历史扩展的稀疏产品/周键、必要月度覆盖、未修改期间、非法值拒绝、版本记录、过期基线拒绝、目标竞争写入、元数据损坏后缓存拒绝，以及产品变更后重新提交查询和全部厂别统计。
- 浏览器：`.venv/Scripts/python.exe tests/e2e/capability_trends_browser.py`。结果及截图位于 `output/test-results/capability-trends/`，临时服务/浏览器进程退出且监听释放。
- 正式迁移：[delivery-final/rebuild-plan.json](../../../../../output/test-results/capability-summary/delivery-final/rebuild-plan.json) 已包含 `migration`、三个正式目标、全部产品Sheet以及原文件备份路径。备份文件为 `北极星报警率与CPK汇总.xlsx.before-capability-split-20261117-135656-accf7f.bak`。
- 正式数据核验：[formal-data-verification.json](../../../../../output/test-results/capability-summary/delivery-final/formal-data-verification.json) 记录 `alarm_unchanged=true`；CPK为8个Sheet、267行，CPM为8个Sheet、177行，两者均 `schema_and_counts_match=true`。
- 报警率内容及格式：[alarm-preservation.json](../../../../../output/test-results/capability-summary/delivery-final/alarm-preservation.json) 对正式报警率文件、原文件及备份逐单元格核对值、公式、字体、填充、边框、对齐、数字格式、行高列宽及合并区，全部一致。源报警率Sheet没有公式单元格。唯一导航差异为拆分后首个报警率页签自动选中，活动单元格及其余视图属性相同；新建只读Excel进程已确认退出。
- 真实后端查询：[real-backend.json](../../../../../output/test-results/capability-summary/delivery-final/real-backend.json) 来自正式文件副本上的实际composition，禁止数据库访问；CPK/CPM首次和重复查询的写回数均为0，返回三月四周及既有年/季汇总列。
- 输入版本与稀疏附加计算：主窗口结果保留在 `verified-current/`；获准附加结果位于 `delivery-final/anomalous/<产品>/`，各自有 `rebuild-inputs.json`，绑定产品、周度范围、输入版本和文件校验值。旧的无版本中间文件不用于正式发布。
- `split-preview-result.json`、`split-preview-guarded-result.json` 与较早 `applied=false` 记录是发布前的验证历史；它们已被上述正式迁移与正式数据核验结果取代，不再表示当前状态受阻。

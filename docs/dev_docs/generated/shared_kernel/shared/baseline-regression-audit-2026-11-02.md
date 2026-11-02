# 24项既有失败审计与处理

日期：2026-11-02。固定起点9c616182；状态：实现及定向验证完成，全量／独立评审进行中。

## 判断

24项在当前起点全部复现。**22项仍有回归价值，保留并修复；2项约束旧实现或旧资源规则，退役并补上当前契约覆盖。** “旧业务的回归测试”并不自动等于无价值：只要业务仍受支持，就应保留。

| 原问题卡／项数 | 当前依据与处理 |
|---|---|
| 01 备件空状态／1 | 正式有效空快照为ok；全厂共用范围不需要产品。修复共同读取器空产品revision缺陷；夹具统一临时生产／消费路径，空快照验证正常无异常。 |
| 02 IJP失败提示／2 | 报表和预警中心独立。显式模拟保存结果不可用，同时验证报表错误与预警错误均安全显示，去掉“必须是第一个错误”的旧顺序假设。 |
| 03 Inline临时配置／4 | 严格全局资源注册仍受支持；临时根目录补临时注册表，保留离群过滤、矩阵及决策上传行为验证。 |
| 04 TT周报缺失值／2 | 有效TT采用白名单；用DSUM_L作为“站点有检测但缺当前TDSUM”的真实输入，保留数据不足黑色及USL着色验证。 |
| 05 CTQ载荷／2 | 分布及模块重载期间缓存仍须正常；补齐临时注册表，保留原业务断言。 |
| 06 CPK／CPM同周生命周期／2 | 当前生产可配置0h，测试应显式48h；保留短跨度→预警→达标→空值全过程。 |
| 07 能力跨度／3 | 同上，显式48h并隔离其它日期政策；已有配置0h参数化继续保留，未修改生产配置。 |
| 08 TT排除重检／1 | 用有效TDSUM身份，继续验证排除发生在重检去重之前。 |
| 09 TT豁免释放／2 | 用有效TDSUM及对应豁免词，保留持久化、读取、生成器和释放证据验证。 |
| 10 MWD输出／1 | 当前monthly_full用于历史发布；契约更新为六个返回键，并将确定性断言扩展到全部六张表。 |
| 11 资源全集检查／1 | 退役：当前允许目录集合及相邻备份／签名，不能要求每个物理文件单独注册。新增临时目录集合验证；同文件其它严格key／路径测试保留。 |
| 12 中午截止／1 | 夹具补齐真实格式厂别并显式隔离无关排除规则；继续验证显示截断、刷新及原快照完整。 |
| 13 Yield页面AST／1 | 退役：页面已消费本地发布结果，不直接调用旧render_alert_center。新增现行发布文件→预警中心四类健康状态验证，已有真实页面发布时序覆盖保留。 |
| 14 Yield只读／1 | 只读并非不读取；补齐公开数据端口的rate override假输入，继续验证read_only=True传递，禁止依赖真实Office。 |

## 实际代码修复

只修改[共同读取器](../../../../../app/sections/shared/alert_results.py)：备件的全厂共用空产品范围采用固定缓存revision，仍由文件／策略／有效性签名控制刷新；普通产品的revision保持原逻辑。未改业务阈值、TT名单、生产全局配置或资源文件。

## 退役边界与恢复

只删除两个具名函数，不删除测试文件、不跳过测试。退役前已在会话说明准确目标与依据。原始函数可从9c616182对应路径恢复；live route由两张问题卡记录新节点。历史交付报告保留当时失败事实，不改写旧结果。

资源规则依据：[CONTEXT](../../../../../CONTEXT.md#resource-path-configuration)。Yield依据：[已发布结果展示设计](../../../../../references/design/inline_domain/shared/architecture-published-alert-display.md)和[Yield结果规则](../../../../../references/domain/yield_domain/shared/rules-yield-alert-results.md)。跨度依据：[CPK/CPM规则](../../../../../references/domain/inline_domain/spc/rules-spc-cpk&cpm-decoration.md)。TT依据：[参数白名单](../../../../../src/inline_domain/core/aoi_tt/parameters.py)及[周报规则](../../../../../references/domain/inline_domain/aoi_tt/rules-aoi-tt-weekly-report.md)。

## 证据

- 起始24项全部失败；两个分组完整文件40项及90项通过；主要修复组64项通过；保留22项＋初版替代路径28项通过。现行健康状态独立文件4项通过。
- 使用固定9c616182的独立工作区收集2845项，最终2849项：恰好退役2项，新增6项（目录集合1、全厂备件1、Yield健康4），净增4。原工作区存在并行Yield改动，其早期2849项清点不作为固定基线；所有原始清点记录保留。
- 所有已完成护栏测试的68个维护文件哈希无变化。初始失败组的Office尝试被阻止；修复组均无数据库／Office／维护目录写入尝试。类型检查共同读取器与新增夹具helper为0 errors、0 warnings。
- 全量、独立Standards／Spec评审及合并验证待补记。不启动新测试Web服务，不操作8890／8503正式服务。

原始证据在原仓库output/test-results/：baseline-audit-initial-20261102、baseline-inline-fixtures-20261102、baseline-audit-aoi-span-20261102、baseline-audit-main-fixes-20261102、baseline-audit-resolved-20261102、baseline-audit-collection-pinned-20261102、baseline-audit-collection-final-20261102。全量证据baseline-audit-full-20261102。原问题卡见[14卡目录](../../../../../.scratch/api-baseline-regressions/issues/)。

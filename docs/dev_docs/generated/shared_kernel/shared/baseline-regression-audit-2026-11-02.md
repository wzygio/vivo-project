# 24项既有失败审计与处理

日期：2026-11-02。固定起点9c616182；状态：完成，已合并master。

## 判断

24项在当前起点全部复现。**23项仍有回归价值，保留并修复；1项仅约束旧Yield页面实现，退役并补上当前契约覆盖。** “旧业务的回归测试”并不自动等于无价值：只要业务仍受支持，就应保留。

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
| 11 资源全集检查／1 | 保留修复：库存保护仍有价值。认可已登记目录集合、锚定主文件的备份／签名和严格空临时模式；补登记历史参考资料集合，新增四个未注册输入反例。原测试节点名保留。 |
| 12 中午截止／1 | 夹具补齐真实格式厂别并显式隔离无关排除规则；继续验证显示截断、刷新及原快照完整。 |
| 13 Yield页面AST／1 | 退役：页面已消费本地发布结果，不直接调用旧render_alert_center。新增现行发布文件→预警中心四类健康状态验证，已有真实页面发布时序覆盖保留。 |
| 14 Yield只读／1 | 只读并非不读取；补齐公开数据端口的rate override假输入，继续验证read_only=True传递，禁止依赖真实Office。 |

## 实际代码修复

业务代码只修改[共同读取器](../../../../../app/sections/shared/alert_results.py)：备件的全厂共用空产品范围采用固定缓存revision，仍由文件／策略／有效性签名控制刷新；普通产品的revision保持原逻辑。未改业务阈值、TT名单或资源文件。配置仅新增resources.shared_kernel.directories.archived_report_inputs=resources/report，补登记现存历史参考资料集合，不新增界面入口。

## 退役边界与恢复

只删除一个旧Yield页面AST函数，不删除测试文件、不跳过测试。退役前已在会话说明准确目标与依据。原始函数可从9c616182对应路径恢复；live route由第13卡记录新节点，第11卡保留原库存节点。历史交付报告保留当时失败事实，不改写旧结果。

资源规则依据：[CONTEXT](../../../../../CONTEXT.md#resource-path-configuration)。Yield依据：[已发布结果展示设计](../../../../../references/design/inline_domain/shared/architecture-published-alert-display.md)和[Yield结果规则](../../../../../references/domain/yield_domain/shared/rules-yield-alert-results.md)。跨度依据：[CPK/CPM规则](../../../../../references/domain/inline_domain/spc/rules-spc-cpk&cpm-decoration.md)。TT依据：[参数白名单](../../../../../src/inline_domain/core/aoi_tt/parameters.py)及[周报规则](../../../../../references/domain/inline_domain/aoi_tt/rules-aoi-tt-weekly-report.md)。

## 验证结果

固定起点9c616182收集2845项，最终收集2856项：只移除旧Yield页面AST一项，新增12项（目录集合、全厂备件、四种Yield健康状态、实际库存负例／伴随文件、IJP汇总），净增11。原其它23个失败节点均保留且通过。原工作区早期清点包含另一组并行Yield改动，不作为固定基线；独立起点工作区及所有原始证据均保留。

首轮完整回归2849项通过。Spec发现初期替代资源getter测试丢失真实库存保护，最终恢复原节点，登记历史资料集合，并补强负例；完整串行重跑2856项全部通过、0失败，413.93秒，113项既有依赖库警告。命令范围：`pytest tests/unit tests/architecture tests/integration/test_anomaly_api_files.py`。没有skip／xfail屏蔽，也没有提高既有UI测试超时。

补强专项曾有42项通过、1项既有矩阵AppTest触发3秒超时，原始失败记录保留；该项在最终完整串行运行中通过。所有完成的隔离工作区保护测试均记录68个资源／数据文件哈希无变化；初始失败组的Office尝试被阻止，修复后的测试无数据库／Office／维护目录写入尝试。未执行需要真实生产数据库、Office或浏览器Web服务的外部集成；页面用Streamlit AppTest验证。

共享读取器同一源码覆盖合并为86.992%，覆盖合并不修改或隐藏专项超时结果。Pyright检查生产模块与两个夹具helper为0 errors、0 warnings。Standards初审0项；Spec初审1项P2库存保护不足，16a62595补强后两轴均0项未关闭。[评审记录](baseline-regression-review-2026-11-02.md)保留完整关闭过程。

## 合并与原工作区验证

已快进合并master至fb1e454b。合并前后并行Yield改动逐一字节比较相同；8890及8503的PID与创建时间保持一致，API ready正常，未停止／重启正式服务或修改监听设置。没有移动或删除业务资源；配置仅补登记现有历史资料集合。

合并后全部变更测试文件201项通过，51.82秒，无数据库／Office／维护目录写入尝试。该轮采用同样写入、DB、Office保护及预检，不为全仓哈希清点读取无关维护工作簿；不宣称整仓字节不变，也不把隔离全量结果当成对并行未提交Yield代码的完整认证。保留原用户改动，最终提交仅补记录，分支／工作区保留供审计。

## 证据位置

原仓库output/test-results/：baseline-audit-initial-20261102（24项起始失败）、baseline-inline-fixtures-20261102（40项）、baseline-audit-aoi-span-20261102（90项）、baseline-audit-main-fixes-20261102（64项）、baseline-audit-resolved-20261102（原22与初版替代28项）、baseline-audit-full-20261102（首轮2849项）、baseline-audit-full-final-20261102（最终2856项及保护摘要）、baseline-audit-collection-closed-20261102（1移除／12新增节点）、baseline-audit-review-fix-20261102（补强及合并覆盖）、baseline-audit-merge-20261102（字节／服务身份比较）、baseline-audit-postmerge-20261102（合并后201项）。

原问题卡见[14卡目录](../../../../../.scratch/api-baseline-regressions/issues/)。源代码提交86264465、资源库存补强16a62595，合并fb1e454b。收尾曾因PowerShell管道编码使记录中的部分中文变为问号，已从正确的16a62595记录版本重新生成并校验；未改变已验证的代码、测试结果或原始证据。

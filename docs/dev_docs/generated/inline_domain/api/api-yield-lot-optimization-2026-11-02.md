# API 2.1：Yield超规Lot优化记录

日期：2026-11-02。模式：development-flow general。基准：344f4e232a3db200e50d1ec1e636c33473d82768；开发分支：feat/yield-lot-api-attribution，隔离worktree开发。需求源为用户指定的feat-API第2.1节及随后“映射在global.yaml可配置”的明确补充。

规格与执行图：[PRD](../../../../../.scratch/yield-lot-api-attribution/PRD.md)、[单一端到端任务](../../../../../.scratch/yield-lot-api-attribution/issues/01-automatic-lot-attribution.md)。沿用已有纯计算、发布文件和HTTP测试边界，无新增服务或数据库读取路径。

## 实现行为

1. `global.yaml → data_source.defect_group_factory_mapping`保存六条初始映射。ConfigLoader验证非空文本映射并规范厂别大写，composition将加载函数注入既有发布服务，计算时获取当前配置。纯Lot计算接收映射参数，不读YAML。
2. Lot计算保留Lot／Code／分组事实，发布额外写入defect_group与factory，工作簿读回保留两列。旧数据维持报表读取兼容，未知分组保留超规但厂别为空。
3. API读取已发布factory，按现有授权及显式厂别筛选；Yield发生站点、层别固定NULL，不访问元数据文件。已释放但缺厂别的记录503，不使用旧归属表补齐，不从GET重算。
4. 其它五类来源仍要求有效站点和层别。Flag=False、公开可见性、查询周、日期精度和分页边界保持原规则；再发布保留持久人工决定。
5. 新建元数据工作簿只含站点字典；同步只验证／更新站点，保留旧Yield归属及其他工作表，无删除维护资料的迁移。
6. 映射属于现有Yield策略签名覆盖的全局data_source。添加／修改后旧Yield发布需按既有有效性规则正常重发，不能只改时间戳或策略签名冒充已重算。

使用及维护步骤由 [API说明](../../../../../references/domain/inline_domain/monitor/guide-sheet-anomaly-api.md) 和 [Yield发布规则](../../../../../references/domain/yield_domain/shared/rules-yield-alert-results.md)拥有，原 [ADR-0037](../../../../../docs/ADR/0037-published-anomaly-api.md)已按用户2.1明确决定修订。

## 验证

- RED/GREEN：六组分组厂别最初缺字段失败；Yield无元数据文件最初失败；新站点模板／同步不再要求旧归属最初失败；全局配置新加载器最初失败，修复后均通过。
- 最终隔离专项235项通过，覆盖API、元数据、发布／读取／人工Flag、配置与真实临时文件HTTP集成；语句覆盖率92.40%，80%门通过。见 [JUnit](../../../../../output/test-results/yield-lot-api-focused-final/junit.xml)、[覆盖率](../../../../../output/test-results/yield-lot-api-focused-final/coverage.json)。
- 护栏同时保护原仓库和隔离worktree，拒绝数据库／Office和维护目录写入；专项测试阶段禁止操作尝试0，62个隔离维护文件前后哈希0差异。护栏预检的故意拒绝单独计数，见 [summary](../../../../../output/test-results/yield-lot-api-focused-final/summary.json)。
- 类型检查使用原项目虚拟环境。当前修改集的alert_results中有3项既有Timestamp／NaT比较诊断，在原344f4e23文件同一语句逐项复现；未作为新问题或通过来报告。其它被检查修改模块未见诊断。
- 完整unit＋architecture快照回归：2756项，2731通过／25失败。24项与前次已登记基线失败集合完全一致；另1项为隔离worktree缺少未纳入Git的IQC SQL参考文件，补入原参考副本后单项通过，未修改原文件或业务实现。不能把完整回归称为全绿。见 [全量JUnit](../../../../../output/test-results/yield-lot-api-full-clean/junit.xml)、[逐项比较](../../../../../output/test-results/yield-lot-api-full-clean/failure-comparison.json)、[参考文件复验](../../../../../output/test-results/yield-lot-api-reference-check/junit.xml)。
- 最终全量护栏观察窗口内62个维护文件哈希无差异；数据库尝试0，一项既有Yield只读fixture的Office尝试被阻止，未实际创建Office。预检还实际验证了子进程替换PYTHONPATH时护栏仍生效。全量快照早于下述托管修复；托管修复另有专项验证。
- 编译、依赖锁检查和文档路由检查通过。依赖清单未变，锁检查在原仓库执行；隔离worktree未安装另一套环境。类型诊断基线对照见`output/test-results/yield-lot-api-static/`。

首轮复用回归runner时临时目录父路径缺失，造成夹具setup错误；已修正后重跑。随后预检发现旧子进程替换PYTHONPATH可绕过继承，且Windows的Popen审计事件executable可能为None；两轮不完整验证已按核验PID身份停止并确认退出。最终使用解析实际命令的启动审计继承护栏，预检通过后重跑，不把早期运行作为完整隔离证据。隔离IQC副本曾观察到额外512字节及企业保护文件头，无法据此归因到某个fixture或声称某个生产文件被修改；该复制文件未纳入提交，未复制回原资源目录。

## Standards

独立只读评审344f4e23→98b2b957：无具体规范违规或阻断缺陷；配置／纯计算／发布／API边界、字段完整性、人工决定保留及历史归属表保留均符合项目约定。补充评审3fb2d69e同样无阻断。

## Spec

独立只读评审：用户2.1及可配置补充均有实现和验证，未发现缺失或错误实现。保留运行限制：原生产结果必须正常重发，不能由GET补字段。补充托管修复不改变业务授权或工作簿。

两轴具体阻断发现均为0，未将测试基线或类型基线诊断伪装为成功。

## 托管恢复

检查发现原API监督进程已退出，只记录PermissionError类型，原Streamlit仍监听8503。最小测试复现了Windows不允许delete-sharing的读句柄导致状态JSON原子替换失败；保存逻辑新增最多20次、每次50ms重试，永久失败仍上抛。模拟冲突与真实Win32句柄两项通过，原六项最小监听／崩溃恢复／正常失败中断清理也通过，所有临时进程和端口完成清理，见 [生命周期JUnit](../../../../../output/test-results/yield-lot-api-host-recovery/junit.xml)。真实句柄测试另一次定向运行通过，不冒充原7项JUnit包含该后加用例。

仅托管工具修复已回填原仓库并恢复原API，未提前应用未完成验证的Yield业务代码。修复不承诺持续超过约1秒或永久权限错误的恢复能力，原退出日志不足以证明每次PermissionError的具体发生位置。

## 运行与排除项

本次不改写正式Lot发布工作簿、不初始化或修改人工Flag、不查询生产数据库、不推送远端。原报表服务不重启；API托管恢复如上。最终代码合并及API进程更新状态以最终记录为准；配置／代码到位不代表旧发布已自动重算。

## 最终交付

沿用本会话用户已明确批准的合并／正式部署授权，开发分支已快进合并至master（e8f337ba包含98b2b957实现及3fb2d69e托管修复）。仅更新8890 API，原8503报表进程1428未重启；凭证、授权、地址及登录启动快捷方式保持原样。

合并前先验证所有拟更新代码文件无用户改动。global.yaml使用三方合并并保留原Windows换行及用户产品备注；只把目标分支的配置块与托管修复放入索引，再执行标准fast-forward，未暂存用户备注／原始需求／业务资源。两份原需求文档哈希保持不变，见 [合并证据](../../../../../output/test-results/yield-lot-api-merge/merge.json)。

合并后129项相关验证通过；观察窗口内247个原仓库维护文件哈希0差异，数据库／Office／维护文件写入尝试0，见 [合并后JUnit](../../../../../output/test-results/yield-lot-api-postmerge/junit.xml)及[护栏摘要](../../../../../output/test-results/yield-lot-api-postmerge/summary.json)。服务本机与本机内网IP的live／ready均200、未认证schema401；新DTO的层别可空已在当前代码schema核验。没有读取生产业务明细验证可用性，见 [运行检查](../../../../../output/test-results/yield-lot-api-merge/runtime-verification.json)。先前跨设备连通性确认保留，不把本次本机探针冒充新远端实测。

需要正常重新发布Yield结果，才能生成带defect_group／factory且符合当前策略签名的正式文件。没有自动迁移旧结果、没有修改人工决定，不将业务源尚待重发描述为已完成数据更新。

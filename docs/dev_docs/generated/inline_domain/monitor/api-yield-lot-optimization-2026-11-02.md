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
- 完整unit＋architecture回归和Standards／Spec双轴评审进行中，最终结果随后写入。

首轮复用回归runner时临时目录父路径缺失，造成夹具setup错误；已改为预先创建的报告目录并干净重跑，首轮不作为验收证据。较早扩大到旧冷进程／warmup集成时，原fixture写了隔离worktree内复制的IQC结果；已只恢复本次新建worktree中的该复制文件，之后采用上述不可移除护栏。该轮不计为生产I/O隔离证据，原仓库业务文件未由本次实施改写。

## 运行与排除项

本次不改写正式Lot发布工作簿、不初始化或修改人工Flag、不查询生产数据库、不推送远端。既有正式API与报表服务在开发／测试期间保持原运行状态。最终代码合并及API进程更新状态以最终记录为准；配置／代码到位不代表旧发布已自动重算。

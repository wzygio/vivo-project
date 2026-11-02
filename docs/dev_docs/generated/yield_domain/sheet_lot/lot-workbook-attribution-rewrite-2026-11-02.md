# Yield Lot发布工作簿改写记录

日期：2026-11-02，Asia/Shanghai。授权：用户明确要求完成`resources/yield_domain/sheet_lot/yield_lot_oos_results.xlsx`改写；本次是既有明细的本地结构迁移，不重新执行报表计算。

## 已完成内容

- 7个产品工作表共15条原有Lot明细，新增`defect_group`、`factory`及有效`flag`列，原有7列的单元格值、行数和顺序逐表核对不变。
- 只读取Z517、Z553、Z576对应的本地Parquet快照，每批65536行、投影Lot／Defect描述／Group列，确认所需5种Code的分组在各产品中唯一，再使用global.yaml的`data_source.defect_group_factory_mapping`确定厂别；没有数据库查询，没有从Code名称猜厂别。
- Z517的1条归属TP，Z553的1条及Z576的13条归属ARRAY，共14条ARRAY、1条TP；其余4个产品保持0条明细。
- 原文件没有人工Flag控制表，新增7张`<产品>__flags`表，15个既有键均初始化为True，未自动释放任何记录。后续人工False决定由既有发布流程持久保留。
- 原14张工作表保留，现为21张。产品元数据添加释放／归属迁移版本及迁移时间，原fingerprint保存在`pre_attribution_fingerprint`，新fingerprint明确标识本地迁移内容；原`policy_signature`、`calculation_signature`、`generated_at`、`source_refreshed_at`、参考日、覆盖窗口和其他既有元数据均保留。

## 备份与校验

主文件改写前备份为 [yield_lot_oos_results.xlsx.bak](../../../../../resources/yield_domain/sheet_lot/yield_lot_oos_results.xlsx.bak)。此前已有的bak另行归档，避免丢失历史备份。

本次正式写入记录位于`output/test-results/yield-lot-workbook-rewrite-20261102-182455/`：

- [manifest.json](../../../../../output/test-results/yield-lot-workbook-rewrite-20261102-182455/manifest.json)：范围、前后文件摘要、产品行数及备份位置。
- [改写前副本](../../../../../output/test-results/yield-lot-workbook-rewrite-20261102-182455/yield_lot_oos_results.before.xlsx)与[此前bak归档](../../../../../output/test-results/yield-lot-workbook-rewrite-20261102-182455/yield_lot_oos_results.previous.bak)。
- [归属计划](../../../../../output/test-results/yield-lot-workbook-rewrite-20261102-182455/attribution-plan.json)：本地快照摘要及唯一Code／Group关系。
- [最终验证](../../../../../output/test-results/yield-lot-workbook-rewrite-20261102-182455/final-verification.json)：读取器可读取全部21张表、7张控制表就绪、15个Flag均True、数据库查询0。

先在临时副本生成并回读候选工作簿，再在现有publication_lock内核对正式文件与备份内容、快照摘要和配置，调用项目原子工作簿替换工具一次提交；之后重新回读全部工作表，与候选值逐项比较，并验证标准bak仍是改写前内容。没有改写趋势工作簿、源快照、业务配置、用户代码或服务监听。

企业保护在普通读取环境与项目授权读取环境中可呈现不同字节视图。初次跨环境hash检查中止后重新检查，表内容一致；正式写入使用同一读取环境的原件／备份比较，并做完整表值校验，不把读取视图差异误作业务变化。

本次两个只读Excel COM实例均按PID／创建时间核验退出；原有用户Excel实例保留。没有关闭用户窗口，没有留下本次Excel后台进程。

## 数据有效性限制

保留旧发布时间意味着不能将本次结构迁移当成数据刷新。最终7个产品均被现有读取器判为stale（旧参考日、来源时间或策略签名不满足当前要求），未通过重写时间或策略签名绕过。需按正常报表／发布流程生成符合当前策略和有效期的新结果，再由操作者决定哪些Flag改为False；本次未对生产数据库或原始不良率进行重算。

只验证了工作簿与本地读取器；API仍能响应健康检查，但额外状态检查显示监督进程身份未存活、已有worker仍监听就绪，本次未擅自停止或重启该正式服务。

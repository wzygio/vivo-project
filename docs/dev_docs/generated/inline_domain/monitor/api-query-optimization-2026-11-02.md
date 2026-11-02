# API 2.2 查询与返回字段交付记录

日期：2026-11-02。状态：实现、验证及评审完成；合并／正式服务更新待下方补记。

## 已确认范围

用户确认包含首尾的上海日期、返回可用部分且明确标记失败来源，以及01→02→03执行顺序和HTTP／真实临时工作簿测试边界。需求及方案见[审阅记录](api-query-requirements-review-2026-11-02.md)和[本地规格](../../../../../.scratch/anomaly-api-query/PRD.md)。独立分支为feat/anomaly-api-query，基点be925c9e。

## 实现

- 所有查询参数可选；零参数默认上一完整ISO周、全部启用且授权产品／分类／厂别、OOS、无行数截断。新增step_id／layer／parameter_name与start_date／end_date。
- 日期进入各域发布结果读取器；Inline共同选择支持显式日期，页面默认上周。上海日期包含首尾，Q-Time／Yield覆盖及有效性检查保留。
- 删除起始站点；Q-Time发生站点／层别仍采用起点，Yield站点／层别仍null。未增加建议中的实测值、阈值、缺陷分组等业务字段。
- 来源失败丢弃该来源全部行，成功来源200加meta.complete／failed_sources；全部失败503加安全失败来源列表。meta.status只表示已返回记录是否有异常，不表示完整性。
- 汇总后按公开时间与身份统一排序、分页；游标绑定实际范围、日期、授权、公开结果与失败来源状态。默认不限行仍受16MiB等既有资源限制，累计公开结果达到容量时提前失败。

## 验证

修改前146项通过。第一轮专项245项通过；其命名模块覆盖率遗漏core，不能表述为全部选中文件覆盖率。随后按文件名选择覆盖范围，247项通过，11个选中文件覆盖94.387%。

评审修复后扩大至Inline共同读取与Excel适配器：355项通过，1个既有CPK界面用例触发3秒超时；13个选中运行文件覆盖93.564%。待并行全量结束后，将该界面用例所在模块和临时工作簿刷新模块完整串行复测，24项全部通过，关闭临时失败；未改动这些用例或提高超时掩盖问题。

全量（unit＋architecture＋本地文件HTTP集成）最终2808项通过、25项失败。与既有清单比较，24项节点完全一致；新增1项是临时文件备份替换发生WinError5的工作簿刷新用例，上述串行模块复测已通过，首轮全量同项也通过。不宣称全量全绿，不修复或删除用户已决定独立跟踪的24项基线问题。

所有这些护栏运行的62个资源／数据哈希均无变化。专项无数据库／Office／维护目录写入越界尝试；全量有1次既有Yield只读测试的Office创建尝试被护栏阻止，位于已知失败集合；无数据库或维护目录写入尝试。新增COM／XML错误回归仅模拟失败，没有启动Office。

证据均在原仓库output/test-results/：`api-query-baseline-20261102`、`api-query-focused-final-20261102`、`api-query-review-fixed-20261102`、`api-query-full-final-20261102`及`api-query-serial-recheck-20261102`。完整回归的差异清单保留原失败和临时故障，不改写原始JUnit结果。

双轴评审初审Standards1项、Spec2项（两个轴独立报告同一时区问题），已通过079b8822全部关闭；最终两轴各0项未关闭。详见[评审记录](api-query-review-2026-11-02.md)。

类型检查命令：`uvx --offline pyright --venvpath D:/wzy/Python/vivo-project api src/inline_domain/application/monitor/sheet_alert_query.py src/inline_domain/core/shared/sheet_oos_alerts.py src/inline_domain/infrastructure/monitor/excel_alarm_store.py src/shared_kernel/infrastructure/api_workbook_cache.py`，结果0 errors、0 warnings。

本次不修改正式工作簿，不查询生产数据库，不修改凭证或授权范围。全部原工作区用户改动保留。非UI改动，无浏览器界面验证要求。使用方式、cursor及未来字段方向由[使用说明](../../../../../references/domain/inline_domain/monitor/guide-sheet-anomaly-api.md)拥有。

## 交付状态

代码提交eeeaf0bc、评审修复079b8822。目标分支合并和既有8890服务验证待补记。原8503报表服务不在操作范围。后台监督进程在操作前状态检查中已退出，但记录中的API worker仍持有8890且ready=true；更新使用现有托管工具核验并停止其自有进程，再重新启动同一正式profile，不修改授权或监听设置。

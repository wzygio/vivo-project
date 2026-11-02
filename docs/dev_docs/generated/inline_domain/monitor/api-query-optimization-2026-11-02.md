# API 2.2 查询与返回字段交付记录

日期：2026-11-02。状态：实现完成，最终验证／评审／交付进行中。

## 已确认范围

用户确认包含首尾的上海日期、返回可用部分且明确标记失败来源，以及01→02→03执行顺序和HTTP／真实临时工作簿测试边界。需求及方案见[审阅记录](api-query-requirements-review-2026-11-02.md)和[本地规格](../../../../../.scratch/anomaly-api-query/PRD.md)。独立分支为feat/anomaly-api-query，基点be925c9e。

## 实现

- 所有查询参数可选；零参数默认上一完整ISO周、全部启用且授权产品／分类／厂别、OOS、无行数截断。新增step_id／layer／parameter_name与start_date／end_date。
- 日期进入各域发布结果读取器；Inline共同选择支持显式日期，页面默认上周。上海日期包含首尾，Q-Time／Yield覆盖及有效性检查保留。
- 删除起始站点；Q-Time发生站点／层别仍采用起点，Yield站点／层别仍null。未增加建议中的实测值、阈值、缺陷分组等业务字段。
- 来源失败丢弃该来源全部行，成功来源200加meta.complete／failed_sources；全部失败503加安全失败来源列表。meta.status只表示已返回记录是否有异常，不表示完整性。
- 汇总后按公开时间与身份统一排序、分页；游标绑定实际范围、日期、授权、公开结果与失败来源状态。默认不限行仍受16MiB等既有资源限制，累计公开结果达到容量时提前失败。

## 验证

修改前146项通过。第一轮专项245项通过，工作簿／数据哈希62项无变化，数据库／Office／维护目录写入护栏无测试越界尝试。该轮命名模块覆盖率遗漏core，不能将其94%表述为全部选中文件覆盖率。最终范围覆盖、完整回归及双轴评审结果待补记。

类型检查命令：`uvx --offline pyright --venvpath D:/wzy/Python/vivo-project api src/inline_domain/application/monitor/sheet_alert_query.py src/inline_domain/core/shared/sheet_oos_alerts.py`，结果0 errors、0 warnings。

本次不修改正式工作簿，不查询生产数据库，不修改凭证或授权范围。全部原工作区用户改动保留。非UI改动，无浏览器界面验证要求。使用方式、cursor及未来字段方向由[使用说明](../../../../../references/domain/inline_domain/monitor/guide-sheet-anomaly-api.md)拥有。

## 交付状态

最终提交、评审、合并和既有8890服务验证待补记。原8503报表服务不在操作范围。

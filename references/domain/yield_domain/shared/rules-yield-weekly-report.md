# 良率周报

从入库不良率分析看板展开「良率周报导出（PPT）」，点击生成，完成后下载。也可从项目目录运行以下命令，默认输出到 `output/reports`。

```powershell
.venv/Scripts/python.exe tools/generate_yield_weekly_report.py
```

- 以上海时间确定上一完整 ISO 周，从周一到周日，周标识使用 ISO 周年。覆盖全局启用清单中的全部产品，按配置顺序每页排两款。
- 正文结论按该周的趋势预警和显示入库日期在该周的 Lot 超规记录填写。读取已发布工作簿，核验策略、来源与发布有效期以及整周覆盖。按周保留历史趋势，读取明确放行的行。Code 级使用 Defect Code，组级使用缺陷组。两个来源均有有效覆盖且没有放行的预警时，填写「无异常波动，无新增异常，无新增超规lot」。工作簿存在但某产品尚未发布时，正文显示对应记录暂无。整个文件缺失或无效、发布覆盖不足时提示生成失败。
- Group 趋势沿用看板配色，展示最近 3 个月、3 周和 7 天。三幅图等宽，日度柱体较窄，Group 图例在日度图右侧纵向排列。各周期没有趋势数据时标为「暂无数据」。趋势源不可用时报错。
- 保留模板封面，按名称填写双产品正文窗格并移除样例页。标题与结论可编辑，图像由 Plotly 与 Kaleido 直接生成，运行环境需要 Chrome 或 Chromium。受企业保护的模板由已获访问权限的本机 PowerPoint 只读打开。
- 模板通过全局资源项 `resources.yield_domain.files.weekly_ppt_template` 定位。报告生成时读取两份预警工作簿，不发布预警。页面在重新执行导出区块时核对周次、产品范围和输入版本，输入有变化时提示重新生成。

需求见 [良率周报生成器](../../../../docs/dev_docs/dev_spec/yield_domain/shared/feat-weekly_report_generator-yield.md)，设计见 [ADR-0050](../../../../docs/ADR/0050-yield-weekly-powerpoint.md)。

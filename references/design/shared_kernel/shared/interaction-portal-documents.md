# 门户导航与专项资料

## 导航

主页技能树和侧边栏共同由 `app/static/config.js` 配置：

- “专项资料 → 台账周报”统一承载台账、周报、解析报告和其他归档；移除独立的“解析资料”分支。
- 原“特殊指标”分支名为“Inline指标”，保留其已有子项。
- “Inline指标 → 密集点测试”在侧边栏与主页树中共同指向密集点测试报表，按产品查看明细和修正后的膜位汇总；公开页面不呈现阶段字段或调整前后对比。
- “寿命测试”为独立一级分支，其下的“寿命测试”继续打开原寿命测试报表；IQC 分支保留蒸镀材料。

## 合并页

`app/pages/专项资料-台账周报.py` 按所在目录展示四类资料。默认目录位于
`resources/shared_kernel/shared/project_files/`，分别由
`resources.shared_kernel.directories` 下的配置项指定。

| 页面模块 | 配置项 | 默认子目录 |
| --- | --- | --- |
| 北极星台账 | `project_ledgers` | `ledgers/` |
| 北极星周报 | `project_weekly_reports` | `weekly_reports/` |
| 解析报告 | `project_analysis_reports` | `analysis_reports/` |
| 其他归档 | `project_archives` | `archives/` |

页面通过 `ConfigLoader.get_domain_resource_directory()` 读取目录，动态列出支持的文件。
上传先选择四类资料之一，随后写入对应目录。解析报告类别仅接受 PPT、PPTX、PDF。
选择资料类别只更新上传区，成功上传后整页刷新资料列表。
下载、查看和预览缓存按目录区分，各模块的同名文件可独立使用。
Excel 台账继续使用既有编辑和保存流程；解析报告继续支持下载和逐页图片预览。
空的解析报告分组提示上传；预览失败使用业务提示，技术原因写入日志。

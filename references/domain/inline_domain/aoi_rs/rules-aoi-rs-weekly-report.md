# AOI_RS 周度 PPT 报告

核验日期：2026-10-20。范围仅 RS Code，不包含 TT Defect、ESD 或其他指标。

## 时间与数值

- 上一周固定为报告参考日对应的上一完整 ISO 周，周一 00:00 至本周一 00:00（右侧不含）。例如 2026-10-19 对应 2026-W42，即 10-12～10-18。跨年使用 ISO 周年，不使用自然年拼接周号。
- 单元格输出该产品、厂别、站点及 RS Code 的“平均每片 RS 个数”，与当前页面折线图相同；不输出“规格内”，不转换为百分数。
- 复用应用层 `build_aoi_rs_period_trend`，保留输入从上一自然月到当前参考日的完整月份上下文，然后精确提取上周标签。不能先把输入截到上周日，否则会改变当前折线图使用的月份投影。
- 不沿用趋势轴“跳过空周”的回退语义。上一周无过货就写“无过货”，不拿更早周代替。
- 分母来自该站点该周 distinct Sheet/Glass，不能以 RS 明细是否存在判断过货。有过货但没有该 Code 缺陷记录时填 `0`；有明细但趋势无法计算时显示“数据不足”。读取失败使整次生成失败，不生成“无过货”替代报告。
- 应用服务在整个查询窗口 RS 明细为空时仍返回过货及规格，用于识别零缺陷产品及配置项目；空明细不触发原有图表渲染。已有共享历史持久化仍按原覆盖范围执行，过货历史使用真实加载的分母。
- 数值通常四位小数并去掉末尾零；极小非零值使用四位有效数字，避免展示为零。
- 数字按对应产品的 MWD_RATIO 规格判定红绿；共用颜色、边界与缺失规格规则见 [周度 PPT 输出设计规范](../../../design/inline_domain/shared/architecture-weekly-summary-ppt.md)。

## 产品、项目与层别

- 产品来自全局启用列表，按配置顺序输出，默认全部。空选代表全部，禁用产品不可出现在报告中；产品范围变化后，包含禁用产品的旧下载失效。
- 厂别默认 ARRAY，支持 ARRAY/OLED/TP；空选代表全部。
- 行由各所选产品的规格、指标元数据和明细项目并集产生；项目由厂别、站点和 Code 唯一确定，不能把相同 Code 的不同站点相加。
- Array 增加“层别”列，每个 Code 一行。根据用户所给站点注释和层别示例，映射如下：

| 站点 | 层别 |
|---|---|
| 11629（PSI_FI_REV） | 3layer |
| 12629（M1_FI_REV） | M1 |
| 13629（M2_FI_REV） | M2 |
| 15629（M3_FI_REV） | M3 |
| 18629（M4_FI_REV） | M4 |

- “Array 站点（层别）”只筛选 Array，空选代表全部层别。未知站点不静默丢弃，以站点编号作为层别。
- OLED/TP 保持“分类、项目、产品”列；同厂别同 Code 若跨多个站点，项目标签追加站点编号，避免出现无法区分的两行。

## 模板与交互

- AOI_RS 页的“RS 周报导出（PPT）”在普通模式即可显示、生成和下载，无需 URL 参数；与 PDF 报告独立，且不受下方页面查询筛选影响。普通模式同样使用当前报告数值与已启用产品范围，正常、空态及失败提示不暴露内部信息。
- 模板由全局资源键 `resources.inline_domain.files.aoi_rs_weekly_ppt_template` 指定，使用资源解析器获取，当前为用户提供的 `references/template/template-PPT.pptx`。
- 模板为单页、960×540 pt，保留母版、Logo、水印及页面资产，创建可编辑表格。蓝色表头、交替底色；最多八款产品汇总于同页，七至八款产品使用 13 pt 表头及 12 pt 数值字号。超过八款产品再分批，项目行仍按高度分页，所有分页重复表头。
- 标题、周次、起止日和数值单位写入每页。所有厂别无项目时生成明确的“暂无 RS Code 项目”页面，而不是空白 PPT。
- 点击生成后，再点击下载；报告字节保存在当前会话，可重复下载。重新生成失败/中断会解除忙状态，保留上次完整报告；筛选变化时明确提示旧报告范围尚未更新。
- 标准 PPTX 直接读入内存；企业保护格式通过本机 PowerPoint 只读打开并 `SaveCopyAs`，在 Office 关闭前捕获标准 Open XML 字节。保护驱动随后可能再次加密磁盘副本，因此不能依赖退出 Office 后再读取该文件。
- 仅模板规范化需要 PowerPoint。规范化按进程内锁串行、按路径/mtime/size 缓存原生字节；临时副本只放 `output/tmp`，成功/失败/异常中断都清理。仅关闭本次打开的文稿，已有 PowerPoint 进程存在时不调用 Application.Quit。模板读取、原生字节缓存和 PPT 写入与 [TT 周报](../aoi_tt/rules-aoi-tt-weekly-report.md)共用；RS 的业务入口及报告字段保持不变。
- 普通界面不暴露内部字段、配置路径、原始异常或数据版本；下载只含业务内容。

## 验证入口

- 数据、真实服务零 RS 分支：`tests/unit/inline_domain/application/aoi_rs/test_weekly_report.py`。
- 文件与版式：`tests/unit/app/sections/aoi_rs/test_aoi_rs_weekly_ppt.py`，覆盖行/列分页及图片关系保留。
- AppTest 及异常中断：`tests/unit/app/sections/aoi_rs/test_aoi_rs_weekly_export.py`。
- 模板读取及 Office 生命周期：`tests/unit/inline_domain/infrastructure/shared/test_ppt_template.py`。
- 真实模板验证使用合成 RS 事实，生成 8 页并由 PowerPoint 重新打开；示例与预览写入 `output/test-results/aoi_rs_weekly/`，未查询生产数据库。

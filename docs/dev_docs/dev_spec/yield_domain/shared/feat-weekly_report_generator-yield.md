# Task：良率周报生成器
- 身份：我是一个OLED显示屏制造商的员工，我的岗位是工艺整合部的大数据分析工程师。
- 项目：当前项目是我开发的用于给客户展示本公司数据的报表。
- 任务：请帮我开发一份“良率周报”生成工具。即调用`src\yield_domain`的数据，按照指定格式，生成一份PPT日报。

## References
1. 如果有不理解的词汇，请参考：`04-Resources\domain\GLOSSARY.md`
2. 数据探查与读取：
    - 数据库链接例程：`src\shared_kernel\infrastructure\db_handler.py`
	- 数据库链接凭证：`.env`

## Data Source
1. 良率预警记录：
    - 趋势预警记录表：`resources\yield_domain\mwd_trend\yield_trend_alert_results.xlsx`
    - lot超规记录表：`resources\yield_domain\sheet_lot\yield_lot_oos_results.xlsx`
2. 入库不良率报表：`app\pages\入库不良率分析看板.py`
3. 报告模板：`resources\yield_domain\北极星周报-template-yield.pptx`
    - P0：标题页，无需改动
    - P2：样例页（只是用于在开发过程中帮助你理解，最终生成的报告中不要包含这一页）
    - P1：模板页。

## Requirements
1. 模板页各窗格的名称及填写规则如下：
    - 标题1：{产品型号}不良率
    - 汇总语句：{产品型号} - {周度}不良率，{结论}
        - {周度}：上个ISO周
        - {结论}：根据“趋势预警记录表”和“lot超规记录表”中，对应“产品型号”-“上周”的记录来编写
            - 无任何异常：“无异常波动，无新增异常，无新增超规lot”
            - 有“趋势预警记录””(仅限“flag”为“FALSE”)：{defect Code}-{rules}
            - 有“lot超规记录”(仅限“flag”为“FALSE”)：{超规lot}-{异常Code}超规
    - 月周天趋势图1：填充对应“产品型号”的Group级“月周天趋势图”
        - 来源：入库不良率报表中，最上方的趋势图。不过目前是三幅图分开，你插入时思考如何同时导入三张图（比如先生成到本地），或者截图（但我不建议截图，因为截图功能并不稳定）
        - 时间窗口：“月周天趋势图”不需要区分“周度”，与当前报表展示的一致即可
2. 产品数量：每页只填写两个产品的数据，但要覆盖“入库不良率报表”中的所有产品，分页填写
3. 时间周期：永远是上个ISO周
4. 前端样式：可参考`app\pages\AOI_RS监控报表.py`中的周报样式

## Workflow
1. 请结合报表和报告模板，分析并上述需求
2. 调用`development-flow`完成开发

【Goal】不断迭代优化，直至顺利生成上周的良率周报
- 除权限问题外不需要询问，直接执行到底，所有决策采用默认选项
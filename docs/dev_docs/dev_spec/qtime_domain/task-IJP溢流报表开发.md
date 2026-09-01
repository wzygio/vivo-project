# Task：Q-Time报表开发
- 身份：我是一家OLED显示屏制造公司的大数据分析工程师
- 项目：我们正在开发的是一款面向客户的企业级报表，现在需要开发其中的IJP溢流监控报表
- 任务：目前内部已经基于FineReport平台（帆软）开发了一份现成的报表，请你根据我提供的sql语句及报表样式，将其使用“python + streamlit”复刻出来（样式一致即可）

## References

### Sql语句
请参考：`docs\dev_docs\dev_spec\qtime_domain\sql-IJP溢流报表开发.md`

### Terms
- IJP：一种打印技术，在OLED制造工艺中用于成膜。具体来说，其对应第二大段工艺——OLED（蒸镀）中的一道子工艺，用来制作TFE（封装层）中的有机层
- 溢流：对于该名词我也并不了解，如果你认为制作报表需要了解该名词含义，可以到网络上搜索。

其它你想要了解的专有名词可以参考：`references\domain\GLOSSARY.md`

### 报表样式
1. 前端样式：
- 开发界面（FineReport中的开发截图）：`docs\dev_docs\dev_spec\qtime_domain\IJP溢流报表样式-开发界面.png`

2. 前端架构：在`app\pages`下新建一个page，具体架构可按照`visionox-dashboard-ui`

3. 后端架构：在`src`下新建一个domain，遵循DDD架构完成开发，具体架构可参考`ARCHITECTURE.md`

## Workflow
1. 请分析报表样式图片，了解呈现样式及所需数据
2. 请分析SQL语句，了解每个每项所需数据的来源：
    - 不断分析直至找到所有数据来源。你可以尝试探查数据库，数据库读取程序可参考：`src\yield_domain\infrastructure\data_loader.py`
    - 输出一份文档至如下路径：`docs\dev_docs\dev_spec\qtime_domain`；无法找到的数据源也记录到其中
3. 按照`development-flow`完成报表开发，开发流程中，如果没有遇到无法解决的问题（比如业务逻辑问题）则自动执行，无需让我确认

## Goal
不断迭代优化直至E2E测试通过
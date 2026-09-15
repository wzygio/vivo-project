# Task：IJP溢流报表开发-孔区
- 身份：我是一家OLED显示屏制造公司的大数据分析工程师
- 项目：我们正在开发的是一款面向客户的企业级报表，现在需要开发其中的IJP溢流监控报表
- 任务：目前内部已经基于FineReport平台（帆软）开发了一份报表，请你根据我提供的sql语句及报表样式，将其使用“python + streamlit”复刻出来（样式一致即可）

## References

### Sql语句
请参考：`docs\dev_docs\dev_spec\indicator_domain\sql-IJP溢流报表开发-孔区.md`

### Terms
- IJP：一种打印技术，在OLED制造工艺中用于成膜。具体来说，其对应第二大段工艺——OLED（蒸镀）中的一道子工艺，用来制作TFE（封装层）中的有机层
- 其它你想要了解的专有名词可以参考：`references\domain\GLOSSARY.md`

## Requirements
当前已经开发了一个外部报表，只不过我们需要补全“孔区”相关的数据。因此请你先尝试能否直接融入`app\pages\IJP溢流监控报表.py`（这取决二者数据结构是否相似），如果不能，再考虑按照如下格式开发新报表。

### Frontend
1. 图像样式：请参考以下图片（内部已开发的报表在FineReport中的截图）
    - `docs\dev_docs\dev_spec\indicator_domain\IJP溢流报表-孔区-1.png`
    - `docs\dev_docs\dev_spec\indicator_domain\IJP溢流报表-孔区-2.png`
2. 其它样式：以上样式参考仅针对图片绘制，至于筛选器、排布方式（比如Expander，每行排布几幅图）等其它样式请请全部与现有报表保持一致：`app\pages\IJP溢流监控报表.py`
3. 前端架构：在`app\pages`下新建一个page，具体架构可按照`visionox-dashboard-ui`

### Backend
1. 后端架构：在`src\indicator_domain`下新建一个子模块，遵循DDD架构完成开发，具体架构可参考`ARCHITECTURE.md`
    - 数据筛选：仅筛选“孔区类型”中的“大孔”，这一点请在infrastructure层实现，避免提取过多数据进行后续计算

### Design
其它设计请参考`CONTEXT.md`（如果涉及）

## Workflow
1. 请分析报表样式图片和已有报表`app\pages\IJP溢流监控报表.py`，了解呈现样式及所需数据
2. 请分析SQL语句，了解每个每项所需数据的来源，不断分析直至找到所有数据来源。输出一份文档至如下路径：`docs\dev_docs\dev_spec\indicator_domain`（无法找到的数据源也记录到其中）
    - 你可以尝试探查数据库，数据库读取程序可参考：`src\yield_domain\infrastructure\data_loader.py`
3. 按照`development-flow`完成报表开发：如果信息缺失，或者有无法解决的业务问题可以寻求我的帮助；否则直接执行到底即可

## Goal
不断迭代优化直至E2E测试通过

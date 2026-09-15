# Task：寿命测试报表开发
- 身份：我是一家OLED显示屏制造公司的大数据分析工程师
- 项目：我们正在开发的是一款面向客户的企业级报表系统，现在需要开发其中的“IQC寿命测试报表”

## References

### Sql语句
- 请参考：`docs\project_files\iqc_domain\蒸镀材料体系报表-sql语句.txt`

### Terms
- 蒸镀材料：在OLED制造工艺中用于蒸镀的材料。具体来说，其对应第二大段工艺——OLED（蒸镀）。
- 其它你想要了解的专有名词可以参考：`references\domain\GLOSSARY.md`

### Requirements
1. 前端样式：当前已经制成UI，直接参考以下报表即可`app\pages\IQC蒸镀材料报表.py`
    - 需要增加以下两个字段：“IQC结果”、“COA结果”（需要你查询sql语句来查看这两个字段的来源）
    - 如果你需要最原始的样式定义，请查看`resources\iqc_domain\IQC来料检验特性数据明细报表.xlsx`中的“sheet2”
2. 前端架构：具体架构可参考`visionox-dashboard-ui`
3. 后端架构：遵循DDD架构完成开发（对应domain为`src\iqc_domain`），具体架构可参考`ARCHITECTURE.md`

## Workflow
1. 请分析并了解前端的呈现样式及所需数据
2. 请分析SQL语句，了解每个每项所需数据的来源。不断分析直至找到所有数据来源，最终输出一份文档至如下路径：`docs\dev_docs\dev_spec\iqc_domain`（无法找到的数据源也记录到其中）
    - 你可以尝试探查数据库，数据库读取程序可参考：`src\yield_domain\infrastructure\data_loader.py`
3. 按照`development-flow`完成报表开发：如果信息缺失，或者有无法解决的业务问题可以寻求我的帮助；否则直接执行到底即可

## Goal
不断迭代优化直至E2E测试通过
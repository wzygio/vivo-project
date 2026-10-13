# Task：蒸镀材料报表开发
- 身份：我是一家OLED显示屏制造公司的大数据分析工程师
- 项目：我们正在开发的是一款面向客户的企业级报表，现在需要开发其中的“IQC蒸镀材料报表”
- 任务：目前内部已经基于FineReport平台（帆软）开发了一份内部报表，请你根据我提供的sql语句及报表样式，将其使用“python + streamlit”复刻出来

## References

### Sql语句
- 内部报表sql语句：`docs\project_files\iqc_domain\eva_materials\蒸镀材料体系报表-sql语句.txt`
- 蒸镀批次物料报表：`docs\project_files\iqc_domain\eva_materials\V3坩埚加料&回收报表-sql语句.md`
    - FineReport截图：`docs\project_files\iqc_domain\eva_materials\V3坩埚加料&回收报表.png`
    - 关键字段：
        - “蒸镀周期”：即“蒸镀批次”
        - “物料规格名”：即“物料号”
### Terms
- 蒸镀材料：在OLED制造工艺中用于蒸镀的材料。具体来说，其对应第二大段工艺——OLED（蒸镀）。
- 其它你想要了解的专有名词可以参考：`references\domain\shared_kernel\shared\GLOSSARY.md`

### Database
- 你可以尝试探查数据库，相关程序可参考：
    - `src\shared_kernel\infrastructure\db_handler.py`
    - `src\yield_domain\infrastructure\data_loader.py`

---

## Task1：报表制作

### Requirements
1. 前端样式：当前已经制成UI，请参考`app\pages\IQC蒸镀材料报表.py`
    - 需要增加以下两个字段：“IQC结果”、“COA结果”（需要你查询sql语句来查看这两个字段的来源）
    - 如果你需要最原始的样式定义，请查看`resources\iqc_domain\IQC来料检验特性数据明细报表.xlsx`中的“sheet2”
2. 前端架构：具体架构可参考`visionox-dashboard-ui`
3. 后端架构：遵循DDD架构完成开发（对应domain为`src\iqc_domain`），具体架构可参考`ARCHITECTURE.md`

## Workflow
1. 请分析并了解前端的呈现样式及所需数据
2. 请分析SQL语句，了解每个每项所需数据的来源。不断分析直至找到所有数据来源，最终输出一份文档至如下路径：`docs\dev_docs\dev_spec\iqc_domain`（无法找到的数据源也记录到其中）
    
3. 按照`development-flow`完成报表开发：如果信息缺失，或者有无法解决的业务问题可以寻求我的帮助；否则直接执行到底即可

## Goal
不断迭代优化直至E2E测试通过

---

## Task2：报表优化
当前报表中的“物料号”是针对所有“产品型号”的，我们需要对其进行筛选，确保只展示合法“产品型号”的数据。

### Requirements
1. “物料号”与“产品型号”之间通过“蒸镀批次”这一字段关联：
    - “物料号”与“蒸镀批次”的关联关系请分析“蒸镀批次物料报表”
    - “产品型号”与“蒸镀批次”的关联关系请分析`app\pages\入库不良率分析看板.py`中的Mapping模块
        - 其对应的sql语句位于：`src\yield_domain\infrastructure\data_loader.py`
2. 产品型号筛选（与其它报表保持一致）：
    - 统一受`config\global.yaml`中的“enabled_products”控制
    - 前端新增筛选条件：“产品型号”筛选，并添加注释`app\components\product_labels.py`

## Workflow
1. 请分析并了解“蒸镀批次物料报表”，确定每项所需数据的来源，并探查数据库确认所需数据对应的数据表是否有访问权限。如果没有请中断并告知我。
2. 按照`development-flow`完成报表开发：
    - Goal：不断迭代优化直至E2E测试通过
    - 如果信息缺失，或者有无法解决的业务问题可以寻求我的帮助；否则直接执行到底即可
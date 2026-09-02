# Task：Q-Time报表开发
- 身份：我是一家OLED显示屏制造公司的大数据分析工程师
- 项目：我们正在开发的是一款面向客户的企业级报表，现在需要开发其中的Q-Time报表
- 任务：目前内部已经基于FineReport平台（帆软）开发了一份现成的报表，请你根据我提供的sql语句及报表样式，将其使用“python + streamlit”复刻出来（样式一致即可）

## References

### Terms
- Q-Time：两个站点之间的过货时间。如果过货时间过长说明生产中某个环节可能发生异常，导致产品滞留，延长OLED材料的暴露时间，进而影响产品质量。

其它你想要了解的专有名词可以参考：`references\domain\GLOSSARY.md`

### SQL语句
帆软报表中，每个数据集对应的sql语句分别如下：

1. `prodcode`：
```
SELECT DISTINCT productspecname FROM EDA.imp_qtime_tzbjx
```

2. `Search`：
```
select * from
(SELECT step_desc,
lot_id,
prod_qty,
sub_prod_type,
f_step,
t_step,
q_spec,
wait_time,
timekey,
CASE WHEN F_STEP LIKE '1%' THEN 'ARRAY'
WHEN F_STEP LIKE '2%' THEN 'OLED'
ELSE 'TP'
END AS SHOP,
prodcode FROM QTIME_TZBJX
WHERE 1=1
AND TIMEKEY>=replace(replace(replace('${cmcbstarttime}','-',''),':',''),' ','')
AND TIMEKEY<replace(replace(replace('${cmcbendttime}','-',''),':',''),' ','')
--AND f_step='${cmbfstep}'
--AND t_step='${cmbtstep}'
AND step_desc='${cmbtSTEPDESC}'
--AND step_desc='Shipping->Cutting'
${IF(LEN(cmbprodcode)=0,"","AND prodcode IN ('"+cmbprodcode+"')")}
--AND STEP_DESC='Array_SHIP->Half Cutting'
--AND TIMEKEY BETWEEN '20251201000000' AND'20260101000000'
) A
where 1=1
AND shop='${cmbtshop}'
order by  step_desc,
lot_id,timekey
```

3. `step_desc`：
```
SELECT DISTINCT step_desc FROM 
(SELECT CASE WHEN F_STEP LIKE '1%' THEN 'ARRAY'
WHEN F_STEP LIKE '2%' THEN 'OLED'
ELSE 'TP'
END AS SHOP, step_desc FROM QTIME_TZBJX
) A
WHERE 1=1
${IF(LEN(cmbtshop)=0,"","AND SHOP IN ('"+cmbtshop+"')")}
```

### 报表样式
1. 前端样式：
    - 开发界面（FineReport中的开发截图）：`docs\dev_docs\dev_spec\indicator_domain\Q-Time报表样式-开发界面.png`
    - 使用界面（其中展示了筛选条件）：`docs\dev_docs\dev_spec\indicator_domain\Q-Time报表样式-使用界面.png`
2. 前端架构：在`app\pages`下新建一个page，具体架构可按照`visionox-dashboard-ui`
3. 后端架构：在`src`下新建一个domain，遵循DDD架构完成开发，具体架构可参考`ARCHITECTURE.md`

## Workflow
1. 请分析报表样式图片，了解呈现样式及所需数据
2. 请分析SQL语句，了解每个每项所需数据的来源：
    - 不断分析直至找到所有数据来源。你可以尝试探查数据库，数据库读取程序可参考：`src\yield_domain\infrastructure\data_loader.py`
    - 输出一份文档至如下路径：`docs\dev_docs\dev_spec\indicator_domain`；无法找到的数据源也记录到其中
3. 按照`development-flow`完成报表开发，开发流程中，如果没有遇到无法解决的问题（比如业务逻辑问题）则自动执行，无需让我确认

## Goal
不断迭代优化直至E2E测试通过

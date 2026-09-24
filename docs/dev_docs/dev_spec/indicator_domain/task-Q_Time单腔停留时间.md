# Task：Q-Time报表开发
- 身份：我是一家OLED显示屏制造公司的大数据分析工程师
- 项目：我们正在开发的是一款面向客户的企业级报表，现在需要优化其中的Q-Time报表，在其中补充蒸镀单腔停留时间的数据。

## References

### Terms
- Q-Time：两个站点之间的过货时间。如果过货时间过长说明生产中某个环节可能发生异常，导致产品滞留，延长OLED材料的暴露时间，进而影响产品质量。
- 四大工艺：
  - **ARRAY（阵列）**：制作 TFT 背板。
  - **OLED（蒸镀／EVA，Evaporation）**：制作有机发光层。
  - **TP（Touch Panel）**：制作触控层。
  - **CELL（屏体封装）**：完成屏体封装及 CT 检测。
- 线体：蒸镀段工艺分为两条线体，分别为“3CEE001/3CEE002”
- 其它你想要了解的专有名词可以参考：`references\domain\GLOSSARY.md`

### Data Source
1. 两条蒸镀线体的当月glass别蒸镀腔室Q-Time数据如下表所示（该表为加密文件，你需要先解密才能读取）：
    - 3CEE001：`resources\indicator_domain\q_time\V3蒸镀机QTime一级报表-3CEE01-0924.xlsx`
    - 3CEE002：`resources\indicator_domain\q_time\V3蒸镀机QTime一级报表-3CEE02-0924.xlsx`
    - 所需数据从第9行开始
    - 关键字段：
        - GlassID
        - LL1Time：进片时间
        - E列至O列（请排除D列）：各腔室间过货时间
2. 筛选产品型号：数据表中的数据是当月所有产品的数据，你需要筛选出（`config\global.yaml`-`enabled_products`）中所包含产品对应的glass_id，该逻辑当前页面（`app\pages\Q_Time监控报表.py`）已实现，复用即可。

可供参考的sql语句如下：
```
select 
	sto.glass_id 
from eda.spc_tzbjx_oled sto
JOIN mdw.dwr_mes_productspec AS dmp
  ON dmp.productspecname = sto.product_spec
where 
	dmp.productcode = 'M626'
	and sto.glass_start_time > '20260901'
limit 10
```

## Requirements
1. 前端样式：在`app\pages\Q_Time监控报表.py`中新建一个模块，置于当前模块的下方。
    - 名称：蒸镀单腔停留时间监控
    - 筛选框：产品型号、线体、腔室
2. 后端样式：在`src\indicator_domain`中的`q_time`这一子模块中构建。

## Workflow
1. 请分析当前报表与数据源，了解呈现样式及数据结构
2. 按照`development-flow`完成报表开发
    - 如果遇到无法解决的问题（比如业务逻辑问题）可让我确认

## Goal
不断迭代优化直至E2E测试通过

# Task1：AOI Total Defect报表制作
请你参考`src\inline_domain`中`aoi_rs`子模块的逻辑，绘制“AOI_TT”的报表

## Terms
- **AOI（Automatic Optical Inspection）**：Array/OLED前段的检测站点，主要用于检测图形、残留、颗粒
- Total Defect（后续简称TT，即total的缩写）：AOI的检测结果，拍照出的defect个数。
    * 直接使用`param_value`这个字段作为defect个数即可
- **RS（Review Station）**：对AOI拍出的defect点位进行复判，判定具体的Code，其与AOI的关系类似于MVI相较于AVI
    * 注意，这里的Code也是一种defect代码，但与Defect Code并不一致，一般用长度为五位的代码指代
- AOI和RS的单位都是个，即不良点位个数，数据库中每一条记录算一个
    * 可以直接对`code_qty`这个字段进行加和计算。

所有不清楚的专业词汇，请先到以下文件中查询：`references\domain\GLOSSARY.md`

## 报表样式
与`aoi_rs`子模块的完全一致

其它关键信息：
- 时间范围：默认为“上一个自然月 1 日 ～ 当前日期（含当天）”，不再设置时间筛选框
- 颗粒度：从大到小依次为“产品型号、厂别、站点、TT、TT个数（即明细）”
- 参数与规格：“产品型号、站点、TT”锁定唯一参数及对应规格
- 筛选框：厂别、站点、Code名称（与`app\pages\AOI_RS监控报表.py`一致）

## 数据源及关键字段
- eda.spc_tzbjx_array：ARRAY测量明细表
    * sheet_id：Sheet ID
    * sheet_start_time：过货时间
    * step_id：站点
    * param_name：RS Code代码
    * param_value：AOI个数
    * product_spec：产品代码，用于追溯产品型号，示例如下
```
from eda.spc_tzbjx_array sta
left join mdw.dwr_mes_productspec dmp
	on dmp.productspecname = sta.product_spec
``` 
- eda.spc_tzbjx_oled：OLED测量明细表
    * glass_id：Glass ID
    * glass_start_time：过货时间
    * 其余字段与上面的报表一致
- eda.spc_tzbjx_tsp：TP测量明细表
    * glass_id：Glass ID
    * glass_start_time：过货时间
    * 其余字段与上面的报表一致
- mdw.dwd_imp_dv_param_spec：参数规格表，包括USL/LSL/UCL/LCL，对于TT，只需要USL/UCL
    * prod_code：产品型号
    * step_id：站点
    * usl：upper specification limit
    * ucl：upper control limit

## Workflow
1. 请先参考`app\pages\AOI_RS监控报表.py`的逻辑链条。了解自己要制作的是一种什么报表，我们即将制作的“AOI_TT”报表与其极其相似
2. 请分析出绘制报表所需要的数据，然后输出到如下路径：`references\domain\aoi_rs\spec-data_source.md`
3. 请探查数据表寻找数据源，直至你所需要的每一项数据都找到了对应的数据源。最终，补全该spec文件中每一项数据的数据源：`references\domain\spec`
    - 请参考`src\shared_kernel\infrastructure\db_handler.py`编写脚本探查数据表
    - 字段字段名称会因不同厂别的数据表而有所差异，部分规律可以参考`src\inline_domain\infrastructure\spc`
    - 如果无法找到数据源（比如有些数据结构你无法理解），请在这里中断并向我询问
4. 找到所有数据源后，请按照`Developement Flow`这一skill完成开发

## Goal
不断完善直至E2E测试通过

--- 

# Task1-1：AOI_TT报表区分particle_size
当前我们能够统计出每个“产品-站点-TDSUM-sheet_id”对应defect点位数，现在需要区分“particle size”（defect点大小类型）

## References
### 数据表
- defect明细表：`eda.ARRAY_DEFECT_T`，用于获取“particle size”
- spc明细表：用于获取缺点点位数（param_value）
```
--eda.spc_tzbjx_array   — ARRAY SPC 测量明细表（时序数据）
--eda.spc_tzbjx_oled    — OLED SPC 测量明细表
--eda.spc_tzbjx_tsp     — TP SPC 测量明细表
```
- 产品规格表：`DWR_MES_PRODUCTSPEC`，用于获取参数规格及产品型号

### 参考sql语句
必要的字段、筛选字段及与其他表的关联方式如下
```
select count(*)
--	step_id ,
--	glass_id , -- 在该表中表示sheet_id
--	item119, -- particle size
--	glass_start_time, -- 过货时间
--	sta_dmp.productcode -- 产品型号
from eda.ARRAY_DEFECT_T adt
left join 
(
	select	
		sta.sheet_id as sheet_id,
		dmp.productcode as productcode
	from eda.spc_tzbjx_array sta
	left JOIN DWR_MES_PRODUCTSPEC dmp
		on sta.product_spec = dmp.PRODUCTSPECNAME -- 获取产品型号
) as sta_dmp
	on adt.glass_id = sta_dmp.sheet_id -- 通过sheet_id/glass_id与主表关联
where 
	adt.item51 = 'AOI' -- 测试类型
	and adt.glass_id  = 'L3MY6800E06'
	and adt.step_id = '13620'
limit 10
```
- 必要字段：
```
--	step_id ,
--	glass_id , -- 在该表中表示sheet_id
--	item119, -- particle size
--	glass_start_time, -- 过货时间
--	sta_dmp.productcode -- 产品型号
```
- 关联方式：
```
left join 
(
	select	
		sta.sheet_id as sheet_id,
		dmp.productcode as productcode
	from eda.spc_tzbjx_array sta
	left JOIN DWR_MES_PRODUCTSPEC dmp
		on sta.product_spec = dmp.PRODUCTSPECNAME -- 获取产品型号
) as sta_dmp
	on adt.glass_id = sta_dmp.sheet_id -- 通过sheet_id/glass_id与主表关联
```
- 筛选条件：
```
	adt.item51 = 'AOI' -- 测试类型
```

## Step1：分析是否需要去重
请你先探查数据表（可参考程序：`src\yield_domain\infrastructure\data_loader.py`），分析是否需要去重：

以下两个sql语句得到的结果完全一致，都是218：
- 明细表统计：
```
select count(*)
from eda.ARRAY_DEFECT_T adt
where 
	adt.item51 = 'AOI' -- 测试类型
	and adt.glass_id  = 'L3MY6800E06'
	and adt.step_id = '13620'
limit 10
```
- 主表查询结果：
```
select
	sheet_id ,
	step_id ,
	param_name ,
	param_value ,
	sheet_start_time 
from eda.spc_tzbjx_array
where 
	sheet_id = 'L3MY6800E06'
	and step_id = '13620'
	and param_name like 'TDSUM'
	and sheet_start_time > '20260801'
limit 10
```

但是执行“### References”中提供的参考sql语句后，得到查询得到的结果不一致，请分析原因是什么造成的，我们是否需要去重？

## Step2：完成优化
请优化AOI_TT的报表，区分“particle size”。按照`developement-flow`完成开发：
1. defect点位数的总数依旧是从“spc明细表”中的param_value获取，但是每种“particle size”的defect点位数则从defect明细表中汇总得到。
    - “particle size”只筛选“O”和“L”这两个等级
2. 前端增加“O”和“L”这两个等级的趋势图，同样是分为月周天、ByLot别、Bysheet别三类
    - 通过一个筛选框来筛选“particle size”，分为“Total”、“O”和“L”这三个选项（可多选，默认全选）
    - 不同“particle size”的趋势图依旧放到同一个Expander下（“站点-参数名称”）

## Goal
请不断迭代优化直至E2E测试通过：如果有无法解决的业务问题可以向我询问，否则直接执行到底完成开发

---

# Task1-1-1：AOI_TT报表优化
当前已经完成了“Task1-1：AOI_TT报表区分particle_size”，我们需要在此基础上进行优化：

## Step1
“particle size”的筛选对于不同工艺段不同：

| 厂别 | 表名 | 字段名 | 可选范围 |
| --- | --- | --- | --- |
| ARRAY | ARRAY_DEFECT_T | item119 |  |
| OLED | OLED_DEFECT_T | item2 | 不启用该功能 |
| TP | TSP_DEFECT_T | item2 | 所有非NULL |

## Step2

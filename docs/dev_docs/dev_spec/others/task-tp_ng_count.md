# Task：统计在库TP NG不良数
请帮助我统计26年在库的TP NG不良panel数。其中关键点有四：
- 26年
- TP NG
- 在库
- panel

##  入库明细表：`DWT_WAREHOUSING_PNL` - 筛选“TP NG”

### 示例：
```
select 
	count(*)
from DWT_WAREHOUSING_PNL dwp
left join 
	(
		select 
			defect_code,
			factory
		from imp_ct_dft_group
	) as icdg
 	on dwp.first_defect_code = icdg.defect_code
where 
	dwp.date_timekey > '20260101'
	and icdg.factory = 'TP'
	and dwp.last_flag = 'Y'
```

### 关键字段：
- dwp.date_timekey：筛选时间
- icdg.factory：筛选厂别。通过defect_code关联后，“TP NG”对应取值为“TP”
- dwp.last_flag：用于去重，筛选除最后一条有效记录

但是“DWT_WAREHOUSING_PNL”中的数据只统计了入库，然而入库的panel很多都已经出库。不能确定是否在库。因此需要第二个数据表。

## 库存明细表：`DWR_WMS_TBLRECLABEL`- 筛选“在库”

### 示例：
```
SELECT
	T.LABELSN ,t.*
FROM
	DWR_WMS_TBLRECLABEL T
INNER JOIN DWR_WMS_TBLRECLABEL T1 ON
	T.PACKBOXNO = T1.LABELSN
	AND T1.ACCOUTSTATUS = 'lm_deliveried'
	AND T1.LABELLEVEL = 3
	AND T1.INVENTORYCODE IN ('2F91','3F91','9F91','9F92','ZF04')
```

### 关键字段：
- T1.ACCOUTSTATUS：在库状态，如果在库则为'lm_deliveried'
- T.PACKBOXNO：箱号
- T.LABELSN：ID（但经过查询后，我发现其中包含各种ID）

### 关键问题：
我们需要通过箱号，才能将“panel_id”关联到“ACCOUTSTATUS”
- 上面这段sql语句是我的一位同事告知我的。它似乎成功实现了“将“panel_id”关联到“ACCOUTSTATUS””，但我不清楚它是如何实现的
- panel_id：长度为15位，且以“L”开头
- box_id：以“W”开头

分析结果如下：
```
关键问题的答案：panel_id 如何关联到 ACCOUTSTATUS
探查数据库后确认，DWR_WMS_TBLRECLABEL 是一张自关联的标签层级表，靠 LABELLEVEL 区分层级：

LABELLEVEL = 1（约 1.9 亿行）：panel 级标签行。LABELSN 就是 panel_id（15 位、L 开头），它的 PACKBOXNO 字段记录所属箱的箱号。
LABELLEVEL = 3（约 400 万行）：箱级标签行。LABELSN 是箱号（W 开头），库存状态 ACCOUTSTATUS 和库位 INVENTORYCODE 只记录在箱行上，panel 行自身的 ACCOUTSTATUS 恒为 lm_initial，不能用来判断在库。
所以关联链是：panel_id = T.LABELSN →（T.PACKBOXNO = T1.LABELSN）→ 箱行 T1，从箱行读取状态。这就是同事那段 SQL 的原理——T 取 panel 行，T1 取满足在库条件的箱行，INNER JOIN 后剩下的 T.LABELSN 即"在库 panel"
```

## Workflow
1. 请探查数据库，明确关键问题的答案：如何通过箱号，才能将“panel_id”关联到“ACCOUTSTATUS”
     - 探查数据库，参考脚本：`src\yield_domain\infrastructure\data_loader.py`
2. 请分析我的需求，编写相应的sql脚本，输出至如下路径：`tools`
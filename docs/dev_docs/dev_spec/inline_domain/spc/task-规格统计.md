# Task：规格统计

## Data Source
- 规格统计表：`docs\project_files\inline_domain\spc\V3 SPC_天柱北极星填报报表V.xlsx`-“规格统计”，关键字段如下
    - 产品型号：对应“产品信息表”中的“productcode”
    - 厂别：对应“产品信息表”中的“factorycname”
    - 量产/非量产：“productspecname”，以“E”结尾即“非量产”，以“P”结尾即“量产”
    - 品名：对应“产品信息表”中的“productspecname”
    - 站点：step_id
    - 参数：parameter
    - 数据类型：固定填写“SPC”

- 产品信息表：`mdw.dwr_mes_productspec`，用于填充“规格统计表”中的信息，参考sql语句如下
```
-- 产品信息表：裁切数、产品型号
select 
	distinct productspecname,
	factoryname,
	dmp.productcode  --产品型号
from mdw.dwr_mes_productspec dmp
where 
    dmp.productspecname = 'G3659FP102FT-007P'
```

## Workflow
1. 请从“产品信息表”中，提取信息并填充“规格统计表”中的前三个字段
2. 请按照“产品型号”分为不同的sheet，每个sheet内依次按照以下字段升序排序：“厂别”、“站点”、“参数”
3. 请比较不同的产品监控的“站点-参数”是否一致，有没有哪款产品覆盖了所有的“站点-参数”
    - 如果没有一款产品覆盖了所有“站点-参数”，则汇总一份输出到一个单独的文件中
4. 请比较第三步得到的汇总文件，与我们从数据库中得到的汇总表比较：`resources\inline_domain\spc\Z622监控站点参数.xlsx`，判断哪个更为齐全（先不要修改）
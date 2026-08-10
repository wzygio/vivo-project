# Task：AOI RS报表制作
请你参考`src\inline_domain`中`spc`子模块的逻辑，绘制“AOI_RS”的报表

## Terms
- **AOI（Automatic Optical Inspection）**：Array/OLED前段的检测站点，主要用于检测图形、残留、颗粒
- **RS（Review Station）**：对AOI拍出的缺陷点位进行复判，判定具体的Code，其与AOI的关系类似于MVI相较于AVI
  * 注意，这里的Code也是一种缺陷代码，但与Defect Code并不一致，一般用长度为五位的代码指代
- AOI和RS的单位都是个，即不良点位个数，数据库中每一条记录算一个
    * 可以直接对`code_qty`这个字段进行加和计算。

所有不清楚的专业词汇，请先到以下文件中查询：`references\domain\GLOSSARY.md`

## 报表样式

报表有三张图，分别为：
1. By月周天趋势图（两月、三周、七天）：
    - 分母：每个时间阶段过货的sheet/glass数（月、周、天）
    - 分子：平均每个sheet/glass的RS Code对应的个数
    - “两月、三周、七天”要跳过空值向前补全，具体逻辑请参考：`app\pages\SPC监控报表.py`
2. By Lot别点线图
    - 计算每个Lot的RS个数
3. By Sheet别点线图
    - 计算每个sheet的RS个数

其它关键信息：
- 时间范围：默认为“上一个自然月 1 日 ～ 当前日期（含当天）”，不再设置时间筛选框
- 颗粒度：从大到小依次为“产品型号、厂别、站点、RS Code、RS个数（即明细）”
- 筛选框：厂别、站点、Code名称（与`app\pages\SPC监控报表.py`一致）

## 数据源及关键字段
RS Code明细表：
- eda.spc_tzbjx_rs_array：Array RS明细表
    * sheet_id：Sheet ID
    * sheet_start_time：过货时间
    * productcode：产品型号
    * step_id：站点
    * rs_code：RS Code代码
    * code_qty：计算字段，取值为1，可以直接对`code_qty`这个字段进行加和来计算RS个数
- eda.spc_tzbjx_rs_oled：OLED RS明细表
    * glass_id：Glass ID
    * glass_start_time：过货时间
    * 其余字段与上面的报表一致
- eda.spc_tzbjx_rs_tsp：TP RS明细表
    * glass_id：Glass ID
    * glass_start_time：过货时间
    * 其余字段与上面的报表一致

RS Code规格表（用于绘制规格线）：
- dwd_imp_rs_code_xishu_fo_tzsbjx：RS规格表

过货明细表：用于计算月周天趋势图中的分母
- eda.spot_eda_array_view_sht_v：array过货明细（计算过货数）
    * sheet_id：Sheet ID
    * sheet_start_time：过货时间
    * step_id：站点
    * product_spec：产品代码，用于追溯产品型号，示例如下
```
from eda.spot_eda_array_view_sht_v seavs
left join mdw.dwr_mes_productspec dmp
	on dmp.productspecname = seavs.product_spec
``` 
- eda.spot_eda_oled_view_gls_v：OLED过货明细（计算分母过货数）
    * glass_id：Glass ID
    * glass_start_time：过货时间
    * 其余字段与上面的报表一致
- eda.spot_eda_tp_view_gls_v：TP过货明细（计算分母过货数）
    * glass_id：Glass ID
    * glass_start_time：过货时间
    * 其余字段与上面的报表一致

## Workflow
1. 请先参考`app\pages\SPC监控报表.py`的逻辑链条。了解自己要制作的是一种什么报表，我们即将制作的“AOI_RS”报表与其极其相似
2. 请分析出绘制报表所需要的数据，然后输出到如下路径：`references\domain\spec\spec-data_source.md`
3. 请探查数据表寻找数据源，直至你所需要的每一项数据都找到了对应的数据源。最终，补全该spec文件中每一项数据的数据源：`references\domain\spec`
    - 请参考`src\shared_kernel\infrastructure\db_handler.py`编写脚本探查数据表
    - 字段字段名称会因不同厂别的数据表而有所差异，部分规律可以参考`src\inline_domain\infrastructure\spc`
    - 如果无法找到数据源（比如有些数据结构你无法理解），请在这里中断并向我询问
4. 找到所有数据源后，请按照`Developement Flow`这一skill完成开发

# Goal
不断完善直至E2E测试通过
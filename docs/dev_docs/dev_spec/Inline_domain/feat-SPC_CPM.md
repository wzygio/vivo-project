# SPC监控报表

## Background
你好，我是一家OLED制造厂的员工，该项目是一个用于给客户展示厂内数据的报表。
现在需要开发一个SPC的CPM监控报表。

## Terms
所有不清楚的专业词汇，请先到以下文件中查询：`references\domain\GLOSSARY.md`

# Task1（已完成）：CPM/CPK-By月周天 + SPC箱线图
针对每个参数绘制以下三幅图像，每组图像放在一个Expander中：
1. 根据每个监控指标的sheet别数据（每个sheet取sheet_mean作为测量值），绘制月周天箱线图：
    - 默认两月（从上个月第一天开始，包含上个月和本月）、三周（从三周前的第一天开始，近三周）、七天（近七天）。
    - 样式：可参考图一
        * 标出UCL/LCL/USL/LSL，并以USL和LSL作为上下限
        * 月周天的箱线图之间要有明显分割。
2. By sheet箱线图-By腔室区分，并按照腔室排序
    - 样式：请参考图二。不同腔室的箱线图使用不同颜色
3. By sheet箱线图-By过货时间排序

## 筛选框
1. 指标：单选框；备选项有“CPM、CPK”，默认为CPM
2. 厂别：单选框
3. 站点：多选框
4. 参数名称：多选框，依旧保留选择站点后，自动选择所有对应参数的逻辑

---

# Task2：报表优化
请修改`app\pages\SPC监控报表.py`中的第二幅图——，将“检测站点”的过货腔室修改为“主站点”的过货腔室

## 数据源及关键字段
mdw.dwd_imp_dv_param_spec：参数信息表。包括规格，主制程站点
    * prod_code：产品型号
    * step_id：检测站点
    * usl/ucl/lsl/lcl：各种规格线
    * main_step_id：主制程站点

## 报表样式
完全参考`app\pages\SPC监控报表.py`中的第二幅图，将其中的检测站点的腔室替换为主制程站点的腔室即可

其它关键信息：
- 时间范围：默认为“上一个自然月 1 日 ～ 当前日期（含当天）”，不再设置时间筛选框
- 颗粒度：从大到小依次为“产品型号、厂别、站点、参数名称、测量值（即明细）”
- 参数与规格：“产品型号、站点、参数名称”锁定唯一参数及对应规格
- 筛选框：厂别、站点、Code名称（与`app\pages\SPC监控报表.py`一致）


## Workflow
1. 请分析并理解当前spc模块的逻辑：
    - 前端：`app\pages\SPC监控报表.py`
    - 后端：`src\inline_domain`中的`spc`子模块
2. 请分析并理解主制程站点的过货腔室的匹配逻辑：
    - 业务逻辑分析：`docs\dev_docs\北极星-过货腔室\北极星-过货腔室_SQL解析报告.md`中的“### 6.5 `long_text_BA37...txt`：主设备/腔室追溯箱线图”
    - 源码：`docs\dev_docs\北极星-过货腔室\long_text_BA373569-2FFE-4848-AC4D-363C2B462531.txt`
3. 请分析出绘制报表所需要的数据，然后输出到如下路径：`references\domain\spc\spec-data_source.md`
3. 请探查数据表寻找数据源，直至你所需要的每一项数据都找到了对应的数据源。最终，补全该spec文件中每一项数据的数据源：`references\domain\spc\spec-data_source.md`
    - 请参考`src\shared_kernel\infrastructure\db_handler.py`编写脚本探查数据表
    - 如果无法找到数据源（比如有些数据结构你无法理解），请在这里中断并向我询问
4. 找到所有数据源后，请按照`Developement Flow`这一skill完成开发

# Goal
不断完善直至E2E测试通过

---

# Task1：SPC报表优化
1. 如果lsl为0
    - 修正规格线：则不再绘制lsl/target/cl。因为这种情况意味着只有上限ucl和usl
    - 排除不计算cpm的参数：不再计算CPM。因为有时候lsl为空时，数据表中也会用0表示
    - PPA
2. 折线图：参数名称中带有UNI

# Task2：制作cpk自动修正逻辑
请仿照超规片（oos）的自动修正逻辑，制作cpk的修饰逻辑：
1. 与超规片不同的是，cpk默认不修饰（一律为False），显示真实值
2. 至于修饰样式，则与超规片的修饰器做在一个Expander中（admin=ture时才显示），但区分不同的选项卡，具体可参考`app\pages\入库不良率分析看板.py`中的“render_trend_override_uploader”

# Task3：CTQ报表制作
请仿照当前的spc报表`app\pages\SPC监控报表.py`，制作ctq报表：
1. ctq不包括cpm/cpk相关的功能，但其它功能与样式与spc完全一致
2. 后端依旧做在inline_domain模块中，但作为一个单独的子模块（相关程序使用单独的子文件夹存放）；前端单独创建新页面
3. 底层数据读取复用`src\inline_domain\infrastructure\spc\repositories\spc_repository.py`即可，筛选“data_type = 'CTQ'”
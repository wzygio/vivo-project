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
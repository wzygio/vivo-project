【Background】
你好，我是一家OLED制造厂的员工，请你帮助我开发一个关键备件的报表，用于监控关键备件的使用情况，判断其是否超规，如果超规我们就会更换。当前框架如“src\equipment_domain”中所示，但它目前完全无法正常运行。需要你帮助我在其基础上继续修改并进行开发。

【目标数据格式】
厂别，备件类型，设备类型，膜层，制程，寿命规格，站点，机台号-腔室，参数名称，测量值，测量时间，使用进度，预警状态
数据来源：
1. 厂别，备件类型，设备类型，膜层，制程，寿命规格，：规格表
2. 站点，机台号-腔室，参数名称，测量值，测量时间：数据表。使用规格表中的“站点，机台号-腔室，参数名称”三个字段去匹配数据表来进行查询
3. 使用进度，预警状态：自行计算。其中“使用进度（百分数） = 测量值/寿命规格”，如果“使用进度＞90%”则显示“预警”，如果““使用进度＞100%””则显示超规，否则则显示正常

【数据格式】
1. 规格表：原始表路径为“D:\wzy\Python\vivo-project\docs\project_files\关键备件\供应商关键备件寿命管控清单 - new.xlsx”，sheet名称为“规格表”：

    a. 字段：厂别，备件类型，设备类型，膜层，制程，寿命规格，站点，机台号-腔室，参数名称。

    b. 站点、机台号-腔室：对应多个值，需要事先拆分

    c. 参数名称：匹配式。对于备件类型为Target的，匹配式为'%TARGETLIFE%_MAX' 或 '%TARGET_KWH%'，对于备件类型为Mask的，匹配式为'%MASKLIFE%_MAX' 或'%PRE_SPRT_KWH%'

2. 数据表：eda.ARRAY_PDS_RESULT_T
站点，机台号-腔室，参数名称，测量值，测量时间
所需字段及其名称依次为：
--1. step_id：站点
--2. sub_equip_id：机台号-腔室（对应设备/part）
--3. param_name：参数名称
--4. value：测量值
--5. glass_start_time：时间
3. 颗粒度层级从大到小，您可以近似理解为：
厂别＞备件类型＞设备类型＞膜层，制程，寿命规格＞站点，机台号-腔室，参数名称，测量值，测量时间，使用进度，预警状态

-------------

【Task1】
1. 请你先读规格表，理解其结构。该文件为加密文件，所以你可能需要先调用skill进行解密。如果解密后发现结构损坏，请直接中断并告知我。
2. 为了便于后续处理，将其拆分为每个单元格中只有一个值的规格表，并存储到resources文件夹下的“critical_parts_baseline.csv”中（替换掉这份不完整的规格表）。
3. 将该部分逻辑写入infrastructure层中，如果resources路径下没有规格表，则重新读取原表并生成（因为原表后续可能会更新）。

-------------

【Task2】
1. 请探查eda.ARRAY_PDS_RESULT_T数据表，了解其具体情况。数据库链接参考“src\shared_kernel\infrastructure\db_handler.py”，你可以参考已有模块（例如“src\spc_domain\infrastructure\data_loader.py”）中的写法
2. 请按照规格表中的“站点，机台号-腔室，参数名称”这个三个字段从数据库中的数据表中查询数据。请注意以下几点：

    a. 规格表中的“参数名称”仅为匹配式

    b. 你需要提取出的字段为step_id，sub_equip_id，param_name，value，glass_start_time，一个示例如下所示：
    “select 
        step_id ,
        sub_equip_id ,
        param_name ,
        value,
        glass_start_time 
    from eda.ARRAY_PDS_RESULT_T
    where 
        step_id = '1K200'
        and sub_equip_id = '3AFS01-SPU-PM5'
        and param_name LIKE '%TARGETLIFR%_MAX'
        and glass_start_time > '20260301' -- 仅查询近三个月的时间即可
    ”以上仅为示例，您可以进行优化

3. 请将查询结果存储为快照，并存放到data\文件夹下
4. 请检测是否查询到真实数据（并不一定规格表中的每一行都能查到数据，有的腔室近三个月可能没有过货，所以没有保养，这是正常情况）

-------------

【Task3】
请基于现有数据快照“data\equipment\part_life_snapshot_56310a1a5da8.parquet”，逐层检查并完善infrastructure/core/application/app，使其适配现有字段和逻辑。
1. application：请测试其输出的数据是否满足【目标数据格式】要求
2. infrastructure：请参考“src\spc_domain\infrastructure\repositories\spc_repository.py”编写数据读取与快照管理逻辑。并测试已经抓取到的快照中的数据格式是否满足【数据格式】中的要求
3. core：计算“使用进度，预警状态”两个字段
4. app：保持样式不变，但是要更换相应字段和顺序，并且新增厂别筛选器

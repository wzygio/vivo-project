# Task：寿命测试报表开发
- 身份：我是一家OLED显示屏制造公司的大数据分析工程师
- 项目：我们正在开发的是一款面向客户的企业级报表系统，现在需要开发其中的“IQC寿命测试报表”

## References

### Sql语句
- 相关数据表及字段如下：
```
 select 
 	product_group , -- 产品型号
 	product_id , -- 产品状态
 	wo_name , -- 量产
 	date_key , -- 批次号
 	panel_id , -- panel_id 
 	test_time , -- 测试时间
 	bu, -- 测试画面
 	luminance , -- 亮度
 	lumi_decay , -- 亮度衰减
 	cie_x, -- CIEx
 	cie_xy, -- CIEy
 	iss, -- Iss
 	curr_decay , -- 电流衰减
 	efficiency , -- 效率
 	eff_decay , -- 效率衰减
 from  m3dwd.dwd_panel_eff_dec_enter
 limit 10
```

### Terms
- 寿命测试：测试显示屏亮度、效率随使用时间的变化（具体信息如需了解可自行查询）
- 其它你想要了解的专有名词可以参考：`references\domain\GLOSSARY.md`

### Database
你可以尝试探查数据库
- 数据库读取程序可参考：`src\yield_domain\infrastructure\data_loader.py`
- 数据库链接程序可参考：`src\shared_kernel\infrastructure\db_handler.py`
- 特别注意：你需要使用`.env`中以“M3”开头的账密来访问数据库，例如“M3_DB_DATABASE”，因此你不能直接使用`src\shared_kernel\infrastructure\db_handler.py`，需要替换其中的变量名

## Requirements

### Frontend
1. 前端样式：要求呈现“明细表”+“趋势图”（当前已经做出的样式报表已不再具有参考价值`app\pages\IQC寿命测试报表.py`）
    - 明细表：请参考`docs\project_files\iqc_domain\ID寿命维护表V1.0.xlsx`（所需字段皆已在上面的sql语句中列出）
    - 趋势图：请参考`docs\project_files\iqc_domain\IQC-寿命测试报表样式.png`
        - x轴：测试时间
        - y轴：效率衰减
        - 曲线：panel_id
    - 筛选框：仅需保留“产品型号”和“批次号”
    - 特殊要求：无论在明细表还是趋势图中，panel_id都不予显示，转而在分组后用数字代替。
        - 分组规则：“产品型号-批次号-测试画面”，正常来讲每个组别只会有五个panel_id
2. 前端架构：具体架构可参考`visionox-dashboard-ui`

### Backend
1. 后端架构：遵循DDD架构完成开发（在`src\iqc_domain`中新建一个子模块，具体架构可参考`ARCHITECTURE.md`中的`inline_domain`）

## Workflow
1. 请分析并了解前端的呈现样式及所需数据
2. 请分析SQL语句，了解每个每项所需数据的来源。不断分析直至找到所有数据来源，最终输出一份文档至如下路径：`docs\dev_docs\dev_spec\iqc_domain`（无法找到的数据源也记录到其中）
3. 按照`development-flow`完成报表开发：如果信息缺失，或者有无法解决的业务问题可以寻求我的帮助；否则直接执行到底即可

## Goal
不断迭代优化直至E2E测试通过   
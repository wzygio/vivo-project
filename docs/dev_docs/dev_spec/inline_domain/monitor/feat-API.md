# Task：自动预警功能
- 身份背景：我是一家OLED显示屏制造公司的大数据分析工程师
- 项目背景：我们正在开发的是一款面向客户的企业级报表，现在想要开发一款API来提取异常数据
- 任务目标：请帮助我对当前`app\pages\自动预警看板.py`涉及到超规片，开发一款API来获取相关数据

## Requirements
开发方案如下：`docs\dev_docs\generated\inline_domain\monitor\sheet-anomaly-api-development-plan.md`

## Data
- 数据源：同`app\pages\自动预警看板.py`一致，只返回已经落盘到本地数据表中的数据
  - 初版仅限超规片相关数据，但包含各模块的lot别数据（你可以创建一个字段来区分二者）
- 必要字段（命名请遵守以下规则）：
  - 分类：即sub_domain，例如“SPC/AOI_TT/AOI_RS”
  - 发生站点：即“站点”
  - 部门：即“厂别”，例如“ARRAY/OLED/TP”
  - 层别：“描述”中，第一个“_”之前的字符，例如“PI2_CURE_THK”，其层别就是“PI2”。“描述”的获取方式如下：
    - 读取工序字典：优先查询 mdw.dwr_mes_processoperationspec_v；查询失败时尝试 mdw.dwr_mes_processoperationspec。使用 oper_code 作为站点编号、description 作为备注。见`src\inline_domain\infrastructure\shared\step_description_loader.py:22`
    - 生成并缓存映射：页面调用 get_cached_step_description_map()，获得“站点编号 → 描述”的字典。见`app/pages/SPC监控报表.py:94`。
  - 监控参数：即“参数名称”
  - 异常事件：即超规类型，例如“OOS/OOC”

## Design
- 筛选规则：可以复用现有的筛选规则（即“共同公开投影与可见性规则”），仅释放“Flag”为“False”的明细（即不进行修饰，释放真实数据）。
  - 如果你发现某些本地数据表中没有“Flag”字段（即没有人工控制是否释放的逻辑），请补全相关逻辑
- APP层接口：可以复用现有的APP层接口（即“共同异常结果查询服务”）

## Workflow
1. 请先分析并理解开发方案，并根据我的要求进行完善
2. 按照要求，调用`development-flow`完成开发
2. 完成后，请编写一份使用说明至如下路径`references\domain\inline_domain\monitor`

--- 

# 2. API优化
请对当前已开发完成的API，根据以下要求完成优化：

## 2.1 API优化：Yield超规Lot
1. 所属厂别：对于yield_domain的lot别超规明细，不应该由人手动维护记录。
  1. 请修先改预警逻辑，在写入明细文件时，直接根据其Defect Code判定所属厂别（即“lot_id - defect_code - defect_group - factory”）
    - 预警逻辑：`src\yield_domain\core\sheet_lot\lot_alerts.py`
    - 明细文件：`resources\yield_domain\sheet_lot\yield_lot_oos_results.xlsx`
    - "defect_group - factory"映射规则：
      - "Array_Line"："ARRAY"
      - "Array_Pixel": "ARRAY"
      - "Array_Mura": "ARRAY"
      - "OLED_Mura": "OLED"
      - "TP_Short NG": "TP"
      - "TP 容值NG": "TP"
  2. 请修改API，不再维护“Yield归属表”
2. 层别：一律填写NULL（这是正确的，因为入库不良本身就无法直接锁定层别）

## 2.2 API优化：查询与返回字段
1. 查询参数：按照以下设计模式，API查询参数中没有任何一个是必选的，即直接获取上一周的所有超规明细。但如果一定要设置一个必选，请将“分类”设为必选
  1. 包含“必要字段”：其与当前查询参数的对应关系如下
    - 分类（sub_domain）：可选，默认全选
    - 发生站点：可选，默认全选
    - 部门（factory）：可选，默认全选
    - 层别：可选，默认全选
    - 监控参数：可选，默认全选
    - 异常事件（event_type）：可选，默认为OOS
  2. 新增参数：
    - 开始时间：可选，默认为上个ISO周的周一
    - 截止时间：可选，默认为上个ISO周的周日
  3. 其余参数可保留：
    - product：可选，默认全选
    - entity_type：可选，默认全选
    - entity_id：可选
    - cursor：可选（完成后请顺便解释一下这个参数的作用）
    - limit：可选，但默认不限制
2. 返回数据：
  - 请删除“起始站点”字段（我认为“发生站点”已经包含，如果你认为不能删除，请解释）
  - 请告诉我返回信息中还能够添加哪些信息？（先不要添加，先告知我，我想了解下后续可能的拓展方向）
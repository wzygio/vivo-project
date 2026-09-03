# Task：自动预警功能
- 身份背景：我是一家OLED显示屏制造公司的大数据分析工程师
- 项目背景：我们正在开发的是一款面向客户的企业级报表，但涉及数据繁多，因此需要自动预警
- 任务目标：请帮助我对当前`src\inline_domain`下的所有子模块，增加自动预警功能，重点包括“单片异常”和“趋势波动”两种

## Design

1. 后端逻辑：我认为可以直接读取sheet明细表中“flag”字段取值为“FALSE”的记录，因为`src\inline_domain\core`这一层的修饰逻辑实际上已经筛选出了超规片，并记录到了对应的明细文件中，无需重复计算。
    - 时间筛选（“sheet_start_time”）：预警范围仅针对ISO周期下的上一周
    - 当前`src\inline_domain`包含四个子模块，其模块名称、对应的页面和sheet明细表如下所示：

| 模块-`src\inline_domain` | 报表-`app\pages` | sheet明细表-`resources` | 
| --- | --- | --- |
| aoi_rs | AOI_RS监控报表 | aoi_rs_sheet_oos_decoration |
| aoi_tt | AOI_TT监控报表 | aoi_tt_sheet_oos_decoration |
| spc | SPC监控报表 | spc_sheet_oos_decoration |
| ctq | CTQ监控报表 | ctq_sheet_oos_decoration |

2. 报警分类：将报警类型分为两类，分别为“单片异常”和“趋势波动”，对应内容分别如下：

| 模块-`src\inline_domain` | 报表-`app\pages` | 单片异常 | 趋势波动 |
| --- | --- | --- | --- |
| aoi_rs | AOI_RS监控报表 | sheet超规（oos） | 暂无 |
| aoi_tt | AOI_TT监控报表 | sheet超规（oos）  | 暂无 |
| spc | SPC监控报表 | sheet超规（oos）  | cpk超规 |
| ctq | CTQ监控报表 | sheet超规（oos）  | 暂无 |
| yield | 入库不良率分析看板 | lot超规 | 良率波动 |

其中“良率波动”的判定逻辑详见：`src\yield_domain\core\abnormal_detector.py`

3. 前端样式：以下几个页面已有自动预警功能，你可以参考它们的样式：
    - `app\pages\SPC监控报表.py`
    - `app\pages\入库不良率分析看板.py`

4. 异常项展示：要像cpk预警那样，自动将异常项的图像展示出来。而不是让用户手动筛选：
    - 样式参考：所有异常项应该放在一个Expander中，具体样式请参考`app\pages\SPC监控报表.py`
    - 良率报表优化：当前`app\pages\入库不良率分析看板.py`并没有实现这一点，需要增加，只不过它筛选的是Defect Code
    - 同步渲染：渲染时注意采用`app\manager\render_gate.py`的设计，所有payload计算完成后再进行渲染，不要让用户有卡顿感

## Workflow
1. 请你先分析我的需求并了解对应代码，验证我的思路是否可行。如果可行，请输出一份PRD文件至如下路径：`docs\PRD`
2. 调用`development-flow`这一skill进行开发（新建branch即可），直至E2E测试通过
    - 如果有无法解决的业务逻辑问题，请向我询问

---

## Task-1：自动预警看板
请进一步优化自动预警看板的前端展示。当前仅展示超规项并自动渲染对应图像，但是客户希望能够展示一个矩阵看板（By子模块）：
- 列表头为各个产品，行表头为各个监控参数
- 每个单元格的内容为是否达标，如果达标则用绿点展示，如果不达标则标注红点

## Problem
难点可能包括以下几项：
- 与我们当前的Header筛选模式（By产品筛选）相违背了
- 计算量巨大，我们需要预先计算出所有产品的后台数据

## Workflow
1. 请以`indicator_domain`中`qtime`为例（`app\pages\Q_Time监控报表.py`），分析这种方式是否可行，包括：
    - 在计算量上有哪些卡点
    - 在架构改动上有哪些巨大的风险点
    - 以上问题是否有解决的可能
2. 最终给出你的意见，并将以上分析结果汇总为一份报告写入如下路径：`docs\dev_docs\generated\others`
3. 我们先以`indicator_domain`中`qtime`为例，生成一份预警矩阵看看效果。请依照你刚刚的分析结果`docs\dev_docs\generated\others\alert-center-matrix-board-feasibility.md`，请生成一份PRD到以下路径：`docs\PRD`
4. 使用`grill-with-docs`对应PRD进行压力测试，排查问题：
    - 如果有无法解决的业务逻辑问题，可以向我询问
5. 调用`developement-flow`完成开发

## Goal
不断迭代优化直至E2E测试通过
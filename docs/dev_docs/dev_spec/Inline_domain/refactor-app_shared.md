# Task：重构APP层
- 身份专业：我是一家OLED显示屏制造公司的大数据分析工程师
- 项目介绍：我们正在开发的是一款面向客户的企业级报表
- 任务背景：请帮助我优化当前报表的前端：`app\sections`。许多应当统一作用于多个页面的前端处理逻辑，当前被分散在各个页面中，我希望建立public pipline，统一作用于所有页面，这样也便于后续维护。

## Requirements
我们探讨的主要是inline_domain的四个子模块，他们之间的关系为：
1. ctq和spc应该保持一致：`app\sections\inline_domain\spc`和`app\sections\inline_domain\ctq`
2. aoi_tt和aoi_rs应该保持一致：`app\sections\inline_domain\aoi_rs`和`app\sections\inline_domain\aoi_tt`

对于ctq和spc，具体如下：
1. 是否绘制折线图还是箱线图
2. 仅绘制上限，不绘制下限：当 LSL 为空或等于 0 时
3. By过货时间趋势图：横坐标轴替换为时间
4. 你认为应该统一的绘图逻辑

对于aoi_tt和aoi_rs，具体如下：
1. 你认为应该统一的绘图逻辑

请问我们能否仿照后端的结构（例如 `src\inline_domain\application`），创造一个shared模块，将多个子模块复用的逻辑放入其中。

## Workflow
1. 请你作为一面大厂的前端架构师，分析当前程序并评估我的想法，并给出企业级项目的标准设计方案
2. 请对照当前结构与目标设计结构，给出详细的PRD：`docs\PRD`
3. 请调用`development-flow`这一skill完成开发。无需E2E测试，我自行完成E2E测试皆可

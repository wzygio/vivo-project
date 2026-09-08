# Task1：自动预警看板优化
[自动预警看板.py](app/pages/自动预警看板.py) ：该页面是一个自动预警看板，它涉及全量查询，但每次查询运行时间过长。

## Design
我想建立一个持久化方案，具体要求如下：
- 能否单独设置一个脚本，每天早七点分析一次，有效期为24H
- 分析完成后将结果保存下来：data；除非时间过期或删除快照，否则直接从快照中读取
- 快照更新：采用增量更新的形式，保存从上个月1日到今天的数据（到了下个月则自动删除上上个月的数据）

## Requirements
持久化方案应该遵守以下要求：
1. 数据复用，即实现一次计算，两个页面共同使用：
    - 示例：以下两个页面复用application层缓存，如果我们将缓存转化为快照，那么快照也应该继承这种复用机制。
        - [自动预警看板.py](app/pages/自动预警看板.py)
        - [Q\\_Time监控报表.py](app/pages/Q_Time监控报表.py)
        - [indicator\\_domain](src/indicator_domain/) 

## Task
我们先在“Q-Time预警看板”上尝试，因为其背后的 [indicator\\_domain](src/indicator_domain/) 逻辑较为简单。

请分析我的需求，并使用 [$development-flow](C:/Users/V0141351//.agents/skills/development-flow/SKILL.md) 完成开发

## Goal
不断迭代优化，直至E2E测试通过

---

# Task1-1：monitor模块优化
接下来我们需要优化“超规片预警看板”
- 后端入口：`src\inline_domain\application\monitor`

## Context
1. 请你先查看并了解该模块的逻辑，它不仅是全量查询，而且单独构建了一条数据链路，这会导致：
    - 查询负载极大
    - 缓存无法复用
2. 但其实对于“超规明细”，`inline_domain`下的其它子模块已经有天生的本地快照，路径如下，无需monitor模块重新运算：
    - `resources\inline_domain`：各模块的“*sheet_oos_decoration.xlsx”
3. 当然，这种方式也有缺点，其它子模块的运行都是访问时才触发，假如一个模块长时间没有被访问，那它的超规明细就不会被更新。但我认为这种代价是可以接受的

## Workflow
1. 请分析我的思路是否可行，包括：
    - 当前超规明细表中的字段能否支持生成“超规片明细看板”
    - 当前超规明细表中的记录不是长期存储，而是只保存近两月的数据，如果采用这种方案，我们需要先将其改为长期存储，增量更新
    - 其它你认为的风险点
2. 如果你最终判断可行，请调用`to-spec`输出一份prd
3. 请调用`development-flow`完成开发（在当前branch而非master branch）

## Goal
不断迭代优化，直至E2E测试通过

---

# Task1-1-1：monitor模块优化
我们需要在自动预警看板中补充上两项原本删除的信息：“OOC、SOOS”

各补充方式依次如下
1. OOC：补充真实数据；仿照oos的判定与修饰逻辑统，补充一套ooc的判定与修饰逻辑。
    - 由于逻辑完全一致，我强烈建议数据链路复用
    - 相关配置文件同样存放于如下路径：`resources\inline_domain`
    - 针对“inline_domain”模块下的所有子模块都要新增一套该逻辑
2. SOOS：一律视为0

## Task
1. 请分析我的需求，并调用`developement-flow`完成开发
- 除非有无法解决的业务问题，否则请直接一步执行到底
2. 完成后，请顺带整理`resources\inline_domain`：
    - 按照子模块名称分别创建对应的子文件夹，并将对应的文件放入其中
    - 请注意“inline_domain”配置文件路径的位于以下文件中：`config\domain\inline_domain.yaml`-`resources:`，请继续遵守该模式并完成对应修改

## Goal
不断迭代优化，直至E2E测试通过

---

# Task1-2：CPK预警看板制作

# Task1：test标准制定
1. 制定一个标准tests架构，我认为你推荐的架构可以采纳：一级采用“层级：unit、contract、integration、e2e、manual；”，二级采用“领域：yield、spc、equipment、shared_kernel、app；”；
2. 是否有必要每次使用tdd都进行全量回归？是否应该仿照`tools/smoke.py`By domain区分？
请思考测试标准，并写入`references\test_references\methods\validation.md`

# Task2：tests管理skill
至于tests程序治理，我是个人开发者。所以从简即可。我的想法是开发两个skill：
运行全量回归并检查哪些测试程序失败。对于失败的测试，依次进行如下处理：
1. 检查ADR中是否有功能更改，如果有明确记录，删除。
2. 检查对应源码是否有新的测试程序并能够正常通过（直接在统计目录下检索即可），如果有，删除。
3. 如果都不符合，输出报告

# Task3：skill整合
我目前创建了很多小skill用于维护项目，请你将他们分为两类并进行整合：
1. 重构：
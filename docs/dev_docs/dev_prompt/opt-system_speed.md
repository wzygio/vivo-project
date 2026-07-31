# Task-refactor：项目速度优化
我当前的项目计算量十分庞大，导致每次更新执行烟测都需要等待很久。
请问是否有可能通过进行架构或算法优化，来实现运行速度提升

## Workflow
1. 请先分析是否存在架构优化点 [ARCHITECTURE.md](d:/wzy/Python/vivo-project/ARCHITECTURE.md) ，再分析是否存在算法优化点
2. 如果存在，请将优化需求转化为prd [$to-prd](C:\\Users\\V0141351\\.agents\\skills\\to-prd\\SKILL.md) 。如果没有，则直接跳过后续步骤即可
3. 将prd拆解为细项issue [$to-issues](C:\\Users\\V0141351\\.agents\\skills\\to-issues\\SKILL.md) ，针对每个issue，应同时列出可能会影响业务逻辑或功能的风险点
4. 尝试补充每个issue中的信息，直至可以开发（ready-for-agent）
5. 调用 [$planning-with-files](C:\\Users\\V0141351\\.agents\\skills\\planning-with-files\\SKILL.md) 构建项目优化计划
6. 调用 [$tdd](C:\\Users\\V0141351\\.agents\\skills\\tdd\\SKILL.md) ，先进行架构重构（如果有），再进行算法优化（如果有）
7. 最后总结项目优化点并输出至 [adr](d:/wzy/Python/vivo-project/docs/adr/) 
8. 最后汇总每个issue的风险点，输出一份项目优化后的风险点清单
9. 调用 [$tdd](C:\\Users\\V0141351\\.agents\\skills\\tdd\\SKILL.md) ，针对这些风险点进行逐一排查，如果有则撤回对应issue的改动

## Rules
1. 您的任何优化行为，都不能影响现有业务逻辑和计算逻辑。虽然有些计算逻辑在您看来冗长而难以理解，但它们是我反复调试后的产物。
2. 您的优化应当仅限于数学计算和Software Engineering领域，决不能触及业务领域。
3. 当然，如果只是有些许风险，我愿意为了冒一点风险来换取大幅优化
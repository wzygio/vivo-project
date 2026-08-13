# Task
谢谢，请将我常用的一套工作流程编写为一个开发流skill

## Context
我常用的一套工作流程如下：
1. 需求理解：请分析并理解需求
2. 需求制定：请将需求转化为issue [$create-local-markdown-issue](C:\\Users\\V0141351\\.agents\\skills\\create-local-markdown-issue\\SKILL.md)
3. 需求完善：请不断补充信息，直至ready-for-agent [$triage](C:\\Users\\V0141351\\.agents\\skills\\triage\\SKILL.md)
4. 计划制定：请基于需求创建计划 [$planning-with-files](C:\\Users\\V0141351\\.agents\\skills\\planning-with-files\\SKILL.md)，包括一份checklist
5. 程序开发：调用 [$tdd](C:\\Users\\V0141351\\.agents\\skills\\tdd\\SKILL.md) 执行计划完成开发，直至所有checklist都达成
6. 程序测试：如果项目包含UI，请不断迭代优化，直至基于浏览器的烟测通过 [$playwright-interactive](C:\\Users\\V0141351\\.agents\\skills\\playwright-interactive\\SKILL.md) 
7. 经验总结：请将最后通过测试的程序中的关键决策与设计总结到ADR中。

## Requirements
该skill应该基于以下flow模块制定：需求制定-计划制定-程序开发与测试-项目沉淀。
1. 将上述flow中的每个步骤分配到每个模块中。
2. 每个模块对应的内容可以单独修改，而不影响其它模块。
   - 这可能需要你在架构上设计上进行思考，比如每个模块分离，通过一个母流程来串联
   - 它应当支持每个模块使用sub-agent来执行（从而避免上下文污染），但默认不使用这种方式（是否启用可调节）
3. 每个步骤调用对应skill，而不是将每个skill的逻辑复制到该skill中。
4. 它的输入应该是一份需求，输出应该是每个模块的产物，分别如下：
   - 需求制定：issue [.scratch](.scratch/) 
   - 计划制定：plan [.planning](.planning/) 
   - 程序开发与测试：程序与测试结果
   - 项目沉淀：ADR [ADR](docs/ADR/) 

## Workflow
1. 请先理解我的skill构建模式
2. 请分析上述开发flow是否有明显漏洞（它可能不够完善，但足够我日常使用。所以请您从是否存在关键缺陷的角度来审查，而不是从企业级项目的流程来审视）
3. 如果没有，请按照要求构建这套skill
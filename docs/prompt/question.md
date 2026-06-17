# Task1

## Context
你好。当前Codex中许多skill出现了重复，原因在于部分skill被同时放在用户路径（“C:\Users\V0141351”）中的.agents和.codex中。

# Workflow
1. 请你确认Codex是否能够完整识别.agents中的skill。
2. 请你评估将所有skill全部迁移至.agents中是否有风险，是否会对其它功能产生影响。
- 比如ECC是我安装的一整套框架，他包含一系列工具，其中也自然有一部分skill。如果迁移它的skill，是否会对其它功能的使用产生影响？比如它是否设定了必须要从.codex的根目录去寻找skill？
3. 如果以上条件都满足，则进行迁移。否则停止。
4. 迁移后，请修改.codex中的配置，将skill的下载和创建的保存位置均设为“.agents\skills”

# Task：架构规则构建

## Background
当前项目已基本使用智能体（例如Codex）编写。针对智能体编程，其中一个关键问题是代码锁定。
传统方式是我明确指出关键程序，或是直接将整个项目架构告知给智能体（`ARCHITECTURE.md`），但随着项目的不断拓展，架构变得愈发复杂，而且往往不能及时更新。
因此我希望构建一种架构规则，让智能体能够根据该规则快速锁定相关代码。我的想法是，针对当前项目中所有重要的子目录，统一使用`ARCHITECTURE.md`中的“Domain Submodule Architecture”。这样一来，每当指派任务时，我只需要明确domain即可，后续我只需要维护“Domain Submodule Architecture”这一非常简单的架构。

我当前已经在以下几处设定了类似规则，你可以参考它们进一步理解我的思想：
- `ARCHITECTURE.md`中的“Three-Level Layout”
- `references\index.md`中的“文件命名规则”

## Task1：文件整理
首先，我们需要在以下几个目录中应用刚刚创建的“Domain Submodule Architecture”，然后将其中现有的文件整理到对应的目录下。

相关目录及对应的特定要求如下：
- `docs\dev_docs\dev_spec`：已有一级结构，需要创建二级结构并整理文件
- `docs\dev_docs\generated`：已有一级结构，需要创建二级结构并整理文件
- `references\design`：舍弃当前结构，按照domain创建结构并整理文件
- `references\domain`：已有一级结构，需要创建二级结构并整理文件
    -  完成后，请同步更新`references\index.md`（文件命名规则不变）
- `resources`：已有一级结构，需要创建二级结构并整理文件。该目录的整理较为复杂，因为你需要同步整理代码中的路径。
    - 首先，你要确保所有所有资源文件路径都在`config\global.yaml`下可配置（请顺便将这一规则写入`CONTEXT.md`,中，后续都需要遵守）。
    - 然后，你需要确保每条路径指向了整理后`resources`中正确的位置。
- `data`：已有二级目录，检查并确认即可

## Task2：架构规则编写
1. 搜索：请将以下规则写入`CONTEXT.md`中
    - “搜索相关代码时，根据`ARCHITECTURE.md`中的“Domain Submodule Architecture”在对应的子目录下寻找”（相当于为以上所有的子目录统一创建了一个“index.md”）
        - 补充到“Important Routes”下即可，请顺带将`CONTEXT.md`中的“Work and Knowledge Artifacts”和“Important Routes”二者合二为一，二者重复了
2. 迭代：请检查并完善`AGENTS.md`中的“Iteration Router”
    - 项目功能改动：更新`references`中的对应文件
    - 项目架构改动：更新`ARCHITECTURE.md`
3. 最后，请审查`HARNESS.md`是否还有存在的必要，如果没有，请将其删除。


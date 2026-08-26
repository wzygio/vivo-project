# Task：Server Remote Repo配置

## Github与本地配置

### 1. GitHub 端的准备与鉴权 SOP、

好的。我们将严格按照标准操作程序（SOP）的要求，详细拆解模块一的每一个关键动作。

这一模块的目标是：在 GitHub 上准备好一个绝对“干净”的“容器”，并获取允许你向这个容器写入代码的“钥匙”。

#### 1.1 创建纯净的远程仓库 (Create a Clean Remote Repo)

在推送现有的本地仓库时，最容易踩的坑就是远程仓库初始化了冲突的文件。为了保证后续同步的绝对顺畅，我们需要创建一个空无一物的仓库。

登录并创建：登录你的 GitHub 账号，点击页面右上角的 + 图标，选择 New repository。

填写基本信息：

Repository name：填写你的项目名称（建议与你本地的 Vivo_project 保持一致，便于识别）。

Description（可选）：简单描述一下这个项目。

Public / Private：根据你的代码敏感度选择“公开”或“私有”。如果你打算让 Gemini 读取，通常需要确保相关的授权设置正确，但私有仓库也能通过后续的 Token 访问。

【关键规避动作】保持仓库为空：

在 "Initialize this repository with:" 区域，绝对不要勾选 Add a README file。

不要选择 Add .gitignore。

不要选择 Choose a license。

（因为你的本地项目已经有了这些文件或结构，如果在这里勾选，GitHub 会生成初始提交，导致本地和远程的历史记录不一致，后续推送会报错被拒。）

完成创建：点击绿色的 Create repository 按钮。

获取 HTTPS 地址：页面跳转后，你会看到一个 Quick setup 页面。确保选中了 HTTPS 选项卡，然后复制那个 .git 结尾的链接（例如：https://github.com/wzygio/vivo-project.git ）。请将这个链接暂时记录在一个记事本中备用。

#### 1.2 生成个人访问令牌 (Generate Personal Access Token - PAT)

自 2021 年起，GitHub 移除了密码推送支持。我们必须生成一串专属的 Token 来代替密码进行身份验证。

1. 进入开发者设置：

   - 点击 GitHub 页面右上角的个人头像，在下拉菜单中选择 Settings（设置）。

   - 在设置页面左侧边栏，一直滚动到底部，点击 Developer settings。


2. 选择 Token 类型：

   - 在左侧菜单中展开 Personal access tokens。

   - 选择 Tokens (classic)。（对于当前的需求，Classic 版本权限配置更直接简单）。


3. 生成新 Token：

   - 点击右上角的 Generate new token 下拉菜单，选择 Generate new token (classic)。（如果系统要求，请重新输入 GitHub 密码验证身份）。


4. 配置 Token 属性：

   - Note：给这个 Token 起个好记的名字，例如 VSCode-Dual-Push-Key。

   - Expiration：建议选择 90 days 或你认为合适的期限（出于安全考虑，不建议选 No expiration）。过期后重新生成替换即可。

   - 【关键权限配置】Select scopes：在长长的权限列表中，只需勾选 repo 这一项的主复选框。勾选它会自动选中其下的所有子项（这代表赋予该 Token 对私人仓库的完整控制权，包括读取和写入代码）。


5. 生成并保存：

   - 滚动到页面最底部，点击绿色的 Generate token。

   - 页面会显示一串以 ghp_ 开头的长字符串。

   - 【特别警告】这是此 Token 唯一一次显示的机会。 一旦你刷新或离开这个页面，将再也无法看到它。请立刻点击旁边的复制按钮，将其与刚才的仓库链接保存在同一个记事本中备用。

完成了以上几步，GitHub 端的“容器”和“钥匙”就已经完全准备就绪了。

### 2. 本地 Git 远程通道配置 (Local Remote Configuration) SOP

最终目标架构：**只保留一个名为 `origin` 的 remote**——fetch 只走 GitHub，push 同时走内网服务器和 GitHub（“一进两出”）。

> 说明：早期方案曾使用独立的 `prod` remote 承载双推，但 `prod` 与 `origin` 目标高度重合、容易引起歧义；且我们不需要从服务器拉取代码。因此统一收敛到 `origin`。

#### 2.1 设定 GitHub 为唯一的 fetch 通道 (Set Fetch URL)

```
# 如果还没有 origin，执行：
git remote add origin <GitHub_URL>

# 如果 origin 已存在，确认/修正其地址：
git remote set-url origin <GitHub_URL>
```

（请将 `<GitHub_URL>` 替换为你在模块一中记录的地址。）

#### 2.2 挂载双推送通道 (Add Dual Push URLs)

默认情况下，push 会使用 fetch 的 URL。我们需要为 `origin` 显式定义**两个** push URL：内网服务器在前（先触发部署 Hook），GitHub 在后（备份）。

请在 Git Bash 中依次执行：
```
# 第一个 push 目标：内网服务器
git remote set-url --add --push origin file:////10.71.7.15/GitRepos/vivo-project.git

# 第二个 push 目标：GitHub
git remote set-url --add --push origin <GitHub_URL>
```

【关键避坑提示】两条命令**都必须执行**，不能只加内网地址。

原因：在从未显式设置过 pushurl 时，push 隐式使用 fetch URL；而第一次执行 `--add --push` 后，这个隐式默认值就失效了，push 列表里只剩你刚添加的那一条。如果漏掉第二条命令，GitHub 反而收不到推送。

#### 2.3 验证多通道配置 (Verification)

配置完成后，检查 `origin` 是否已经拥有了“一进两出”的能力。

请执行：
```
git remote -v
```

【SOP 标准预期结果】：

你应该看到类似下面的输出。请注意，fetch 只有一行（GitHub），但 push 有两行（服务器 + GitHub）：
```
origin  https://github.com/你的用户名/你的仓库名.git (fetch)
origin  file:////10.71.7.15/GitRepos/vivo-project.git (push)
origin  https://github.com/你的用户名/你的仓库名.git (push)
```

工程师深度解析：为什么这么做？

在 Git 的底层逻辑中，当你执行 `git push origin` 时，Git 会扫描该远程名称下所有定义的 pushurl 并依次推送。

Fetch (拉取)：只从 GitHub 拉取。我们不需要从服务器 repo pull——服务器只是部署/备份的接收端。

Push (推送)：Git 会依次连接这两个地址。这意味着你的代码会先推入内网服务器（触发 post-receive Hook 脚本完成自动部署），紧接着立刻备份到 GitHub（供 Gemini 访问）。

### 3. 分支追踪与底层联通测试 (Tracking & CLI Test) SOP

#### 3.1 建立追踪并执行首次推送 (Set Upstream & First Push)

我们将使用 -u 参数。这不仅仅是推送代码，更是在本地 master 分支和远程 origin 之间建立一条“永久热线”。这样以后你在 VS Code 点“同步”时，它才不会问你“你想推送到哪？”。

请在 Git Bash 中执行：
```
# 假设你的主分支叫 master（根据你之前的截图确认）
git push -u origin master
```

#### 3.2 处理 GitHub 身份验证 (Authentication)

当你执行上述命令后，由于是第一次连接 GitHub，系统会弹出验证要求。

弹窗提示：可能会弹出一个 Windows 原生的登录框（Git Credential Manager）。

不要选浏览器登录：如果弹出选项，请选择 "Token" 或 "Personal Access Token" 方式。

填写凭据：
```
Username: 输入你的 GitHub 用户名。

Password: 【极其重要】 粘贴你在模块一中保存的那串以 ghp_ 开头的 Personal Access Token (PAT)。不要输入你的 GitHub 登录密码！

自动记忆：输入正确后，Windows 会自动将这串 Token 存入“凭据管理器”。下次推送时，它将处于“静默状态”，不再打扰你。
```

#### 3.3 查看命令行反馈 (Review Output)

推送完成后，请仔细观察控制台输出。

【SOP 标准预期结果】：

你会看到两段类似的推送进度，分别指向内网地址和 GitHub 地址：
```
Enumerating objects: ... Done.
...
To file:////10.71.7.15/GitRepos/vivo-project.git
 * [new branch]      master -> master
Branch 'master' set up to track remote branch 'master' from 'origin'.

To https://github.com/你的用户名/你的仓库名.git
 * [new branch]      master -> master
```

只要看到两个 [new branch] master -> master，就说明双向通道全部打通！

#### 3.4 验证同步状态 (Verification)

为了确保 Gemini 真的能看到代码，请刷新你的 GitHub 仓库网页。如果你能看到 src、Home.py 等文件出现在网页上，说明这一步大功告成。

【资深工程师的小贴士】如果这一步报错（例如 rejected），通常是因为你在模块一中不小心勾选了创建 README，导致远程有本地没有的文件。如果遇到这种情况，请告诉我，我们需要执行一次 git pull --rebase。


### 4. VS Code 集成与最终验证 (VS Code Integration)

目标：配置完成后，在 VS Code 中点击源代码管理面板的 **“Sync Changes”（同步更改）**按钮，即可一次完成“从 GitHub 拉取 + 同时推送到内网服务器和 GitHub”。

#### 4.1 原理：Sync Changes 到底做了什么？

VS Code 的 “Sync Changes” 不是一个魔法按钮，它等价于对当前分支的 **upstream（上游分支）** 依次执行 `git pull` 和 `git push`。

因此一键双推成立的前提是两条：

1. 当前分支已经设置了 upstream（即模块三的 `git push -u origin master` 已完成）；
2. upstream 指向的 remote（即 `origin`）已经按模块二配好了“一个 fetch + 两个 push URL”。

满足这两条后，Sync 的 pull 走 fetch URL（GitHub），push 自动遍历两个 push URL——无需在 VS Code 内做任何额外设置。

#### 4.2 自查与手动修复 upstream (Set Upstream)

如果点击 Sync 时 VS Code 弹出 “This branch has no remote branch” 之类的提示，说明 upstream 丢了（例如切换过追踪对象、或重建过分支）。自行修复的方法：

```
# 查看当前分支的追踪状态
git status -sb
# 预期第一行输出：## master...origin/master

# 如果显示没有追踪，手动建立（假设当前在 master 分支上）：
git branch --set-upstream-to=origin/master master
```

#### 4.3 最终验证 (Final Verification)

按以下顺序逐项确认：

1. **通道验证**（对应模块二）：
   ```
   git remote -v
   ```
   预期：1 行 fetch（GitHub）+ 2 行 push（服务器、GitHub）。

2. **追踪验证**：
   ```
   git status -sb
   ```
   预期第一行：`## master...origin/master`。

3. **双推干跑验证**（不实际推送，只演练）：
   ```
   git push --dry-run origin master
   ```
   【SOP 标准预期结果】：输出**两段** `Everything up-to-date`（或两段推送进度），分别对应内网地址和 GitHub 地址。只有一段说明 push URL 配错了，回到模块二排查。

4. **VS Code 端到端验证**：随便提交一个小改动，点击 “Sync Changes”，然后分别确认：
   - 内网服务器触发了 post-receive Hook（部署日志有记录，例如 UNC Deployment 输出）；
   - GitHub 网页端能看到这次提交。

资深工程师的小贴士：

- Sync 的 pull 只走 GitHub。如果某天 GitHub 访问不通但内网正常，push 到服务器的那段依然会成功，GitHub 段会报错——这不影响部署，网络恢复后再 Sync 一次即可补齐。
- 不要在 VS Code 设置里开启 `git.useForcePushWithLease` 之外的强制推送开关；正常情况下 Sync 永远不应该需要 force push。

## 堡垒机部署

### 1. 建立同步管道：全链路网络路径化（共享文件夹，无需配置SSH）

🛠️ 部署实操：方案一 (Git Bare Repo + Hooks)

接下来，我们将在服务器（Windows 堡垒机）上构建一个纯净的去中心化同步管道。

核心思想：我们不在服务器的运行目录里直接 git pull，而是建立一个裸仓库 (Bare Repository) 充当“中央中转站”。当您本地把代码 Push 到裸仓库时，触发自动脚本（Hook），将代码精确“弹射”到生产运行目录中。

#### 1.1 在服务器建立“裸仓库”和“生产运行目录”

请在 Windows 堡垒机上打开 PowerShell，执行以下命令构建目录结构：
```
PowerShell
# 1. 创建生产运行目录 (您的项目实际运行的路径)
mkdir C:\Deploy\vivo-project-test

# 2. 创建裸仓库目录 (专门用来接收 Push，不包含工作区代码)
mkdir C:\GitRepos\vivo-project.git

# 3. 进入裸仓库并初始化
cd C:\GitRepos\vivo-project.git
git init --bare
```

#### 1.2 配置服务器端自动化 Hook (触发器)

我们需要在裸仓库中编写一个 post-receive 钩子。只要它收到您本地的 Push 操作，就会自动将最新代码解包到 C:\Deploy\vivo-project-test。

🎯 Target File: C:\GitRepos\vivo-project.git\hooks\post-receive 🛠️ Action: Create (请注意，文件没有扩展名)

使用记事本或任何文本编辑器，在堡垒机的上述路径创建该文件，并输入以下内容（请注意在 Git Bash 语境下，Windows 路径需使用正斜杠 /）：
```
Bash
#!/bin/sh
# 定义生产代码的存放目标路径
TARGET="C:/Deploy/vivo-project-test"
# 定义裸仓库自身的路径
GIT_DIR="C:/GitRepos/vivo-project.git"

# 强制将 Git 树中的代码检出覆盖到目标运行目录中
git --work-tree="$TARGET" --git-dir="$GIT_DIR" checkout -f main

# 输出日志到终端（您在本地 Push 时能看到这句话）
echo "✅ [Enterprise Executer] Deployment to $TARGET completed successfully."
```

#### 1.3 在本地 PC 添加 Remote 并进行 Push (假设通过 SSH)

回到您的本地 PC。假设堡垒机的 IP 是 192.168.1.100，SSH 登录用户名是 Administrator。

在您本地项目根目录的终端中执行：
```
Bash
# 1. 添加远端服务器的裸仓库地址 (注意 Windows 绝对路径的 SSH 写法)
git remote add prod ssh://Administrator@192.168.1.100/C:/GitRepos/vivo-project.git

# 2. 将本地的 main 分支推送到 prod 环境
git push prod main
```

注：如果您本地的主分支叫 master 而不是 main，请将上述命令及步骤 2 中的 main 替换为 master。如果堡垒机尚未配置 OpenSSH Server 服务，我们可以切换为基于局域网共享文件夹（SMB）的本地路径方案。

### 2. 详细步骤

#### 2.1 安装Git

安装地址：
https://git-scm.com/install/windows

- 有管理员权限直接安装standalone版本
- 没有则安装Portable版本

#### 2.2 开放共享权限：每个项目都需单独创建裸仓库和运行目录

您的思维非常敏锐！这是许多刚接触高级 Git 架构的工程师都会产生的终极疑问。

作为您的 Executer，我给您的明确答案是：我们绝对、且必须保留服务器上的 GitRepos 文件夹。

为了彻底解开您的疑惑，我们需要从企业级 Git 架构的“职责边界（Domain Boundaries）”来拆解这个问题。

##### 🧠 架构解析：为什么不能直接推送到 Deploy？

您可能会想：“既然是本地电脑在干活，我能不能不要中间商（裸仓库），直接把代码 Push 到 Deploy 文件夹里？”

答案是：Git 底层核心机制严厉禁止这种行为。

工作区冲突防御机制：在 Git 的设计哲学中，如果您直接向一个“带有工作区（也就是有实际代码文件暴露在外）”的普通仓库 Push 代码，而服务器正好有人（或有程序）在这个目录里查看文件，这会导致严重的索引崩溃（Detached HEAD 或者是工作树脏污）。Git 默认会直接报错并拒绝推送（refusing to update checked out branch）。

纯净的数据中心（GitRepos）：这就是为什么我们要建立一个裸仓库（Bare Repository, GitRepos）。裸仓库里没有您能看到的项目文件（如 .py 源码），它只有压缩后的二进制历史记录（Objects）和索引（Refs）。它就像一个绝对安全的“数据黑盒”，专门用来接收和存储 Push 过来的版本历史，绝不会产生文件读写冲突。

隔离的运行环境（Deploy）：Deploy 则是从黑盒中“解压（Checkout）”出来的最新、最纯净的代码快照，供堡垒机上的 Python 解释器运行。

企业级 DDD（领域驱动设计）职责划分：

GitRepos (裸仓库) = 唯一的版本控制数据库（Source of Truth）。

Deploy (运行目录) = 无状态的生产运行环境（只读快照）。

Hook 脚本 = 连接这两者的自动化流水线。

##### 🔍 澄清误区：Hook 到底存在哪里？

您刚才问：“是不是直接利用本地的 hooks 脚本？”

这是一个非常关键的误区，需要纠正：Hook 脚本是真实存储在服务器的 GitRepos 文件夹中的，而不是存放在您本地电脑的 .git 里！

让我们还原一下通过 SMB（方案乙）推送的极客级物理执行路径：

推送数据：您在本地敲下 git push，本地 Git 把代码打包，通过 SMB 协议，像存文件一样塞进堡垒机的 //10.71.7.15/GitRepos/vivo-project.git 数据库里。

读取 Hook：数据存完后，本地 Git 进程通过网络“看了一眼”堡垒机的目录，发现里面有一个 hooks/post-receive 文件。

异地执行：本地 Git 进程通过网络读取了这个文件的代码，然后在您本地 PC 的内存和 CPU 里运行了这段逻辑。

跨网解压：因为我们在 Hook 脚本里写了全网络路径（//10.71.7.15/Deploy/...），所以本地 CPU 又通过网络，把刚才存进 GitRepos 的二进制代码解压，一个个变成了文本文件，写到了堡垒机的 Deploy 目录下。

总结：脚本存储在远端（服务器），由远端统一定义自动化规则，只是迫于没有 Git 服务端进程的无奈，借用了本地的算力来执行而已。这完全符合中心化配置、去中心化执行的企业级运维规范。

##### Next Step (行动建议)：

现在，我们已经在架构层面上达成了完全的共识。这套 GitRepos (接收) + Hook (本地算力触发) -> Deploy (跨网覆盖) 的流水线，就是您当前极限环境下的 SOTA 解法。

请问我们可以继续执行之前定下的方案乙 的三个步骤了吗？

如果您准备好了，请按照上一条回复中的步骤，在堡垒机上开放 Deploy 共享，更新远端 Hook 脚本为 //10.71.7.15/... 网络绝对路径，并再次尝试本地的 Push 操作。我在此随时待命分析您的执行日志！

### Appendix 

#### 根目录

作为您的 Executer，我需要向您澄清一个关于 Windows 底层安全架构的经典“陷阱”。您观察得很细致，但结论需要修正：这确确实实是一个权限问题，而非路径不存在。

##### 🔍 根因分析 (Root Cause Analysis)

在 PowerShell 返回的日志中，第一条报错明确指出了 CategoryInfo : PermissionDenied 和 UnauthorizedAccessException。而紧接着的 ObjectNotFound（找不到路径）只是 PowerShell 的一种保护机制——当系统判定您没有权限读取某个网络目录时，它会向客户端直接返回“该路径不存在”，以防止恶意试探目录结构。

为什么我们设置了共享权限，还会报权限拒绝？

在 Windows 的网络存储体系中，访问控制由两层独立的安全策略叠加而成，最终权限取两者交集（最严格的那一项）：

Share Permissions (共享权限)：这是网络层面的“大门”。我们在堡垒机执行 New-SmbShare -FullAccess "Everyone"，确实把这扇大门对所有人敞开了。

NTFS Permissions (文件系统权限)：这是硬盘物理层面的“内门”。E:\wzy\GitRepos 这个文件夹在硬盘上创建时，默认只有堡垒机的 Administrator 或创建者拥有读写权限。您的本地账号 V0141351 就算进了大门，也会被 NTFS 的内门无情拦截。

##### 🛠️ 解决方案：打通 NTFS 底层权限

我们需要在堡垒机上，将 E:\wzy\GitRepos 这个物理文件夹的 NTFS 权限也赋予 Everyone。

请在堡垒机上，以管理员身份打开 PowerShell，执行以下命令：
```
PowerShell

🎯 Target Context: [Windows Bastion Host - PowerShell (Run as Administrator)]
🛠️ Action: Modify NTFS Permissions

# 赋予 Everyone 组对该文件夹的完全控制权
# (OI)(CI) 代表向下继承给所有子文件夹和文件，F 代表 Full Control
icacls "E:\wzy\GitRepos" /grant Everyone:"(OI)(CI)F"
```
执行后，如果您看到终端输出类似 已处理文件: E:\wzy\GitRepos 以及 成功处理了 1 个文件；处理 0 个文件时失败，说明底层权限已经打通。

##### 🚀 再次验证与部署 (Next Step)

完成上述堡垒机操作后，请切回您的本地 PC 的终端，再次执行刚才的测试：
```
PowerShell

🎯 Target Context: [Local PC - PowerShell]

ls \\10.71.7.15\GitRepos\vivo-project.git
```

预期结果与下一步：

这一次，您应该不会再看到红色的错误，而是能正常列出裸仓库内部的文件（如 HEAD, config 等）。

一旦 ls 成功，请立即在本地项目根目录中执行部署命令：
```
Bash

git remote add prod file:////10.71.7.15/GitRepos/vivo-project.git
git push prod main
```
请告诉我赋予 NTFS 权限后，测试是否顺利通过？如果成功 Push，我们的去中心化 Git 部署管线就彻底打通了！

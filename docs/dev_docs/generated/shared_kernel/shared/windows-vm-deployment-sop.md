# 天柱专项报表系统迁移到 Windows 虚拟机 SOP

核对日期：2026-11-27。本文以在 Windows 虚拟机上运行一份独立的报表系统为例。先在远程桌面里打开看板，确认业务数据后，再按需要开放内网浏览器访问或启用定时任务。

**可以用远程桌面复制文件，但只复制 `vivo-project` 文件夹通常无法直接运行。** 这个项目还引用了同级 `packages` 目录里的两个 Python 包。虚拟机需要重新安装依赖，并准备所需的业务文件和数据库访问。若只是想通过远程桌面使用现有看板，可以在能访问原服务的远程会话里打开现有网址，走完下列步骤才会在虚拟机上建立一套独立服务。

## 1. 复制项目和本地依赖

用远程桌面的复制粘贴功能，把以下内容放进虚拟机的同一个父目录。示例采用 `D:\services`，也可以换成运行账户有读写权限的目录。

```text
D:\services\
├─ vivo-project\                 项目文件，内含 pyproject.toml 和 uv.lock
└─ packages\
   ├─ common_utils\              此目录内直接有 pyproject.toml 和 src\
   └─ file_decryption\           此目录内直接有 pyproject.toml 和 src\
```

在源电脑上，两个本地包位于 `D:\wzy\Python\packages\`。每个包带上 `src/`、`pyproject.toml` 和包构建引用的文件，例如 `README.md`。根据项目的相对路径，`packages` 要与 `vivo-project` 同级。 [依赖配置](../../../../../pyproject.toml)

| 内容 | 怎么处理 |
| --- | --- |
| 程序和配置 | 带上 `app/`、`src/`、`config/`、`api/`、`tools/`、`.streamlit/`、`.python-version`、`pyproject.toml`、`uv.lock`、`start_streamlit.bat` 和 `run_hidden.vbs`。复制后确认点开头的目录和文件也已到位。 |
| `resources/` 和外部资源 | 复制需要迁移的规格、人工维护台账、历史基线、模板和已发布结果。按原目录保留所需配套文件。检查 `config/global.yaml` 中的资源配置，若有旧盘符或共享目录，改为虚拟机上可访问的位置。 |
| `data/` | 保留业务所需的源数据 Parquet 快照、历史记录和已发布结果。它们被 Git 忽略，只从 Git 获取源码会遗漏这些文件。复制前确认选用哪一份，正在更新的文件应选已写完的一致版本。 |
| 环境和临时文件 | `.venv/` 在虚拟机重新创建。日志、缓存和可重建的 `output/` 内容可按需省略。需要保留的业务导出另外归档。 |
| 凭据 | 通用复制包排除 `.env`、凭据备份和账户私有配置。数据库、API 或邮件凭据按各自需要，在虚拟机上通过公司认可的渠道单独配置。两个本地包也同样排除自己的 `.venv/` 和私有配置。 |

做正式迁移时，以已确认的工作簿和快照为准。首次试跑使用独立副本，尤其避免两台机器同时更新同一份台账或发布目录。 [资源与数据归属](../../../../../CONTEXT.md#hard-boundaries)

## 2. 在虚拟机创建 Python 环境

在虚拟机安装 `uv`，使用本项目固定的 **Python 3.11**。例如使用匹配的 Windows x64 环境。有可用的 Python 3.11 时，`uv` 会寻找合适的解释器。机器缺少解释器且允许联网时，可先运行 `uv python install 3.11`。 [uv 安装说明](https://docs.astral.sh/uv/getting-started/installation/) · [Python 安装说明](https://docs.astral.sh/uv/guides/install-python/)

在**虚拟机的 PowerShell** 执行。示例路径按实际目录调整：

```powershell
Set-Location 'D:\services\vivo-project'
Test-Path ..\packages\common_utils\pyproject.toml
Test-Path ..\packages\file_decryption\pyproject.toml
uv --version
uv sync --locked --no-dev --python 3.11
```

两个 `Test-Path` 都应返回 `True`。同步成功后，下面的命令应输出 Python `3.11.x`、Streamlit 版本和 `imports OK`。 [uv 锁定与安装说明](https://docs.astral.sh/uv/concepts/projects/sync/)

```powershell
& .\.venv\Scripts\python.exe -c "import sys, streamlit, fr_common_utils, fr_file_decryption, psycopg2, pyarrow; print(sys.version.split()[0], streamlit.__version__, 'imports OK')"
```

**虚拟机无法访问软件源时**，先准备公司可用的依赖源，或在可联网的同规格 Windows 上按当前 `uv.lock` 准备离线包。离线包需包括 Python 3.11、`uv`、当前锁定的第三方包，以及构建两个本地包需要的依赖，例如 `hatchling`。交付前用空环境做一次断网安装。当前仓库 `OfflineRepo/wheels/` 中的 Streamlit 是 `1.55.0`，而这份代码要求至少 `1.57.0`，锁定版本是 `1.60.0`，因此这批旧包需要更新。 [项目环境声明](../../../../../.python-version) · [锁文件](../../../../../uv.lock)

## 3. 准备取数和报表所需条件

- **数据库**。在虚拟机为项目运行账户配置获准的 PostgreSQL 只读连接。项目可从根目录的私有 `.env` 读取 `DB_HOST`、`DB_PORT`、`DB_DATABASE`、`DB_USER`、`DB_PASSWORD`。在虚拟机运行 `Test-NetConnection -ComputerName <数据库主机> -Port <数据库端口>`（将占位内容替换为实际值）。`TcpTestSucceeded : True` 表示端口可达，账号权限和取数仍要通过报表验证。 [数据库配置读取代码](../../../../../src/shared_kernel/infrastructure/db_handler.py) · [微软端口检查说明](https://learn.microsoft.com/powershell/module/nettcpip/test-netconnection)
- **企业保护文件和导出**。如果要读取受保护的 Excel 或 PowerPoint，核对虚拟机已安装相应桌面 Office，且实际运行账户能打开文件。若使用 AOI RS 批量图片导出，准备 Chrome 或 Chromium，并实际导出一份样例。 [PowerPoint 模板读取代码](../../../../../src/shared_kernel/infrastructure/ppt_template.py) · [导出依赖](../../../../../pyproject.toml)
- **本机权限和时间**。确认运行账户能读取配置和业务文件，并能写入所用的数据、资源和日志目录。核对虚拟机的时间和时区与原部署一致，避免报表日期、每日截止时间和任务触发时间偏移。 [报表日期规则](../../../../../CONTEXT.md#time-and-data-semantics)

## 4. 启动并验收

1. **启动看板**。确保虚拟机的 `8503` 端口可用，在项目根目录运行 `.\start_streamlit.bat`，或在资源管理器双击 `start_streamlit.bat`。脚本默认使用 `8503`，会验证该端口和健康响应。失败时查看 `output/logs/streamlit-startup/` 中最新的 JSON 和 stderr 日志。 [启动脚本](../../../../../tools/restart_streamlit.ps1) · [服务重启说明](../../../../../references/design/shared_kernel/shared/data-flow-infrastructure-refresh.md)
2. **在远程桌面里打开看板**。用虚拟机里的浏览器打开 `http://127.0.0.1:8503/`。这是确认虚拟机上这份服务能打开的最短路径。若只通过远程桌面使用，这一步验证了浏览器访问路径，接着检查业务数据。
3. **验证业务数据**。用一个已知的产品和较短的历史日期范围，查询目标报表，同原系统核对关键行数或指标。若用到自动预警，再找一组已知有预警的样例核对，确认已发布结果随迁移到位。首页或健康检查通过只代表程序能应答。 [预警看板数据来源](../../../../../references/design/inline_domain/shared/architecture-published-alert-display.md)
4. **如需从其他电脑的浏览器打开**，在虚拟机上用 `ipconfig` 查它的内网 IPv4，再从获准的内网电脑测试 `http://<虚拟机IPv4>:8503/`。原网址 `http://10.72.26.31:8503/` 仍指向原地址。若虚拟机里能打开、其他电脑打不开，在虚拟机用 `Get-NetTCPConnection -State Listen -LocalPort 8503` 核对监听地址。如果只监听 `127.0.0.1`，浏览器需在虚拟机内访问。如果已监听内网地址或所有地址，再检查 Windows 防火墙和虚拟机的网络可达性。 [Streamlit 地址和端口配置](https://docs.streamlit.io/develop/concepts/configuration/options)

## 5. 只有需要长期接替原服务时才做

- **先验收再切换**。保留原服务作为回退。验收后更新使用者的入口。如果切换失败，把访问入口切回原服务，并核对期间新增的人工记录和发布结果，避免覆盖。
- **按需求重新配置任务**。Windows 任务计划程序中的任务和当前用户的登录启动项属于原机器的配置。需要定时重启、巡检或邮件时，在虚拟机上按各自的部署说明重新配置并验证触发结果。正式切换时确认只由选定的实例负责巡检、发布和正式邮件，避免重复运行。 [Streamlit 定时运行说明](../../../../../references/design/shared_kernel/shared/data-flow-infrastructure-refresh.md) · [巡检配置](../../../../../references/domain/api/architecture-automatic-inspection.md) · [邮件配置](../../../../../references/domain/api/notification/rules-alert-notification.md)
- **按需迁移异常 API**。看板是 `8503`，异常 API 是独立服务，现行部署采用 `8890`。若一起迁移，按 API 部署说明在虚拟机运行账户下配置授权，并在必要时更新调用方及邮件任务的 API 地址。先验收取数，再恢复正式调用和发信。 [API 部署说明](../../../../../references/domain/api/interaction-sheet-anomaly-api-deployment.md) · [邮件任务的 API 地址配置](../../../../../config/domain/api/notification.yaml)

## 本次核对结果

在源电脑核对了本地依赖路径、锁文件、旧离线包和启动脚本。`uv lock --check --offline` 通过，现有环境中 Python `3.11.9` 和 Streamlit `1.60.0` 的关键依赖导入通过。目标虚拟机的安装、网络、Office 权限和业务数据需按上面的步骤验收。

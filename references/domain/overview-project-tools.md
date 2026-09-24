# 项目工具脚本说明

核验日期：2026-09-24。本文介绍 [tools/](../../tools/) 当前保留的 10 个可执行脚本。
`__init__.py` 仅用于 Python 包导入，不是需要手动执行的工具。

## 1. 选择工具

| 需求 | 工具 | 主要影响 |
|---|---|---|
| 重启报表服务、释放进程内存缓存 | [restart_streamlit.ps1](../../tools/restart_streamlit.ps1) | 结束旧服务并启动新服务 |
| 提前计算当天预警矩阵 | [warm_alert_matrix.py](../../tools/warm_alert_matrix.py) | 查询、计算并发布矩阵状态快照 |
| 注册预警矩阵每日任务 | [register_alert_matrix_task.ps1](../../tools/register_alert_matrix_task.ps1) | 新建或替换 Windows 计划任务 |
| 刷新各厂别 Q-Time 数据 | [refresh_qtime_snapshots.py](../../tools/refresh_qtime_snapshots.py) | 查询数据库并更新本地快照 |
| 注册 Q-Time 每日刷新任务 | [register_qtime_snapshot_task.ps1](../../tools/register_qtime_snapshot_task.ps1) | 新建或替换 Windows 计划任务 |
| 停写后重建 Inline 原始快照 | [refresh_inline_snapshots.py](../../tools/refresh_inline_snapshots.py) | 预览；显式应用时先删除指定快照再重建 |
| 刷新入库良率修饰表 | [update_yield_modifier_table.py](../../tools/update_yield_modifier_table.py) | 读取明细并写回修饰工作簿 |
| 首次生成关键部件模拟快照 | [fabricate_equipment_data.py](../../tools/fabricate_equipment_data.py) | 根据规格生成本地模拟数据 |
| 更新已有关键部件模拟快照 | [update_fabricated_equipment_data.py](../../tools/update_fabricated_equipment_data.py) | 按新鲜度规则更新模拟数据 |
| 执行指定范围的单元测试 | [smoke.py](../../tools/smoke.py) | 调用 pytest，不用于服务启动 |

以下命令均在项目根目录的 PowerShell 中运行，Python 使用项目 `.venv`。
依赖环境应提前准备好；启动脚本不会自动执行 `uv sync`。
除标明的预览、`--check` 和测试命令外，工具可能连接数据库、写文件或更改计划任务。

## 2. 启动与重启服务

日常入口仍为根目录的 `start_streamlit.bat`；计划任务通过 `run_hidden.vbs` 隐藏运行 BAT。
两者最终调用同一个 PowerShell 重启脚本。直接执行等效入口：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\tools\restart_streamlit.ps1
```

默认端口为 **8503**，入口为 `app/Home.py`。脚本先验证 `.venv` 可用，再识别本项目旧进程、
终止并验证退出、启动新进程、检查监听归属及健康接口，最后记录结果。
`-Port` 可指定端口，`-StopTimeoutSeconds` 默认 20 秒，`-StartupTimeoutSeconds` 默认 60 秒。
返回码 `0` 表示启动检查成功，`1` 表示失败。

每次运行的 JSON 状态、stdout 和 stderr 写入 `output/logs/streamlit-startup/`。
重启会清空旧进程的 Streamlit 内存数据缓存及资源缓存，但不会主动全量刷新磁盘数据快照。
不要让多个计划任务同时调用同一项目、同一端口的启动入口。
完整机制见 [系统刷新与重启机制](../design/system_design/data-flow-infrastructure-refresh.md)。

## 3. 预警矩阵预计算与计划任务

`warm_alert_matrix.py` 复用现有矩阵计算入口，生成当天状态并发布到
`output/cache/alert_matrix/daily.json`。它不是全部领域原始数据的强制全量刷新器。

```powershell
# 只检查并创建所需目录，不查询业务数据
.\.venv\Scripts\python.exe .\tools\warm_alert_matrix.py --check

# 实际计算并发布当天矩阵
.\.venv\Scripts\python.exe .\tools\warm_alert_matrix.py
```

运行日志位于 `output/logs/alert_matrix_warmup/`。返回码：`0` 全部成功，`1` 矩阵中有错误单元格，
`2` 整体执行失败。存在错误单元格时可能仍发布带错误状态的矩阵，不能只按文件存在判断成功。

注册每日任务：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\tools\register_alert_matrix_task.ps1
```

默认任务名 `vivo-project Alert Matrix Warmup`，时间 **09:02**，安排在每日 09:00 服务重启之后；
可用 `-TaskName`、`-At` 修改。这是固定时间触发，不等待服务健康检查成功。
矩阵任务在独立 Python 进程中运行，发布的是磁盘状态快照，不直接填充 Streamlit 服务进程的内存缓存；
服务重启不会删除已发布的 `daily.json`，后续页面仍需校验快照有效性后复用。
同名任务已存在时默认拒绝替换，明确更新时使用 `-Force`。
任务使用项目 `.venv\Scripts\pythonw.exe` 和当前用户的交互登录会话，涉及 Excel COM 时需要用户已登录。
注册脚本本身不执行预计算；日常触发执行的是 Python 脚本。

## 4. Q-Time 刷新与计划任务

`refresh_qtime_snapshots.py` 通过应用服务依次刷新 ARRAY、OLED、TP 的共享滚动快照。
默认增量刷新；回刷天数读取全局配置。`--full` 改为对应滚动窗口的全量重取。

```powershell
.\.venv\Scripts\python.exe .\tools\refresh_qtime_snapshots.py
.\.venv\Scripts\python.exe .\tools\refresh_qtime_snapshots.py --full
# 指定报表日期
.\.venv\Scripts\python.exe .\tools\refresh_qtime_snapshots.py --as-of 2026-09-24
```

标准输出为各快照的 JSON 结果。返回码：`0` 全部从数据库刷新成功，`1` 存在旧快照回退，
`2` 执行异常。旧快照可读不代表本次数据库刷新成功。

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\tools\register_qtime_snapshot_task.ps1
```

默认注册 `vivo-project Q-Time snapshot refresh`，每日 **07:00**；支持 `-TaskName`、`-At`、
`-PythonPath` 和 `-Force`。默认优先使用项目 `.venv`，不存在时查找 PATH 中的 Python。
使用 `-Force` 会移除并重新注册同名任务。脚本存在不表示部署机器已经注册了该任务。

## 5. Inline 原始快照重建

`refresh_inline_snapshots.py` 用于维护时重建共享测量和 AOI_RS 原始快照，正常页面读取不依赖它。
要求使用当前 `data/inline_domain/<module>/` 布局。必须提供产品和截止日期：

```powershell
# 默认只列出计划，不删除或重新取数
.\.venv\Scripts\python.exe .\tools\refresh_inline_snapshots.py --products M626 M678 --end-date 2026-09-24

# 确认应用和刷新任务均已停止写入后，才执行这一条
.\.venv\Scripts\python.exe .\tools\refresh_inline_snapshots.py --products M626 M678 --end-date 2026-09-24 --apply --writers-stopped
```

`--writers-stopped` 是操作者确认标志，不会自动停止服务。应用阶段先删除计划中识别出的原始快照
及配套元数据，再经仓储入口重新获取；重建失败不会自动恢复已删文件，不适合作为日常定时刷新命令。
工具不修改 Excel。预览返回 `0`；重建全部成功返回 `0`，部分或全部失败返回 `1`，参数错误返回 `2`。
仍有旧产品目录原始快照时会拒绝重建；旧目录迁移工具已退出当前工具集，需要单独处理历史迁移。

## 6. 入库良率修饰表维护

`update_yield_modifier_table.py` 与页面共用 `sync_modifier_table`，更新目标月份的“当月良损”，
补充缺失行，并按“指定良损”签名变化重新计算“缩放倍数”。

```powershell
.\.venv\Scripts\python.exe .\tools\update_yield_modifier_table.py --product M678
.\.venv\Scripts\python.exe .\tools\update_yield_modifier_table.py --product M678 --month 2026-09
```

`--product` 必填，月份默认当前月。工具读取快照/数据库明细，写入配置解析出的修饰工作簿；
它没有预览模式。未取得 panel 明细时返回 `1`，正常同步返回 `0`。
操作前确认产品、月份与维护意图，避免把写回业务台账的命令当成查看命令。

## 7. 关键部件模拟数据维护

这两份脚本服务于关键部件领域已有的“真实数据优先、模拟数据回退”机制。
它们生成或更新模拟快照，不负责从生产数据库采集真实寿命数据。
默认规格基线为 `resources/equipment_domain/critical_parts_baseline.csv`，输出目录取领域运行配置，
可通过 `--output-dir` 覆盖。

```powershell
# 首次生成；已有对应快照时默认拒绝覆盖
.\.venv\Scripts\python.exe .\tools\fabricate_equipment_data.py

# 更新已有快照；默认遵循 24 小时新鲜度判断
.\.venv\Scripts\python.exe .\tools\update_fabricated_equipment_data.py
```

- 生成脚本：`--baseline` 指定基线，`--seed` 指定随机种子，`--as-of` 指定生成截止时间；
  `--overwrite` 允许覆盖已有对应快照。
- 更新脚本：`--baseline` 指定基线，`--now` 指定新鲜度判断时间，`--force` 强制更新。
- 两者输出 JSON 摘要及快照路径。更新脚本的 `updated=false` 可能表示仍在有效期内而跳过更新，
  应结合摘要判断，不等同于报错。

业务约束见 [关键部件模拟数据决策](../../docs/ADR/0003-equipment-real-first-fabricated-fallback.md)。

## 8. 单元测试入口

```powershell
.\.venv\Scripts\python.exe .\tools\smoke.py spc
.\.venv\Scripts\python.exe .\tools\smoke.py equipment
.\.venv\Scripts\python.exe .\tools\smoke.py yield
.\.venv\Scripts\python.exe .\tools\smoke.py all
```

`smoke.py` 按已有规则选择单元测试并调用 pytest，返回 pytest 原生退出码。
无参数时等同于 `all`，执行 `tests/unit/`；不包括 architecture、integration 或浏览器测试。
它不是生产服务的健康检查，也不能替代计划任务执行记录和启动日志。

## 9. 本轮清理范围

已移除旧资源汇总与快照目录迁移脚本，以及确认不再使用的 PPA 提取/周报、TP NG 在库统计、
指标改善离线分析和堡垒机 HTML 清理工具；相应专用测试及失效引用一并清理。
本次删除对象是工具源码及配套说明、测试，业务原始数据、人工维护工作簿和既有分析产物保留。

# 自动巡检：业务与技术说明

统一入口为项目根目录下 `python -m api.inspection`；已完成全范围实际运行验收。

巡检调用各领域已有 application 服务刷新数据、计算预警和发布本地结果。
API及页面继续读取这些结果，HTTP查询不会触发生产。无需有人打开报表页面。

## 范围

| 领域 | 生产内容 |
|---|---|
| SPC | OOS、OOC、CPK、CPM，以及能力项目的完整Sheet成员快照 |
| CTQ | OOS、OOC |
| AOI_TT、AOI_RS | OOS、OOC及趋势评估；RS保留Sheet／Lot身份 |
| Yield | 周趋势与Lot超规，沿用既有业务口径 |
| Q-Time | ARRAY、OLED、TP全部厂别的快照与告警 |
| IJP | AA边界、孔内两个区域 |
| 关键备件 | 全厂共享寿命超限结果 |
| IQC蒸镀 | 启用产品范围的上周IQC／COA NG结果 |

范围包含自动预警看板全部13行，并生产AOI两类趋势及API使用的OOC。
这不会自动扩展API的返回范围；Yield/AOI趋势、IJP、备件和IQC仍由原展示入口消费。

## 一次执行做什么

1. 读取全局启用产品，以完整产品／厂别／参数范围生产。
2. 通过各域既有仓储刷新必要数据；数据库会话强制只读且设定有限超时。
3. 调用原有规则和发布流程，不重写另一套预警算法，也不自动释放人工决定。
4. 读回产物核对本次生产、日期覆盖及健康状态。陈旧回退、缺规格、缺产物均不能算成功。
5. 各模块失败相互隔离；所有必需模块、产品完成才记为整体成功。

计算完成但没有异常，必须有成功生产的证据；不能仅因为台账缺少记录而假定已计算。
保留已有人工flag、历史决定、规格和源快照；文件写入沿用原有锁与发布边界。
正常生产本身可能维护原报表已有的修饰台账，这与访问对应生产报表时行为一致。

## 程序组织

- `api/inspection/application.py`：完整范围检查、逐项执行、互斥和原子完成记录。
- `composition.py`及各`producers_*`：装配真实领域生产者并核验结果。
- `yield_publication.py`、`qtime_publication.py`、`spc_publication.py`：迁入的单域维护入口。
- `legacy_publication.py`：原Yield/IJP选择性巡检兼容逻辑。
- `matrix_warmup.py`：领域生产完成后，读取发布结果并写入看板状态缓存。
- `__main__.py`：统一一次性命令，依次执行生产与看板预热。原`tools`脚本仅转交调用。

生成日志位于 `output/logs/alert_inspection/`。每次一个独立JSON，`latest.json`为最新回执。
记录目标日期、启用产品、完整模块清单、进程身份及逐项结果；不记录密码、SQL或原始驱动异常。
`running`表示仍在运行，`success`表示全部完成，`partial`表示有模块失败，`failed`表示整体中断。
回执不是业务事实来源，具体数据仍以各域已发布结果为准。

## 定时运行

按用户要求，原任务`vivo-project Yield IJP Alert Publication`改为每天07:15执行统一巡检。
同一任务在领域生产结束后立即预热看板，原09:02的`vivo-project Alert Matrix Warmup`
独立任务已合并撤销。部分领域失败仍汇总看板状态；生产或预热失败时任务返回非零。
生产回执和预热日志分别保留，邮件门禁仍核验领域生产回执。预热不查询数据库，
不补算预警，缓存位于`output/cache/alert_matrix/daily.json`。
邮件使用独立任务，每周一07:20及每周二11:00触发。正式任务按业务配置分派责任人。
邮件任务先核验当天巡检回执；若巡检仍在运行，会等待其完成，而不是在07:20直接发送旧数据。
失败或缺少完整成功回执时停止发信并记录失败。不会把陈旧`running`文件当成真实运行进程。

任务使用当前登录账户、项目虚拟环境和隐藏控制台，防止同名任务重叠启动。
电脑须开机，账户须保持登录（可锁屏），并能访问企业数据库、API、SMTP及受保护文件。
任务错过时间后的补跑遵循Windows任务配置；不能在关机或注销时保证按时生产。

```powershell
# 检查范围，不执行生产、业务取数或发信
.venv\Scripts\python.exe -m api.inspection --check

# 手动执行完整生产与校验
.venv\Scripts\python.exe -m api.inspection

# 仅打印将注册的任务，不执行任何任务或覆盖定义
powershell.exe -NoProfile -File api/inspection/register_tasks.ps1 -MailConfigPath D:/wzy/Python/array-projects/config/config.yaml

# 明确应用：先备份，再更新两个指定任务并读回核验
powershell.exe -NoProfile -File api/inspection/register_tasks.ps1 -MailConfigPath D:/wzy/Python/array-projects/config/config.yaml -Apply

# 仅更新合并后的巡检任务，并撤销旧独立预热任务；先备份，不改邮件任务
powershell.exe -NoProfile -File api/inspection/register_tasks.ps1 -InspectionOnly -Apply

# 手动执行带完整性门禁的邮件任务；省略--test-mode即使用正式责任人配置
.venv\Scripts\python.exe -m api.notification.scheduled --mail-config D:/wzy/Python/array-projects/config/config.yaml --test-mode
```

私有邮件YAML仅提供SMTP账户凭证，API密钥由显式环境文件或进程环境提供。
任务定义只含凭证文件路径，不保存密码。登记时从未来的触发点开始，避免把已错过的今日时间
当成需要立刻补跑的任务。旧 `tools/register_alert_result_publication_task.ps1` 已退役，
不会再生成每30分钟任务；兼容的专用Q-Time注册入口转交 `api/inspection/register_qtime_task.ps1`。

重复运行会重新验证／生产。巡检不会自行发送邮件；邮件重复运行可能重复投递，故不自动重试不确定投递。
只有当前日期全部必需范围校验成功，才能将“没有公开预警”解释为本次生产后的结果。

## 本次验收

2026-11-12真实全范围巡检40／40项成功，耗时约12分55秒；邮件入口等待巡检完成后，
读取实际API的2条公开预警，SMTP接受1封仅发往授权验收邮箱的邮件。
496项回归与25项补充测试通过。生产回执和验收证据分别在
`output/logs/alert_inspection/`、`output/test-results/automatic-inspection/`。
每天07:15巡检、每周一07:20及周二11:00邮件的Windows任务已登记并读回核验。
07:20是邮件触发时间；实际发送会等本轮巡检成功完成。

`python -m api.inspection`中的`-m`表示按Python模块名执行程序。
`api.inspection`对应项目中的`api/inspection/`包，实际入口文件为
`api/inspection/__main__.py`。Windows任务的启动程序是虚拟环境的`pythonw.exe`，
参数是`-m api.inspection`，工作目录是项目根目录；`pythonw.exe`用于隐藏控制台窗口。
所以它仍执行Python程序，只是使用包名定位入口，不需要另建bat文件。

合并验证：314项相关测试通过；禁止数据库连接的真实预热成功生成91格（79达标、
3预警、9无数据、0加载失败）。已备份并撤销独立预热任务，保留的任务仍为每日07:15，
邮件任务定义保持一致。证据位于`output/test-results/matrix-warmup-merge/`。

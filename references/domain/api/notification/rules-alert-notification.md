# 预警邮件自动推送

实现入口：`python -m api.notification`。业务配置：
[`config/domain/api_domain.yaml`](../../../../config/domain/api_domain.yaml)。
需求：[预警信息推送服务](../../../../docs/dev_docs/dev_spec/aoi/task-预警信息推送服务.md)。

## 取数与分派

每次运行调用现有 HTTP 客户端，不传任何查询参数，也不添加分页参数。
日期、全部产品／分类／厂别和事件范围均由 API 的授权与默认值决定：上一完整 ISO 周，上海时区。
不访问数据库，不补算报表，也不改变 API 正式服务。

按每条记录的“分类”和“部门”匹配 `notification.routes`。配置必须覆盖六类、三个厂别，
且不能存在重叠路由；每个路由可配置多个收件人。全部记录完成路由检查后才开始发送。
未知分类、缺失厂别或非法配置会终止任务，不静默丢弃记录。
同一收件人的多分类／厂别合并成一封邮件。无抄送或密送。

默认责任人以 YAML 为唯一事实来源；更新责任人只需修改该配置，无需调整代码。
测试模式统一覆盖为 `wangzy03@visionox.com`，再按实际收件人合并，因此通常只发送一封测试邮件。
私有邮件配置中的 `MAIL_RECEIVERS`、`MAIL_CC` 及其它路由字段均不生效。

## 完整性、内容及退出码

- 完整且空的结果成功退出，不发邮件。
- `incomplete_policy: notify` 默认向相关责任人发送可用记录及明确的来源缺失提示，
  不把不可用来源解释为无异常。失败信息未包含厂别时，通知该分类三个厂别的责任人；
  即使无明细也发送来源缺失通知。提示只包括公开来源字段。
- `incomplete_policy: abort` 在任何来源不完整时终止，不发送邮件。
- API 整体失败、非法响应或意外分页直接失败，无数据库回退。
- 正文列出日期、分类／厂别／事件计数、最多20条明细及适用的数据限制说明。
  邮件主题和正文不显示测试标识、时区标签或“预览”字样；测试收件人覆盖仍然生效。
  汇总和明细使用内联表格样式，明细合并为8列；正文指标最多显示四位小数，
  每条能力项目正文最多展示5个片号，超过时明确提示总数并指向附件。
  CSV 附件包含该收件人的全部公开明细、17个固定字段和 UTF-8 BOM；
  能力项目保留完整 Sheet ID JSON 数组，普通片／Lot ID 保留文本。
  不按对象编号去重，不写入内部列。HTML 转义，CSV 公式文本加单引号保护。
- 退出码：`0` 完整成功（包括预览及完整空结果）；`3` 已完成预览或发送，但来源不完整；
  `2` 配置、API 或投递失败。`abort` 策略的不完整结果为 `2`。

输出只记录安全摘要：明细数、计划邮件数、确认发送数、完整性、缺失来源数和是否预览。
SMTP 失败保留此前已确认发送数量。SMTP 接受意味着服务器接受投递，不证明邮箱端已经收件。

## 凭证与一次运行

在项目根目录执行，使用已准备好的 `.venv`。运行环境提供 `API_KEY`（兼容 `VIVO_API_KEY`）、
`MAIL_USER`、`MAIL_SENDER` 及 `MAIL_PASSWORD` 或逗号分隔的 `MAIL_PASSWORDS`。
凭证不放入业务 YAML 或命令行。模块导入不会取数或发信。

可通过 `--env-file` 显式读取本地环境文件，仅提取通知所需键，进程环境优先，
不修改进程环境，不插值变量。无此参数不会自动读取 `.env`。
`--mail-config` 可读取原邮件例程格式的私有 YAML，仅使用 `MAIL_USER`、`MAIL_SENDER`、
`MAIL_PASSWORDS`／`MAIL_PASSWORD`，且优先于邮件环境变量；不会使用文件内的 SMTP 主机、端口或收件人。
SMTP 主机、端口、安全方式和超时统一由业务 YAML 的 `notification.smtp` 决定。

```powershell
# 安全预览：实际取数和统计，不发送，不读取邮件凭证
.venv\Scripts\python.exe -m api.notification --env-file .env --test-mode

# 使用用户指定例程的现有私有邮件配置，所有收件人覆盖为测试邮箱
.venv\Scripts\python.exe -m api.notification --env-file .env --mail-config D:\wzy\Python\array-projects\config\config.yaml --send --test-mode

# 正式推送：所有凭证已在任务进程环境中配置时
.venv\Scripts\python.exe -m api.notification --send
```

无需引用其它项目源码；私有 YAML 是可选输入，不是隐藏的跨项目依赖。
采用所提供例程的 `smtp.qiye.aliyun.com:587`，默认 STARTTLS 并校验证书。
支持 `ssl` 或明确配置的 `plain`；不会在 TLS 失败后自动降级。
仅认证拒绝时轮换候选密码，有限连接超时。收件人拒绝、断线或其它不确定投递不自动重试。

## 定时运行与重试边界

模块是一次性批处理入口，可由 Windows 任务计划程序每周一运行一次；
无需常驻线程或 Web 端点。由运维选择时间，建议在上周结果发布完成后执行。
任务的程序设为项目 `.venv\Scripts\python.exe` 的绝对路径，
参数设为 `-m api.notification --send`，起始目录设为项目根目录；
如使用文件凭证，加对应的绝对路径参数。运行账户须有配置文件读取权及 API／SMTP 网络访问权。
关闭重叠启动和自动失败重试，保留标准输出及退出码。

本模块没有跨运行去重台账：重跑已经成功的任务会再次发信。
若部分邮件成功后失败，核对摘要中的 `sent_count` 和邮件记录，再决定处理方式；
失败的当前邮件也可能已经被服务器接受。不要直接整批自动重跑。
2026-11-12起已注册独立邮件任务，每周一07:20、每周二11:00触发，先等待当天完整巡检成功。
调度入口为 `python -m api.notification.scheduled`，其完整性门禁与统一任务注册方法见
[自动巡检说明](../architecture-automatic-inspection.md)。上面的普通入口仍可独立手动使用。

## 验证入口

```powershell
.venv\Scripts\python.exe -m pytest tests/unit/api/test_notification.py tests/integration/test_notification_delivery.py tests/e2e/test_notification_e2e.py -q
```

自动化 E2E 使用独立随机端口的最小 HTTP／SMTP 测试服务，不连接数据库。
正常、异常和中断路径均核对自建线程退出、子进程变化及监听释放。
企业实测与自动化证据位于 `output/test-results/alert-notification/`，实际验收见
[ADR-0043](../../../../docs/ADR/0043-weekly-alert-notification.md)。

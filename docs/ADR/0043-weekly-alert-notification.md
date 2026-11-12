# 0043 — API 预警周报邮件推送

日期：2026-11-11。状态：已实现并验证；本地开发分支 `feat/alert-notification`，未提交或合并。

## 背景与决定

根据[用户需求](../dev_docs/dev_spec/aoi/task-预警信息推送服务.md)，在根目录 `api` 下新增
`notification` 子域，复用现有公开 API 的零参数查询，按分类和厂别通知责任人。
Core 负责纯路由与公开投影；Application 编排取数和投递；Infrastructure 适配现有 HTTP 客户端、
SMTP 和 MIME 格式；Settings 加载经过校验的路由和显式凭证输入；`__main__` 为一次性调度入口。

业务配置和收件人由 `config/domain/api_domain.yaml` 唯一维护，凭证通过进程环境、
显式环境文件或原例程格式的私有邮件 YAML 注入。邮件头和 SMTP 信封使用同一收件人；
测试模式在分组前统一覆盖，禁止私有配置携带其它收件人、抄送或密送。

默认发送可用明细并明确提示来源缺失；可配置为不完整时取消整个任务。
完整空结果不发送。正文为有限预览，CSV 保留全部公开记录和能力项目片号数组。
同一实际收件人合并邮件，所有路由在首封发送前验证。

## 替代方案与后果

- 直接读发布工作簿或导入服务端 Application：会绕过已有 HTTP 授权和契约，未采用。
- 在 API Web 进程中增加调度或发送端点：混合查询与外部副作用，未采用。
  一次性命令可以由现有任务计划系统调度，也无需改动正式 API 进程。
- 直接复用 `fr_common_utils.email.send_mail_common`：已检查所提供例程；该工具使用明文 SMTP、
  将错误转为布尔值、打印原始异常且忽略收件人部分拒绝，不能满足本任务的 TLS、
  安全诊断及不确定投递边界，因此独立适配标准库 `smtplib`，未改动共享包。
- 全部不完整结果一律取消：保留为可配置策略；默认显式通知可用数据和缺失来源，避免漏报。

代价：不提供跨运行去重或自动补投。仅认证拒绝允许尝试下一个密码，
其它 SMTP 故障不自动重发。失败摘要保留确认发送数量，当前失败邮件仍可能已被接受。
运维须控制任务重叠和重跑，缺少来源时修复上游发布，不由本服务补算。
配置的全部授权范围仍取决于所使用 API 凭证，不能将零参数理解为超出授权的全库访问。

## 验证

自动化覆盖全部18个分类／厂别路由、零查询参数、收件人覆盖、
空结果和缺失来源、附件全量和片号数组、公式与 HTML 转义、
TLS、候选凭证轮换、投递失败及临时服务生命周期。入口与详细运行契约见
[通知规则](../../references/domain/api/notification/rules-alert-notification.md)。

最终专项及关联 API 回归：354 passed；通知模块覆盖率94%。
命令包括 `tests/unit/api`、`tests/unit/test_fetch_sheet_anomalies.py`、
`tests/integration/test_anomaly_api_client.py`、`tests/integration/test_anomaly_api_default_events.py`、
`tests/integration/test_notification_delivery.py` 和 `tests/e2e/test_notification_e2e.py`，
使用 `--cov=api.notification`。另通过 Python 编译及 `git diff --check`。
现有 Starlette/httpx 与 pandas 弃用提示未影响结果；未运行全仓库其它业务套件。

代码与安全评审未发现未关闭的阻断项。已修复部分投递失败丢失成功数量的问题，
并以“第二封失败、确认成功1封”测试验证。运行时服务清理记录均显示 `cleaned=true`。

真实 API 零参数查询返回 2026-11-02 至 2026-11-08 的2条记录，完整且无失败来源；
使用例程已有私有凭证并采用 STARTTLS，SMTP 于 2026-11-11 17:33（上海）接受1封测试邮件，
唯一收件人为 `wangzy03@visionox.com`，无抄送或密送。未验证邮箱端实际收件。
无数据库访问，无生产服务启停，无定时任务注册。

证据：`output/test-results/alert-notification/{live-preview,live-delivery}.json`、
`regression.xml`、`coverage.xml` 和各 `*-http.json`／`*-smtp.json` 清理记录。
规格及票据位于 `.scratch/alert-notification/`，未改动用户原始任务正文。

## 邮件展示调整

2026-11-11，用户确认收到原版邮件后，要求删除测试和时区等模板字样并改善表格。
主题及正文已移除测试标识、时区标签和“预览”提示，收件人覆盖逻辑保留。
正文采用8列业务明细、内联深色表头、隔行底色和长片号换行，窄屏明细区可横向滚动。
正文指标最多四位小数、每条能力项目最多显示5个片号并明确提示完整名单在附件；
CSV保留原始字段、全部片号和数值精度。

本轮54项相关测试通过，展示改动评审无阻断问题。
桌面与窄屏浏览器检查覆盖正常、来源缺失和无明细通知，未替代具体邮件客户端的渲染验收。
18:13（上海）SMTP再次接受1封优化后的邮件，含2条记录，唯一收件人为
`wangzy03@visionox.com`，无抄送或密送。证据为
`output/test-results/alert-notification/polish-live-delivery.json`、
`polish-regression.xml`、`polish-layout.json` 及 `polish-*.png`。
本轮测试监听及浏览器进程清理记录均已核对。

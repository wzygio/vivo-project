# API 1.4.0：默认全部事件与本机密钥自动填入

日期：2026-11-04。状态：实现、测试及正式验证完成。

后续同日调整：用户要求直接用 `uv run --no-sync python tools/fetch_sheet_anomalies.py`，查询 CMD、独立启动器及其专用测试已移除。以下启动器行为和 6 项测试保留为当时的历史验收记录；当前客户端不自动加载项目 `.env`，现行使用方法以调用说明为准。

## 行为

省略 `event_type` 时，在已有授权产品／分类范围内选择全部适用事件：SPC 为 OOS、OOC、CPK、CPM；CTQ／AOI 为 OOS、OOC；Yield／Q-Time 仅 OOS。显式填写仍只查询该事件。参数不接受 `ALL` 或空字符串代表全选，应省略。混合结果按每行“异常事件”区分，失败来源继续带具体事件；全局分页、字节、超时和认证约束保留。

本次保留 API Key 校验。API Key 不是 HTTP 技术的强制要求，但现有服务依赖它绑定调用方权限和限流。没有启用匿名用户，也没有给无密钥请求代填服务端默认密钥。

独立调用程序读取进程环境 `API_KEY`，兼容 `VIVO_API_KEY`；窗口星号显示、查询后清空输入框，后续可从环境再次取得。本机一键入口改由 `tools/launch_anomaly_api_query.py` 启动：没有进程凭证时，只读项目 `.env` 的 `API_KEY`，仅注入本次运行，正常／异常退出均恢复环境。独立脚本不读密钥文件；不分发本机 `.env`，不在代码、导出和日志写入真实密钥。

## 验证

- API／客户端／真实文件／网络回归：378 passed。命令包含 `tests/unit/api`、`tests/unit/test_fetch_sheet_anomalies.py`、`tests/integration/test_anomaly_api_{files,capability,default_events}.py` 及 `tests/e2e/test_anomaly_api_sheet_membership_e2e.py`，结果见 `output/test-results/api-default-events/regression.xml`。
- 新默认事件契约专项最终 10 passed：支持范围、非 SPC 授权、四类真实文件合并、特定事件失败隔离、OpenAPI 默认全选、无凭证拒绝，以及真实 HTTP 每页一条完整取得四种事件。结果见 `output/test-results/api-default-events/default-contract.xml`。
- 本机启动器 6 passed：原环境优先、只加载所需字段、缺文件沿用普通调用、正常和异常退出恢复、不改写文件、不打印凭证。命令：`uv run --no-sync python -m pytest tests/unit/test_launch_anomaly_api_query.py -q`。
- 专项与综合回归存在交集，以上计数不累加。未重跑全仓库业务套件，也未调整无关用户改动或删除旧回归。
- 旧 OOS 专项现显式指定事件，继续保持原身份、权限、失败和分页断言；默认全选另有真实文件／网络测试。GUI 真实隐藏窗口检查默认“全部”、环境密钥星号填入及完整下载。临时网络服务均在 finally 内退出并验证监听释放，正式服务保留。

## 正式服务实测

2026-11-04 15:39 使用既有 host manager 加载 8890 正式服务；旧进程族退出及原监听释放均验证，新进程就绪，原部署配置字节不变。15:43 完成带密钥验证，密钥仅在内存使用。

| 检查 | 结果 |
|---|---|
| OpenAPI | 1.4.0，event_type 可省略、默认全部适用事件 |
| 无密钥请求 | 业务与 OpenAPI 均 401 |
| M626／SPC／上一完整周 | OOS 0、OOC 0、CPK 1、CPM 1；省略事件返回这 2 项，complete=true |
| M626／SPC 分页 | 每页 1 项共 2 页，结果与不分页完全一致 |
| 零参数全部授权范围 | 2 项，与四类显式查询的记录多重集合及失败集合完全一致 |
| 零参数完整性 | 24 个来源组合缺资料，complete=false；这些组合现在包含更多事件，不解释为无异常 |
| 本机密钥默认填入 | 读取现有项目 `.env`，只注入本次进程，退出恢复环境，未打印或保存密钥 |
| 正式服务 | 原配置／授权保持，8890 监听保留且健康 |

证据：`output/test-results/api-default-events/{deployment,formal-verification}.json`；临时默认事件监听记录为同目录 `listener-*.json`。本轮没有启动数据库生产或维护工作簿。

当前使用方法见 [调用说明](../../../../../references/domain/inline_domain/api/api-caller-guide-2026-11-03.md)、[部署说明](../../../../../references/domain/inline_domain/api/guide-sheet-anomaly-api.md) 和 [程序介绍](../../../../../references/domain/inline_domain/api/architecture-sheet-anomaly-api.md)。

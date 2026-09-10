# H1 / H4 实施与验证记录

日期：2026-09-09。开发分支：`feat/backend-health-and-ports`；目标分支：`master`；对照基线：`ebd466a`。本文件是开发分支交付记录，尚未合并或部署。

## H1：明确失败语义，把数据健康状态传到使用者

完成的行为：

- Yield 数据源故障不再伪装成成功空表；跨月分片任一读取失败，中止发布新的完整窗口。存在旧快照时带 `stale` 回退，否则明确不可用。成功空窗口仍是成功，并可复用快照。
- 统一原生字典健康契约，保留覆盖源时间、最后成功读取时间和安全错误码。旧快照缺少可验证元数据时显示 `unknown`，不补造新鲜时间。
- Q-Time 将健康状态从仓储传过服务、缓存和筛选，直到页面与预警矩阵。缓存只保存原生 payload，在外部重建结果类型，遵守 ADR-0001，也验证了模块热重载后的缓存命中。
- Yield 三个页面、Q-Time 查询页显示健康信息；非 fresh 的结果不能给出当前正常结论。已发现的历史异常仍可展示。Q-Time 刷新失败会降级已展示结果，跨日会话要求重新查询。
- 三厂 Q-Time 聚合保留每厂状态，缺失或陈旧来源不会被当成零异常。当前采用保守整体现状未知规则，可能降低局部产品的绿色覆盖率。
- 数据库创建及本次涉及的 AOI-RS 查询日志使用稳定错误码、异常类型和关联编号，不输出原始驱动消息或 SQL 错误堆栈。

`fresh` 包含有效 TTL 内的快照复用，不表示每次渲染都已实时连接数据库；`partial` 作为契约状态保留，本次整窗口查询不发布部分成功结果。DataFrame attrs 是兼容载体，关键转换处显式提取和传递，不能假设所有 pandas 操作都自动保留它。

## H4：补齐正在使用的出站端口

| 消费方 | 本次端口覆盖 | 生产装配 |
|---|---|---|
| Inline application | Sheet 决策、能力修饰、签名、资源路径及 Throughput 持久化 | composition + 现有 Excel/快照适配器 |
| Yield application | Panel、Array 时间与良率修饰读取 | YieldDataPort + report_data_adapter |
| Equipment application | 基准、快照与刷新 | PartsDataPort + report_data_adapter |

现有调用方式通过受控默认 resolver 兼容，新测试可显式传入内存端口。显式端口调用绕过共享 Streamlit 数据缓存，防止相同业务参数的两个数据源串用结果；默认生产调用保留缓存及清理能力。

新增 AST 架构守卫，覆盖 application 的具体基础设施依赖、Core 出向依赖和跨域私有访问。存量例外精确登记，新增越界和已经迁移却未删除的例外都会失败。该守卫不声称能检测任意动态导入或所有隐式文件读写；Yield Alert/Modifier 等少量历史入口继续渐进迁移。本次不是全仓接口化。

详细契约见[数据健康与出站端口设计](../../../../references/design/feat_design/architecture-data-health-and-outbound-ports.md)。

## 验证证据与限制

广回归在 `output/tmp/backend-health-regression/20260909-204239-319511` 的隔离副本运行，使用原虚拟环境，副本导入路径与资源根经过确认。没有加载生产 `.env`，阻止真实数据库网络及 Excel COM，允许内存 SQLite。明确排除两个真实数据库集成文件：`test_spc_db.py`、`test_equipment_parts_db.py`。

广回归结果为 **1343 passed / 8 failed**，不是全绿：

| 失败分类 | 数量 | 对照与处理 |
|---|---:|---|
| 旧加密工作簿诊断假设不符合当前正常 ZIP 文件 | 3 | `master ebd466a` 同条件复现，未扩大本次修改 |
| Equipment 固定日期前推 4 天假设 | 1 | `master ebd466a` 同条件复现，未修改业务策略 |
| 自动预警页面标题断言 | 3 | 工作区独立标题改动从 Inline 改成超规片；保留该改动，未纳入本次提交 |
| Q-Time 矩阵集成 fake 缺健康元数据 | 1 | 本次契约变化引起，已为成功 fake 显式标注 fresh，并定点复测 |

初次隔离运行还曾因测试临时目录父路径及 `.streamlit` 配置复制不完整失败；修正隔离环境后重新执行，上述正式结果没有这些环境错误。没有将它们计为项目缺陷。

修复后的最终相关隔离复测为 **308 passed / 3 warnings**，涵盖健康状态、端口、缓存、架构守卫、冷启动导入和受影响集成链路；Q-Time 新增的合法空查询两种路径（直接查询、空快照复用）另行隔离验证 **2 passed**。这两次均退出 0，不与广回归重叠计数相加。

精确运行目录与命令保留在 `output/test-results/backend-health-focused-command.json`，输出在 `backend-health-focused-final.log`、`backend-health-empty-qtime-final.log`。汇总记录为 `backend-health-regression-summary.json`。59 个改动 Python 文件语法解析通过，改动文档本地链接检查及 `git diff --check` 通过。

日志保留于 `output/test-results/backend-health-offline-regression-final.log`、`backend-health-master-integration-baseline.log` 及同目录 baseline 日志。对照失败不应当被解释为已经具备可重复的全绿发布基线。

浏览器使用隔离 `tests/e2e/fixtures/qtime_app.py` 和新增 `tests/e2e/backend_health.js`：验证旧数据、空旧数据、正常图表，1440 / 768 / 375 像素宽度无横向溢出，未捕获页面异常。截图保留在 `output/test-results/backend-health-*.png`，已目视检查手机及空旧数据场景。没有既有视觉基线，因此不宣称像素回归通过。

Standards 和 Spec 两轴复审均无剩余阻断。复审发现的端口缓存串用、首次导入循环、过期数据绿色结论、刷新失败旧会话以及跨日会话问题均已修复，并有相应回归。环境未安装 pyright/ruff，本次不宣称完成类型检查；不修改虚拟环境来补装工具。

未执行真实数据库连通、生产资源写入验收、干净环境安装或真实部署。工作区并发变化的工作簿、备份、签名和 Excel 临时文件保留原状、排除提交，不将其变化归因于本次代码交付。

## H5 及暂缓范围

[个人开发者测试与发布检查解释](solo-developer-testing-and-release-explained.md)说明现有验收价值、具体缺口、失败后果、低成本测试方法和建议命令。本轮 H5 仅文档，没有新增发布平台或修改依赖锁定方案。

H2 的原子写入、版本冲突、审计及 H3 的认证授权均按用户决定暂缓。本次不改变这些能力。

## 待合并后落地的 ADR 草案

状态：Proposed。主题：应用出站端口与显式数据健康契约。

背景：DataFrame 空结果无法单独区分业务无数据与源失败；默认静态服务直接依赖仓储，难以替换并隔离缓存。

决策：应用拥有实际消费的出站协议，组合根选择基础设施；既有入口保留单一兼容 resolver。健康信息采用原生字典，穿过缓存和展示边界；完整窗口任一分片失败时不发布部分新事实。未知/陈旧来源不得支持当前正常结论。显式端口调用不共享生产数据缓存。

取舍：保留现有 DataFrame 和调用接口可控制迁移规模；attrs 需要显式传递，resolver 是受约束例外。三厂聚合采取保守未知判断，牺牲部分绿色结果覆盖以避免误报正常。暂不引入事件总线、分布式事务或全量重写。

验证：对应本记录中的仓储、服务、缓存、预警、架构与浏览器证据。获准合并后再按仓库 ADR 编号和索引规则转为正式记录；本草案不宣称已被接受。

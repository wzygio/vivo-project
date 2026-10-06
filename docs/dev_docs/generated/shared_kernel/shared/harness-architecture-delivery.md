# 架构规则与资源迁移交付记录

核验日期：2026-10-06。范围：[任务说明](../../../dev_spec/shared_kernel/shared/refactor-harness_arch.md)。本记录说明已实施结果；当前规则由根文档和 [ADR-0033](../../../../ADR/0033-domain-submodule-artifacts-and-global-resources.md) 持有。

## 组织与路由

开发说明、生成评估、领域知识、设计和资源均按 `domain/submodule` 归属组织。
共清点 195 个资产，执行 170 次移动；文件名沿用原名。Inline 包含
`aoi_rs/aoi_tt/ctq/spc/monitor/shared`；Indicator 使用 `qtime/ijp/ijp_hole`；
IQC 使用 `eva_materials/lifetime`；Yield 使用 `mapping/mwd_trend/sheet_lot`。
域内公共资料放 `shared`，项目级资料放 `shared_kernel/shared`。
设备资产归 `equipment_domain/parts`，设备源码仍保持实际已有的包结构。

- [ARCHITECTURE.md](../../../../../ARCHITECTURE.md#domain-submodule-architecture) 是能力清单与目录语法的权威入口，按域和子模块缩小检索范围。
- [CONTEXT.md](../../../../../CONTEXT.md#important-routes) 合并重要路由与产物生命周期，并持有全局资源配置要求、渐进阅读和文档职责。
- [AGENTS.md](../../../../../AGENTS.md#iteration-router) 将功能变化路由到对应 references，架构变化路由到 ARCHITECTURE。
- [references/index.md](../../../../../references/index.md) 更新迁移路径和缺失的业务入口；文件命名规则保持不变。
- CONTEXT 仍承载展示、时间、数据库、缓存等独有约束，因此保留。HARNESS 的有效维护规则已转入 CONTEXT，重复入口已退休。旧评估注明历史适用范围。

`data` 只检查，没有迁移或刷新。其原有报告、历史和快照生命周期目录保持原位。
迁移清单及原始哈希见 [.planning 中的 manifest](../../../../../.planning/2026-10-06-harness-architecture/migration_manifest.json)。

## 全局资源位置

[config/global.yaml](../../../../../config/global.yaml) 注册 119 条完整文件路径和两个可上传集合目录。
119 条覆盖全部 116 个实际资源，另有 3 个未提供的可选输入。Domain YAML 保留业务策略，
产品 YAML 保留 Sheet 选择；它们不再维护另一套资源位置。

`ConfigLoader` 支持项目相对路径和外部绝对路径，Yield 配置构造时注入完整资源描述。
Inline OOS/OOC、AOI、SPC、CPK、矩阵缓存、IQC 示例读取、设备命令以及资料页面均使用配置路径。
读写、上传下载和缓存签名保留独立目录及配置文件名；显式注入的路径与端口继续隔离正式资源。
普通资料页面的失败提示不回传技术异常或路径，下载名称仅使用文件名。

审查发现并关闭两项问题：

1. 损坏或缺失的 global YAML 现在停止资源解析；有效旧隔离配置缺少 resources 段才使用兼容分支。
2. SPC OOS 的旧 `Delete` 决策持久化按明确的 OOS/OOC 类型判断，工作簿改名后仍归为 `False`；OOC 保留 `Delete`，人工决策页保持原样。

备份、签名和历史提取副本保留。116 个资源及 99 个 data 文件逐一与迁移前 SHA256 匹配，
包括用户原先修改的 SPC 工作簿和备份；没有用 Git 基线覆盖它们。

## 验证

最终组合测试：**470 passed**，耗时 61.59 秒，30 条现有 pandas FutureWarning。测试覆盖：

- `tests/architecture`：依赖及修饰边界。
- Inline application shared/AOI TT/AOI RS/SPC/monitor，以及对应矩阵、CPK 和报表 sections。
- 新资源注册表、改名/外部工作簿、独立 OOS/OOC 位置、缓存签名和监控注入隔离。
- Yield 修饰表装配与上传 Sheet、设备 CLI/配置/离线快照及基线读取。
- 临时工作簿下的 Sheet OOS 刷新、预警矩阵，以及默认中午截止和各仓储的截止边界。

执行前缀为 `.venv\Scripts\python.exe -m pytest -q`。完整组合命令及最终结果保存在
[验证命令记录](../../../../../.planning/2026-10-06-harness-architecture/verification.md)。
此外检查全部 195 个迁移目标、五棵目录的归属、资源注册覆盖、57 个变动 Python 文件的语法、有效 Markdown 链接/锚点及 `git diff --check`，均通过。

测试环境调整：设备覆盖测试显式采用全日报表策略，避免下午运行时与已建立的中午截止断言冲突；
生产策略未改变，默认截止有独立测试。旧 IQC 测试改用路径注入；Inline 隔离测试提供临时全局配置。

## 已知边界

QTime 人工决策工作簿、IQC inspection/lifetime demo JSON 仍未提供，配置保留但不生成虚构文件。
当前正式 IQC 报表通过数据库适配器读取，示例读取不用于填补正式数据。

未迁移的 `docs/project_files/inline_domain` 历史 SQL 资料有 5 个原本缺失的 long_text 附件链接；
保留原资料，未制造附件。此次迁移和修改的根文档、知识、开发说明、评估及 ADR 导航无断链。

没有查询生产数据库、刷新生产快照或运行真实 Office 转换；未做浏览器交互回归。
离线测试和静态页面检查验证配置链路，不能替代真实数据库/Office 运行证据。

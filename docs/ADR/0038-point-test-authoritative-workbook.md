# ADR-0038: 密集点测试的工作簿保真报表边界

- Status: Accepted
- Date: 2026-11-03
- Authorization: 用户要求 development-flow 全流程并指定所有选项采用默认推荐方案。
- Related: ADR-0021（Indicator 同级子模块）、ADR-0033（资源注册与子模块归属）

## Context

用户提供的密集点测试工作簿，首张 detail 只有调整后 6 张大板的 936 条测点；summary 则含调整前、调整后、修正后三类共 216 条膜位记录。调整前的大板不在 detail 中，且 detail 没有显式膜位映射。summary 的同一大板/膜位在不同阶段有独立数值，因此从 detail 重算或套用实时报告时间过滤都会改变用户维护的内容。

该工作簿是企业加密格式；磁盘转换结果可能被客户端重新加密。现有通用单表 reader 在 pandas 路径会改写重复表头，不能恢复此汇总的原始布局。需要在不改变全局 reader 默认行为、不修改源文件的条件下读取完整块。

## Decision

1. Indicator 新增同级 point_test 能力，application 消费其拥有的工作簿读取端口，core 负责纯布局解析/验证，infrastructure 负责文件读取；composition 是配置路径与 adapter 的组装位置。Inline指标只是门户归类，不改变后端领域所有权。
2. 产品以 `<product>-detail` / `<product>-summary` 配对；展示与原生导出在前端传输前按全局 enabled_products 过滤并保留配置顺序，标签复用公共注释格式。停用产品选择即时清理。
3. summary 作为权威维护结果，保留阶段、大板、膜位及五项统计值；不从 detail 重算、不补造调整前明细，也不推断 SITE_NAME 到膜位关系。三阶段是用户明确要求的业务分类，不由管理员模式决定。
4. 该静态测试报表保留来源时间和值，不应用实时 Indicator 查询的日期前推、当日截止或日期排除。这一例外仅属于 point_test 文件报告，Q-Time/IJP 的既有政策不变。
5. 标准文件按原始行解析；加密文件仅操作 output 内临时副本，复用 fr_file_decryption 的 excel-values 读取，立即捕获字节并清理。调用方持有外层 COM apartment，内部代理返回释放后才卸载，避免退出诊断。缓存只保存原生容器，按完整路径和文件代次区分，并受 TTL/容量与手动清理约束。

## Alternatives Considered

- 从 detail 计算各阶段：规则集中，但缺少调整前事实与膜位映射，无法产生与来源一致的三阶段结果，拒绝采用。
- 复用实时测量/日期投影链路：统一现有策略，但会让静态明细与维护汇总失配，并引入不需要的查询生命周期，拒绝采用。
- 通用单表 reader 后拼回第一行：复用 I/O，但 pandas 会为重复表头增加后缀，跨标准/加密路径的原始布局不一致，未采用。
- 将明文转换长期保存为资源：首读后更快，但会产生第二来源、配置和失效责任，而且企业客户端可能重新加密，未采用。

## Consequences and Constraints

报表可以追溯到用户维护值，新产品通过新增同结构配对进入来源，但仍受全局启用范围约束。领域层不依赖 Excel 或 Streamlit，最高测试边界是应用服务及实际 presentation section。

代价是不能承诺根据新明细自动更新汇总，且加密读取需要 Windows Office 与已有解密包。新布局、统计重算或数据库来源应另立需求，不在 reader 中静默猜测。损坏、缺配对与缺指标是读取失败；不拿旧缓存伪装成成功报表。

## Verification and Traceability

- [需求与默认计划](../../.scratch/inline-point-test/PRD.md)、[完成票](../../.scratch/inline-point-test/issues/01-point-test-report.md)
- [业务规则](../../references/domain/indicator_domain/point_test/rules-point-test-report.md)、[数据分析](../dev_docs/generated/indicator_domain/point_test/data-analysis-2026-11-03.md)
- [应用服务](../../src/indicator_domain/application/point_test/service.py)、[纯解析](../../src/indicator_domain/core/point_test/report.py)、[adapter](../../src/indicator_domain/infrastructure/point_test/workbook.py)、[前端](../../app/sections/indicator_domain/point_test/dashboard.py)
- [应用测试](../../tests/unit/indicator_domain/point_test/test_report.py)、[工作簿集成测试](../../tests/integration/indicator_domain/test_point_test_workbook.py)、[AppTest](../../tests/e2e/test_point_test_apptest.py)、[浏览器入口](../../tests/e2e/run_point_test_browser.py)
- [交付及评审证据](../dev_docs/generated/indicator_domain/point_test/delivery-2026-11-03.md)：新增能力覆盖率 96%，32 项针对性检查通过，真实文件/浏览器通过；全库 15 个失败全部在改动前基线复现。
- 实现提交 `a948c641` 已快进交付到本地 master；合并后再次执行针对性检查，32 passed、1 live skipped。

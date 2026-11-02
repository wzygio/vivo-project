# 24项基线失败处理规格与结论

Status: implementation-complete-validation-pending
Source: 用户2026-11-02明确要求：对旧业务逻辑测试删除，否则修复。此授权覆盖原14张问题卡的判断与处理，不授权删除仍有价值的边界或回滚其它用户改动。
Base:9c616182；branch:fix/baseline-regressions-20261102。

## 契约

按每个原始pytest Item判断。失败、老旧或当前不绿都不是删除依据；当前CONTEXT、领域规则及公开调用者提供依据。24个原始节点由既有failure-comparison.remaining_nodes固定。22个保留并修复，2个退役后以当前接口替代；不删除任何整个测试文件。

只允许一个已有实际业务缺陷修复：全厂备件预警空产品范围不应请求单产品缓存revision。其余为测试配置、有效TT身份、缺失字段、严格注册表、数据端口及当前输出契约修复。不得更改生产跨度配置、放宽注册表、移除日期排除或强行释放数据。

## 退役及替代

- test_global_resource_registry::test_all_existing_resource_files_have_a_global_registry_entry：旧全目录逐文件规则与当前目录集合／备份伴随文件规则冲突。替代为隔离目录集合验证，保留同文件未注册key拒绝、文件路径及运行配置等其余覆盖。
- test_yield_alert_health::test_yield_dashboard_passes_all_displayed_source_health_to_alert_center：只检查页面AST对旧函数的调用。现行页面消费已发布结果；替代为真实临时发布文件到现行预警中心的fresh/stale/unknown/partial四场景，已有真实页面发布时序测试继续保留，旧通用组件其它测试保留。

## 验证及交付

全部变更通过既有公开函数、真实临时文件和Streamlit AppTest观察。guard禁止DB／Office／维护目录写入。先验证旧22与替代，再文件／域范围，再完整unit+architecture+本地API文件集成；与固定起点collection比较每项移除和新增。不得以skip／xfail或扩大容忍值隐藏失败。

保留并行Yield改动、真实资源和正式服务。目标交付原工作区master，不重启8890或8503。最终审计报告、14卡状态、评审与完整验证是完成证据。

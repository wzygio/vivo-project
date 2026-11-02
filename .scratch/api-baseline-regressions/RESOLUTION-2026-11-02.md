# 24项基线失败处理规格与结论

Status: implementation-and-validation-complete
Source: 用户2026-11-02明确要求：对旧业务逻辑测试删除，否则修复。此授权覆盖原14张问题卡的判断与处理，不授权删除仍有价值的边界或回滚其它用户改动。
Base:9c616182；branch:fix/baseline-regressions-20261102。

## 契约

按每个原始pytest Item判断。失败、老旧或当前不绿都不是删除依据；当前CONTEXT、领域规则及公开调用者提供依据。24个原始节点由既有failure-comparison.remaining_nodes固定。23个保留并修复，1个退役后以当前接口替代；不删除任何整个测试文件。

已有实际业务缺陷修复：全厂备件预警空产品范围不应请求单产品缓存revision。另补登记resources/report历史资料集合，并保留真实资源归属检查；其它为测试配置、有效TT身份、缺失字段、严格注册表、数据端口及当前输出契约修复。不得更改生产跨度配置、放宽注册表、移除日期排除或强行释放数据。

## 退役及替代

- test_global_resource_registry::test_all_existing_resource_files_have_a_global_registry_entry：经Spec复核改为保留修复。真实资源库存审计仍有价值；纠正“集合成员和备份均需单独file key”的旧假设，识别已注册集合、锚定已注册主文件的伴随文件及明确空临时标记。四个反例保证未注册正式输入仍会失败。
- test_yield_alert_health::test_yield_dashboard_passes_all_displayed_source_health_to_alert_center：只检查页面AST对旧函数的调用。现行页面消费已发布结果；替代为真实临时发布文件到现行预警中心的fresh/stale/unknown/partial四场景，已有真实页面发布时序测试继续保留，旧通用组件其它测试保留。

## 验证及交付

全部变更通过既有公开函数、真实临时文件和Streamlit AppTest观察。guard禁止DB／Office／维护目录写入。先验证旧22与替代，再文件／域范围，再完整unit+architecture+本地API文件集成；与固定起点collection比较每项移除和新增。不得以skip／xfail或扩大容忍值隐藏失败。

保留并行Yield改动、真实资源和正式服务。目标交付原工作区master，不重启8890或8503。最终审计报告、14卡状态、评审与完整验证是完成证据。


Spec复核后的最终决定：退役仅1项旧Yield页面AST。资源库存审计保留原节点名，并新增真实库存／负例保护；不依靠删除库存约束让测试变绿。两个8位十六进制名的0字节占位文件保留原位，只有该空临时模式被识别为非维护输入，非空或有扩展名的正式输入仍必须归属。


??????2856???0???collection2845->2856???AST???12??????0???????????????????????????

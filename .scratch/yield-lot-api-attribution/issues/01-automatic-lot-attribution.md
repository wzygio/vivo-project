# 01: 自动发布Yield Lot厂别并提供NULL层别API

Status: implemented-verified
Blocked by: None

What to build: 用户需求2.1的完整路径：Lot/Code→defect_group→factory写入发布结果，API无需Yield归属表即可按授权返回Lot，站点和层别均NULL。

- [x] 六类分组由global.yaml配置，原Lot判据／日期／不良率不变，未知分组不丢异常。
- [x] 实际发布文件包含分组和厂别，人工False决定及历史跨再发布保留。
- [x] HTTP直接使用发布厂别，Yield不访问字典或归属；层别和站点NULL。
- [x] 厂别授权／过滤有效，旧已释放记录缺厂别503；其它来源仍拒绝缺层别。
- [x] 站点模板／同步不再要求Yield归属，保留已有工作表和维护资料。
- [x] 专项235通过／92.40%，双轴评审无阻断；类型检查3项既有诊断与原文件同语句一致，无新增。完整回归剩余为已登记24项基线，缺参考文件的额外1项复验通过；不宣称全套通过。

Evidence: docs/dev_docs/generated/inline_domain/monitor/api-yield-lot-optimization-2026-11-02.md. 98b2b957 implementation,3fb2d69e contained host recovery. No production workbook rewrite or new credentials.

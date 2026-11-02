# 01: 自动发布Yield Lot厂别并提供NULL层别API

Status: ready-for-agent
Blocked by: None

What to build: 用户需求2.1的完整路径：Lot/Code→defect_group→factory写入发布结果，API无需Yield归属表即可按授权返回Lot，站点和层别均NULL。

- [ ] 六类分组正确映射，原Lot判据／日期／不良率不变，未知分组不丢异常。
- [ ] 实际发布文件包含分组和厂别，人工False决定及历史跨再发布保留。
- [ ] HTTP直接使用发布厂别，Yield不访问字典或归属；层别和站点NULL。
- [ ] 厂别授权／过滤有效，旧已释放记录缺厂别503；其它来源仍拒绝缺层别。
- [ ] 站点模板／同步不再要求Yield归属，保留已有工作表和维护资料。
- [ ] 针对性回归、类型检查及双轴评审通过；知识说明与ADR修订可追踪。

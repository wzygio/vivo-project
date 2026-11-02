# 04: 集成、安全评审与使用说明
Status: implemented-verified
Blocked by: 01,02,03

- [x] 相关回归、安全及Standards/Spec评审通过；完整回归的24项已复现基线失败按用户确认独立跟踪，不标记为通过。
- [x] 当前政策/授权变更使旧游标失效，临时服务隔离/清理有证据。
- [x] 依赖、配置模板、启动/同步/人工维护命令、领域使用说明完整。
- [x] 架构路由、正式ADR-0037、实现800ffbc2及收尾修复1e22d029可追踪；用户已确认并完成合并，最终127项相关验证通过。

Evidence: focused267passed,92.37%coverage; wholeunit/architecture2708passed/24failed, exactsame24reproduced withbaseline source. Both review findings closed. User approved independent baseline tracking, merge and ADR on2026-11-02. Master contains800ffbc2 and finalfix1e22d029;127relatedchecks passed.14 needs-triage cards cover all24baseline nodes; no baseline fix implemented.

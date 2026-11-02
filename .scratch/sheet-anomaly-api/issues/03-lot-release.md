# 03: Q-Time/Yield人工释放到Lot API
Status: implemented-verified
Blocked by: 01,02

- [x] Q-Time保留False发布证据、原台账与失效规则；站点及层别采用f_step。
- [x] Yield持久人工flag默认True，False释放；原子发布保留历史和其他产品。
- [x] 旧无Flag结果不自动放行；提供显式维护/重发路径。
- [x] Yield厂别必需、站点可NULL，其余必要字段缺失503；不猜测归属。
- [x] 原有有效性/覆盖、无SQL及只读字节断言通过。

Evidence: API file/HTTP integration, domain release/metadata tests and targeted126 checks passed; finalproject regression and merge tracked by04.

# Findings
- 当前7个单产品header页面：SPC/CTQ/AOI_RS/AOI_TT及Yield分析/ByLot/BySheet；均有product_cache_scope但共用全产品revision，跨指标失效。
- 多产品矩阵、Q-Time、IJP、备件、IQC已隐藏产品header；专项资料页面需确认实际无header调用，不强加产品控件。
- ADR0022旧决策拒绝CPK Excel及规定整体缓存，与本次明确批准方向相反；以新需求替代相关旧决策，不改业务判据。
- 矩阵CPK→SpcReportService，矩阵整体_cache需移除；ExcelAlarmReader已用于4个OOS灯。
- 实施后：CPK状态/详情均Excel-only；每cell独立cache，源码签名只跟踪本指标依赖；Q-Time使用实际参考日共享shop入口。
- Standards review发现Yield/SPC图像memo仍用旧版本；已迁移并以真实RenderGate计数器回归验证。Spec复审无阻塞。
- 浏览器下拉菜单存在渲染时序；E2E使用可见option等待/有限重试，通过实际选择和计数器确认定向刷新。
- 产品Workbook为共享物理文件，编辑文件可能使本指标多个产品重新读取；这不同于手动revision跨产品泄漏。

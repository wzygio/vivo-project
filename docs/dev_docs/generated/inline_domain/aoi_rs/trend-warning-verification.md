# AOI_RS 趋势预警交付证据

日期：2026-10-19。范围：用户确认后的昨日双规则趋势预警；development-flow `general` 模式，单个端到端工单。

## 需求与交付

- [规格及计划](../../../dev_spec/inline_domain/aoi_rs/trend-warning-proposal.md)：按用户最新指令修订，以昨天为目标日，原双比较命名为“当日超规”，新增连续两个相邻日均严格超过 1.2 倍的规则，OR 合并。
- 工单：`.scratch/aoi-rs-trend-alert/issues/01-trend-alert.md`。
- [领域规则](../../../../../references/domain/inline_domain/aoi_rs/rules-aoi-rs-trend-alerts.md)：最终公式、有效日、时间窗、缓存、普通展示与独立自动图像。
- 沿用 ADR-0001 的原生缓存边界、ADR-0007 的密度口径和 ADR-0017 的结构化预警展示。未引入新的跨域依赖、数据源或架构边界；局部业务规则维护在领域文档，未新增 ADR。

## 自动验证

相关回归 **281 passed，25 warnings**。警告来自既有 pandas 类型/空帧拼接路径；无失败。

```powershell
.venv/Scripts/python.exe -m pytest tests/unit/inline_domain/core/aoi_rs tests/unit/inline_domain/application/aoi_rs/test_aoi_rs_service.py tests/unit/inline_domain/infrastructure/aoi_rs tests/unit/app/sections/aoi_rs/test_aoi_rs_dashboard.py tests/unit/app/sections/aoi_rs/test_aoi_rs_export.py tests/unit/app/sections/aoi_rs/test_aoi_rs_sheet_oos_alerts.py tests/unit/app/sections/aoi_rs/test_aoi_rs_trend_warning_ui.py tests/unit/app/pages/test_aoi_rs_page.py tests/unit/test_aoi_rs_config.py tests/architecture tests/unit/inline_domain/application/test_payload_cache_ttl.py tests/unit/app/components/test_hot_reload.py tests/unit/app/components/test_indicator_product_cache.py tests/unit/app/components/test_page_header_product_cache.py -q --tb=short --disable-warnings
```

覆盖：两个规则单独/同时/均未命中、严格 20% 边界与分数舍入、自然日缺测与有效零值、跨年、零分母/无效值、组间隔离、T−6 保留、当日月基准与图值一致、产品/决策 revision 与跨天缓存、查询门控、现有单片异常及 PDF 回归、应用依赖与缓存架构守卫。

规则和服务字段的首次测试均在实现前观察到失败，再实现转绿。评审发现的浮点边界也先增加失败回归再修复。

## 双轴评审

- Standards：0 项发现。
- Spec：发现 1 项 P2 浮点边界误报，已关闭。密度 `1/6 → 1/5` 正好 20%，使用统一相对容差消除二进制舍入误报；增加两例回归，复核无新增问题。

## UI 验证与清理

使用 `tests/e2e/fixtures/aoi_rs_trend_app.py`，无数据库、无维护工作簿写入，普通展示模式。

- 有警时只有 A1PPS 的 3 张图；稳定 B1 不出图。
- 表格及列菜单只有 12 项业务列；图表 hover 模板不含内部标记或源值比较。
- 无警、数据不足、加载失败三条路径均确认无残留预警图像。
- 桌面无页面横向溢出；390×844 视口无页面横向溢出，表格在自身容器内滚动。
- 浏览器会话已关闭。临时服务端口 18519，启动前检查空闲；用 Windows Job Object 管理本次进程树，覆盖控制器异常退出时的子进程终止。结束后核验全部已记录进程退出、监听释放。

机器证据与截图位于 `output/test-results/aoi_rs_trend_*`，包括 `browser_audit.json`、`ui_lifecycle.json`、desktop/mobile、列菜单及三种状态截图。临时脚本位于 `output/tmp/`，未纳入版本库。

## 验证边界

未连接生产数据库，未测试生产数据规模性能，也未运行整个仓库所有领域的完整测试。回归范围为本次 AOI_RS 能力、相关缓存及架构守卫。工作区存在其他正在进行的周报和门户等改动；保留其文件及状态，不纳入本次提交。

开发分支：`codex/aoi-rs-trend-alert`。本次只创建本地提交，未推送或合并其他分支。

## 2026-10-20：五天正值均值与七天内跨日比较

用户最终指令取消将均值改成七天：保留 `[T−6,T−2]` 五个自然日，仅平均正密度；相邻比较从 `[T−7,T)` 取最近两条有效记录 P/Q，允许跨日，真实零值仍可作为比较记录。T 继续固定为昨天。P/Q 共用同一下界，不逐次延长回溯。

- Core 增加实际比较日期；应用取得 `[T−7,T]` 的完整八天日轴，主图/PDF 仍使用原紧凑日期轴，月基准截止保持一致。
- 页面显示“上次有效日”“再前有效日”“基线5日均值”，共有 14 项业务列。策略版本升级使旧评估缓存失效。
- 领域规则第 8 节包含完整实际 Core 程序，工具已确认代码段与源文件精确一致，并完成 Python 语法与文档链接检查。
- 相关 AOI_RS、架构及 payload TTL 回归：**246 passed，25 warnings**；新增跨日 UI 和 14 列白名单断言单独复验：**6 passed**。没有生产数据库查询。
- Standards / Spec 增量双轴评审均无阻断发现。
- 真实浏览器验证：目标 10月18日，P=10月15日、Q=10月12日，仅命中 Code 的 3 张图；列菜单不含内部字段；无警、数据不足、失败状态无残留图像；390px 窄屏无页面横向溢出。
- 证据：`output/test-results/aoi_rs_trend_sparse_*`。浏览器已关闭；Windows Job Object 管理的本次服务进程树已退出，**18529 端口释放已核实**。
- 用户并行周报、门户及其他修改保留，不纳入本次增量提交。

本轮回归命令：

```powershell
.venv/Scripts/python.exe -m pytest tests/unit/inline_domain/core/aoi_rs tests/unit/inline_domain/application/aoi_rs/test_aoi_rs_service.py tests/unit/inline_domain/infrastructure/aoi_rs tests/unit/app/sections/aoi_rs/test_aoi_rs_dashboard.py tests/unit/app/sections/aoi_rs/test_aoi_rs_export.py tests/unit/app/sections/aoi_rs/test_aoi_rs_sheet_oos_alerts.py tests/unit/app/sections/aoi_rs/test_aoi_rs_trend_warning_ui.py tests/unit/app/pages/test_aoi_rs_page.py tests/unit/test_aoi_rs_config.py tests/architecture tests/unit/inline_domain/application/test_payload_cache_ttl.py -q --tb=short --disable-warnings
```

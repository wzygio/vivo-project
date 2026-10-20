# AOI_TT 趋势预警验证

日期：2026-10-20。需求入口：用户要求将已确认的 AOI_RS 趋势预警复刻到 AOI_TT。执行模式 general，规格与任务状态位于本地 `.scratch/aoi-tt-trend-alert/PRD.md`。

## 交付范围

共用 RS/TT 纯密度判定与精简展示中心；TT 独立保留测量片分母和四键粒径序列。固定昨日、七天跨日比较、五天正值均值、两规则 OR 及严格 1.2 倍边界沿用已确认行为。页面自动显示趋势区，不依赖手动查询，自动绘制命中粒径三图。

本次延续 ADR-0001 原生缓存边界、ADR-0008 TT 分母、ADR-0016/0017 共享展示与自动图像、ADR-0024/0025 粒径聚合。未引入新的跨层依赖、运行库或存储边界；规则与局部复用实现由领域文档及 ARCHITECTURE 维护，不为相同既有决定新建 ADR。

## 自动化验证

执行 `.venv/Scripts/python.exe -m pytest`，限定以下范围，带 `-q --tb=short --disable-warnings`，最终 **340 passed，25 warnings**：

- `tests/unit/inline_domain/core/aoi_rs`、`core/aoi_tt`。
- `tests/unit/inline_domain/application/aoi_rs/test_aoi_rs_service.py`、`application/aoi_tt`。
- `tests/unit/inline_domain/infrastructure/aoi_rs`、`infrastructure/aoi_tt`。
- `tests/unit/app/sections/aoi_rs/` 下 dashboard、export、sheet_oos_alerts、trend_warning_ui 四组。
- `tests/unit/app/sections/aoi_tt`；`tests/unit/app/pages/test_aoi_rs_page.py`、`test_aoi_tt_page.py`。
- `tests/architecture`、`tests/unit/inline_domain/application/test_payload_cache_ttl.py`。

核心覆盖保留原 RS 的日期、缺测、零值、浮点阈值及回溯边界用例；TT 补充粒径独立、缓存日期/revision/决策失效、查询门控、十二列白名单、三个区域图表 ID 唯一性，以及 TT 片集合不一致时的分母一致性。

## UI 验证

仅启动 `tests/e2e/fixtures/aoi_tt_trend_app.py` 无数据库夹具，临时端口 18549。浏览器验证有预警、跨日预警、无预警、数据不足、加载失败五种状态；当日/T-1/T-2 标签、精简表格、无解释横幅、命中粒径筛选通过。桌面 1440×1000、手机 390×844 无整页横向溢出；同指标在趋势/手动/单片异常区共存十二图，无异常。

证据位于 `output/test-results/aoi_tt_trend_browser_audit.json`、`aoi_tt_trend_desktop.png`、`aoi_tt_trend_mobile.png`。生命周期记录 `aoi_tt_trend_ui_lifecycle.json` 标记 cleaned，已核实本次进程树退出和端口释放。浏览器自动化通过 agent-browser 执行；状态交互使用已观察 DOM 的原生点击。

## 双轴评审

- Standards：发现并关闭一项 P2，原自动图先筛选 TT 后聚合会缩小站点分母。改为完整报表明细聚合、命中键仅控制输出，复核无新增问题。
- Spec：独立发现同一 P2；新增 DSUM_L 每天数量 10、同站点其他 TT 额外 3/2/1 片的用例，表格与自动图均为 2.5、3.333、5，且只输出命中组。复核关闭。

## 边界

未查询生产数据库，未通过真实产品数据验证，不更改正式服务。测试使用临时工作簿路径隔离。工作区已有周报、PPA 及其他并行修改，本次仅提交趋势相关改动；回归数字反映运行时工作区状态，不代表全仓套件。

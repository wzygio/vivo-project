# Progress — AOI_TT 报表

## 2026-08-10 Session 1

- development-flow 模块 1 完成：issue `.scratch/aoi-tt-report/issues/01-create-aoi-tt-report.md`（ready-for-agent）。
- 模块 2 完成：本 plan 创建；镜像 AOI_RS 五层布局；用户经任务文档预先授权全流程。
- 数据源探查（3 轮，前置完成）：
  - 主探查 `.scratch/probe_aoi_tt.py` → 三表结构、step×param 分布、规格表结构；
  - 补充 1 `.scratch/probe_aoi_tt_supp.py` → TT 识别规则（规格表 `param_type IS NULL` 72 行全为 TT 参数，与测量表 `%SUM%` 交叉一致）；
  - 补充 2 `.scratch/probe_aoi_tt_supp2.py` → 过货视图无 AOI 站点（xx620/21320/43620）记录；TDSUM/DSUM 每片必测（distinct sheet == 行数），分母改用测量表自身 distinct sheet。
  - 结论固化：`references/domain/aoi_tt/spec-data_source.md`。
- Phase 0：回归基线 `pytest tests/unit/inline_domain -q` → **72 passed**（2026-08-10）。
- Phase 1-3 源码完成：
  - `src/inline_domain/infrastructure/aoi_tt/data_loader.py`（AoiTtQueryConfig + load_tt_param_set/load_tt_details/load_tt_spec_limits；三厂 UNION ALL + 字典 join + (step,param) 组合过滤）；
  - `src/inline_domain/core/aoi_tt/aoi_tt_calculator.py`（趋势=Σtt_qty÷检测片数、throughput 0 填充、By Lot/Sheet、attach usl/ucl 按 step_id+tt_name）；
  - `src/inline_domain/application/aoi_tt/aoi_tt_service.py`（缓存 payload→ViewModel，ADR-0001）；
  - `app/sections/aoi_tt/aoi_tt_dashboard.py`（筛选级联+查询门控+三图，USL 红虚线/UCL 橙点线）；
  - `app/pages/AOI_TT监控报表.py`（固定窗口，签名 aoi_tt_report_v1）；
  - `resources/static/config.js` 注册 AOI_TT_REPORT（SPC监控 分组两处）。
  - 导入冒烟 + 计算器/图表手工验证通过（trend 6 traces、lot 3 traces，比值/规格数值正确）。
- E2E（playwright-cli + Streamlit 8503，`tests/e2e/aoi_tt_report.js`）：
  - 第 1 次失败：站点选择后未等 Code 自动全选即查按钮状态（竞态），且首个 option 为"全选"伪选项；
  - 修复：显式选 11620 + 等待 `Selected . Code名称` combobox + waitForFunction 等按钮可用；
  - 复跑通过：截图 `output/screenshots/aoi_tt_e2e.png`（M626/ARRAY/11620/TDSUM 三图，USL=618/UCL=468 线可见）。
- 单元测试：31 项新增一次全绿（DAO sqlite ATTACH 契约 5 + 计算器 10 + 服务 3 + Dashboard 10 + 页面 2），零源码修复。
  - 回归：`tests/unit/inline_domain + tests/unit/app` → **171 passed**（基线 72 无回归）；
  - 全量：`tests/unit`（排除既有收集错误 test_shadow_ema）→ **319 passed / 5 failed**，5 个为既有跨域失败（code_selector×2、compliance_xlsx×1、yield_global_data_policy×2），与 RS 交付登记一致，无新增。
- 模块 4 完成：ADR-0008 `docs/ADR/0008-aoi-tt-param-identification-and-denominator.md`（TT 识别规则 + 分母口径）。
- issue 验收全勾选，交付证据已追加。**development-flow 四阶段门全部通过，任务关闭。**

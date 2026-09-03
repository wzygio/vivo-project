# Findings: alert-matrix-board

## 调研结论（2026-09-02，来自两个 explore 子代理，详见可行性报告）

### 全产品管线已存在

- `app/pages/自动预警看板.py:138-153` 固定 `prod_code="ALL"`；`monitor_service.py:281` `fetch_dashboard_data_dict`（`max_entries=1`）逐产品循环（:311-314 `discover_monitor_products`）。
- 缓存键先例：`自动预警看板.py:107-117` 预算 7 产品 × (spc, ctq) 的 revision + 决策签名。
- 页面忽略 Header 产品筛选，自带控制台前端切片（`monitor_dashboard.py:44-69`, `filter_and_rollup_monitor_data:649`）。

### 数据规模

- inline 量测 ~246 万行 / ~30MB parquet（M626 490K、M673 185K、M678 614K、Z517 304K、Z553 5K、Z571 858K、Z576 0.7K）。
- yield 快照 ~354 万行（Z571 986K + M678 995K 最大）。aoi_rs ~43 万行。
- inline 修饰工作簿 ≤ 400K（`resources/inline_domain/`）。qtime 无本地文件，纯库查询。
- 启用产品 7 个：`config/global.yaml:28-35`。

### 卡点与风险（→ PRD §4/§6 对策）

- `decorated_features.py:79` `max_entries=12` < 矩阵所需 14~21 条目 → 调 32。
- qtime 零缓存直查生产库；服务层 `products: tuple = ()` 空即全产品（`service.py:60`、`repository.py:116-118`）；ADR-0019 有权限/超时前科 → 补缓存层。
- `monitor_service.py:655-712` `safe_refresh_snapshots` 单产品失败阻断整体 → 单元格级降级。
- 决策签名预算 IO：7 产品 × 多 scope stat 工作簿（`decision_signature.py:35-41`），企业加密 xlsx 有 COM 回退（`sheet_oos_decoration.py:217-233`）→ file_stat 两阶段门控已存在。

### 可复用资产

- 预警判据：`build_sheet_oos_alerts`（`sheet_oos_alerts.py:40-67`）、SPC CPK 预警（`spc_dashboard.py:143/160`）、yield `compute_lot_oos_records` + `AbnormalDetector` 结构化记录（`abnormal_detector.py:37-148`）、qtime `build_qtime_alerts`（`alerts.py:19-28`）。
- 渲染：`RenderGate`（`render_gate.py:32-67`）；SPC 预警图像 memo 签名模式（`spc_dashboard.py:762-828`）；yield `render_alert_code_expanders`。
- ViewModel 字典载荷模式（ADR-0001）规避 cache_data 序列化陷阱。

### qtime 现状细节

- 页面 44 行薄壳；section `dashboard.py` 查询按钮 + session_state 签名 `(shop, step_options, product)`；`cached_funcs=[]`。
- 数据源 `mdw.qtime_tzbjx`；修饰台账 `resources/indicator_domain/qtime/qtime_oos_decoration.xlsx`（目录按需创建）。
- flag=False 词表含 "false/0/no/n/否/不修饰/不截断"（`alerts.py:8`）。

## 既有测试基线

- 上一期交付记录：`tests/unit` 555 passed、integration 9 passed（2026-08-25，可能有增长）；HEAD 既有 10 项失败与本改动无关（需开工时重新核实基线）。

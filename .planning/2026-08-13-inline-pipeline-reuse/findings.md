# Findings: Inline Pipeline 复用

## 代码事实（两轮 explore 核查，2026-08-13）

### 段1 迁移对象（spc_repository.py，实际路径无 repositories/ 子目录）

- 制备管线主体 `:166-223`：rename/coercion/去重 → LOSS 过滤(`:179`) → 白名单 merge+classify+data_type 过滤(`:184-201`) → 异常点(`:202`) → 时间/维度过滤(`:203-213`) → 主制程追溯(`:216-223`)。
- `_apply_outlier_filters :225-397`：规则 `resources/spc_outlier_filters.xlsx`（加密，COM 解密，写 CSV 副作用 `:259-260`）；规则键 prod/step/param 不带 data_type → 对 monitor(ALL) 的 AOI 行也生效；顺序在 data_type 过滤之后，迁移必须保序。
- 规格覆盖 `:46-152`：YAML `config/products/<prod>.yaml` 键 `spc_spec_override`；ctq/monitor 实际共用；aoi_tt 绕过（直接用 metadata.get_parameter_specs）。
- `get_scrap_data :402-499` + `_infer_factory_from_step :501-524`：仅 monitor 消费；数据源 `resources/scrap_sheets.xlsx`（全产品一个文件）+ `config/scrap_factory_mapping.yaml`；与 measurement 三 port 零耦合。
- `main_process_trace.py`：纯 DataFrame 函数，仅 spc_repository 使用；`measurement_preprocessor.py` 的 LOSS 过滤在 data_type 过滤之前执行。
- `composition.py:18` import 缺 `src.` 前缀。

### 段2 事实

- CTQ 修饰：`resources/ctq_sheet_oos_decoration.xlsx`，根目录、每产品一个 sheet（`ctq_data_decoration.py:18,30-38,71-79`）；与 SPC 共用引擎 `core/spc/spc_sheet_oos_decoration.py`，仅文件名参数不同；缺 sheet = 空修饰语义（引擎 `:185-202`）。
- monitor 现状：ALL 类型统一走 SPC 修饰（`monitor_service.py:306,527`）；AOI 行被 SPC 修饰意外覆盖；判定按 data_type 分组 `enable_soos=(type!='AOI')`（`:320-323`）。
- 前端监控类型 `['ALL','SPC','CTQ','AOI','报废']`（`monitor_dashboard.py:51-58`）；主页面固定以 ALL 调一次，前端切片（`:571-576`）；强刷清缓存 `自动预警看板.py:72`。
- monitor-AOI vs aoi_tt：①参数识别不同（白名单 data_type NULL→AOI vs 规格表 param_type IS NULL）②monitor-AOI 被 SPC 修饰，aoi_tt 无修饰 ③monitor 丢 lot_id，aoi_tt 保留 lot ④语义：报警 vs TT 均值。同底：`data/<prod>` 共享快照。→ 对齐到"同源+免修饰"；参数集统一列 Out of scope。
- aoi_rs：链路完全独立（自有 DAO），确认不在范围。

### 缓存/契约约束

- ADR-0001：st.cache_data 只跨 DataFrame/原生容器/标量；ViewModel 缓存外组装。
- 现有缓存模式：下划线前缀参数不参与 hash（`_db_manager`/`_data_port`），key 含 query_config_json + snapshot_signature。
- 既有全量 pytest 基线：7 个与本任务无关的失败（ADR-0012 Verification 记录）。

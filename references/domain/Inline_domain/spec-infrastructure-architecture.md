# Inline Domain · Infrastructure 架构规范

> **范围**: [`src/inline_domain/infrastructure/`](../../../src/inline_domain/infrastructure/)
> **最后更新**: 2026-08-13（段1重构定稿）
> **关联**: [ADR-0012](../../../docs/ADR/0012-shared-inline-measurement-snapshot.md) · [inline_domain.md](./inline_domain.md)

---

## 1. 分层总览

```
┌────────────────────────────────────────────────────────────┐
│ Application（spc / ctq / monitor / aoi_tt service）          │
│   各自持有本模块 port，repository↔service 1:1                 │
├────────────────────────────────────────────────────────────┤
│ Infrastructure                                              │
│  measurement/        共享测量能力（唯一数据源入口）            │
│  spc/                SPC 薄投影（data_type_filter="SPC"）     │
│  ctq/                CTQ 薄投影（data_type_filter="CTQ"）     │
│  aoi_tt/             AOI_TT 投影（规格表 param_type 识别）    │
│  monitor/            monitor 门面 + 报废适配器                │
│  aoi_rs/             独立链路（不在共享体系内）                │
├────────────────────────────────────────────────────────────┤
│ 装配：src/inline_domain/composition.py（唯一组合根）          │
└────────────────────────────────────────────────────────────┘
```

**核心原则**：spc / ctq / aoi_tt / monitor 是**平行业务模块**。任何可复用的取数与
制备逻辑只允许归属 `measurement/`；业务模块的 repository 只做投影与门面，不承载
跨模块共享逻辑。

---

## 2. measurement/ —— 共享测量能力

| 文件 | 职责 | 对外接口 |
|---|---|---|
| `measurement_data_loader.py` | 三厂测量 DAO：一次参数化 UNION ALL（ARRAY/OLED/TP）+ 产品字典 join，产品/时间过滤下推 SQL | `load_raw_measurements(db, start, end, prod)` |
| `measurement_metadata_loader.py` | 参数元数据 DAO：白名单目录 `eda.IMP_SPC_TZBJX`、规格表 `mdw.dwd_imp_dv_param_spec` | `InlineMeasurementMetadataRepository`（实现 `MeasurementMetadataPort`） |
| `measurement_snapshot_repository.py` | 产品级原始 Parquet 快照：3 个月滚动窗口、8h TTL、策略版本、原子写、进程内锁、失败降级 | `InlineMeasurementSnapshotRepository`（实现 `MeasurementSnapshotPort`） |
| `main_process_history_repository.py` | 主制程 OUT 履历 DAO（6 条 factory×route SQL） | `InlineMainProcessHistoryRepository`（实现 `MainProcessHistoryPort`） |
| `main_process_trace.py` | 主制程追溯**纯函数**（路由 + 最近前序匹配，零 I/O） | `attach_main_process_spec` / `apply_main_process_history` |
| `measurement_preprocessor.py` | 排除参数关键字过滤（`LOSS`），纯函数 | `filter_excluded_param_names` |
| `measurement_preparation.py` | **共享制备管线**（见 §3）+ 规格线查询与 YAML 覆盖 | `InlineMeasurementPreparationRepository`（实现 `MeasurementPreparationPort`） |

快照稳定字段超集：`factory, prod_code, start_time, sheet_id, lot_id, step_id,
param_name, site_name, unit_id, param_value`。**任何派生规则不回写原始快照。**

---

## 3. 共享制备管线（measurement_preparation.py）

`InlineMeasurementPreparationRepository` 组合 snapshot / metadata / history 三 port，
对外两方法：

- `get_prepared_measurements(config: SpcQueryConfig, force_refresh=False)`
- `get_spec_limits(prod_code)`（DB 规格 + `config/products/<prod>.yaml` 的
  `spc_spec_override` 三重匹配覆盖，键名保留兼容）

### 3.1 管线顺序（行为契约，变更视为口径变更）

```
清洗（start_time→sheet_start_time、类型 coercion、dropna）
  → 排除参数过滤（LOSS 关键字）
  → 排序去重（prod/factory/sheet/step/param/site 六键 keep="last"）
  → 白名单 merge（classify_param_type 分类）+ data_type 注入 + data_type_filter 过滤
  → 异常点过滤（resources/spc_outlier_filters.xlsx，规则键 prod/step/param）
  → 时间窗口 [start, end+1d) + factory/step_id/param_name 维度过滤
  → 主制程追溯（attach_main_process_spec → 履历查询 → apply_main_process_history）
```

**保序约束**：

1. 排除参数过滤在 data_type 过滤**之前** → 所有类型的数据都剔 LOSS。
2. 异常点过滤在 data_type 过滤**之后** → monitor 传 `ALL` 时规则作用于 AOI 行。
3. 白名单为空 → 返回空；白名单查询失败（None）→ 全量放行并标 `data_type='UNKNOWN'`。

### 3.2 data_type 分类

`core/monitor/monitor_param_classifier.classify_param_type`：NULL/空白 → `AOI`，
其余去白转大写（`SPC`/`CTQ`）。分类在制备层经白名单 merge 注入。

---

## 4. 业务模块（平行薄投影）

| 模块 | Repository | 投影口径 |
|---|---|---|
| spc | `spc/spc_repository.py`（约 30 行） | 委托制备 port，`data_type_filter="SPC"` 由应用层固定 |
| ctq | `ctq/ctq_repository.py` | 委托制备 port，`data_type_filter="CTQ"` 在 repository 注入 |
| aoi_tt | `aoi_tt/aoi_tt_repository.py` | 直接用快照 + 规格 DAO；规格表 `param_type IS NULL` 识别 TT；保留 lot；不走白名单/异常点/追溯/覆盖 |
| monitor | `monitor/monitor_repository.py`（门面）+ `monitor/scrap_repository.py`（报废） | 制备 port 传 `data_type_filter="ALL"`；报废读 `resources/scrap_sheets.xlsx`（全产品单文件）+ `config/scrap_factory_mapping.yaml` 厂别推断，伪装为 OOC 行 |
| aoi_rs | `aoi_rs/data_loader.py` | 独立 DAO，不在共享体系内 |

**注意**：monitor 的 AOI 与 aoi_tt 不是同一份数据（参数识别、lot 粒度、语义均不同），
两者仅共享同一原始快照。monitor-AOI 免 Sheet OOS 修饰（D3 对齐）；参数识别集统一
为已知差异，单独立项。

---

## 5. 应用层共享缓存（application/shared/decorated_features.py）

修饰引擎（`core/spc/spc_sheet_oos_decoration.py`）与特征计算本是共享领域逻辑；
模块间唯一差异是**修饰口径（scope）**：

```python
@st.cache_data(show_spinner=False, max_entries=12, ttl=4 * 60 * 60)
def fetch_decorated_features(_features_source, prod_code, scope,
                             start_date, end_date, snapshot_signature="") -> dict
```

- scope：`spc` → `resources/spc_sheet_oos_decoration.xlsx`；`ctq` →
  `resources/ctq_sheet_oos_decoration.xlsx`；`none` → 免修饰（monitor 的 AOI 行）。
- 缓存 key 含时间窗口：窗口一致时跨模块命中同一条目（一致性由此保证）。
- 审计文件落盘语义：缓存 miss 时写一次，命中不重写。
- 返回值只含原生结构（ADR-0001）；ViewModel 在缓存外组装。
- monitor 按 data_type 分组路由：SPC→spc、CTQ→ctq（D2）、AOI→none（D3）。
- 强刷链路：三个页面的 `funcs_to_clear` 均登记该函数。

无工作簿的最简自动修饰由 `core/shared/auto_decoration.py::auto_clip_over_spec`
提供（超规值截断为线内 5%~15% span 的确定性伪随机值，单边规格以 0 为下界），
当前消费方：aoi_tt（tt_qty vs usl）与 aoi_rs（code_qty vs spec，规格按 sheet 级
type_flag 优先去重）。

---

## 6. 装配（composition.py）

唯一组合根。构建链：
`build_raw_measurement_repository` → `build_measurement_preparation_repository`
→ `build_spc_repository` / `build_ctq_repository` / `build_monitor_repository`
（= spc 投影 + scrap 适配器）/ `build_aoi_tt_repository`（快照 + metadata，无 history）。
页面经工厂注入（如 `partial(build_monitor_repository, db_manager)`），
application 层不得 import infrastructure。

---

## 7. 测试边界

- 特征化安全网：`tests/unit/inline_domain/test_pipeline_characterization.py`
  （锁定制备顺序的可观察结果、规格覆盖、scrap 契约、monitor 聚合口径）。
- 全量基线：7 个与本域无关的既有失败（加密 xlsx 诊断 ×3、Yield 配置 ×2、
  Code selector ×2）。

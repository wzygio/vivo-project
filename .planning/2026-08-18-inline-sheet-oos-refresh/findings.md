# Findings：Inline Sheet OOS 修饰刷新与决策持久化

## 代码现状（2026-08-18 核实）

- `app/components/page_header.py`
  - 已有产品 revision 机制：`get/bump_product_cache_revision`（`output/tmp/product_cache_revisions/`）、
    `build_product_cache_signature`、`invalidate_page_cache(product_code=...)`。
  - `_refresh_data_callback`（L121-139）：只跑 L1 handlers，成功后 toast“需要重读页面缓存时，请点击刷新缓存”，不失效 L2。
  - `_hard_reset_callback`：invalidate（产品级或全局）+ 清 ViewModel memo + 模块重载 + 配置重读。
- `src/inline_domain/application/shared/decorated_features.py`
  - `fetch_decorated_features`：`@st.cache_data(max_entries=12, ttl=4h)`，L148-154 任意 miss 均 `persist=True`。
  - payload 含 `sheet_oos_decoration` dict（decoration_df/path/sheet）。
- `src/inline_domain/application/shared/decorated_data.py`
  - `prepare_decorated_data(..., persist=True)`；scope→工作簿映射 `SCOPE_DECORATION_FILE_NAME`。
- `src/inline_domain/core/shared/sheet_oos_decoration.py`
  - `persist_sheet_oos_decoration`（L297-313）：当前明细 LEFT JOIN 旧 sheet flag 重建，历史键丢失。
  - 三态语义、`_stable_fraction`、键列参数化（`key_columns`）已存在，勿动。
- `src/shared_kernel/utils/excel_tools.py`
  - `read_workbook_sheet`：openpyxl 失败回退 COM；sheet 缺失返回空 df。
  - `replace_workbook_sheet`（L93-142）：返回 None；`PermissionError` 仅告警；
    加密文件 COM 读全表后 `unlink()` + 整体重写为明文。
- `src/inline_domain/application/ctq/ctq_service.py:81`：`@st.cache_data(show_spinner=False, max_entries=1)` 无 TTL。
- 页面 `snapshot_signature` base 各不相同：`SPC监控报表.py:45`、CTQ 页、
  `自动预警看板.py:123`（`MONITOR_PAGE_CACHE_SIGNATURE`），均经 `build_product_cache_signature` 拼产品 revision。
- `app/sections/spc/spc_dashboard.py:306-375`：管理区下载单 sheet“修饰表”；
  上传直接 `replace_workbook_sheet` 覆盖产品 sheet，无成功结果校验。
- `SPC监控报表.py:78-90`：`render_page_header(..., product_cache_scope=current_product,
  refresh_handlers=[refresh_raw_measurements(...)])`；管理区渲染 `render_spc_decoration_admin`。

## 测试基线（待 Phase 0 记录）

- 既有失败基线参考（2026-08-14 记录）：yield_global_data_policy ×2、code_selector_filter ×2 等。
- 测试运行：`.venv/Scripts/python -m pytest tests/unit -q`。

## 风险点

- worktree 中跑测试需确认 import 解析（src 布局 / editable install 指向主仓的风险）→ Phase 0.1 验证。
- 真实 `resources/*_sheet_oos_decoration.xlsx` 可能被企业加密软件锁定 → 测试一律用临时目录；
  UI 验收前先备份真实工作簿。
- Streamlit `st.cache_data` 在非 Streamlit 上下文的测试方式：参考既有 `tests/unit/inline_domain` 做法。

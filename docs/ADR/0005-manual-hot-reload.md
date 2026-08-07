# adr-0005：热重载从自动降级为手动

- Status: Accepted
- Date: 2026-08-06
- Scope: `app/components/page_header.py`、`app/utils/reloader.py`、`app/Home.py`、`.streamlit/config.toml`

## Context

SPC 监控报表出现"厂别筛选需连续点击约三次才能切换"的故障：前两次点击后
选项回退到默认值，并伴随长时间加载/卡顿。实机复现（Playwright + 与生产相同的
Streamlit 1.60.0）证明筛选组件代码无 bug；日志（`output/logs/app.log_info.log`）
显示用户会话期间页面被反复重置：

1. 配置未改动却在 90 秒内发生 4 次完整配置加载（session_state 被反复清空，
   所有筛选 widget 回默认值）；
2. 两秒内最多 3 次 Deep Reload——`get_project_revision` 把
   `resources/**/*.xlsx/csv` 的 mtime 纳入指纹，而应用每次重建 payload 都会重写
   修饰 xlsx，企业加密软件也会异步触碰 xlsx，指纹不稳定导致
   `setup_hot_reload` 在页头 `st.rerun()` 打断当前 run 并卸载约 93 个模块。

用户约束：保留对 `.py` / `.yaml` 的监控（追踪范围不变），但将自动热重载
降级为手动——缓存刷新、代码重载、配置文件重读统一收敛到页头"刷新缓存"按钮。

## Decision

1. **被动检测**：`setup_hot_reload` 改为 `detect_project_changes()`。检测到
   变更时仅更新 `last_code_revision` 并置位 `code_update_pending` 标记，
   页头在"刷新缓存"按钮旁渲染提示；**不卸载模块、不 `st.rerun()`**。
   `get_project_revision` 的追踪范围（.py/.yaml/.xlsx/.csv）保持不变。
2. **手动生效**：`_hard_reset_callback`（"刷新缓存"）固定四步：
   缓存失效 → 清视图状态 → `deep_reload_modules()` +
   `SessionManager.load_and_set_config()`（总是执行，不再仅限 global 分支）
   → 清除 `code_update_pending`。按钮点击天然触发 rerun，不额外调用。
3. **关闭 `runOnSave`**（`.streamlit/config.toml`）：自动模型下它在保存 .py 时
   整页 rerun 但不卸载模块，属于无意义打断；手动模型下应为 false。

## Consequences

- 用户操作不再被热重载打断；页面 rerun 次数显著下降。
- 开发者工作流变化：修改代码/配置后页面不再自动生效，需点一次"刷新缓存"
  （页头有变更提示）。这是降级换来的稳定性，经用户确认接受。
- 会话重置的环境因素（多标签、F5、企业安全软件干预 websocket）不在本决策
  范围内，作为开放问题观察。
- 配套测试：`tests/unit/app/components/test_hot_reload.py` 覆盖被动检测
  不重载/不 rerun、pending 标记置位与清除、手动回调四步。

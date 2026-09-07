# ADR-0026: Inline OOS 长期历史与共享事实看板

Status: Accepted
Date: 2026-09-07

## Context

自动预警页面原 `MonitorAnalysisService` 会跨全部产品重新读取量测与规格、计算 Sheet 特征及
OOS/OOC/SOOS，再构造多套聚合。SPC、CTQ、AOI_TT、AOI_RS 页面其实已经在正常查询时
产生 Sheet OOS 明细和人工决策工作簿，但工作簿产品 sheet 只保留当前窗口，无法作为长期历史。

## Decision

- 四个生产者在既有 OOS 明细生成后，以查询覆盖窗口增量更新
  `data/inline_domain/oos_history/` 下的 scope × 产品 Parquet。
- 更新使用半开窗口替换：窗口内旧事实删除后写入新事实，窗口外历史保留；成功空结果也更新。
- Parquet 保存稳定事实，不把可变 `flag` 作为权威字段；读取时合并 Excel `__flags` 决策。
- monitor 改为只读“超规事实看板”，展示 `flag=False` 的数量、趋势、Top 站点和明细，不再
  展示无法由异常事实证明的抽检分母、告警率、OOC、SOOS；报废保持独立链路。
- 预警矩阵历史优先，历史尚未建立时只读回退当前工作簿，不在矩阵读取路径隐式写盘。
- 页面保留查询门控。产品 × scope 最后更新时间仅 `?admin=true` 展示。

## Consequences

- monitor 查询不再触发全量量测/规格/特征计算，四个页面与矩阵复用同一事实结果。
- 子模块长期不访问时对应历史会陈旧；管理员刷新状态显式呈现该事实。
- 首次上线可运行 `uv run python tools/bootstrap_inline_oos_history.py --dry-run` 检查迁移，确认后
  去掉 `--dry-run` 从当前工作簿建立起始历史。
- Excel 继续承担人工决策和当前审计；长期增长、原子替换与覆盖元数据由 Parquet 仓储承担。

## Verification

- 仓储、应用、生产者、页面和矩阵单元/集成测试。
- `tests/e2e/oos_monitor_dashboard.js`：查询门控、结果渲染、admin 可见性、普通用户不可见性、
  768px viewport-fit 和浏览器控制台错误检查。

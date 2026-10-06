# 产品页面缓存边界审查

更新日期：2026-09-08。

## 刷新契约

实际展示单产品的页面显式声明 `product_cache_scope` 与 `product_cache_indicators`。管理员点击“刷新缓存”只推进这些指标和当前产品的版本，并重读该产品配置；不执行 `func.clear()`、全局缓存清理或后端模块卸载。指标名即稳定业务 key，不再增加 domain 层级。

共享版本接口位于 `app/components/indicator_cache.py`。版本写入 `output/tmp/indicator_product_cache_revisions`，原子替换，并用于 application 的 `snapshot_signature`；Inline application 另外收到显式 `product_revision`。不存在版本文件时使用 `0`；读取权限等异常不假装缓存仍然有效。

“刷新数据”是单独的 L1 维护操作：原有当前产品刷新函数成功后才推进对应指标版本。底层物理数据源可能由多个指标共用，因此主动刷新 L1 与仅刷新计算缓存不同。

## 单产品页面

| 页面 | 当前产品负责的指标 | application 缓存实参 |
|---|---|---|
| AOI_RS监控报表 | `aoi_rs_sheet_oos` | 产品查询配置 + 指标签名 + 显式 revision |
| AOI_TT监控报表 | `aoi_tt_sheet_oos` | 产品查询配置 + 指标签名 + 显式 revision |
| CTQ监控报表 | `ctq_sheet_oos` | 产品查询配置 + 指标签名 + 显式 revision |
| SPC监控报表 | `spc_sheet_oos`、`spc_cpk_trend` | SPC 报表本身共同计算两项，使用两版本组合签名，仍按产品隔离 |
| 入库不良率分析看板 | `yield_trend_fluctuation`、`yield_lot_oos`、`yield_sheet_oos` | 趋势、Lot、Sheet 分别使用自身指标签名，不用三者组合签名 |
| 入库不良率ByLot明细表 | `yield_lot_oos` | 产品配置 + Lot 指标签名 |
| 入库不良率BySheet明细表 | `yield_sheet_oos` | 产品配置 + Sheet 指标签名 |

Yield 分析看板含 Sheet 明细，因此比全指标矩阵的两项 Yield 指标多声明 `yield_sheet_oos`。主页面、明细页面和矩阵共享刷新版本；具体日期、资源签名或 application 参数不同时仍可能是不同缓存条目，不能把共享版本误称为所有调用必然命中同一条缓存。

## 非单产品页面

Q-Time监控报表、IJP溢流监控报表、IQC寿命测试报表、IQC蒸镀材料报表、关键备件报表、自动预警看板均已显式关闭页头产品选择。专项资料页面未调用产品筛选页头。本次未发现需要新移除的遗留产品 header。

自动预警看板关闭通用“刷新缓存”按钮，使用矩阵自身的指标×产品定向刷新入口，避免通用全量清理重新影响其他指标。维护按钮及刷新标识只在 `?admin=true` 显示。

## 验证边界

计数器测试覆盖：刷新 A/P1 后只有 A/P1 重新调用，B/P1、A/P2、B/P2 均保持命中。页头测试确保不调用函数整体 clear 或模块热重载；AST 审查覆盖全部七个单产品页头声明，并排除旧全产品 revision API。页面执行测试采用模拟 service 和决策数据，不连接业务数据库、不写生产 Excel。

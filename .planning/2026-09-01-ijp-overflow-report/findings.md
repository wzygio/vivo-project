# Findings: IJP 溢流报表开发

## 需求事实

- FineReport 报表 = 筛选区 + `OLED RS Overflow By天` 堆叠占比图 + 明细表（Print Time/ProductCode/Glass ID/Printer/Panel ID/原图/Panel Location/CODE_RATIO）。
- 13 组数据集：6 组筛选项 + 2 组明细（SEARCH/SERACH1）+ 2 组边框占比 + 1 组 By天 + 2 组 Total 占比。
- 复刻范围按样式截图收敛：筛选 + By天 图 + 明细表；Total/边框占比图为 FineReport 辅助视图，不作为首版页面区块（如后续需要可扩展 port）。

## 数据库事实（探查证据 output/tmp/ijp_db_probe_result.md）

- 全部 7 项数据源可访问；30 天窗口白名单过滤后约 30.2 万行。
- `GLASS_START_TIME`/`CUT_START_TIME` 为 timestamp；`EVENT_TIME` 为 varchar（唯一需要 `<> 'NaT'` 的列）。
- `DWR_MES_PRODUCTSPEC`（PRODUCTSPECNAME/PRODUCTCODE）≠ `DWR_MES_PRODUCTSPEC_V`（FACTORY/PROD_ID/product_code）。
- 5 台 IJP 设备白名单在 `EDA.OLED_CHAMBER_HST_T.SUB_EQUIP_ID` 全部存在。
- `SUBSTRING(RS_DEFECT_IMAGE_NAME,57,14)` = 12 位 GLASS_ID + 2 位 panel 码，真实样例验证通过。
- 工单类型枚举（FACTORY='OLED'）：CYSY/DOESY/ESLC/EXMJF/GYSY/P/SLCFG/XMSY/XPSY/ZJSY。

## 仓库模式事实

- 薄页面入口模板：`app/pages/Q_Time监控报表.py`；section/chart 分层：`app/sections/qtime_domain/`、`app/charts/qtime_domain/`。
- 仓储模式：sqlalchemy text + 绑定参数 + expanding bindparam；异常包装为领域错误（稳定文案）。
- 集成测试模式：SQLite 内存库 + ATTACH 模拟多 schema（`tests/integration/qtime_domain/test_qtime_repository_sql.py`）。
- E2E 模式：`tests/e2e/fixtures/<name>_app.py` 隔离 fixture + playwright-cli 执行 `tests/e2e/*.js`，产物落 `output/test-results/`。
- 红线：不改 `DatabaseManager` 单例；E2E 产物不进仓库根/src；不改用户资源文件。

## 开放问题

- 无。Target 值语义已定为图表参考线输入；样式截图中 FineReport 特有元素（参数面板编辑态等）不复刻。

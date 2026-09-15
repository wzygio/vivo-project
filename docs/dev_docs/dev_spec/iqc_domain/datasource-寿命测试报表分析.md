# IQC 寿命测试报表：需求与数据源分析

日期：2026-09-15。来源：[开发需求](task-寿命测试报表开发.md)、`docs/project_files/iqc_domain/ID寿命维护表V1.0.xlsx` 与同目录样式 PNG。

## 核验结果

已通过 M3 只读事务核验 `m3dwd.dwd_panel_eff_dec_enter`：100 行，M678 / 2026/3/10；W、R、G、B 每组 25 行、5 个 panel_id、5 个数值时间点（0、100、200、300、400），测点键无重复。
表中所需列均为 varchar；需在领域边界进行数值转换。UTF8 中文原值为“屏体”“量产”，不是源数据乱码。
Excel 企业加密，使用 enterprise-excel-markdown 的 Excel COM 只读单元格提取确认：102 行、19 列；第 1 行标题、第 2 行表头、100 行测点。SaveAs 产物仍被企业加密，不能作为 openpyxl 验证通过的依据。

## 字段来源

全部来自上述单表，无连接；删除需求 SQL 最后一列后的多余逗号，正式查询不使用 LIMIT 10。

| 数据库字段 | 明细列 | 口径 |
| --- | --- | --- |
| product_group | 产品型号 | Excel“项目” |
| product_id | 产品状态 | 原值，当前屏体 |
| wo_name | 量产 | Excel“阶段”，保留原值，不按字段名推测工单 |
| date_key | 批次号 | 字符串标识；不作为报告日期前推 |
| panel_id | 样品编号 | 产品型号+批次号+测试画面内按原 ID 字符串排序，1..N；原 ID 不送前端 |
| test_time | 测试时间 | 非负有限数值排序；现有资料未确认单位，暂不标 h |
| bu | 测试画面 | W/R/G/B；其它画面也保留 |
| luminance | 亮度 | 源值 |
| lumi_decay | 亮度衰减 | 原比例，基准约 1.0 |
| cie_x | CIEx | 源值 |
| cie_xy | CIEy | 按用户映射，数据库实际列名确为 cie_xy |
| iss | Iss | 源值；未确认单位不补单位 |
| curr_decay | 电流衰减 | 原比例 |
| efficiency | 效率 | 源值 |
| eff_decay | 效率衰减 | 曲线纵轴，保留原比例；不是 1-eff_decay |

## 数据契约与边界

- Excel 中 CIE-u/CIE-v/JNCD 在库中也有 cie_u/cie_v/jncd，但不在本次明确要求的 SQL 列中，暂不展示。内部 ext_user/ext_time 不展示。
- 未解决资料项仅为测试时间和部分指标的物理单位，均不影响按原值展示；不推算寿命和判定阈值。
- 空数值以及 /、- 等缺测标记保持空白，页面提示测量缺失，曲线保留断点；无效样品标识、无效时间或冲突重复测点使读取失败。完全重复测点去重。
- 编号先在全组生成再筛选，五个样品不是限制；不跨批次/画面混线。同一数据集乱序、翻页、筛选不会改变编号；新样品加入排序更靠前的位置时可能重排，本期没有持久编号表。
- M3 独立连接池，只读事务、连接/语句超时、参数隐藏；不改动原 DatabaseManager，不覆盖 DB_*。
- 样品 ID 及额外字段在服务返回前删除；页面、图表 JSON、表格、HTML、缓存只接收公开列。
- 页面包含产品型号、产品状态、批次号三个业务筛选。产品选项来自全局 enabled_products，并在展示前限制数据范围；按产品/批次组织 Expander，W/R/G/B 每行四图、横轴步长 100。全局缓存 TTL，失败不缓存、不回退示例。

## 开发与验收

计划与验收记录：`.scratch/iqc-lifetime/PRD.md` 及其 issues。

### 实现

- `src/iqc_domain/{application,core,infrastructure}/lifetime/`：应用读端口、匿名投影与校验、M3 独立池及只读查询。
- `app/sections/iqc_domain/lifetime/`：公开数据缓存、三个联动筛选框、明细分页；`app/charts/iqc_domain/lifetime.py`：分组曲线构造。页面向共享页头注册本报表缓存函数，关闭快照刷新入口，沿用页头管理模式下的“刷新缓存”。
- 原寿命页面改为调用正式 section，保留既有示例资源供其它使用者；更新 CONTEXT、ARCHITECTURE 与 [ADR-0030](../../../ADR/0030-iqc-lifetime-anonymous-m3-boundary.md)。

### 验收结果（2026-09-15）

| 验证 | 结果 |
| --- | --- |
| 聚焦单元、仓储、缓存并发重载、AppTest、M3 实库对账、架构 | 29 passed |
| 原 IQC 示例和蒸镀材料相关回归 | 15 passed |
| Playwright 真实 M3 页面 | passed：100 行、15 列、4 图、每图 5 样品、时间 0/100/200/300/400、筛选、刷新、真实 hover |
| Playwright 受控页面 | passed：故障、空集、恢复、缺测断线、9 样品、109 条数据末页 9 条、分页复位、产品切换清理旧批次 |
| 普通用户内容 | HTML、表格和图表数据/hover 未检出原 ID、panel_id、权限提示或数据库细节 |
| 浏览器宽度 | 1365、768、390px，无页面横向溢出，表格可滚动到最后一列 |
| 独立规范/需求评审 | 各 0 个未解决问题；缓存并发重载验证缺口已补测关闭 |

执行命令（仓库根目录，PowerShell）：

```powershell
$env:IQC_LIFETIME_LIVE_DB='1'
.venv/Scripts/python.exe -m pytest tests/unit/test_iqc_lifetime.py tests/unit/test_iqc_lifetime_repository.py tests/e2e/test_iqc_lifetime_apptest.py tests/integration/test_iqc_lifetime_live.py tests/architecture -q
.venv/Scripts/python.exe -m pytest tests/e2e/test_iqc_evaporation_apptest.py tests/unit/test_iqc_evaporation.py tests/unit/test_iqc_evaporation_cache.py tests/unit/test_iqc_evaporation_repository.py tests/test_iqc_demo_reports.py -q
.venv/Scripts/python.exe -m streamlit run app/pages/IQC寿命测试报表.py --server.port=8521 --server.headless=true
.venv/Scripts/python.exe -m streamlit run tests/e2e/fixtures/iqc_lifetime_app.py --server.port=8522 --server.headless=true
```

浏览器脚本从 `output/test-results/iqc-lifetime/` 执行，确保自动日志和截图落在允许目录：

```powershell
playwright-cli -s=iqc-lifetime open http://localhost:8521 --browser=msedge
playwright-cli -s=iqc-lifetime run-code --filename=D:/wzy/Python/vivo-project/tests/e2e/iqc_lifetime_report.js
playwright-cli -s=iqc-lifetime run-code --filename=D:/wzy/Python/vivo-project/tests/e2e/iqc_lifetime_states.js
```

实库浏览器脚本断言当前验收样本总量；如果 M3 数据增长，更新该验收样本预期。独立实库对账按实际分组读取，不依赖固定总行数。
截图与日志：`output/test-results/iqc-lifetime/`，含 `live-hover.png`、`live-detail.png`、三种宽度截图及受控状态截图，`focused.xml` 保存聚焦测试结果。

### 全库回归限制

- `pytest -q` 会收集 output 下历史工作副本及 tools 目录，出现 4 个收集错误；改为明确执行 `pytest tests -q`。
- 当前全 tests 结果：1485 passed、2 skipped、9 failed。其中旧蒸镀材料 AppTest 首次 3 秒超时，单独复测通过；其余 8 项在开发前基点 `7bf9b0a` 的独立工作副本复现（1472 passed、1 skipped、8 failed）。新增缓存重载测试于此后补充并通过聚焦验证。
- 8 项既有失败：Inline Excel-only E2E 触发原页面 DB 构造（1）；设备 real/fabricated 结果条数断言（1）；SPC 加密文件诊断与当前文件格式不符（3）；自动预警页面门控断言（3）。本次未修改这些业务模块。
- 回归输出包含已有 Windows COM `0x80010108` 诊断及弃用提示，测试进程仍完成并生成汇总。记录在 `regression-tests.log/xml` 与 `baseline.log/xml`。
- 环境没有 pyright/ruff/coverage 插件，本次未声称完成静态类型或覆盖率百分比验证。未部署、远程推送或合并 master；开发提交保留在 `feat/iqc-lifetime-report`。

### 2026-09-15 后续交互优化

根据用户后续要求，取消独立“刷新数据”按钮，使用页头缓存刷新；添加产品状态；产品选项和“全部”结果范围统一受 enabled_products 控制（原则写入 CONTEXT）；每个产品/批次使用一个默认展开的 Expander，桌面 W/R/G/B 四图同排，窄屏沿用 Streamlit 自适应换行。横轴只调整刻度为 100 的整数步长，不舍入或重采样测点。

验证：49 项相关测试通过，包含页头既有行为回归、真实 M3 对账、配置变化后清理旧选择、产品状态筛选及空启用范围。真实与受控 Playwright 均通过，增加 Expander 开合、四图位置、图例不遮挡曲线、三筛选、禁用产品不展示、实际页头缓存刷新等断言。
本轮未重复全库回归；前述全库限制属于首次开发的历史证据。浏览器截图保存在 `output/test-results/iqc-lifetime-optimization/`。

# IJP 孔区报表：数据来源、口径与验证

日期：2026-09-15。需求：[任务书](task-IJP溢流报表开发-孔区.md)，依据：[FineReport SQL](sql-IJP溢流报表开发-孔区.md)。

## 结论与联调限制

采用已有 IJP 页面中的“孔区（大孔）”入口；独立 `ijp_hole` 子模块处理孔区，避免将旧边框 C3DM 系列的分母和处理规则用于 C3RA 系列。

只读数据库探查确认 `eda.oled_ijp_defect_t` 存在，`pg_attribute` 中的字段与 SQL 一致。但当前连接账号 `has_table_privilege(...,'SELECT') = false`，实际 `SELECT 1 ... LIMIT 1` 返回 SQLSTATE **42501**。因此尚不能验证真实记录分布、查询耗时或与 FineReport 的数值对账。没有用其它缺陷源冒充本表，也没有修改数据库权限。

需要数据库负责人为报表运行账号补齐该表及环境所需底层分区的只读权限，然后重新查询与对账。本文不记录账号、密码或连接串。

## 来源追溯到基础表

系统目录 `pg_class/pg_attribute` 和视图依赖 `pg_rewrite/pg_depend` 的只读探查结果：

| 报表对象 | 实际关系与上游 | 用途及所需字段 | 核实情况 |
|---|---|---|---|
| 缺陷源 | `eda.oled_ijp_defect_t` 基础表 | `shop, step_id, glass_id, img_dir, image_name, code, update_time` | 字段存在；无 SELECT 权限，内容未验证 |
| 腔室履历 | `eda.oled_chamber_hst_t` 基础表 | `cut_id, cut_start_time, product_id, equip_id, sub_equip_id, item5` | 列及类型可见 |
| 产品规格 JOIN | `mdw.dwr_mes_productspec` 基础表 | `productspecname, productcode` | 列及类型可见 |
| 产品筛选原数据集 | `mdw.dwr_mes_productspec_v` → `mdw.dwr_mes_productspec` | `factory, prod_id, product_code` | 视图依赖已核实 |
| 工厂筛选原数据集 | `mdw.dwr_mes_mesfactory_v` → `mdw.dwr_mes_mesfactory` | `factory` | 视图依赖已核实 |
| 批次筛选（沿用现有页面） | `eda.dwd_glass_oled_cycle_v3` 基础表 | `glass_id, pici, prod_code, event_time` | 列及类型可见；虽然名字含 V3，目录标记为普通表 |
| 工单类型（沿用配置范围） | `mdw.dwr_mes_productrequest_v` → `mdw.dwr_mes_productrequest` | `sub_prod_id, sub_prod_type` | 视图依赖已核实 |

`update_time` 和 `cut_start_time` 为 timestamp without time zone；`event_time` 为 varchar。SQL 保留原有 search_path 下未带 schema 的 MES 关系名，与既有 adapter 一致。

## 各 FineReport 数据集的落位

| 数据集 | 原始逻辑 | 本次实现 |
|---|---|---|
| FACTORY | 工厂固定 OLED | 固定 OLED 工艺步骤，不新增工厂筛选 |
| PRODCODE | OLED 产品型号 | 按 `ConfigLoader.get_enabled_products()` 的启用产品顺序提供选项，SQL 同时约束查询范围 |
| PRODUCT | 产品型号 → 产品规格 | 保留产品规格 JOIN，不新增旧页面没有的规格筛选 |
| SEARCH | 大小孔明细 | 本次不显示明细，与现有 IJP 页面保持一致 |
| SEARCH_BYDAY_D | 大孔按天比例 | “按天”堆叠图 |
| SEARCH_BYDAY_X | 小孔按天比例 | 不在本任务范围 |
| SEARCH_D | 大孔、玻璃内 CODE 数与占比 | “玻璃”堆叠图 |
| SERACH_TOTAL_D | 大孔、产品/设备/打印机 CODE 占比 | “Total”分组柱状图 |
| SERACH_TOTAL_X | 小孔整体比例 | 不在本任务范围 |
| SEARCH_X | 小孔明细 | 不在本任务范围 |

批次由 Cycle 表提供；工单类型按现有 IJP 使用的全局配置限制。这两项是与当前页面对齐的约束，孔区原 SQL 没有这两个筛选。

## 字段和计数血缘

1. `image_data = SUBSTR(img_dir,4) || '/' || image_name`。
2. `panel_id` 是路径第十段前 14 字符，`panel_location` 是第 15–17 字符。两者来自图像命名，而非独立面板表。
3. 大孔：`step_id='21200'`、`code IN ('C3RA1','C3RA2','C3RA3')`，且路径第十段第 17 字符为 `0–4`。`5–9` 对 C3RA1/2 为小孔；其它为未分类。本次 SQL 在读取缺陷的 CTE 内直接排除小孔、未知类型与无效路径。
4. `D.glass_id = H.cut_id` 关联腔室，限制四台打印机：`3CEE01/02-IK2-PR1/2`。`H.product_id = P.productspecname` 取得产品型号。页面线体沿用既有 IJP 的打印机前六位分组。
5. 原 SQL `GROUP BY` 实际上先去重缺陷投影，再做窗口计数。本次保留 `update_time, glass_id, code, image_data, productcode, equip_id, sub_equip_id` 粒度，以免重复腔室/规格记录把比例权重放大。批次和工单用 `EXISTS`，不额外增加记录。
6. `code_num` 为去重记录数；玻璃按产品/线体/打印机/玻璃分组，Total 按产品/线体/打印机分组，按天按产品/线体/打印机/显示日分组。分子为指定 CODE 数，分母为该组大孔三种 CODE 数之和，保留三位小数后显示百分比。
7. Total 从计数相加后计算，不能平均各玻璃或各天百分比。CODE 选择发生在分母确定之后，单选 C3RA2 不会把其比例重算为 100%。缺少的 CODE 补 0；整个组无事实则不伪造正常结果。
8. 原图 URL 为 SQL 内的固定图像服务前缀加 `image_data`，不依赖新数据表。当前无明细/图片下载，因此前端不接收图像路径。

## 时间窗口差异

| 部分 | 缺陷时间 | 腔室履历时间 |
|---|---|---|
| 玻璃、Total | 查询开始到结束，包含端点 | 查询开始到结束，包含端点 |
| 按天 | 同上，缺陷窗口不扩展 | 查询开始前 7 天到结束，包含端点 |

仓储一次查询七天追溯的去重事实，用 `in_window` 标记是否存在窗口内履历，分别生成三张报表。该字段仅在后端使用，不进入图表或公开结果。

页面沿用上月 1 日至服务器今天的查询范围。仓储先应用当日截止时间，再将显示窗口反算为源窗口；聚合后只在仓储输出边界前推 day。历史日保持全天。Session 结果签名包括时间策略、截止策略、产品配置和工单配置。

## 样式与未知项

参考图片为 FineReport 设计器占位图，只有“系列1/2/3”和“分类A/B/C”，不能据此推断真实数量或 CODE 中文含义。采用三色（绿/蓝/黄）、百分比堆叠与 Total 分组形式；具体产品/线体/打印机层级由当前页面 Expander 承担。沿用每行最多三图和四列筛选。

未提供且未臆造：C3RA1/2/3 的业务中文释义、Target 阈值、真实样本数、缺陷表写入该库之前的设备/ETL 血缘。数据库来源追溯到 SQL 所需基础表为止。

## 验证证据

- SQL 契约测试：`tests/integration/indicator_domain/test_ijp_hole_repository.py`，SQLite 在数据库端注册等价 `split_part`，实际执行生产 SQL；覆盖大小孔分类、源记录/多表重复、产品/线体/批次/工单范围、注入字符串、七天窗口及时间截止。
- 规则测试：`tests/unit/indicator_domain/test_ijp_hole.py`，覆盖三视图分母、加权和缺失 CODE。
- 浏览器：`tests/e2e/ijp_hole_report.js` 运行实际页面、实际服务和 SQL adapter，以合成数据库提供正常数据；失败场景走 adapter 真实异常封装。三视图、CODE 筛选、空/错误、区域切换和 1365×768 宽度检查通过。
- 截图：`output/test-results/ijp-hole/`，包含 `hole-glass-chart.png`、`hole-total-chart.png`、`hole-day-chart.png` 和空/失败状态。
- 旧报表 `tests/e2e/ijp_overflow_report.js` 回归通过，保留四台打印机、两月三周及无明细行为。
- 真实环境尚未通过权限后的查询与性能验收，不能将隔离 E2E 等同于生产数据验收。

### 最终执行记录

```powershell
.venv/Scripts/python.exe -m pytest tests/unit/indicator_domain/core/ijp tests/unit/indicator_domain/application/ijp tests/unit/indicator_domain/infrastructure/ijp tests/unit/indicator_domain/test_ijp_hole.py tests/integration/indicator_domain/test_ijp_repository_sql.py tests/integration/indicator_domain/test_ijp_hole_repository.py tests/unit/app/pages/test_ijp_page.py tests/unit/app/sections/indicator_domain/ijp tests/unit/app/charts/indicator_domain/ijp tests/architecture -q
```

结果：90 passed，1 个既有 IJP 异常测试的 pandas 连接类型 warning。此前包含 Q-Time 的聚焦回归 142 passed。未运行全仓业务测试，验证范围为 IJP 及架构约束。

浏览器服务：`streamlit run tests/e2e/fixtures/ijp_hole_app.py --server.port 8512 --server.headless true`。在 `output/test-results/ijp-hole/` 工作目录运行 `playwright-cli -s=ijp-hole run-code --filename=D:/wzy/Python/vivo-project/tests/e2e/ijp_hole_report.js`，最终返回 PASS。旧边框 fixture 使用 8511 端口及原 E2E 脚本，返回通过。

安全审查覆盖 SQL 参数化、产品隔离、时间转换、公开结果与异常清理，无明确缺陷；静态编译和 diff 空白检查通过。代码未提交/合并，真实库权限检查结果单独保留，不以测试 fixture 替代生产验收。

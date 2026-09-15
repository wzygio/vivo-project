# 蒸镀材料报表数据源与实现验收

日期：2026-09-15。需求：[task-蒸镀材料报表开发.md](task-蒸镀材料报表开发.md)。
SQL 原件：[蒸镀材料体系报表-sql语句.txt](../../../project_files/iqc_domain/蒸镀材料体系报表-sql语句.txt)。

## 1. 样式与功能范围

现有 `app/pages/IQC蒸镀材料报表.py` 原调用 Excel 示例。已读取工作簿
`resources/iqc_domain/IQC来料检验特性数据明细报表.xlsx` 的 sheet2（129 行含表头、25 列），
与既有 HTML 样式核对：蓝底白字表头、浅蓝交替行、固定序号、表内横纵滚动，数值显示三位小数、缺失保留空白。
原顺序保留，末尾增加 IQC结果、COA结果，共 27 列。大量数据每页 100 条，所有行均可分页访问。

查询日期按给定 SQL 的 **检验时间 CHECKEDDATE**，界面标明“检验日期范围”；它与报检日期 REQUESTDATE 不同。
原示例的日期控件按报检日期过滤，本次真实查询改为所给 SQL 的检验日期口径。
查询后可组合筛选项目名、工厂、物料分类、特性项目、IQC结果、COA结果。
SQL 默认排除 MASK，其余分类均保留；“有机”可通过分类控件筛选，不自行将整个查询缩成有机材料。

## 2. 可见字段逐项映射

别名：Q = `mdw.dwr_wms_tblqcticket`；U = `mdw.dwr_wms_tblmitem_b`；
S = `mdw.dwr_wms_tblqcticketsample`；M = `mdw.imp_iqc_mat_info`；
T = `mdw.imp_iqc_mat_info_tbtj`。

| 页面字段（顺序） | 来源 | 语义与处理 |
|---|---|---|
| 序号 | 应用层生成 | 完整结果排序后从 1 编号；筛选保留原序号 |
| 项目名 | Q.EATTRIBUTE10 / PROD_CODE | 原值，如通用；不绑定门户单一产品 |
| 量产/非量产 | Q.EATTRIBUTE15 / MATERIALTYPE | SQL 原本通过同 ticketno/ticketseq/transbillno/transbillseq 回查 Q，本实现直接取当前 Q 行字段；真实数据已对账一致 |
| 报检日期 | Q.REQUESTDATE | 时间戳；在仓储输出边界使用显示时间策略 |
| 检验时间 | Q.CHECKEDDATE | SQL 日期筛选字段；同样转换显示时间并应用当日截止 |
| 物料号 | Q.MITEMNAME | 字符串，不转浮点，保留编码 |
| 物料描述 | U.MITEMDESC | U.MITEMNAME=Q.MITEMNAME 且 U.ORGID=5000 |
| 检验人员 | S.EATTRIBUTE1 / CHECK_USER | 不是 Q.CHECKEDUSER |
| 物料分类 | S.EATTRIBUTE12 / MATER_TYPE | 须存在于 `dwr_wms_tblmitemset.SETCODE` |
| 工厂 | Q.EATTRIBUTE5 | 原值，如 V3；不把 OLED 工艺名覆盖到此列 |
| 特性项目 | M.CHA_ITEM | 从目录展开；未测项目仍存在 |
| 规格样式1 | T.SPEC_REQ2 | **上限比较符**，不是 SPEC_REQ1 |
| 规格上限 | T.UPP_SPEC | 同一 T 行，原数值 |
| 规格样式2 | T.SPEC_REQ1 | **下限比较符** |
| 规格下限 | T.LOW_SPEC | 同一 T 行，原数值 |
| IQC-1 / IQC-2 / IQC-3 / IQC-4 / IQC-5 | T.IQC_1 / IQC_2 / IQC_3 / IQC_4 / IQC_5 | 五个测点各自独立；NULL 不补零 |
| COA-1 / COA-2 / COA-3 / COA-4 / COA-5 | T.COA_1 / COA_2 / COA_3 / COA_4 / COA_5 | 五个测点各自独立；NULL 不补零 |
| IQC结果 | T.IQC_RESULT | 存储结果原值，不按 IQC_1_RESULT 重算 |
| COA结果 | T.COA_RESULT | 存储结果原值，不按 COA_1_RESULT 重算 |

## 3. 关联与过滤

```text
Q 检验批 -> U 物料主数据（物料名、组织）
         -> S 检验样本（ticketno + lotno + itemname）
              -> SETCODE 集合验证物料分类
              -> M 特性目录（mat_type；有机还匹配物料描述与 to_comment）
                   -> T 结果（mat_type + ticketno + cha_item）
```

- Q：ORGID=5000，TICKETTYPE LIKE '%IQC%'，CHECKEDSAMPLEQTY 非 NULL。
- Q：EATTRIBUTE1 不含免检或为空；EATTRIBUTE10 不等于 `/` 或为空；EATTRIBUTE12 不等于清除统计或为空。
- S：CHECKMODE NOT LIKE '%免检%'；NULL 与原 SQL 一样不能通过该条件。
- S 分类在物料集合中且不为 MASK；LEFT JOIN 后这些 WHERE 条件使不满足的样本/票据不进入结果。
- M 先对 `(mat_type, cha_item, to_comment)` 做 DISTINCT。有机的描述匹配采用
  `MITEMDESC LIKE '%' || COALESCE(TO_COMMENT, '') || '%'`；其他分类只按分类关联。
- T 保留 LEFT JOIN：未录入测点/规格/结果时保留 M 项目，不将 T 当作目录。目录多个备注匹配同一项目时，保留原 SQL 的行数语义，不擅自聚合去重。
- 原 SQL 按物料描述排序；同描述下增加检验时间倒序、物料号和特性项目排序，便于稳定阅读。
- 日期使用绑定参数，前端自然日双端包含转换为 `[开始日00:00, 结束日次日00:00)`。
  先通过全局策略反算源窗口，再将 REQUESTDATE/CHECKEDDATE 一次性前推到显示时间。
  当日截止使用全局 noon inclusive，历史结束日不截为半天。数据库事实不修改、不落地为显示时间快照。

## 4. 原 SQL 其余输出的来源

下列字段不在 sheet2 的 27 列展示契约内，已追溯但不发送至浏览器或导出。

| SQL 字段 | 来源/算法 |
|---|---|
| MITEMUOM | U.MITEMUOM |
| LOTNO、PONO、EATTRIBUTE3、TICKETNO、VENDORCODE | Q 同名字段 |
| VENDORNAME | `mdw.dwr_wms_tbllmsupplier.VENDORNAME`，按 Q.VENDORCODE + Q.ORGID 回查 |
| FINNALRESULT | `mdw.dwr_wms_tblqctreatment.TYPENAME`，TYPECODE=Q.TICKETRESULT |
| VENDORLONO | `mdw.dwr_wms_tbldocbilldtl.VENDORLONO`，BILLNO=Q.QCDOCUMENTNO、SEQ=Q.QCDOCUMENTSEQ::numeric、ORGID=5000 |
| BeiZhu、CHECKEDMEMO、EATTRIBUTE12 | Q.EATTRIBUTE1、Q.CHECKEDMEMO、Q.EATTRIBUTE12 |
| okqty、ngqty | S 同名字段；不是 Q.CHECKEDOKQTY/CHECKEDNGQTY |
| IQC_REPORT | S.EATTRIBUTE15 字符串通过 SUBSTR 拼成下载 URL |
| IQC_AVE、COA_AVE、IQC_CHA、COA_CHA | T 同名已存字段，SQL 不定义其上游计算 |
| ifexist | T 的上下限同时 NULL 为“否”，否则“是” |
| IQC_1_RESULT 至 IQC_5_RESULT；COA_1_RESULT 至 COA_5_RESULT | 对对应测点与两侧规格比较；违反 `>= <= > < =` 任一边界为 NG，否则 OK。原 CASE 对 NULL 会落入 OK；本报表不将这些派生字段当作整体 IQC/COA 结果 |

FineReport 模板参数：dtStartDate/dtEndtDate 为检验时间窗口；cmcbProdCode、cmcbMiteName、
cmcbchkResult 分别筛 Q.EATTRIBUTE10、Q.MITEMNAME、Q.CHECKEDRESULT；cmcbProdType、
cmcbVendorName、cmcbFinelResult、cmcbMaterType 筛 MATERIALTYPE、VENDORNAME、FINNALRESULT、
MATER_TYPE；txLotno 精确筛 LOTNO；cmcbExist 筛 ifexist。未选择参数保持原 SQL 空选择语义。
本次 UI 延续现有页面需求，并未新增这些原 SQL 隐藏筛选器。

## 5. 数据库核查与缺失说明

2026-09-15 使用既有 DatabaseManager、只读事务查询 catalog、字段及数据，未记录账号、连接信息或凭证。

- 九张来源表均定位到 `mdw` schema。
- 首次 `information_schema.columns` 看不到 M；进一步 `pg_class + has_table_privilege` 确认表存在但无 SELECT。
  用户随后开通权限；复查为 true，完整 SELECT 成功。未采用跳过目录或伪数据替代。
- T 当前 192 行，全为有机；其 IQC_RESULT、COA_RESULT 均为 NULL。字段真实存在，结果值尚未提供。
- `2026-08-16` 至 `2026-09-15` 显示窗口完整返回 2,518 行、27 列；有机 591 行，
  HPLC-A 43 行（目录展开包含未录入值的票据）。显示检验时间范围为 2026-08-16 15:43:36 至 2026-09-14 19:30:30。
- **仍未定义的信息**：T 的整体结果、均值、差值在上游由何作业生成，以及 NULL 应何时补齐，提供的 SQL 没有定义。
  本任务只读取它们，不推断、不写回；无找不到来源的可见字段。

## 6. 验证入口

### 代码归属

蒸镀材料后端分别位于 `src/iqc_domain/application/eva_materials/`、
`src/iqc_domain/core/eva_materials/`、`src/iqc_domain/infrastructure/eva_materials/`；
页面 section 位于 `app/sections/iqc_domain/eva_materials/`。域级 `composition.py` 负责依赖组装，
IQC 表格渲染器仍由蒸镀材料和示例页面共享。目录迁移不改变 SQL、业务字段或页面行为。

### 测试

- 服务/规则：`tests/unit/test_iqc_evaporation.py`。
- 源窗口、截止、失败：`tests/unit/test_iqc_evaporation_repository.py`。
- 缓存热重载和策略隔离：`tests/unit/test_iqc_evaporation_cache.py`。
- 本地关系型 SQL 验证：`tests/integration/test_iqc_evaporation_sql.py`。
- 真实数据库对账：设置 `IQC_LIVE_DB=1`，运行 `tests/integration/test_iqc_evaporation_live.py`。
  该测试按原 SQL 的空筛选参数执行完整查询，对当前实现与原 SQL 的 26 个业务字段逐行多重集比较（序号除外）。
  已通过：2,518 行全字段对账一致。
- 页面 AppTest：`tests/e2e/test_iqc_evaporation_apptest.py`。
- 浏览器：`tests/e2e/iqc_evaporation_report.js`，真实页面服务端口 8517；
  截图及运行证据放在 `output/test-results/iqc-evaporation/`。

### 最终验收（2026-09-15）

| 检查 | 结果 |
|---|---|
| `python -m pytest tests/unit/test_iqc_evaporation.py tests/unit/test_iqc_evaporation_repository.py tests/unit/test_iqc_evaporation_cache.py tests/e2e/test_iqc_evaporation_apptest.py tests/integration/test_iqc_evaporation_sql.py tests/test_iqc_demo_reports.py tests/architecture -q` | 29 passed |
| `IQC_LIVE_DB=1` + `python -m pytest tests/integration/test_iqc_evaporation_live.py -q` | 1 passed；与原 SQL 逐行对账 |
| `playwright-cli run-code --filename=tests/e2e/iqc_evaporation_report.js` | passed；真实数据库页面、27 列、分页、有机/HPLC-A 筛选、空集、1365/768 两种宽度 |
| `playwright-cli run-code --filename=tests/e2e/iqc_evaporation_states.js` | passed；受控端口故障、恢复、末页完整性、NG/空白结果、筛选空集 |
| Standards / Spec 双轴代码评审 | 0 项未关闭发现 |

运行浏览器命令时工作目录设为 `output/test-results/iqc-evaporation/`，脚本路径使用绝对路径；
受控场景通过 `streamlit run tests/e2e/fixtures/iqc_evaporation_app.py --server.port=8518` 启动。
真实脚本依赖当前窗口存在足够记录和 HPLC-A 特性，固定业务行为由受控场景保障。
未执行无关领域的全量 pytest 回归。架构决定：[ADR-0029](../../../ADR/0029-iqc-evaporation-source-contract.md)。

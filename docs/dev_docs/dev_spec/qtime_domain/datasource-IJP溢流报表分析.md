# IJP 溢流报表 — 数据来源分析

> 分析对象：`sql-IJP溢流报表开发.md`（FineReport 数据集 SQL）+ `IJP溢流报表样式-开发界面.png`
> 数据库探查记录：`output/tmp/ijp_db_probe_result.md`（2026-09 探查，全部数据源验证通过）

## 1. 报表构成（来自样式截图）

- **筛选区**：工厂、开始时间、结束时间、产品名称(PROD_ID)、工单类型(SUB_PROD_TYPE)、线体(EQP_ID 前 6 位)、设备(SUB_EQUIP_ID)、产品型号(PRODUCTCODE)、GlassID、CODE(RS_CODE)、边框(PANEL_LOCATION)、Target值、批次(PICI)、Cycle(CYCLE_ID)。
- **图表区**：`OLED RS Overflow By天` —— 按天 × 设备/产品的 RS_CODE 占比堆叠柱状图（100% 堆叠）。
- **明细表**：Print Time / ProductCode / Glass ID / Printer(SUB_EQUIP_ID) / Panel ID / 原图（图片URL超链接）/ Panel Location / CODE_RATIO。

## 2. 数据集 → 数据源映射

| FineReport 数据集 | 用途 | 数据源 | 验证结论 |
|---|---|---|---|
| `CYCLE` | Cycle 筛选项 | `EDA.DWD_GLASS_OLED_CYCLE_V3`（EVENT_TIME 为 varchar，需 `::TIMESTAMP` 且 `<> 'NaT'`） | ✅ |
| `FACTORY` | 工厂筛选项 | `DWR_MES_MESFACTORY_V`（factory='OLED'） | ✅ |
| `PICI` | 批次筛选项 | `EDA.DWD_GLASS_OLED_CYCLE_V3`（PICI） | ✅ |
| `PRODCODE` | 产品型号筛选项 | `DWR_MES_PRODUCTSPEC_V.product_code`（factory like '%OLED%'） | ✅ |
| `PRODUCT` / `SEARCH`（首段） | 产品名称筛选项 | `DWR_MES_PRODUCTSPEC_V.prod_id` | ✅ |
| `SEARCH_SUBPRODTYPE` | 工单类型筛选项 | `DWR_MES_PRODUCTREQUEST_V.sub_prod_type`（FACTORY='OLED'） | ✅ |
| `SEARCH`（主明细） / `SERACH1` | 明细表 | 主表 `EDA.SPOT_EDA_OLED_VIEW_DFT_V D` + `DWR_MES_PRODUCTSPEC P` + `EDA.OLED_CHAMBER_HST_T H` + `DWR_MES_PRODUCTREQUEST_V V` + `EDA.DWD_GLASS_OLED_CYCLE_V3 T` | ✅ |
| `SEARCH_BYBORDER` / `SEARCH_BYBORDER1` | 按边框 CODE 占比 | 同上五表 | ✅ |
| `SERACH_BYDAY` | By天 堆叠图 | 同上五表（时间窗口向前扩 7 天） | ✅ |
| `SEARCH_TOTAL` / `SERACH_TOTAL2` | Total 占比图（按设备 / 按产品） | 同上五表 | ✅ |

## 3. 核心明细查询语义（SEARCH / SERACH1）

```text
FROM EDA.SPOT_EDA_OLED_VIEW_DFT_V D            -- AOI/RS 缺陷明细（约 1080 万行/30 天，173 列）
LEFT JOIN DWR_MES_PRODUCTSPEC P                -- 注意：基础表（PRODUCTSPECNAME/PRODUCTCODE），非 _V
       ON P.PRODUCTSPECNAME = D.PRODUCT_SPEC
LEFT JOIN EDA.OLED_CHAMBER_HST_T H             -- 腔室履历，提供 Printer(SUB_EQUIP_ID) 与工单挂接
       ON D.GLASS_ID = H.CUT_ID
      AND H.CUT_START_TIME >= :start AND H.CUT_START_TIME <= :end
LEFT JOIN DWR_MES_PRODUCTREQUEST_V V           -- 工单类型
       ON H.ITEM5 = V.SUB_PROD_ID
LEFT JOIN EDA.DWD_GLASS_OLED_CYCLE_V3 T        -- 批次(PICI)/Cycle
       ON D.GLASS_ID = T.GLASS_ID
WHERE H.SUB_EQUIP_ID IN ('3CEE01-IK2-PR1','3CEE01-IK2-PR2',
                         '3CEE02-IK2-PR1','3CEE02-IK2-PR2','3CEE04-IKT-PRT')  -- 5 台 IJP 打印设备白名单
  AND D.RS_CODE IN ('C3DM0'..'C3DM5','C3RA1'..'C3RA3','C3ZC1','C3BH1','C3BH2') -- 12 个溢流相关 RS_CODE 白名单
  AND D.GLASS_START_TIME BETWEEN :start AND :end
```

- 设备白名单在 WHERE 中引用 LEFT JOIN 表 H，实际语义等同 INNER JOIN（原 SQL 语义，保留）。
- `PANEL_ID = SUBSTRING(RS_DEFECT_IMAGE_NAME, 57, 14)`：已用真实样例验证，提取结果为 `GLASS_ID + 2 位 panel 码`（如 `L3N464E03182CA`）。
- `PANEL_LOCATION`：由图片名位置后缀映射为 TOP/BOTTOM/LEFT/RIGHT/四角；`SERACH1` 变体对 C3DM% 与非 C3DM% 用不同解析（后者映射 KONGLEFT/KONGTOP/...），BOTTOM 另按 B0~B9 细分为 BOTTOM0~9（UNION ALL 第二支）。
- 原图 URL：`'http://10.73.17.41/IMG_WEB/V3/' || RS_DEFECT_IMAGE_NAME`。
- `CODE_RATIO = COUNT(RS_CODE) OVER (PARTITION BY GLASS_ID, RS_CODE) / COUNT OVER (PARTITION BY GLASS_ID)`，ROUND 3 位。

## 4. 与原 SQL 的必要偏差（已验证）

1. **`GLASS_START_TIME` / `CUT_START_TIME` 是 timestamp 类型（非文本）**。`CYCLE`/`PICI` 数据集中的 `EVENT_TIME <> 'NaT'` 仅适用于 varchar 的 `EVENT_TIME`；对 timestamp 列做 `<> 'NaT'` 会直接报错（探查中已复现 `invalid input syntax for type timestamp: "NaT"`）。复刻时：timestamp 列不做 NaT 判断；`EVENT_TIME`（varchar）保留 `::TIMESTAMP` + `<> 'NaT'`。
2. **`DWR_MES_PRODUCTSPEC`（基础表）与 `DWR_MES_PRODUCTSPEC_V`（视图）是两张不同的表**：基础表有 `PRODUCTSPECNAME/PRODUCTCODE` 但无 `FACTORY/PROD_ID`；视图反之。明细 JOIN 用基础表，筛选项用视图，不可互换。
3. 表名大小写/schema 前缀无需变体，`EDA.` / `DWR_MES_` 大写形式直接可用。

## 5. 样式截图中有但 SQL 未提供数据集的筛选项

| 筛选项 | 推断来源 | 状态 |
|---|---|---|
| 线体（cmcbLine） | `SUBSTRING(H.SUB_EQUIP_ID, 1, 6)`，可由设备白名单推导：`3CEE01`/`3CEE02`/`3CEE04` | ✅ 可派生，无需独立数据集 |
| 设备（cmcbSubProdID） | 5 台 IJP 设备白名单即全集 | ✅ 常量 |
| CODE（cmcbCode） | 12 个 RS_CODE 白名单即全集 | ✅ 常量 |
| 边框（cmcbPanelLocation） | PANEL_LOCATION 枚举：TOP/BOTTOM/LEFT/RIGHT/LEFTTOP/RIGHTTOP/LEFTBOTTOM/RIGHTBOTTOM | ✅ 常量 |
| Target值 | 图上未见对应 SQL；FineReport 中通常作为占比图的预警参考线参数 | ⚠️ 无数据源，复刻为图表 Target 参考线输入 |
| 工单类型 | `SEARCH_SUBPRODTYPE` 已覆盖 | ✅ |

## 6. 结论

全部数据源均已验证可访问，无需降级数据源。复刻所需输入：
- 时间戳列直接绑定参数（`>=` / `<=`，闭区间，与原 SQL 一致；`SERACH_BYDAY` 起始时间向前扩 7 天）。
- 所有 `IN` 过滤使用绑定参数（expanding），不使用 FineReport 字符串拼接。
- 核心明细 30 天窗口约 30 万行（白名单过滤后），聚合图在 SQL 侧完成 GROUP BY；明细表需分页/限量展示。

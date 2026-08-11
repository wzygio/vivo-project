# Findings — AOI_TT 报表

## 数据源（详见 references/domain/aoi_tt/spec-data_source.md）

- TT 明细：`eda.spc_tzbjx_{array,oled,tsp}`，三元组同 SPC/RS 惯例；含 `lot_id`。
- TT 参数识别：规格表 `mdw.dwd_imp_dv_param_spec` 中 `param_type IS NULL`（72 行 = TDSUM/DSUM_L/DSUM_O/TOTAL_O_L × 8 产品 × 7 站点）。
- AOI 站点：ARRAY 11620/12620/13620/15620/18620；OLED 21320（DSUM_L+DSUM_O 双参数）；TP 43620。
- 每 (step, sheet, param) 恰 1 行；param_value 为整数缺陷计数。
- 分母：过货视图无 AOI 站点记录 → 测量表自身 distinct sheet（每片必测已验证）。
- 规格：usl/ucl（越小越好），(prod,step,param) 唯一，无 factory/type_flag 维度。
- 产品字典 `mdw.dwr_mes_productspec` 同 RS 链路。

## 与任务文档/RS 的偏差

1. 文档"param_name：RS Code代码"为笔误——TT 明细 param_name 是 TT 参数名。
2. 无过货视图分母（RS 口径不可用）→ 自含分母，少一路查询。
3. 规格无 type_flag → USL/UCL 三图共用；By Lot 图上规格线会因 lot 加和量级而贴近轴底，属规格唯一口径的固有表现。
4. TOTAL_O_L 有规格无测量（疑 DSUM_L 旧名），指标由明细驱动不出现。

## E2E 经验

- Streamlit 多选下拉第一个 option 可能是"全选"伪选项，E2E 须按名称显式选。
- 级联筛选（站点→Code 自动全选）存在重跑竞态：须等目标 combobox 的 `Selected ...` aria-label 出现后再操作查询按钮。

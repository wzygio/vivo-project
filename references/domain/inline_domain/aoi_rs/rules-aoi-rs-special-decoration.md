# AOI_RS 专用修饰：厂别、日期与顺序处理

核验日期：2026-10-10。普通单边规格修饰由 [Inline 概览第7.5节](../shared/overview-inline.md#75-aoi_rs按图表口径的单边-spec-截断) 介绍；本文负责指定厂别和日期内的专用规则。

## 1. 配置与日期范围

```yaml
aoi_rs:
  special_decoration:
    factories:
      - "OLED"
    start_date: "2026-09-21"
    end_date: "today"
```

厂别集合为空时关闭专用规则；未选厂别和窗口外图点沿用普通单边修饰。日期为报表显示日期，首尾均包含，结束日覆盖全天，`today` 为服务器当日。省略日期时默认 2026-09-21 至当天。无效日期、反向区间和错误配置结构报错。

这是修饰窗口，不是数据排除窗口：区间外数据仍参与报表。全域 `data_exclusion` 先在基础设施排除命中的数据，见 [日期排除规则](../shared/rules-inline-date-exclusion.md)。

## 2. 专用处理顺序

| 阶段 | 规则 |
|---|---|
| Sheet | 以聚合图点的 `first_start_time` 判定日期。超出 `SHEET_ID` / `GLASS_ID` 规格的值修饰为 `[0, 0.5 × spec]` 内的稳定哈希值；仅支持非负有效规格。没有越过原规格的 Sheet 不执行这一步。 |
| 明细投影 | 按各 Sheet 原始明细计数占比分配修饰后总量，不修改源快照。 |
| Lot | 使用 Sheet 修饰后的计数重算 Lot，保留原过货分母；仍超过 `LOT_RATIO` 的 Lot 归零，其对应 Sheet 同步归零。 |
| 周/日趋势 | 使用专用修饰后的明细计算月基准，再将符合日期窗口的周/日密度限制在所属月份密度的1.3倍，同时令周期 `rs_qty = value × sheet_qty`。月密度不执行限幅。 |

顺序为 Sheet → 明细投影与 Lot 重算 → Lot 归零 → 周/日限幅，不能交换。修饰只改报表投影，不回写数据库或原始 RS 快照；过货分母不因这些数值修饰改变。

## 3. 日期边界与决策

- Sheet 按该图点首次事件时间归属窗口，同一 Sheet 的明细按这一归属整体投影，避免不同时间行部分缩放导致总量不一致。
- 明细投影按源 Sheet 的首次时间判定窗口；窗口内被 Delete 的 Sheet 即使已从图点中移除，其源明细也会在投影时剔除，不会重新进入趋势统计。
- Lot 只有全部缺陷事件均在窗口内才应用专用处理。跨窗口 Lot 沿用普通规则，避免专用归零连带改动窗口外 Sheet；其中窗口内 Sheet 仍可执行 Sheet 专用修饰。
- 周周期必须从周一至本行截止日全部位于窗口内才限幅；部分跨窗口周保留原周期统计。日行按当天判定。
- 跨月周的月基准仍取截至报表查询日的周期结束日所属月份；月基准包含允许参与报表的全月数据，不按专用窗口裁剪成局部月。
- `flag=True` / 无决策允许修饰，False 保留当前阶段值，Delete 删除该图点；参数豁免继续阻止自动数值修饰。Sheet 与 Lot 决策独立。按既有规则，允许 Lot 归零时会同步归零其 Sheet。
- 首次时间归属窗口外的 Sheet 明细不接收专用数值投影；它们的 Lot/Sheet 图点仍沿用普通规则。显示时间转换和日期排除均已在基础设施完成，不在 Core 重复前推。

## 4. 调用、缓存与验证

ConfigLoader 解析 `(start_date, resolved_end_date)`；`AoiRsReportService` 把日期元组、厂别集合及规则版本放入原生报表缓存键，并传给 `prepare_aoi_rs_decoration()` 和纯 Core 规则。修改开始日、结束日或跨天后无需等待 TTL 重建。页面与 PDF 共用 `build_aoi_rs_period_trend()`，读取同一配置。

主要实现：`core/aoi_rs/aoi_rs_special_decoration.py`、`application/aoi_rs/decoration_service.py`、`application/aoi_rs/aoi_rs_service.py`。

`test_aoi_rs_special_window.py` 覆盖日期首尾、区间外常规处理、源数据保留、窗口内 Delete 明细剔除、跨窗口 Lot 及部分周期；应用服务测试验证窗口变化触发缓存失效，原顺序修饰测试继续验证 Sheet→Lot→周期关系。

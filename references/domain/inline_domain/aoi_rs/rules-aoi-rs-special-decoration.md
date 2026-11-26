# AOI_RS 专用修饰的厂别、产品、日期与处理顺序

更新日期：2026-11-26。专用修饰按厂别、产品和报表日期生效。普通单边规格修饰见 [Inline 概览第7.5节](../shared/overview-inline.md#75-aoi_rs按图表口径的单边-spec-截断)。

## 1. 配置与日期范围

```yaml
aoi_rs:
  special_decoration:
    enabled: true
    sheet_threshold_ratio: 0.5
    sheet_replacement_ratio: 0.25
    factories:
      - "OLED"
    products_by_factory:
      ARRAY: ["Z622"]
    start_date: "2026-09-21"
    end_date: "today"
```

`enabled` 默认 `false`，仅显式设置布尔值 `true` 才启用专用规则。`factories` 列出整厂启用的厂别；启用时省略该字段沿用 OLED 全产品的默认范围。`products_by_factory` 按厂别列出启用的产品。一个厂别同时出现在两处时，以产品清单为准，空清单表示该厂别停用。仅按产品配置时可将 `factories` 设为 `[]`。厂别和产品代码匹配时忽略大小写与首尾空格。

当前启用 **OLED 全产品**及 **ARRAY-Z622**，其余组合沿用普通单边修饰。日期采用报表显示日期，首尾均包含，结束日覆盖全天，`today` 为服务器当日。省略日期时默认 2026-09-21 至当天。窗口外图点沿用普通单边修饰。无效日期、反向区间和错误配置结构报错。

两个比例位于 `config/domain/inline_domain/aoi_rs.yaml` 的 `aoi_rs.special_decoration` 下：`sheet_threshold_ratio` 为 Sheet 触发阈值相对于规格的倍数，默认 `0.5`；`sheet_replacement_ratio` 为稳定哈希值上限相对于规格的倍数，默认 `0.25`。省略时采用默认值；两项都必须是有限非负数，不接受布尔值、字符串、空值、负数或非有限数。触发条件为严格大于阈值，恰好等于阈值时保留原值。哈希种子沿用产品、厂别、站点、RS Code、图类型与图点 ID，改变上限比例只缩放相同图点的哈希值。

这是修饰窗口，不是数据排除窗口：区间外数据仍参与报表。全域 `data_exclusion` 先在基础设施排除命中的数据，见 [日期排除规则](../shared/rules-inline-date-exclusion.md)。

## 2. 专用处理顺序

| 阶段 | 规则 |
|---|---|
| Sheet | 以聚合图点的 `first_start_time` 判定日期。超出 `SHEET_ID` / `GLASS_ID` 规格 0.5 倍的值修饰为 `[0, 0.25 × spec]` 内的稳定哈希值；两个倍数分别由 `sheet_threshold_ratio`、`sheet_replacement_ratio` 配置。仅支持非负有效规格，没有越过配置阈值的 Sheet 不执行这一步。 |
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

ConfigLoader 按查询产品解析生效厂别、日期窗口和规格比例。`AoiRsReportService` 将这些值和规则版本放入该产品的报表缓存键，并传给 `prepare_aoi_rs_decoration()` 和 Core。产品的生效厂别、比例、窗口或解析后的结束日变化时，按新的缓存键计算。页面、PDF 和周报共用 `build_aoi_rs_period_trend()`，趋势预警也使用对应产品的专用明细投影和限幅。AOI_RS 周期监控按产品记录生效厂别及其他规则参数。仅改变产品清单且该产品的生效厂别不变时，其历史策略签名保持一致。

主要实现：`core/aoi_rs/aoi_rs_special_decoration.py`、`application/aoi_rs/decoration_service.py`、`application/aoi_rs/aoi_rs_service.py`。

`test_aoi_rs_config.py` 验证产品清单、旧式厂别列表、比例默认值和非法配置。`test_aoi_rs_sequential_decoration.py` 与 `test_aoi_rs_special_window.py` 分别验证顺序处理和日期边界。应用服务测试验证同一厂别内的产品隔离、趋势限幅及缓存重建。周期监控测试验证产品清单变化仅影响相关产品的历史策略签名。

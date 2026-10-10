# Inline 厂别与日期排除规则

核验日期：2026-10-10。配置入口为 `config/domain/inline_domain.yaml` 的 `data_exclusion`。

## 1. 执行边界与顺序

**逐条事实的日期排除由 infrastructure 执行。对外数据端口返回的明细已排除命中记录，随后才进入去重、Sheet 特征、OOS/OOC 修饰、趋势和 CPK/CPM 计算。** Application 编排这些步骤，Core 不再负责逐条事实的日期排除。

```text
数据库 / 原始快照 / 已维护的工作簿
  → 转换为报表显示时间（源快照仍保留源时间）
  → 应用报表时间截止
  → infrastructure 按厂别和显示日期排除
  → 清洗、最新点位去重、异常点规则、主制程追溯
  → 修饰前 Sheet 特征与 OOS/OOC 判定
  → 点位修饰与特征重算
  → 周期统计、趋势、CPK/CPM、报警及过货分母
```

已维护的异常历史与 Excel 明细本身使用显示时间，读取时直接排除，不重复前推。共享制备与 TT 适配器在去重入口还会校验、过滤输入，保证排除发生在最新点位选择之前。例如有效旧测量和排除期间的复测具有同一去重键时，复测先被剔除，旧测量仍可参与后续计算。

共享实现位于 `src/inline_domain/infrastructure/shared/date_exclusion.py`。纯函数 `exclude_inline_factory_dates()` 接受显式策略并返回副本；`apply_inline_date_exclusion()` 在每次读取时获取当前配置。输入必须具备 `factory` 和对应事件时间列；启用策略而有效非空输入缺列时抛错，不静默放行。

## 2. 配置语义

- `factories` 为厂别集合，匹配时忽略大小写和两端空格；空集合关闭排除。
- `start_date`、`end_date` 按显示日期匹配，首尾日期均包含，覆盖结束日全天。
- `end_date: today` 由配置加载器解析为服务器当日，解析后的日期参与缓存键。
- 只有厂别和日期同时命中才排除；其他厂别及区间外记录保留。
- 无法解析的时间不能判定为命中；后续各自的无效时间清洗规则仍生效。

当前配置排除 OLED、TP 的 2026-10-01 至 2026-10-06。若启用 `offset_days=4`，显示 10 月 1 日对应的源日期为 9 月 27 日；匹配显示日期，而非直接匹配源日期。

## 3. 覆盖的数据入口

| 入口 | 排除时机与时间字段 |
|---|---|
| 共享测量快照（SPC / CTQ / Monitor / AOI_TT） | 新取数、快照命中、强刷和断库回退统一在输出投影过滤 `start_time`；共享制备在去重前过滤 `sheet_start_time`。 |
| AOI_TT 单片明细 | 规格匹配、复测选择和 TT 修饰之前过滤 `start_time`。 |
| Particle Size 真实计数 | 在 SQL 的 `MIN` / `COUNT` 聚合之前，将允许的显示日期窗口拆开并转换为源窗口查询；结果输出再按 `start_time` 过滤。 |
| AOI_RS 缺陷及过货明细 | 两个快照仓储入口均在显示时间转换后过滤 `start_time`，保证分子和分母同口径。 |
| OOS / OOC 历史 | 各 scope 的事件时间列在仓储读取和更新的返回值中过滤。 |
| 过货历史 | 仓储返回前过滤 `event_date`。 |
| 异常工作簿与回退明细 | Infrastructure 读取适配器按实际事件时间列过滤；可配置文件名不参与业务时间列推断。 |
| 报废数据 | 推断厂别并转换显示时间后过滤 `sheet_start_time`。 |

新生成 OOS/OOC 明细只使用允许的输入，因此排除记录不会先生成异常再依靠页面筛选隐藏。缓存键仍包含解析后的排除策略；AOI 页面工作簿缓存也包含策略，修改配置无需等待工作簿修改或 TTL 到期。

## 4. 持久化与周期汇总边界

排除作用于报表消费的数据副本，不删除数据库记录、原始 Parquet 或人工决策。历史更新使用未过滤的旧历史合并覆盖窗口，避免读取投影导致覆盖窗口之外的源历史被误删。原有覆盖窗口替换和生成明细刷新规则继续有效；这不承诺重新生成的产品明细保留所有旧行。历史快照元数据的行数描述源文件，而非过滤后的返回行数。

逐条事实排除与已维护的周期汇总不同：只有周/月/季/年数值而没有逐条时间的记录，不能直接调用日期过滤器重新拆分。Core 中保留 `exclude_inline_period_records()`，用于这些周期记录的可用性判定；既有汇总补零、当前周或上一完整周处理及历史年季月保留规则继续执行，详见 [报警汇总口径](overview-inline.md#76-monitor报警过滤汇总与矩阵显示) 与 [CPK/CPM 汇总规则](../monitor/rules-cpk-summary-workbook.md)。

SPC 的 OOS 点位修饰与能力计算先后关系见 [SPC 能力计算规则](../spc/rules-spc-cpk&cpm.md#13-oosooc-与-cpkcpm-的先后关系)。日期排除先于这里的所有 Sheet 判定和点位修饰。

## 5. 验证入口

`tests/unit/inline_domain/infrastructure/shared/test_date_exclusion_boundary.py` 覆盖显示日期首尾、非目标厂别、新取数/缓存/强刷/断库回退、源文件保留、去重前排除、OOS 输入、Particle SQL 查询窗口、OOS/OOC/过货历史、配置文件改名与 AOI 页面缓存策略切换。

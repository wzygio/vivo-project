# SPC 专用点位修饰：规格中央区间与日期窗口

核验日期：2026-10-10。本文负责 SPC 专用点位修饰规则；能力公式由 [Cpk/Cpm 计算规则](rules-spc-cpk&cpm.md) 维护。

## 1. 配置与执行顺序

配置入口为 `config/domain/inline_domain.yaml`。下列以中央50%演示；当前仓库配置为 `central_fraction: 0.7`（中央70%），计算始终服从实际配置：

```yaml
spc:
  point_decoration:
    enabled: true
    central_fraction: 0.5
    start_date: "2026-10-05"
    end_date: "today"
```

`enabled: false` 或旧配置没有该节时沿用传统 OOS 点位修饰。`central_fraction` 必须在 `(0, 1]` 内；日期必须为有效 `YYYY-MM-DD`，结束日期也支持 `today`。首尾日期均包含，结束日覆盖全天；`today` 按服务器当日解析。非法配置报错，不静默切回旧模式。

按点位的 `sheet_start_time`（报表显示时间）判定专用窗口。先执行基础设施[日期排除](../shared/rules-inline-date-exclusion.md)、清洗、去重和异常点规则，再计算修饰前特征与异常事实，随后串行执行两个修饰阶段：

```text
原始点位 → 传统 OOS 修饰 → SPC 专用中央区间修饰 → 重算 Sheet 特征 → CPK/CPM
```

第一阶段在全部允许参与报表的日期内，仅将越过原 USL/LSL 的点移入原规格区间。第二阶段以第一阶段的输出为输入，只对配置日期内、仍位于中央区间之外的点进一步修饰；中央区间始终由原 USL/LSL 派生。窗口外或专用规则关闭时只执行第一阶段。两个阶段均尊重 False/旧 Delete，不删点，不回写源快照。

## 2. 修饰区间与动作

令 `span = USL - LSL`、`midpoint = (USL + LSL) / 2`、`r = central_fraction`，有效目标区间为：

```text
lower = midpoint - span × r / 2
upper = midpoint + span × r / 2
```

解析器省略比例时默认 `r=0.5`，保留规格跨度中间的50%，与“LSL、USL 各乘0.5”不同。例如 LSL=2、USL=10 时，`r=0.5` 目标为 `[4, 8]`；当前配置 `r=0.7` 对应 `[3.2, 8.8]`。规格限本身仍是 2、10，不被改写。

| 条件 | 行为 |
|---|---|
| 窗口内，传统修饰后的点大于 upper | 第二阶段移入目标区间上侧：`upper - margin`。 |
| 窗口内，传统修饰后的点小于 lower | 第二阶段移入目标区间下侧：`lower + margin`。 |
| 窗口内，传统修饰后的点位于 `[lower, upper]`，含边界 | 第二阶段保留第一阶段输出。 |
| 窗口外或新规则关闭 | 只保留第一阶段传统修饰的输出。 |
| `flag=False` 或 SPC 旧 `Delete` | 保留原值，优先于新旧自动修饰。 |
| `flag=True`、空值或无匹配决策 | 允许修饰。 |
| 缺少有限双边规格，或 USL≤LSL | 保留原点位。 |

### margin 的计算

每个阶段独立计算 margin，跨度和哈希种子取该阶段的目标区间及输入测量值：

```text
span = 当前阶段的目标上限 − 当前阶段的目标下限
seed = prod_code | step_id | param_name | sheet_id | site_name | unit_id | 输入测量值 | side
digest = SHA256(seed 的 UTF-8 编码)
h = int(digest 前12位十六进制字符, 16) / 0xFFFFFFFFFFFF
margin = (0.05 + h × 0.10) × span
上侧输出 = 当前阶段的目标上限 − margin
下侧输出 = 当前阶段的目标下限 + margin
```

`seed` 各字段按代码的字符串表示以 `|` 拼接，缺失值用空字符串；`side` 为 `upper` 或 `lower`。`h` 在 `[0,1]` 内，因此 margin 为当前目标跨度的5%～15%，相同输入重复计算和打乱行顺序后结果一致。

第一阶段使用原规格跨度 `USL−LSL` 和原始点位值；第二阶段使用中央区间跨度 `upper−lower` 和**第一阶段输出值**。若第一阶段改值，第二阶段的哈希也随之改变。margin 不取决于超规距离、标准差或 CPK/CPM；5%～15%的系数目前固定在代码中，未开放配置。

例如原规格 `[2,10]`：传统 margin 为0.4～1.2，上侧输出8.8～9.6、下侧输出2.4～3.2。中央50%为 `[4,8]`：专用 margin 为0.2～0.6，上侧输出7.4～7.8、下侧输出4.2～4.6。因此原值11先进入8.8～9.6，再以这一结果作为输入进入7.4～7.8；原值9在传统阶段保持9，只在专用阶段修饰。不改时间和原规格限，也不强制覆盖人工 False。

因此，目标区间外但原 USL/LSL 内的点也会被修饰，无须先进入 OOS 明细。只触发 OOC 的点同样可能因为越过中央区间而被修饰；这仍是按 USL/LSL 派生区间的规则，不消费 OOC 决策，也不按 UCL/LCL 截断。OOS/OOC 明细继续使用修饰前特征和原规格线判定。

## 3. Sheet 特征与能力输入

`apply_spc_point_decoration()` 返回完整修饰点位；`prepare_decorated_data()` 对同一批点位重算 `sheet_mean`、`sheet_max`、`sheet_min`。SPC 的 Sheet 特征时间窗仍为上周一至查询截止日，历史修饰点位保留用于分布图。

`SpcReportService` 使用这批修饰后特征及对应点位计算上一完整 ISO 周的 CPK/CPM。当前配置下 μ 来自重算后的 Sheet 均值等权平均，σ 来自修饰点位样本标准差；不再次按 OOS/OOC 分类筛点。能力豁免和后续能力台账替换继续执行，公式不改变，也不保证修饰后必然达标。

## 4. 分层与缓存

ConfigLoader 解析为 `(central_fraction, start_date, resolved_end_date)` 的原生元组；Application 传给纯 Core 规则。该元组进入 SPC 报表、共享特征、Monitor SPC 计算及实时监控来源签名，避免比例、日期或 `today` 变化后复用旧数据。

规则入口：[core/spc/spc_point_decoration.py](../../../../src/inline_domain/core/spc/spc_point_decoration.py) 拥有 `SpcPointDecorationPolicy`、`apply_spc_point_decoration()` 和专用中央区间阶段，明确先执行传统截回再执行专用规则。`core/shared/sheet_oos_decoration.py` 只提供通用点位规格与决策关联、布尔规范化和稳定截回能力，不包含 SPC 日期或中央区间规则。

Application 入口仍为 `application/shared/sheet_oos_decoration_service.py`、`application/shared/decorated_data.py`、`application/spc/spc_service.py`，SPC/Monitor 共用同一规则入口；`core/spc/spc_calculator.py` 继续只计算传入的最终修饰样本，不读取修饰配置。

验证入口：`test_spc_point_band.py` 覆盖传统输出作为专用 margin 哈希输入的执行顺序、中央区间、日期首尾、False/旧 Delete、无效/缺失规格、100%目标保留传统输出、稳定性、源数据不变，以及 Sheet 特征与 CPK/CPM 使用相同点位；`test_spc_point_policy_cache.py` 覆盖比例和日期变化穿透两级报表缓存及 Monitor 来源签名。

# SPC 超规片与点位修饰规则

核验日期：2026-10-26。本文负责 SPC 超规片判定、人工修饰决策、传统与专用点位修饰，以及它们与预警过滤的关系；能力公式由 [Cpk/Cpm 计算规则](rules-spc-cpk&cpm.md) 维护，页面预警筛选由 [SPC 数据流](data-flow-spc.md#54-组装全产品能力与单片预警) 维护。

## 1. 配置与执行顺序

配置入口为 `config/domain/inline_domain.yaml`。核验时配置为 `central_fraction: 0.5`（中央50%），计算始终服从实际配置：

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

### 1.1 超规片判定与明细

先对修饰前点位计算 Sheet 特征，再按原规格判断超规。以下比较均为严格不等式，恰好等于规格限不算超规：

| 修饰前 Sheet 特征 | `oos_type` |
|---|---|
| 仅 `sheet_max > usl` | `USL` |
| 仅 `sheet_min < lsl` | `LSL` |
| 两者同时成立 | `USL/LSL` |

`sheet_mean` 不作为单片 OOS 的判定条件。比较不能成立的缺失规格侧不触发对应超规；但“可判定单侧超规”不代表“可执行修饰”，自动改值仍须满足第2节的有效双边规格条件。

超规明细保留修饰前的 `sheet_max`、`sheet_min`、`sheet_mean`、规格和超规类型，并附上修饰决策。点位修饰完成后另行重算报表 Sheet 特征，不将异常明细改成修饰后的正常值。因此，明细仍记载超规与图表点位已修饰可以同时成立。

### 1.2 人工决策与工作簿

工作簿位置由 `config/global.yaml` 的 `resources.inline_domain.files.spc_sheet_oos_decoration` 配置，经现有资源解析器读取；核验时为 `resources/inline_domain/spc/spc_sheet_oos_decoration.xlsx`。工作簿内：

- 产品同名 sheet（如 `M678`）是自动生成的超规明细及有效 `flag`。
- `<产品>__flags`（如 `M678__flags`）保存人工决策，是修饰时读取的决策来源。
- `__refresh_meta__` 保存刷新元数据。通用明细刷新与台账持久化规则见 [Sheet OOS 机制](../shared/rules-sheet-oos-decoration.md)。

人工决策按 `prod_code + step_id + param_name + sheet_id` 四列匹配，该组合下全部点位共用一个决策；`factory`、时间和点位名称不参与决策键。决策直接关联点位，不要求该组合当前仍在超规明细中。规格则按 `prod_code + step_id + param_name` 关联。

SPC 使用布尔决策：`True` 允许自动修饰，`False` 保留原值；无匹配决策或旧记录 flag 为空时默认允许修饰。同键旧决策重复时执行端取最后一条；新上传拒绝重复键、空 flag 和 `Delete`，只接受 True/False 及受支持的兼容表示。旧工作簿中的 `Delete` 在 SPC 执行时按 False 兼容，不删除点位。该规则不同于其他 Inline 模块可能保留的三态删除语义。

下载包含当前明细与独立决策台账，上传只更新决策台账，再经原修饰链路重算。人工决定是否允许修饰，不直接填写替换测量值；替换值由下面的稳定哈希规则确定。

## 2. 修饰区间与动作

令 `span = USL - LSL`、`midpoint = (USL + LSL) / 2`、`r = central_fraction`，有效目标区间为：

```text
lower = midpoint - span × r / 2
upper = midpoint + span × r / 2
```

解析器省略比例时默认 `r=0.5`，保留规格跨度中间的50%，与“LSL、USL 各乘0.5”不同。例如 LSL=2、USL=10 时，`r=0.5` 目标为 `[4, 8]`；若配置为 `r=0.7` 则对应 `[3.2, 8.8]`。规格限本身仍是 2、10，不被改写。

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

## 5. 修饰、能力豁免与预警过滤的边界

以下配置各自生效，不能用其中一项推导另一项的行为：

| 机制 | 配置或决策 | 作用 |
|---|---|---|
| SPC 点位修饰 | `spc.point_decoration` 与人工 Sheet `flag` | 决定点位是否改值，以及专用修饰的区间和日期。 |
| 自动修饰参数豁免 | `auto_decoration.exempt_param_name_contains` | 当前用于 AOI_TT/AOI_RS，不自动扩展 SPC Sheet 修饰范围；具体规则由 [Sheet OOS 机制](../shared/rules-sheet-oos-decoration.md) 的“AOI 参数豁免与有效明细状态”维护。 |
| SPC 能力计算豁免 | `spc.spc_cpk.exempt_param_name_contains` | 命中参数不计算 CPK/CPM，仍保留点位、Sheet 特征和指标供查询。 |
| SPC 页面单片预警过滤 | `spc.sheet_oos_alerts.excluded_param_name_contains` | 从单片预警表及对应自动预警图排除命中参数，不修改 flag 或测量值。 |

SPC 页面先取上一 ISO 周且 `flag=False` 的异常明细，再执行独立参数预警过滤。核验时过滤项为 `PPA`，按参数名普通文本、不区分大小写匹配。因此，即使 PPA 明细的 flag=False，也不显示该单片预警；常规查询仍可查看其数据。具体页面规则见 [预警组装](data-flow-spc.md#54-组装全产品能力与单片预警)。

预警过滤不会反向改变 SPC 两阶段修饰；PPA 是否保留未修饰值仍由 SPC 自己的人工决策和修饰规则决定。CPK/CPM 能力台账修饰则发生在点位修饰和能力计算之后，不反向改写点位，详见 [能力明细与修饰规则](rules-spc-cpk&cpm-decoration.md)。

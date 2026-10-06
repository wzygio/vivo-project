# Q-Time 数据修饰逻辑

## 范围与代码依据

本文记录截至 2026-10-06 的站间 / Lot 级 Q-Time 当前实现。规则入口为
[decoration.py](../../../../src/indicator_domain/core/qtime/decoration.py)，完整监控用例为
[QTimeReportService](../../../../src/indicator_domain/application/qtime/service.py)。
同目录下的 `chamber.py` 与单腔停留报表属于独立逻辑，不使用本文三态决策流程。

本文是开发设计文档。普通页面与下载内容的投影要求由
[非管理员展示边界](../../../../CONTEXT.md#non-administrator-presentation-boundary)约束。
通用职责与快照边界参见
[数据修饰架构](../../feat_design/architecture-data-decoration.md)及
[ADR-0023](../../../../docs/ADR/0023-qtime-local-source-snapshot-and-decoration-boundary.md)。

## 输入、业务键与结果

| 对象 | 字段 / 含义 |
|---|---|
| 源明细 `details` | 修饰所需字段为 `shop, prodcode, f_step, t_step, step_desc, lot_id, timekey, q_spec, wait_time`；其他明细列随报表结果保留 |
| 决策匹配键 | `prodcode + step_desc + lot_id + timekey`，顺序固定；`shop, f_step, t_step` 不参与决策匹配 |
| 决策台账 `decisions` | 四个匹配键及 `flag`，不需要等待时间或规格 |
| 当前超规明细 `decoration` | 上述九个必需字段、计算出的 `over_hours` 及合并后的 `flag` |
| 追溯列 | 非空明细经规格准备后增加 `q_spec_raw, wait_time_raw`，保存进入修饰流程时的仓储输出值 |

键归一化只执行 `fillna("") -> astype(str) -> strip()`，不改变大小写，也不重新解析日期。
仓储先将源 `timekey` 规范为秒级，输出时应用日期前推与截止时间政策，公开键格式为
`YYYYMMDDHHMMSS`。因此决策键使用仓储输出的报表时间；改变时间政策后，原台账键可能
不再命中。源微秒精度保留用于仓储截止时间过滤，不进入此处的业务键。

`apply_qtime_decoration` 返回 `QTimeDecorationResult(details, decoration)`：

- `details`：三态动作处理后的明细，包含保留下来的原有附加列。
- `decoration`：修饰前的当前超规证据及决策，包括 `Delete` 记录；不包含追溯列。

应用监控结果另外返回 `alerts, decisions, decoration_path, data_health`。
各规则从输入复制数据，源数据库与原始 Parquet 不被修饰或删除。

## 完整执行顺序

```text
仓储输出 raw_details + 人工 decisions
                  |
                  v
apply_qtime_spec_overrides        保存追溯列，应用可选路径规格
                  |
                  v
build_qtime_oos_detail            按有效规格识别超规候选
                  |
                  v
_merge_decisions                  归一化键，合并 flag，未命中默认 True
                  |
                  v
apply_qtime_decoration
        |                               |
        v                               v
三态处理后的 details              decoration（未修饰的超规证据）
        |                               |
        v                               v
可选 constrain_qtime_display       build_qtime_alerts（只选 False）
        |                               |
        v                               v
监控 details                       监控 alerts
```

`get_report()` / `get_current_report()` 仅返回仓储数据；上述步骤发生在
`get_current_monitoring()`，缓存入口复用该监控用例。
监控窗口为上月 1 日零点至 `as_of` 次日零点的半开区间，默认 `as_of` 为服务器当天；
实际仓储输出还受项目当日截止时间政策限制。
`data_health` 从仓储输出传递到监控结果以及 `details`、`alerts`，修饰不会提升源健康状态。

## 规格准备与超规识别

### 可选路径规格覆盖

`apply_qtime_spec_overrides(details, overrides)` 在非空明细上先保存追溯列，
即使 `overrides={}` 也执行。路径按 `shop/f_step/t_step` 的字符串精确匹配，
例如 `OLED/21200/21300`；只修改命中记录的 `q_spec`。

每个覆盖值经 `float()` 转换，必须有限且大于零，否则抛出 `ValueError`。
空输入直接返回副本，不执行覆盖值校验。此函数假定非空输入包含路径、规格及等待时间列。
追溯列记录的是本次调用前的值；重复调用会重新赋值，不能用它反复覆盖来恢复最初来源。

当前[领域配置](../../../../config/domain/indicator_domain.yaml)没有配置 `qtime.spec_overrides`，
生产组合根因此传入空映射；这里记录的是已实现的可选能力。

### 超规候选

`build_qtime_oos_detail` 将 `q_spec`、`wait_time` 转为数值，转换失败视为缺失，然后筛选：

```text
q_spec > 0 且 wait_time > q_spec
over_hours = round(wait_time - q_spec, 6)
```

等待时间等于规格不算超规。零、负数或缺失规格，以及缺失等待时间，不产生候选。
原有 `over_hours` 不复用，而是重算。空输入或缺少必需列时，候选函数返回固定列的空表。
这一容错只属于候选函数，完整应用用例的规格准备仍要求完整输入结构。

候选函数未额外检查无穷数；其正规格判定与后面的展示限幅有限性检查不同。

## 决策合并与三态动作

### flag 归一化

| 输入 | Core 归一化结果 |
|---|---|
| 去空格、忽略大小写后为 `delete` | `Delete` |
| 缺失值或未命中台账 | `True` |
| 布尔值 | 保留原值 |
| `false, 0, no, n, 否, 不修饰, 不截断` | `False` |
| 其他字符串 / 可转为字符串的值 | `True` |

数值也走字符串判断：整数 `0` 转为 `"0"` 后是 `False`，浮点 `0.0` 转为
`"0.0"` 后却是 `True`。上传校验允许数值 0/1，但当前归一化没有统一数值类型；
不能把“允许上传”理解成所有零值都能正确表达 `False`。这是现有实现细节，本文未改变它。

台账不存在、为空、缺 `flag` 或缺任何匹配键时，当前候选全部默认 `True`。
台账键归一化后只保留四键与动作；同键重复时保留最后一条。系统只匹配当前候选，
台账中的历史或当前非超规记录不产生动作，也不进入当前候选明细。

### 动作效果

| flag | 三态阶段 `details` | `decoration` | `alerts` |
|---|---|---|---|
| `True` | 等待时间替换为确定性的规格内值 | 保留修饰前超规值及动作 | 不进入 |
| `False` | 保留等待时间；后续可被独立展示限幅改变 | 保留修饰前超规值及动作 | 进入 |
| `Delete` | 从当前报表明细排除 | 仍保留候选及动作 | 不进入 |

动作按四键左合并回完整明细，要求 `many_to_one`。当前超规明细没有按键去重，因此
若超规候选存在重复四键，合并会报错，不会静默选择一条。同键的其他明细也会匹配动作；
四键的区分能力是当前实现的前提，代码没有把厂别或源微秒补进键。
空明细或无候选时，直接返回明细副本与空候选，不执行动作合并。

## 确定性等待时间与独立展示限幅

### 规格内数值公式

`_decorated_wait_time` 使用固定四键顺序生成稳定值：

```text
seed = prodcode + "|" + step_desc + "|" + lot_id + "|" + timekey
fraction = int(SHA256(UTF-8(seed)).hexdigest()[:12], 16)
ratio = 0.85 + fraction / 0xFFFFFFFFFFFF * 0.1
value = min(round(q_spec * ratio, 6), math.nextafter(q_spec, 0.0))
```

倍率落在 `[0.85, 0.95]`，取六位小数，并用浮点相邻值确保结果严格小于有效正规格。
相同键与规格得到相同结果；键或规格改变时结果可改变。种子不包含等待时间、厂别或
源微秒，不使用进程随机状态。例如规格为 24 小时时，修饰值约为 20.4～22.8 小时。

### constrain_qtime_display

这是三态处理之后的独立步骤，条件为：

```text
0 < q_spec < +inf 且 wait_time >= q_spec
```

命中行使用同一确定性公式。`False` 行以及恰好等于规格、没有进入超规候选的行也会限幅；
`Delete` 行在前一步已被排除。其他行保留等待时间，追溯列保持不变。
规格或等待时间转数值失败的行不命中；存在命中行时整列等待时间转为浮点数。

服务构造参数默认 `constrain_display=False`，但当前生产配置为
`qtime.constrain_display: true`。预警从 `decoration` 取 `False` 行并按 `timekey`
倒序稳定排序，完全不依赖展示限幅后的 `details`。

以规格 24 为例，当前配置下：

| 等待时间 / 决策 | 候选 | 监控 details | 预警 |
|---|---|---|---|
| 25 / 无决策 | `True` 候选 | 约 20.4～22.8 | 无 |
| 25 / `False` | `False` 候选 | 约 20.4～22.8 | 保留等待时间 25、超规 1 |
| 25 / `Delete` | `Delete` 候选 | 排除 | 无 |
| 24 / 无决策 | 无 | 约 20.4～22.8 | 无 |
| 10 / 无决策 | 无 | 10 | 无 |

## 台账上传、下载与持久化

上传校验与下载 payload 在
[decoration_service.py](../../../../src/indicator_domain/application/qtime/decoration_service.py)，
存储适配在
[decoration_repository.py](../../../../src/indicator_domain/infrastructure/qtime/decoration_repository.py)。
当前资源路径由组合根及配置解析为 `resources/indicator_domain/qtime/qtime_oos_decoration.xlsx`。

- 上传优先读取 `决策台账` sheet；不存在该名称时读取第一个 sheet。
- 必须包含四键与 `flag`；键按相同规则归一化，重复键直接拒绝。当前未额外拒绝空键。
- 上传的 flag 必须是布尔值、数值 0/1，或合法动作词：
  `true, 1, yes, y, 是, 修饰, 截断, false, 0, no, n, 否, 不修饰, 不截断, delete`。
  缺失、空字符串或其他值拒绝。上传校验比 Core 的宽松归一化更严格。
- 校验失败返回 error，不保存；成功后替换工作簿中的 `决策台账` sheet，保持其他 sheet。
  上传台账整体替换该 sheet，不与旧台账自动合并。
- 下载生成两个 sheet：`当前超规明细` 为当前 `decoration`；`决策台账` 先拼当前候选键与
  旧台账，再按四键保留最后一条，所以旧人工决策优先，并保留窗口外的历史决策。
- 查询仅加载台账并计算当前候选，不把当前超规明细自动写回磁盘。下载返回 bytes；
  保存发生在有效上传时。
- 读取时补齐缺失台账列，Core 再执行归一化；这不等同于重新走严格上传校验。
  工作簿读写失败通过 `QTimeDecorationAccessError` 暴露，不伪装为没有决策。

## 缓存与验证入口

[cached_monitoring.py](../../../../src/indicator_domain/application/qtime/cached_monitoring.py)
缓存原生 payload，缓存外重建监控结果。键包含厂别、站点、产品、归一化后的 `as_of`、
决策工作簿的 `mtime_ns + size`、计算版本，以及服务源签名；服务签名还包含
`constrain_display` 与排序后的规格覆盖映射。TTL 读取全局配置。
决策文件 stat 变化后使用新缓存条目；这属于文件属性签名，不是内容哈希。

厂别级共享入口一次获取全站点与全产品结果，供 Q-Time 页面与预警矩阵复用。
源 Parquet 保留源事实与源时间，修饰及展示限幅结果不写入源快照。

以下现有测试是对应行为的验证入口；本文没有增加或改变运行逻辑：

| 检查内容 | 现有测试 |
|---|---|
| 超规、三态、路径规格、False / 等规格展示限幅、输入保留 | [Core decoration](../../../../tests/unit/indicator_domain/core/qtime/test_decoration.py) |
| 只筛选确认超规的 False 行 | [Core alerts](../../../../tests/unit/indicator_domain/core/qtime/test_alerts.py) |
| 上传动作解析、下载预填及历史保留 | [Decoration service](../../../../tests/unit/indicator_domain/application/qtime/test_decoration_service.py) |
| 监控编排、两种限幅开关、预警证据及上传保存 | [Report service](../../../../tests/unit/indicator_domain/application/qtime/test_service.py) |
| 台账读写往返 | [Decoration repository](../../../../tests/unit/indicator_domain/infrastructure/qtime/test_decoration_repository.py) |
| 决策文件 stat、监控缓存与刷新入口 | [Cached monitoring](../../../../tests/unit/indicator_domain/application/qtime/test_cached_monitoring.py) |

这些测试覆盖的范围有限；例如重复超规键、未知 flag 的宽松归一化与无穷数行为，
上文按当前源代码记录，不能据此声称已有完整测试覆盖。

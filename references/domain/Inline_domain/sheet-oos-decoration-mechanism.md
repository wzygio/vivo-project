# Inline Sheet OOS 数据修饰机制

> 分析对象：`src/inline_domain/core/shared/sheet_oos_decoration.py` 及其 SPC/CTQ/Monitor 调用链  
> 重点文件：`resources/inline_domain/spc_sheet_oos_decoration.xlsx`  
> 核验日期：2026-08-18

## 1. 结论

Sheet OOS 修饰不是对源数据库或 Inline Parquet 快照的回写，而是在报表计算期间对内存中的点位测量值执行三态处理：

- `True`：把越过规格线的点位值确定性地截回规格区间内；
- `False`：保留真实越规值；
- `Delete`：从当前报表数据集中删除对应产品、站点、参数、Sheet 的全部点位。

`resources/inline_domain/spc_sheet_oos_decoration.xlsx` 同时承担两个职责：

1. 保存当前查询窗口内识别出的 Sheet 级 OOS 明细；
2. 保存用户针对这些明细维护的 `flag` 决策。

该文件不是只追加的历史台账。共享计算缓存未命中时，系统会以“本次查询窗口内的当前 OOS 集合”为准重建当前产品 sheet：仍然存在的记录按业务键继承旧 `flag`，新增记录默认 `True`，已不在当前 OOS 集合中的记录会从该产品 sheet 消失。

## 2. 所属边界与调用关系

```text
SPC 页面 / CTQ 页面 / 自动预警
        │
        ▼
应用服务（spc_service / ctq_service / monitor_service）
        │
        ▼
fetch_decorated_features                 Streamlit L2 缓存
        │                                key: 产品、scope、日期窗口、快照签名
        ▼
prepare_decorated_data
        ├── 原始点位数据 + 规格 → 原始 Sheet 特征
        ├── prepare_sheet_oos_decoration
        │     ├── 识别当前 OOS 明细
        │     ├── 读取并合并工作簿中的人工 flag
        │     ├── 缓存 miss 时替换当前产品 sheet
        │     └── 对点位值执行 True / False / Delete
        └── 用修饰后的点位重新计算 Sheet 特征
              │
              ▼
        SPC/CTQ 图表、CPM/CPK、自动预警
```

主要所有权如下：

| 模块 | 职责 |
|---|---|
| `application/shared/decorated_features.py` | 共享缓存边界；按 `scope` 路由 SPC/CTQ/免修饰口径 |
| `application/shared/decorated_data.py` | 先计算原始 Sheet 特征，执行修饰，再重算特征 |
| `core/shared/sheet_oos_decoration.py` | OOS 识别、工作簿合并、三态处理、确定性截断 |
| `shared_kernel/utils/excel_tools.py` | 指定 sheet 的读取与替换；企业加密 Excel 的 COM 回退 |
| `app/sections/spc/spc_dashboard.py` | 管理员下载、上传、覆盖和缓存刷新入口；CTQ 复用该 UI |

## 3. 工作簿路由

SPC 与 CTQ 共用同一套引擎，但使用不同工作簿，避免两种口径的人工决策互相污染：

| `scope` | 工作簿 | sheet 名 |
|---|---|---|
| `spc` | `resources/inline_domain/spc_sheet_oos_decoration.xlsx` | 产品号，如 `M678` |
| `ctq` | `resources/inline_domain/ctq_sheet_oos_decoration.xlsx` | 产品号 |
| `none` | 不读写工作簿 | 不适用 |

默认运行路径固定到项目根目录的 `resources/`，不是 `resources/<product>/`。测试可以通过 `product_dir` 显式覆盖该位置。

自动预警按 `data_type` 分组路由：SPC 组使用 `spc`，CTQ 组使用 `ctq`，AOI 组使用 `none`。因此自动预警与对应 SPC/CTQ 报表在产品、日期窗口和快照签名一致时，可以复用同一条共享计算缓存。

## 4. `spc_sheet_oos_decoration.xlsx` 当前状态

2026-08-18 对真实文件进行只读核验时：

- 文件位于 `resources/` 根目录，大小 118,959 字节；
- 文件头不是标准 xlsx ZIP，也不是 OLE Excel，属于系统支持的企业加密文件；
- 工作簿含 `M673`、`Z517`、`M678`、`Z571`、`M626` 五个产品 sheet；
- 每个 sheet 均使用下文定义的 13 列结构；
- 四列业务键均未发现重复。

| 产品 sheet | 明细数 | `True` | `False` | `Delete` |
|---|---:|---:|---:|---:|
| M673 | 469 | 436 | 33 | 0 |
| Z517 | 0 | 0 | 0 | 0 |
| M678 | 386 | 363 | 22 | 1 |
| Z571 | 609 | 573 | 36 | 0 |
| M626 | 39 | 33 | 6 | 0 |
| 合计 | 1,503 | 1,405 | 97 | 1 |

以上数据仅是检查时快照。文件会在后续缓存 miss、日期窗口变化、产品刷新或管理员上传后改变。

## 5. 明细结构与匹配键

标准列顺序如下：

| 列 | 含义 | 是否参与人工决策匹配 |
|---|---|---|
| `factory` | 厂别 | 否 |
| `prod_code` | 产品号 | 是 |
| `step_id` | 站点/工序 | 是 |
| `param_name` | 参数名 | 是 |
| `sheet_id` | Sheet 标识 | 是 |
| `sheet_start_time` | Sheet 过站/量测时间 | 否 |
| `sheet_max` | Sheet 内点位最大值 | 否 |
| `sheet_min` | Sheet 内点位最小值 | 否 |
| `sheet_mean` | Sheet 内点位均值 | 否 |
| `usl` | 规格上限 | 否 |
| `lsl` | 规格下限 | 否 |
| `oos_type` | `USL`、`LSL` 或 `USL/LSL` | 否 |
| `flag` | 人工修饰动作 | 否，作为被继承的状态 |

人工状态的唯一匹配键为：

```text
(prod_code, step_id, param_name, sheet_id)
```

键列在匹配前会把空值填为 `""` 并统一转成字符串。若旧工作簿存在重复键，读取合并时保留最后一行的 `flag`。`factory`、时间、统计值和规格值变化不会阻止旧 `flag` 被继承；这是当前实现的重要数据语义。

## 6. OOS 明细生成规则

系统先基于未修饰的原始点位数据计算 Sheet 特征，再按以下严格不等式识别 OOS：

```text
upper_oos = sheet_max > usl
lower_oos = sheet_min < lsl
```

- 只越上限：`oos_type = USL`；
- 只越下限：`oos_type = LSL`；
- 同时越上下限：`oos_type = USL/LSL`；
- 等于规格线不算 OOS；
- 数值转换失败或缺少必需列时，不会生成对应 OOS 明细。

输出按 `factory、prod_code、step_id、param_name、sheet_start_time、sheet_id` 稳定排序。

## 7. 明细文件的自动更新机制

### 7.1 触发条件

真正执行工作簿刷新的是 `fetch_decorated_features()` 的缓存 miss。其有效缓存键包括：

```text
(prod_code, scope, start_date, end_date, snapshot_signature)
```

缓存命中时直接返回上次计算结果，不读取也不重写工作簿。SPC 共享特征缓存最多 12 个条目，其 TTL 与上层 SPC 报表缓存的 TTL 统一由 `config/global.yaml` 的 `service_cache.ttl_hours` 段配置（当前均为 4 小时）。以下情况通常会形成 miss：

- 首次访问；
- 产品、SPC/CTQ scope 或日期窗口变化；
- 产品快照签名变化；
- 点击页头“刷新缓存”使当前产品 revision 变化；
- 管理员上传修饰表后执行全局 `st.cache_data.clear()`；
- TTL 到期或缓存条目被淘汰。

仅在文件系统中直接编辑工作簿，不会自动进入上述缓存键。现有缓存仍可能继续生效，必须点击“刷新缓存”或通过上传入口清缓存后才能保证重读。

### 7.2 更新算法

对当前产品 sheet 的更新步骤是：

1. 用原始点位和规格计算当前日期窗口的 Sheet 特征；
2. 从特征中筛出当前 OOS 明细 `detail_df`；
3. 读取工作簿中以产品号命名的旧 sheet；
4. 只取旧表的四列业务键与 `flag`，重复键保留最后一行；
5. 当前明细左连接旧 `flag`；
6. 未匹配到旧状态的新增 OOS 行默认 `flag=True`；
7. 旧表中不再出现在当前明细里的行不会进入新结果；
8. 用合并结果替换工作簿中的当前产品 sheet；
9. 依据合并后的三态动作处理点位数据；
10. 用处理后的点位重新计算 Sheet 特征，供报表和预警消费。

因此，更新行为可概括为：

```text
新产品 sheet = 当前窗口 OOS 明细 LEFT JOIN 旧产品 sheet 的 flag
```

它不是追加、增量同步或历史归档。

### 7.3 对工作簿其他内容的影响

- 标准 xlsx：删除并重建目标产品 sheet，其他 sheet 保留；目标 sheet 原有格式、公式、批注和数据验证不会保留；
- 目标 sheet 重建后通常移动到工作簿末尾；
- 文件不存在：创建仅含当前产品 sheet 的新工作簿；
- 产品 sheet 不存在：按“无旧人工状态”处理，新明细全部默认 `True`；
- 当前没有 OOS：仍会把目标产品 sheet 重建为空表头；
- 工作簿被 Excel 占用导致 `PermissionError`：底层仅记录告警并跳过写入，但上层仍会继续使用本次内存合并结果。

### 7.4 企业加密文件

当前真实 SPC 工作簿属于企业加密文件：

1. 标准读取失败后，系统通过本机 Excel COM 透明读取目标 sheet；
2. 如果 openpyxl 无法打开工作簿，写入路径会通过 COM 读出全部 sheet；
3. 随后删除原文件，并用 openpyxl 整体重写为明文标准 xlsx。

若 openpyxl 和 COM 都无法读取已有文件，系统抛出 `SheetOosDecorationReadError`，不会用空表覆盖不可读文件。SPC 服务会把它包装为 `SpcDecorationFileError`，页面停止并提示用户检查文件后刷新缓存。

需要注意：企业加密回退写入采用“删除原文件后整体重写”，不是临时文件原子替换；同时会改变文件的加密状态和格式属性。

## 8. 三态修饰语义

### 8.1 `True`：确定性截断

`True` 也是空值、未知字符串和新增行的默认解释。只修改超过有效上下限的 `param_value`，线内点保持原值。

对双边规格，设 `span = usl - lsl`，稳定哈希生成 `fraction ∈ [0, 1]`：

```text
margin = (0.05 + fraction × 0.10) × span
上越规新值 = usl - margin
下越规新值 = lsl + margin
```

即新值位于规格跨度向内 5%～15% 的位置。哈希种子包含产品、站点、参数、Sheet、点位、设备、原值和越规方向，所以同一行重复计算得到相同结果，便于复现。缺少任一规格或 `usl <= lsl` 时不截断。

### 8.2 `False`：保留真实值

以下值会解析为 False（忽略大小写并去除首尾空格）：

```text
False, 0, no, n, 否, 不修饰, 不截断
```

对应 Sheet/参数的真实点位值保留，修饰后重算的 `sheet_max`、`sheet_min`、`sheet_mean` 仍会反映真实越规。

### 8.3 `Delete`：从报表数据集中排除

`Delete` 不区分大小写。系统按四列业务键删除匹配的全部原始点位，因此该 Sheet/参数不会进入后续 Sheet 特征、图表、能力计算或自动预警。它不会删除数据库记录、Parquet 快照或工作簿中的其他参数。

## 9. 管理员下载与上传

管理员页面提供以下操作：

1. 下载：把当前内存中的产品修饰表导出为单 sheet 文件，sheet 名为“修饰表”；
2. 上传：默认读取上传文件的第一个 sheet；
3. 校验：仅要求四列业务键和 `flag` 存在；
4. 覆盖：直接替换共享工作簿中的当前产品 sheet；
5. 生效：调用全局 `st.cache_data.clear()` 并 rerun。

上传入口不会严格校验完整 13 列、`flag` 枚举、重复键或产品号是否与当前 sheet 一致。上传的额外列可短暂写入工作簿，但下一次自动刷新会重新收敛到标准 13 列。建议上传前保留下载表结构，只修改 `flag`。

## 10. 数据一致性与已知边界

### 10.1 已有保护

- 读取失败时不会静默把已有用户状态当作空表；
- 更新一个产品 sheet 时保留其他产品 sheet；
- 新 OOS 默认修饰，避免漏处理；
- 截断值由稳定哈希生成，可重复计算；
- 原始测量快照不被修饰结果反向污染；
- 缓存载荷只保存 DataFrame、dict、字符串和标量，符合 ADR-0001。

### 10.2 风险与运维注意事项

| 风险 | 当前表现 |
|---|---|
| 工作簿不是历史台账 | 日期窗口缩短或 OOS 消失时，旧明细会被淘汰 |
| 匹配键不含厂别和时间 | 同四键记录会继承旧 `flag`，即使厂别、时间或规格已变化 |
| 非原子写入 | 标准写入直接保存原文件；加密回退还会先删除原文件 |
| 缺少跨进程锁 | 多个 Streamlit worker 同时缓存 miss 时可能竞争写同一工作簿 |
| 文件占用被吞掉 | 写入跳过但调用方仍拿到内存结果，文件状态与当前页面可能暂时不一致 |
| 目标 sheet 格式丢失 | 自动刷新会删除并重建目标 sheet，只保留表格数据 |
| 上传校验较弱 | 错误产品号、重复键或未知 flag 可能被接受；未知 flag 会按 True 解释 |
| 手工外部修改受缓存影响 | 不清缓存时，页面不保证立即读取新 flag |
| 加密状态可能改变 | COM 读取后由 openpyxl 重写会产出明文标准 xlsx |

## 11. 推荐操作流程

1. 从管理员后台下载当前产品修饰表；
2. 只修改 `flag`，不要修改四列业务键；
3. 关闭本机或共享位置中正在打开的原工作簿；
4. 通过页面上传并点击“确认覆盖并刷新”；
5. rerun 后重新下载，核对目标记录的 `flag`；
6. 对 `Delete` 决策额外确认该参数的 Sheet 明细已从图表和能力计算中消失；
7. 若直接编辑共享工作簿，编辑完成后必须点击页头“刷新缓存”。

如需长期保留历史 OOS 与人工决策，不应依赖当前工作簿；应另建只追加的审计存储，并记录生效时间、操作者、原值、动作和查询窗口。

## 12. 测试与源码索引

现有测试覆盖：

- OOS 明细识别和 `USL/LSL` 分类；
- True 确定性截断及参数专用边界；
- False 保留原值；
- Delete 删除匹配点位；
- 旧 `flag` 继承、新行默认 True、重复键取最后一行；
- 企业加密读取 COM 回退及不可读文件保护；
- 替换当前产品 sheet 时保留其他 sheet；
- SPC/CTQ scope 路由；
- 缓存键对产品、scope、窗口和签名的隔离。

关键源码：

- `src/inline_domain/core/shared/sheet_oos_decoration.py`
- `src/inline_domain/application/shared/decorated_data.py`
- `src/inline_domain/application/shared/decorated_features.py`
- `src/inline_domain/application/spc/spc_service.py`
- `src/inline_domain/application/monitor/monitor_service.py`
- `src/shared_kernel/utils/excel_tools.py`
- `app/sections/spc/spc_dashboard.py`
- `app/components/page_header.py`
- `tests/unit/inline_domain/core/shared/test_sheet_oos_decoration.py`
- `tests/unit/inline_domain/application/shared/test_decorated_data.py`
- `tests/unit/inline_domain/application/shared/test_decorated_features.py`


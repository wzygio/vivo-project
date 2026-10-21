# Inline 周度 PPT 报告输出设计规范

核验日期：2026-10-21。适用于 AOI_RS、AOI_TT 的共用周报输出。

## 1. 目标与范围

将上一完整 ISO 周各产品的 density 写入可编辑的 PPT 表格，并按对应产品与指标的规格判定数字颜色。沿用配置模板的母版、Logo、水印，普通模式可生成和下载，每页最多八款产品。

业务取数与计算继续由各自应用模块负责。RS 的过货分母、TT 的检测片分母、Array 层别及 TT 粒径规则分别见 [RS 周报规则](../../../domain/inline_domain/aoi_rs/rules-aoi-rs-weekly-report.md)和 [TT 周报规则](../../../domain/inline_domain/aoi_tt/rules-aoi-tt-weekly-report.md)，本规范拥有共用输出契约、版式及颜色规则。

## 2. 数据流与职责

```text
各产品的应用服务 ViewModel（当前报告明细与规格）
  → RS / TT 周表组装（提取准确周次、匹配规格、判定）
  → WeeklySummaryTable / WeeklySummaryRow
  → 共用 PPT 写入（分页、文字、颜色、模板资产）
  → 当前会话报告字节 → 独立下载动作
```

| 责任 | 所属模块 |
|---|---|
| 当前报表数据、已启用产品、按产品隔离的查询与缓存身份 | 既有 RS / TT 服务及各自导出 section |
| 固定周次、density、对应规格、数值格式及规格判定 | `application/aoi_rs/weekly_report.py`、`application/aoi_tt/weekly_report.py` |
| 原生周表结构与共用判定/格式函数 | `application/shared/weekly_summary.py` |
| 可编辑 PPT、母版资产、行/列分页与颜色映射 | `app/charts/inline_domain/weekly_summary_ppt.py` |
| 报告分类、单位、文件元数据 | RS / TT 的薄 PPT 包装器 |
| 保护模板规范化及临时资源清理 | `infrastructure/shared/ppt_template.py` |
| 模板原生字节缓存 | `app/sections/inline_domain/shared/weekly_ppt_template.py` |

PPT 写入模块不查询数据库、不读取业务规格文件，也不从展示数字反算 density 或再次判定规格。其唯一业务判定输入为应用层传入的逐产品结果。

## 3. 周度值与规格匹配

- 周窗口固定为上一周一 00:00 至本周一 00:00，右侧不含。使用 ISO 周年，不因空周回退到更早一周。
- density 复用当前折线图的应用层结果；保留当前报表月份上下文。颜色使用完整精度的 density 判定，先判定、后格式化。
- 各产品分别匹配规格；同一个站点或项目在不同产品中的规格不能混用。
- **RS**：复用 `core/aoi_rs/aoi_rs_calculator.attach_spec_values(chart_kind="mwd")`，仅使用 `type_flag=MWD_RATIO` 的 `spec`。匹配键为当前产品范围内的 `factory + step_id + rs_code`，不使用 Lot 或 Sheet 规格代替。
- **TT**：复用 `core/aoi_tt/aoi_tt_calculator.attach_spec_values` 的 `usl`，按当前产品范围内的 `step_id + tt_name` 匹配。USL 为规格上限，UCL 为管控上限，不用于本次规格颜色判定。各粒径沿用当前 TT 图表中该参数的同一 USL。
- 重复规格沿用现有图表匹配函数的组合内最大值口径，不在导出端另取较小值或平均值。
- 当前两类 density 均为单边上限判定：有限数值 `density <= upper_limit` 为规格内，`density > upper_limit` 为超规。等于上限属于规格内，零上限也参与正常判定。
- 规格缺失、无法解析或非有限时，不虚构合格/超规结论；判定为空。数值缺失或非有限时同样不判定。
- 数字格式通常为四位小数并去掉末尾零；极小非零值使用四位有效数字。例如真实值 `1.00000001`、上限 `1`，即使数字格式化后显示 `1`，仍按真实超规结果标红。

## 4. 共用数据契约

`WeeklySummaryTable` 包含厂别、有序产品列表、项目行、周开始日及不包含的周结束日。

`WeeklySummaryRow` 包含站点、层别、项目标识、项目标签及以下两组按产品顺序排列的数据：

| 字段 | 内容与约束 |
|---|---|
| `values: tuple[str, ...]` | 数字文本或业务状态；与 `table.products` 一一对应 |
| `within_spec: tuple[bool \| None, ...]` | `True`：规格内；`False`：超规；`None`：没有可靠判定 |

- `within_spec` 默认空元组，兼容原有只提供文本的调用，文本保持黑色。
- 非空 `within_spec` 必须与完整产品列表等长；长度不一致时拒绝生成，避免颜色串列。
- 所有产品分页使用相同的原始产品下标，同时切分数字和判定。第九款产品不能取用第一款的颜色。
- “无过货”和“数据不足”对应的判定均为空。RS 已知有过货且某 Code 缺陷为零时，以数值 `0` 参与对应规格判定；TT 缺少参数/粒径测量时不补零。
- 输出只包含业务标签、数字及颜色，不将内部判定字段、原始规格对象或其他内部字段写成可见列。

## 5. 颜色与字体

| 内容 / 判定 | 字体颜色 | RGB |
|---|---|---|
| 有效数字且 `within_spec=True` | 绿色 | `008000` |
| 有效数字且 `within_spec=False` | 红色 | `FF0000` |
| 无过货 | 黑色 | `000000` |
| 数据不足、无有效规格或未提供判定的数字 | 黑色 | `000000` |
| 分类、层别、项目标签 | 黑色 | `000000` |
| 表头 | 白色 | `FFFFFF` |

颜色只改变产品单元格的字体，不改变原有底色或数据值。即使调用方错误地给文字状态附上布尔判定，写入层也必须让非数字状态保持黑色。

## 6. 版式与交互

- 模板由全局资源配置解析；RS / TT 可分别配置模板键，当前均指向用户提供的 `template-PPT.pptx`。模板只读，不覆盖原文件。
- 基础页尺寸为 960×540 pt。保留母版与图像关系；输出页写入厂别标题、周次、起止日、数值单位和页码。
- Array 列为“分类、层别、项目、产品…”；OLED/TP 列为“分类、项目、产品…”。同一层别的相邻行可合并层别单元格。
- 蓝色表头、紫色交替底色、白色边线。最多八个产品列；七至八款时表头 13 pt、产品数字 12 pt，其余常规数字 13 pt。项目行按名称预计高度分页，不丢弃项目或覆盖页脚。
- 项目行或产品列分页均重复表头；母版与 Logo 在每页保留，颜色与对应项目、产品一起保留。
- 生成与下载分离，成功报告字节保存在当前会话，可重复下载；失败或异常中断释放忙状态，保留上次完整报告。产品停用后移除包含该产品的旧下载。
- 普通模式只展示业务内容和安全失败提示；技术异常记录在服务日志。

## 7. 模板与资源生命周期

标准 PPTX 直接读取为内存字节。企业保护格式通过本机 PowerPoint 只读打开并另存临时副本，在 Office 关闭前取得标准 Open XML 字节；不能依赖其后可能重新加密的磁盘副本。

模板缓存只存原生 bytes，身份包含路径、mtime 和文件大小；同一路径模板可在 RS/TT 间复用。临时副本仅写入 `output/tmp`，成功、失败及异常中断均清理。只关闭本次打开的文稿，保留用户已有 PowerPoint 会话。

## 8. 验收要求与证据

- 直接通过完整周报 builder 检查最终 PPT 单元格及 RGB，避免只验证内部颜色常量。
- 覆盖规格内、等于上限、超规、零值与零上限、缺少规格、无过货、数据不足、格式化前后数值不同、各产品规格不同和第九款产品的分页颜色。
- 覆盖 RS 的 MWD_RATIO 与 Lot/Sheet 规格隔离，TT 的 USL 与 UCL 区分，以及 Total/子粒径独立输出。
- 保留 TT/RS 的普通模式生成、失败、中断、重复下载、禁用产品、行分页、模板图像与 Office 生命周期回归。
- 颜色文件用例：`tests/unit/app/sections/shared/test_weekly_summary_density_colors.py`。真实模板验证使用合成数据，样例与预览归 `output/test-results/weekly_summary_colors/`，不查询生产数据库。

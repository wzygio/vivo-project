# MWD 指定良损方案评估与 Merge 建议

> 算法的数据流、公式和跨月“平滑”细节见
> [MWD 指定良损算法逻辑](mwd-processor-opt-algorithm.md)。

## 一、评估结论

评估对象：`D:\wzy\Python\vivo-project-mwd`，分支 `feat/mwd-processor-opt`。

结论：**方案方向可取，但当前 worktree 不建议直接 merge 到 master。**

新方案把原来的“Code baseline → EMA → 趋势调节 → 月度对账 → 月/周/日人工覆盖”
改成“指定月度良损 → 确定性日度生成 → 周/月直接聚合”。业务控制入口更集中，
核心流程也更容易说明和维护。原 5 个趋势模块约 1321 行被删除，新引入的
`daily_generator.py` 与 `modifier_table.py` 合计约 528 行；MWD 门面也明显缩短。
因此，该方案确实降低了认知复杂度和维护入口数量。

当前仍需处理的可靠性与集成问题如下：

1. “MWD 与 Mapping 不良数统一”目前只表示共享控制来源和级联前目标水准，
   不保证最终不良数严格相等；
2. 跨月插值只保证逐月归一化前的基线连续，最终日度整数仍可能在月界明显跳变；
3. 分支基于旧 master，且包含多类无关或冲突变更，不能原样合并。

推荐做法是：**从最新 master 新建干净分支，只移植指定良损方案的核心代码、配置、
测试和领域文档，处理本报告剩余阻断项后再合入。**

## 二、复杂度评估

### 2.1 得到改善的部分

- 删除 Code baseline、EMA、TrendRegulator、月度对账和人工覆盖优先级链路；
- 月、周、日只有一个事实来源：最终日度整数；
- 不再维护月、周、日三套覆盖结果的一致性；
- 业务只维护一个月度“指定良损”入口；
- Group 与 Code 分别服从各自 Sheet 的人工指定，业务控制边界明确。

因此，**认知复杂度、配置复杂度和维护复杂度显著降低**。

### 2.2 没有根本改变的部分

`daily_generator.py` 内部仍包含 `allocate_integer_counts`。容量饱和与余数分配逻辑
仍然存在，只是输入从“EMA 后月度对账目标”变成“指定良损月度目标”。废弃的
`allocation.py` 和无调用方的月度对账函数已删除。

从渐进复杂度看，新旧方案都主要按 Code、日期线性处理，并在每个自然月执行整数
分配。一个自然月最多约 31 天，整数分配本身通常不是实际性能瓶颈。

### 2.3 新增的复杂度风险

Mapping 上调采用物理复制 DataFrame 行：

```text
输出行数 ≈ 原始缺陷行数 × 缩放倍数
```

正常业务倍率通常小于 1。实现增加了轻量防御：非有限、负值或超过 10 倍的倍率
记录错误并按 1.0 回退，避免异常配置触发无界复制；未引入额外配置或复杂限流系统。

## 三、剩余审查发现

### HIGH-4：最终 Mapping 与 MWD 并未严格统一

当前代码只统一级联前控制水准。Mapping 仍受最新五批次筛选、批次级联、整数截断
和零基数限制。现有 E2E 甚至记录了上调倍率未改变最终 Mapping 数量。

合入前必须由业务确认以下二选一：

1. 接受“统一指定来源和变化方向”，保留级联最终裁决；或
2. 要求最终计数一致，并重新设计 Mapping 级联与数据范围。

在确认前，“确保不良数一致”的验收描述不可判定为已满足。

### HIGH-5：分支不能按现状直接合并

分支 merge base 为 `1fdbe6b`，评估时本地 master 为 `a891cfb`，master 已包含大量
后续架构和页面改动。`git merge-tree` 显示：

- `.planning/.active_plan`、领域算法文档等文本冲突；
- `codebaseline.xlsx`、`趋势图人工修正.xlsx`、SPC/CTQ 工作簿及 XMind 等二进制冲突；
- 旧 MWD 模块与测试存在多项 modify/delete 冲突；
- feature 提交混入 Inline 文档、规划文件、XMind 和多个无关 Excel 二进制变更；
- master 已有 ADR-0016，feature 又新增 `0016-yield...`，存在 ADR 编号重复。

此外，worktree 还有未提交的程序、文档、Excel 修改和未跟踪工具。这些内容不会随
当前 HEAD 的普通 merge 完整进入 master。

合入前要求：不要原样 merge 或逐提交 cherry-pick。应从最新 master 创建新分支，
只移植经过确认的 MWD、Mapping、modifier 代码、配置、测试和领域文档。

### MEDIUM-1：跨月“平滑”只在归一化前成立

月中锚点线性插值形成的基线在月界连续，但系统随后按自然月独立归一化到各月精确
目标。相邻月份使用不同的归一化系数，最终日度整数仍可能跳变。

固定日投入、关闭噪声、六月目标 0.1%、七月目标 0.4% 的反例为：

```text
6 月 30 日不良数 = 179
7 月 1 日不良数 = 284
相邻日上升约 58.7%
```

详细计算过程和约束优先级见算法文档的
[跨月“平滑”详细逻辑](mwd-processor-opt-algorithm.md#四跨月平滑详细逻辑)。

当前测试只验证插值基线连续，没有验证 `generate_daily_counts` 最终输出的月界连续性。
建议明确业务可接受的最大相邻日跳变并增加最终日度验收测试。若必须严格平滑，需要
在“月度精确合计”和“边界连续”之间定义优化目标。

## 四、测试与验证结果

### 4.1 MWD、Mapping 与写回定向测试

覆盖：

```text
tests/unit/test_daily_generator.py
tests/unit/test_modifier_table.py
tests/unit/test_mapping_monthly_factor.py
tests/unit/test_yield_service_modifier_wiring.py
tests/unit/test_defect_panel_count_alignment.py
tests/unit/test_mapping_layout.py
tests/unit/test_mapping_random_modification.py
tests/unit/test_excel_tools_workbook_sheets.py
tests/unit/test_excel_tools_com.py
```

当前结果：`96 passed, 24 warnings`，未再出现缺 Sheet 触发的 COM fatal exception。

### 4.2 Yield smoke

命令：

```powershell
python tools/smoke.py yield
```

当前结果：`132 passed, 4 failed, 24 warnings`，进程退出码为 1。

4 个失败为当前 worktree 已知基线：

- `test_code_selector_filter.py` 2 项；
- `test_yield_global_data_policy.py` 2 项。

### 4.3 算法补充反例

额外验证结果：

1. 归一化前基线连续不等于最终日度跨月连续；
2. Code 与 Group Sheet 的指定目标分别驱动对应趋势；
3. Mapping 上调可能被后续级联天花板抵消。

## 五、Merge 前置条件

建议满足以下条件后重新评估：

1. 从最新 master 新建干净迁移分支；
2. 只移植本功能相关文件，排除 Inline、XMind、SPC/CTQ 工作簿和无关 planning churn；
3. 处理 ADR 编号冲突；
4. 补充月份标签和重复键校验；
5. 明确“Mapping/MWD 一致”的业务定义并同步 ADR、PRD、测试；
6. 增加最终日度跨月边界测试，而非只测插值基线；
7. 为 backfill 工具增加单元测试、dry-run 证据和可恢复写回策略；
8. 在迁移后的最新 master 上运行干净的 Yield smoke、相关全量单测和浏览器 E2E；
9. merge 前保持 worktree clean，所有业务 Excel 修改必须有明确来源与审核记录。

## 六、最终判断

### 6.1 算法方向

**通过，建议保留并迁移。**

指定良损直接生成月度整数目标、日度作为同一级唯一事实源、Group/Code 分别服从人工
指定，是比旧链路更容易交接和解释的模型。

### 6.2 当前实现

**有条件不通过。**

正常输入下核心测试能够通过，但 Mapping 最终口径和跨月最终结果仍有缺口。

### 6.3 当前分支直接 merge 到 master

**不通过。**

当前分支落后 master，存在文本、二进制和 modify/delete 冲突、提交污染、ADR 编号
冲突及未提交交付物。正确路径是从最新 master 做一次受控移植，而不是直接 merge
当前 worktree。

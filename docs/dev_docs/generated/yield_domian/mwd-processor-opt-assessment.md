# MWD 指定良损方案评估与合并记录

> 算法的数据流、公式和跨月“平滑”细节见
> [MWD 指定良损算法逻辑](mwd-processor-opt-algorithm.md)。

## 一、最终结论

评估来源：`D:\wzy\Python\vivo-project-mwd`，分支
`feat/mwd-processor-opt`；目标分支：`D:\wzy\Python\vivo-project` 的
`master`。

结论：**算法方案通过；HIGH-4 与 MEDIUM-1 的行为边界已由业务接受；HIGH-5 通过
选择性 merge 解决。**

新方案把“Code baseline → EMA → 趋势调节 → 月度对账 → 月/周/日人工覆盖”改为
“指定月度良损 → 确定性日度生成 → 周/月直接聚合”。Code 与 Group 分别服从
对应 Sheet 的人工指定，月、周、日只维护本级最终日度整数这一个事实源。

## 二、已确认的审查结论

### HIGH-4：接受 Mapping 与 MWD 最终计数不完全一致

此项不再作为缺陷或合并阻断项。

MWD 与 Mapping 只需共享 Code Sheet 的指定来源和月度调节方向，不要求最终计数
严格一致。Mapping 在月度倍率之后还有独立业务逻辑，包括最新批次选择、整数抽样或
复制、位置修饰和批次级联衰减；这些逻辑会继续决定 Mapping 最终结果。

因此验收边界为：

- MWD 使用指定良损生成月、周、日趋势；
- Mapping 使用同一指定来源计算月度倍率；
- Mapping 后续优化逻辑保持独立；
- 不以“Mapping 最终计数等于 MWD 月度整数”作为验收条件。

### MEDIUM-1：接受最终日度整数可能在月界跳变

此项不再作为缺陷或合并阻断项。

月中锚点线性插值保证的是归一化前基线连续。系统随后按自然月独立归一化，以保证
每个月的整数合计精确达到目标；相邻月份的归一化系数不同，因此最终日度整数仍可能
在月界跳变。

业务接受以下优先级：

1. 单日不良数不超过当日投入；
2. 月度整数合计精确达到目标；
3. 在前两项约束内保留跨月插值形成的相对形状；
4. 不要求最终日度整数在月界严格连续。

### HIGH-5：通过选择性 merge 解决

未对旧 feature 分支执行普通内容合并，也未逐提交 cherry-pick。合并以最新 master
为默认内容，通过双亲 merge commit 只引入 MWD 强相关白名单；双方都修改过的文件
以 master 最新结构为底稿手工接入 MWD 变化。

纳入当前分支版本的内容包括：

- MWD 指定良损核心模块及旧链路删除；
- 修饰表读取、校验、写回与共享 Excel 边界；
- Code Sheet 的 Mapping 月度倍率接线和轻量倍率防御；
- Yield service、页面上传入口、产品路径配置；
- 相关单元测试、E2E、smoke 路由、运维工具和领域文档；
- `resources/入库良率修饰表.xlsx`。

保留 master 版本的内容包括：

- 所有 Inline、SPC、CTQ、Equipment、XMind 和 planning 变更；
- 所有与本次 MWD 算法无关的源码、测试和文档；
- `resources/趋势图人工修正.xlsx`；
- `resources/mapping_config.xlsx`；
- `resources/codebaseline.xlsx`；
- 其他 SPC/CTQ/OOS/规格类业务工作簿。

交叉文件处理：

- `ARCHITECTURE.md`：保留 master 的 Inline/目录重构，只更新 Yield 数据流；
- `app/pages/入库不良率分析看板.py`：保留 master 的新 charts/sections 路径和预警
  页面逻辑，只接入修饰表上传、签名和 service 参数；
- `references/domain/yield_domian/mwd_trend_processor_algorithm.md`：按新算法重写
  稳定领域规则，并路由到完整算法文档；
- Yield ADR 从冲突的 `0016` 改号为 `0018`。

## 三、复杂度与风险评估

### 3.1 得到改善的部分

- 删除 Code baseline、EMA、TrendRegulator、月度对账和人工覆盖优先级链路；
- 月、周、日只有一个事实来源：最终日度整数；
- Group 与 Code 分别服从各自 Sheet 的人工指定；
- 业务只维护一个月度“指定良损”入口；
- 旧 `allocation.py` 已删除，仍需使用的整数分配保留在 `daily_generator.py`。

因此认知复杂度、配置复杂度和维护入口数量均得到降低。

### 3.2 保留的风险边界

Mapping 上调通过复制 DataFrame 行实现。正常业务倍率通常小于 1；实现对非有限值、
负值和超过 10 倍的倍率记录错误并按 `1.0` 回退，避免异常配置触发无界复制。

已接受的非保证包括：

- 最终日度整数在月界严格连续；
- Mapping 与 MWD 最终计数严格相等；
- Mapping 上调经过批次级联后一定增加最终计数。

## 四、验证记录

feature worktree 合并前验证：

- MWD、Mapping、Excel 写回相关测试：`96 passed, 24 warnings`；
- Yield smoke：`132 passed, 4 failed, 24 warnings`；
- 4 个失败为既有基线：`test_code_selector_filter.py` 2 项，
  `test_yield_global_data_policy.py` 2 项；
- 缺失 Sheet 不再触发 COM fatal exception。

master 选择性合并后的提交前验证：

- MWD、Mapping、Excel 写回及上传入口定向测试：`96 passed, 24 warnings`；
- Yield smoke：`133 passed, 5 failed, 24 warnings`；
- 失败项均属于最新 master 的既有范围且对应文件未被本次 merge 修改：
  `test_code_selector_filter.py` 2 项、`test_yield_global_data_policy.py` 2 项、
  `test_yield_dashboard_plotly_keys.py` 1 项；
- `tools/backfill_modifier_table_specified.py --dry-run` 成功完成 10 个产品级 Sheet
  的只读预览，未写回业务工作簿；
- 相关 Python 文件编译通过，暂存区 `git diff --check` 通过。

## 五、最终判断

- 算法方向：**通过**；
- HIGH-4：**业务接受，不阻断**；
- MEDIUM-1：**业务接受，不阻断**；
- HIGH-5：**已通过选择性 merge 策略解决**；
- 合并范围：**仅限 MWD 强相关白名单，其他内容保留 master**。

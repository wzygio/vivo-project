# `yield_domain` Delivery Gate 执行报告

- 执行日期：2026-07-30
- 项目目录：`D:\wzy\Python\vivo-project`
- 目标范围：`src/yield_domain/`
- Skill：`delivery-gate` v1.1.1
- 脚本：`hooks/quality-gate.py`
- 最终状态：**BLOCKED**

## 1. 执行结论

本次 Delivery Gate 未通过，存在两个独立阻断层：

1. `delivery-gate` 原始机械脚本因磁盘空间低于临界阈值返回 `exit 2`；
2. 项目定义的 Yield smoke 返回 `exit 1`，共有 6 个失败测试。

因此当前不能把 `yield_domain` 标记为“已通过交付门禁”。

本次任务只生成教程和评审文档，没有修改 `yield_domain` 业务源码，也没有尝试删除数据、修改
测试或降低门禁阈值。

## 2. 兼容执行方法

### 2.1 为什么需要兼容输入

该 Skill 是 Claude Code Stop hook，通常从 stdin 接收 Claude transcript 或包含
`transcript_path` 的 JSON。当前执行环境是 Codex，不能直接提供同格式的真实 Stop transcript。

为真实运行原始脚本而不是手工模拟判断，本次采用以下兼容方式：

1. 工作目录保持为项目根；
2. 设置 `CLAUDE_PROJECT_DIR` 为项目根；
3. 向 stdin 输入 4 条结构化 `Edit` 记录，代表本轮四份文档产物；
4. 不安装 Hook，不修改 Claude/Codex 设置；
5. 记录原始退出码和 stderr。

代表性输入：

```json
{"name":"Edit","target":"delivery-gate-tutorial"}
{"name":"Edit","target":"delivery-gate-yield-review"}
{"name":"Edit","target":"api-design-tutorial"}
{"name":"Edit","target":"api-design-yield-review"}
```

这使脚本将任务识别为复杂任务，但其他检查仍由原始脚本执行。

### 2.2 实际命令语义

```text
CLAUDE_PROJECT_DIR = D:\wzy\Python\vivo-project
stdin              = 4 个结构化 Edit 记录
script             = quality-gate.py
```

## 3. 原始 Delivery Gate 结果

```text
DELIVERY_GATE_EXIT_CODE=2
WARNING: Blocked: disk space at 14GB (<15GB). Free space before continuing.
```

判定：

| Check | 状态 | 证据 |
|---|---|---|
| Disk critical threshold | BLOCK | 约 14 GB，低于 15 GB |
| Transcript minimum length | NOT EVALUATED | 磁盘检查先行并立即退出 |
| Rationalization patterns | NOT EVALUATED | 磁盘临界阻断后未执行 |
| Complex edit count | NOT EVALUATED | 磁盘临界阻断后未执行 |
| Memory directory lookup | NOT EVALUATED | 磁盘临界阻断后未执行 |
| Learning library freshness | NOT EVALUATED | 磁盘临界阻断后未执行 |

不能把后五项写成 PASS，因为脚本采用 fail-fast，在磁盘阻断处已经退出。

## 4. 磁盘阻断的影响

低磁盘空间不说明 Yield 算法错误，但会降低交付过程的可靠性：

- 测试临时文件可能无法完整写入；
- Parquet 或 Excel 产物可能写入失败；
- 缓存和日志可能截断；
- 数据库客户端、Python 环境或包缓存更新可能失败；
- 后续验证可能出现与源码无关的环境噪声。

当前剩余空间仅比阈值低约 1 GB，不建议以“刚好清出一点空间”为目标。更稳妥的目标是：

- 至少高于 15 GB 以解除硬阻断；
- 优先恢复到 30 GB 以上以离开 Warning 区；
- 条件允许时恢复到 50 GB 以上以离开 Reminder 区。

本次没有清理任何文件。清理需要用户明确指定范围，并应优先针对可重建缓存、临时文件或确认无用的
构建产物，不能触碰 `resources/`、业务数据或未知目录。

## 5. Yield 工程验证

`delivery-gate` 自身不检查构建和测试。根据项目 `ARCHITECTURE.md` 的验证入口，额外执行：

```powershell
uv run python tools/smoke.py yield
```

结果：

```text
88 passed, 6 failed, 31 warnings
exit code: 1
```

### 失败清单

1. `test_code_selector_filter.py::test_batch_code_options_return_all_eligible_codes_without_placeholder`
2. `test_code_selector_filter.py::test_batch_code_options_keep_eligible_codes_across_groups`
3. `test_shadow_ema.py::TestShadowEMA::test_spike_rejection_logic`
4. `test_shadow_ema.py::TestShadowEMA::test_zero_denominator`
5. `test_yield_global_data_policy.py::test_yield_data_policy_is_defined_once_in_global_config`
6. `test_yield_global_data_policy.py::test_yield_data_policy_is_built_once_from_validated_app_config`

### 失败分类

#### Code selector API contract

前两项失败：

```text
TypeError:
build_batch_code_options_by_group()
missing 1 required positional argument: 'count_threshold'
```

这表明函数签名或调用方测试契约已经不一致。由于用户本次只要求评审，未授权修复，报告不判断应修改
实现还是更新调用方；需要由该功能的业务契约决定。

#### Shadow EMA behavior

两项失败分别涉及：

- 平稳期基准高于测试允许范围；
- 分母为 0 时返回平滑值而不是 0。

这些属于核心计算语义，项目硬边界要求没有专门任务和回归证明时不得擅自重构。

#### Yield global data policy

全局配置当前比测试预期多出：

- `TP_Short NG`
- `TP 容值NG`

需要确认是业务范围已经扩展但测试未更新，还是配置发生了不符合契约的变化。

### Warning

31 个 warning 主要包括：

- pandas chained assignment 将在 pandas 3.0 改变行为；
- `M` 月频率别名已弃用，应迁移到 `ME`；
- `pyproject.toml` 中 `tool.uv.dev-dependencies` 已弃用。

这些 warning 本次未作为单独硬阻断，但应进入技术债清单。

## 6. 综合门禁矩阵

| Gate | 结果 | 是否阻断 |
|---|---|---|
| Delivery Gate disk | FAIL：14 GB < 15 GB | 是 |
| Delivery Gate rationalization | 未执行 | 未知 |
| Delivery Gate learning capture | 未执行 | 未知 |
| Yield smoke | FAIL：88 passed / 6 failed | 是 |
| Full pytest | 未执行 | 未知 |
| Coverage ≥ 80% | 未执行 | 未知 |
| Static type check | 未配置为本次入口 | 未知 |
| Lint/format | 未确认仓库命令 | 未知 |
| Browser E2E | 未执行 | 未知 |
| Security scan | 未执行 | 未知 |

## 7. 为什么结果必须是 BLOCKED

至少两个硬事实不满足：

```text
delivery-gate exit code = 2
yield smoke exit code   = 1
```

即使本轮只新增 Markdown，这份报告的对象是 `yield_domain` 当前交付状态，而不是“文档能否保存”。
因此不能因文档生成成功而把领域模块判定为可交付。

## 8. 推荐恢复顺序

### Step 1：安全释放磁盘空间

由用户确认可清理范围。完成后只读确认剩余空间，避免针对未知目录执行递归删除。

### Step 2：重新执行原始 Delivery Gate

磁盘解除阻断后，脚本才会继续检查：

- 合理化模式；
- 复杂度；
- 项目 memory；
- 学习库新鲜度。

当前不能预测这些后续项一定通过。

### Step 3：分类处理 6 个 smoke 失败

按风险顺序：

1. 确认 Shadow EMA 的业务权威口径；
2. 确认全局 Defect Group 配置是否已正式扩展；
3. 确认 `count_threshold` 是新增必填契约还是应有默认值；
4. 在修复前保存 characterization evidence。

### Step 4：重跑 Yield smoke

要求退出码为 0。不得通过删除测试或弱化断言实现。

### Step 5：补充尚未执行的交付证据

至少包括：

- full pytest；
- 项目已配置时的 coverage、lint、type；
- 关键 Streamlit 页面或浏览器流程；
- 缓存模块热重载回归；
- SQL 和文件边界安全检查。

## 9. 判定规则

下一次可以标记为 PASS 的最低条件：

- [ ] 原始 Delivery Gate 返回 0；
- [ ] Yield smoke 返回 0；
- [ ] 所有后续未执行项被明确评估或经责任人接受；
- [ ] 没有通过删除数据、测试或证据规避失败；
- [ ] 结果记录包含命令、退出码和适用范围。

## 10. 最终状态

```text
DELIVERY STATUS: BLOCKED

Primary blocker:
  Disk free ≈ 14 GB, below delivery-gate critical threshold 15 GB.

Independent blocker:
  Yield smoke: 88 passed, 6 failed, 31 warnings.

Not evaluated:
  Learning freshness and rationalization checks,
  because the delivery-gate exited at the disk check.
```

这不是对 `yield_domain` 业务价值的否定，而是一个严格的证据结论：当前环境与验证结果尚不足以支持
“通过交付门禁”的声明。

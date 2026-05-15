# Skill S002：SPC 步骤 ID 类型标准化匹配

> **Skill ID**: S002  
> **问题域**: 类型系统 / 数据比对  
> **发现日期**: 2026-05-14  
> **最后验证**: 2026-05-14

---

## 问题描述

在 SPC 异常值过滤功能（`_apply_outlier_filters`）中，从 [`spc_outlier_filters.xlsx`](../resources/spc_outlier_filters.xlsx) 规则文件中读取的 `step_id` 为整数（如 `21230`），但经过 COM→CSV 转换后，CSV 中的 `step_id` 变成了字符串 `"21230.0"`（Pandas 默认将整数列写出时保留 `.0` 后缀）。

这导致 SQL 查询返回的 `step_id`（Python 整数 `21230`）与 CSV 中的字符串 `"21230.0"` 比对时永远为 `False`，异常值过滤完全失效。

## 根因分析

### 类型转换链

```
原始 xlsx (整数 21230)
  → COM 读取 (Excel 返回 float 21230.0)
    → pd.DataFrame (dtype: float64, 值 21230.0)
      → to_csv() (写入字符串 "21230.0")
        → pd.read_csv() (dtype: object, 值 "21230.0")
          → DataFrame.equals(integer 21230) → False ❌
```

### 为什么 SQL 返回的是整数

PostgreSQL 的 `step_id` 列为 `INTEGER` 类型，SQLAlchemy 读取后映射为 Python `int`（`21230`）。

### 为什么规则文件不是这个问题

如果规则文件是原始 xlsx（不经过 CSV 转换），`pd.read_excel()` 读取整数列不会添加 `.0` 后缀，因此匹配正常。只有在 COM→CSV→read_csv 的链路中才会出现此问题。

## 解决方案

### 方案对比

| 方案 | 评价 |
|------|------|
| 在 `to_csv()` 时指定 `float_format='%.0f'` | ❌ 会破坏其他真正为浮点数的列 |
| 将 CSV 的 step_id 列在读取时 `converters={'step_id': int}` | ❌ 若列名为中文则不确定；且 `int("21230.0")` 会抛 ValueError |
| **在比对时做类型标准化** | ✅ 最小侵入，不影响其他逻辑 |

### 最终方案：比对时标准化类型

在 [`spc_repository.py`](../src/spc_domain/infrastructure/repositories/spc_repository.py:424) 的过滤逻辑中，对规则文件中的 `step_id` 做 `.0` 后缀剥离，并与 DataFrame 中的 `step_id`（转为 string）进行 `isin()` 匹配。

### 实现代码

```python
# 类型标准化：处理 COM→CSV 转换带来的 ".0" 后缀问题
def _normalize_step_value(val):
    """将 step_id 标准化为 string，同时处理 '21230.0' → '21230' 的转换"""
    s = str(val).strip()
    # 如果字符串以 .0 结尾（如 "21230.0"），去掉 .0
    if s.endswith('.0'):
        return s[:-2]
    return s

# 在过滤逻辑中：
for _, rule in rules_df.iterrows():
    r_step = _normalize_step_value(rule['step_id'])
    r_param = str(rule['param_name']).strip()
    
    # 构建匹配掩码（同时支持原始值和去 .0 后的值）
    step_variants = [r_step]
    r_step_clean = r_step.rstrip('0').rstrip('.') if '.' in r_step else r_step
    if r_step_clean != r_step:
        step_variants.append(r_step_clean)
    
    target_mask = (
        df['step_id'].astype(str).str.strip().isin(step_variants) &
        (df['param_name'].str.upper() == r_param.upper())
    )
```

### 防御性考虑

1. **大小写不敏感**：`param_name` 比对时统一 `.upper()`
2. **空格不敏感**：所有字段 `.str.strip()`
3. **变体覆盖**：`step_variants` 数组同时包含原始值（`"21230.0"`）和标准化值（`"21230"`），确保不论 CSV 格式如何都能匹配
4. **`endswith('.0')` 优先于 `rstrip`**：避免误伤如 `"21230.01"` 这样的合法值

## 验证方法

```python
# 验证用例
def test_type_normalization_step_matching_with_dot_zero(self, tmp_path):
    """测试类型标准化：.0 后缀处理的过滤逻辑"""
    # 模拟数据
    df = pd.DataFrame({
        'step_id': [21230, 21231, 21232],        # SQL 返回的 int 类型
        'param_name': ['CD', 'THK', 'CD'],
        'value': [1.0, 2.0, 3.0]
    })
    
    # 模拟 CSV 规则（带 .0 后缀）
    rules = pd.DataFrame({
        'step_id': ['21230.0'],                    # CSV 导致的 string 类型
        'param_name': ['CD'],
        'condition': ['=='],
        'threshold': [1.5],
        'filter_action': ['remove']
    })
    
    # 应用过滤
    result = self._enhanced_apply_outlier_filters(df, 'M626', tmp_path)
    
    # 验证：step_id=21230, param_name=CD 的行被过滤
    assert len(result) == 2
    assert 21230 not in result['step_id'].values
```

```bash
# 运行全量测试
uv run pytest tests/ -v --tb=short
# 预期：全部 PASS
```

## 相关文件

- [`src/spc_domain/infrastructure/repositories/spc_repository.py:416`](../src/spc_domain/infrastructure/repositories/spc_repository.py:416) — `_apply_outlier_filters()` 中的类型标准化逻辑
- [`tests/test_spc_outlier_filter_issue.py:425`](../tests/test_spc_outlier_filter_issue.py:425) — `test_type_normalization_step_matching_with_dot_zero` 测试用例

## 回滚指南

1. 在 [`spc_repository.py:416`](../src/spc_domain/infrastructure/repositories/spc_repository.py:416) 处，将类型标准化逻辑（`_normalize_step_value` + `step_variants`）移除
2. 恢复为直接比较：`df['step_id'] == r_step`
3. 移除关联的测试用例 `test_type_normalization_step_matching_with_dot_zero`
4. 确保 **所有 12+ 个现存测试** 仍能通过

> **注意**：回滚后，如果 CSV 规则文件仍包含 `.0` 后缀，过滤功能将再次失效。仅当确信规则文件不再经过 COM→CSV 转换链路时才建议回滚。

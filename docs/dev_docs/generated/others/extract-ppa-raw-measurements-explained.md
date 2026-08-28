# `extract_ppa_raw_measurements.py` 代码逻辑详解

## 1. 脚本的作用

`tools/extract_ppa_raw_measurements.py` 不连接数据库，也不执行 SQL。它处理的是已经保存在本地的 Parquet 快照文件。

脚本会依次处理多个产品：

1. 找到该产品的 Inline 测量数据 Parquet 文件；
2. 将 Parquet 文件完整读取为 Pandas `DataFrame`；
3. 筛选参数名包含 `PPA`、且测量开始时间位于指定时间窗口内的数据；
4. 将每个产品的筛选结果写入同一个 Excel 文件的独立工作表。

默认数据流如下：

```text
data/<产品代码>/inline_measurements_<产品代码>.parquet
        ↓ pd.read_parquet
Pandas DataFrame
        ↓ 参数名条件 + 开始时间条件
筛选后的 DataFrame
        ↓ to_excel
output/ppa_raw_measurements_202607.xlsx
```

默认处理的产品是：

```text
M626、M673、M678、Z571、Z517
```

---

## 2. 文件开头：导入依赖

```python
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
```

各模块的用途如下：

| 模块 | 用途 |
|---|---|
| `argparse` | 接收命令行参数，例如产品、日期和输出路径 |
| `logging` | 输出“跳过文件”“提取多少行”“输出完成”等运行信息 |
| `sys` | 修改 Python 模块搜索路径 |
| `pathlib.Path` | 以跨平台方式拼接和检查文件路径 |
| `pandas` | 读取 Parquet、筛选表格数据并写入 Excel |

`from __future__ import annotations` 会延迟解析类型注解。对这个脚本而言，它不会改变业务逻辑。

---

## 3. 项目根目录和默认参数

### 3.1 计算项目根目录

```python
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
```

假设脚本路径是：

```text
D:\wzy\Python\vivo-project\tools\extract_ppa_raw_measurements.py
```

那么：

```text
Path(__file__).resolve()          = ...\tools\extract_ppa_raw_measurements.py
.parents[0]                       = ...\tools
.parents[1]                       = ...\vivo-project
PROJECT_ROOT                      = ...\vivo-project
```

接着，脚本把项目根目录插入 `sys.path`。当前脚本没有导入项目内部模块，因此这一步目前不是筛选或导出所必需的，但可以保证将来引用项目模块时能够找到它们。

### 3.2 默认值

```python
DEFAULT_DATA_DIR = PROJECT_ROOT / "data"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "output" / "ppa_raw_measurements_202607.xlsx"
DEFAULT_PRODUCTS = ("M626", "M673", "M678", "Z571", "Z517")
DEFAULT_START_DATE = "2026-07-01"
DEFAULT_END_DATE = "2026-07-31"  # exclusive
PPA_KEYWORD = "PPA"
```

它们分别表示：

| 常量 | 默认含义 |
|---|---|
| `DEFAULT_DATA_DIR` | 从项目的 `data/` 目录读取快照 |
| `DEFAULT_OUTPUT_PATH` | 默认 Excel 输出路径 |
| `DEFAULT_PRODUCTS` | 默认需要处理的产品代码 |
| `DEFAULT_START_DATE` | 时间窗口起点，包含该时刻 |
| `DEFAULT_END_DATE` | 时间窗口终点，不包含该时刻 |
| `PPA_KEYWORD` | 参数名必须包含的关键字 |

这里的 `end-date` 是排他边界。默认条件实际上是：

```text
2026-07-01 00:00:00 <= start_time < 2026-07-31 00:00:00
```

因此默认值会包含 7 月 1 日至 7 月 30 日的数据，但不会包含 7 月 31 日的数据。如果目标是提取完整的 2026 年 7 月，结束日期应传入 `2026-08-01`。

---

## 4. 核心函数：读取并筛选一个产品

函数定义如下：

```python
def extract_ppa_measurements(
    snapshot_path: Path,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
```

它接收三个参数：

| 参数 | 含义 |
|---|---|
| `snapshot_path` | 某个产品的 Parquet 快照路径 |
| `start` | 筛选开始时间，包含 |
| `end` | 筛选结束时间，不包含 |

返回值是筛选后的 `DataFrame`。

### 4.1 读取 Parquet

```python
snapshot = pd.read_parquet(snapshot_path)
```

这一行将整个 Parquet 文件读取为二维表格 `snapshot`。可以把 `DataFrame` 理解为内存中的 Excel 工作表：

```text
行：一条测量记录
列：param_name、start_time 以及其他测量字段
```

这一行读取的是本地文件，不是数据库表，所以不会产生 SQL。

### 4.2 统一转换时间列

```python
snapshot["start_time"] = pd.to_datetime(
    snapshot["start_time"],
    errors="coerce",
)
```

原脚本把这段写在一行。它将 `start_time` 列转换为 Pandas 日期时间类型，以便与 `start`、`end` 比较。

`errors="coerce"` 表示：无法解析的值不会导致程序立即报错，而会被转换成 `NaT`，即日期时间类型中的空值。`NaT` 与开始或结束时间比较时不会满足筛选条件，因此相应记录最终会被排除。

---

## 5. “通过 Pandas 条件筛选数据”体现在哪里

核心代码就是下面这六行：

```python
mask = (
    snapshot["param_name"].astype(str).str.contains(
        PPA_KEYWORD,
        case=False,
        na=False,
    )
    & (snapshot["start_time"] >= start)
    & (snapshot["start_time"] < end)
)
return snapshot.loc[mask].reset_index(drop=True)
```

原脚本把第一个条件写在一行，这里只是为了说明而换行，逻辑完全相同。

### 5.1 Pandas 条件不是只返回一个真假值

下面的表达式：

```python
snapshot["start_time"] >= start
```

会将 `start_time` 列的每一行分别与 `start` 比较，得到一列布尔值，而不是单个布尔值。例如：

| 行号 | `start_time` | 是否 `>= 2026-07-01` |
|---:|---|---|
| 0 | `2026-06-30 23:59` | `False` |
| 1 | `2026-07-01 08:00` | `True` |
| 2 | `2026-07-15 10:00` | `True` |

这种与原表行数相同、每行都是 `True` 或 `False` 的序列叫作布尔掩码（Boolean mask）。变量名 `mask` 就来源于此。

### 5.2 条件一：参数名包含 `PPA`

```python
snapshot["param_name"].astype(str).str.contains(
    PPA_KEYWORD,
    case=False,
    na=False,
)
```

它可以拆成四步理解：

1. `snapshot["param_name"]`：取出 `param_name` 这一列；
2. `.astype(str)`：将每个值转换为字符串；
3. `.str.contains("PPA", ...)`：逐行判断字符串中是否包含 `PPA`；
4. 生成一列 `True`/`False`。

参数含义如下：

| 参数 | 含义 |
|---|---|
| `PPA_KEYWORD` | 实际值是字符串 `"PPA"` |
| `case=False` | 忽略大小写，因此 `PPA`、`ppa`、`Ppa` 都能匹配 |
| `na=False` | 遇到缺失值时按不匹配处理 |

示例：

| `param_name` | 条件结果 |
|---|---|
| `PPA_WIDTH` | `True` |
| `PRE_PPA_VALUE` | `True` |
| `ppa_offset` | `True` |
| `CD_WIDTH` | `False` |
| 空值 | `False` |

`str.contains` 默认将关键字按正则表达式解释。不过当前关键字 `PPA` 不含正则特殊字符，因此这里与普通的“包含子字符串”效果一致。

### 5.3 条件二：时间大于或等于开始时间

```python
(snapshot["start_time"] >= start)
```

它保证窗口左边界是包含关系：

```text
start_time == start  → 保留
start_time >  start  → 保留
start_time <  start  → 排除
```

SQL 中大致对应：

```sql
start_time >= :start
```

### 5.4 条件三：时间小于结束时间

```python
(snapshot["start_time"] < end)
```

它保证窗口右边界是排除关系：

```text
start_time <  end  → 保留
start_time == end  → 排除
start_time >  end  → 排除
```

SQL 中大致对应：

```sql
start_time < :end
```

这种 `[start, end)` 的半开区间适合连续按天或按月分批处理，因为相邻时间窗口不会重复计算边界上的记录。例如：

```text
7 月窗口：[2026-07-01, 2026-08-01)
8 月窗口：[2026-08-01, 2026-09-01)
```

### 5.5 使用 `&` 合并三个条件

```python
条件一 & 条件二 & 条件三
```

Pandas 中的 `&` 表示对两侧布尔序列逐行执行“并且”。只有三个条件全部为 `True`，最终 `mask` 对应位置才是 `True`。

假设原始数据如下：

| 行号 | `param_name` | `start_time` | 包含 PPA | 不早于开始 | 早于结束 | 最终 `mask` |
|---:|---|---|---|---|---|---|
| 0 | `PPA_WIDTH` | `2026-06-30 23:59` | `True` | `False` | `True` | `False` |
| 1 | `CD_WIDTH` | `2026-07-10 08:00` | `False` | `True` | `True` | `False` |
| 2 | `ppa_offset` | `2026-07-10 09:00` | `True` | `True` | `True` | `True` |
| 3 | `PPA_HEIGHT` | `2026-07-31 00:00` | `True` | `True` | `False` | `False` |

最后只有第 2 行的三个条件都成立。

在 Pandas 中应使用 `&` 合并这种逐行条件，不能改成 Python 的 `and`。每个比较条件外层的括号也很重要，它们确保各条件按预期组合。

### 5.6 用 `.loc[mask]` 真正执行筛选

```python
snapshot.loc[mask]
```

`mask` 只是记录“每一行是否保留”。真正从 `snapshot` 取出符合条件的行，是在 `.loc[mask]` 这里发生的：

```text
mask 为 True  → 保留该行
mask 为 False → 丢弃该行
```

以上一节的示例数据为例，结果只剩：

| 原行号 | `param_name` | `start_time` |
|---:|---|---|
| 2 | `ppa_offset` | `2026-07-10 09:00` |

### 5.7 重建连续行号

```python
.reset_index(drop=True)
```

筛选后保留下来的原始行号可能是 `2、8、15`。`reset_index` 会将它们重新编号为 `0、1、2`。

`drop=True` 表示丢弃旧行号，不把旧行号额外保存为 Excel 中的一列。函数最终返回的就是这个重新编号后的筛选结果。

### 5.8 与 SQL 的对应关系

这段 Pandas 逻辑可以近似理解为：

```sql
SELECT *
FROM inline_measurements
WHERE param_name ILIKE '%PPA%'
  AND start_time >= :start
  AND start_time < :end;
```

这里用 PostgreSQL 的 `ILIKE` 比 `LIKE` 更准确，因为 Python 代码设置了 `case=False`，匹配时忽略大小写。这只是帮助理解的等价表达；脚本本身没有执行这条 SQL。

---

## 6. `main()`：解析参数并组织整个执行流程

### 6.1 创建命令行参数解析器

```python
parser = argparse.ArgumentParser(description=__doc__)
```

`description=__doc__` 会将文件顶部的模块说明作为命令行帮助文本。执行以下命令可以查看帮助：

```powershell
python tools/extract_ppa_raw_measurements.py --help
```

### 6.2 可用参数

```python
parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)
parser.add_argument("--products", nargs="+", default=list(DEFAULT_PRODUCTS))
parser.add_argument("--start-date", default=DEFAULT_START_DATE)
parser.add_argument("--end-date", default=DEFAULT_END_DATE, help="exclusive")
args = parser.parse_args()
```

| 参数 | 用途 | 默认值 |
|---|---|---|
| `--data-dir` | Parquet 数据根目录 | `data/` |
| `--output` | Excel 输出路径 | `output/ppa_raw_measurements_202607.xlsx` |
| `--products` | 一个或多个产品代码 | 五个默认产品 |
| `--start-date` | 包含的开始日期 | `2026-07-01` |
| `--end-date` | 不包含的结束日期 | `2026-07-31` |

`nargs="+"` 表示 `--products` 后面必须至少提供一个值，并且可以一次提供多个值，例如：

```powershell
python tools/extract_ppa_raw_measurements.py --products M626 M673
```

### 6.3 初始化日志和转换日期参数

```python
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
start = pd.Timestamp(args.start_date)
end = pd.Timestamp(args.end_date)
```

第一行设置日志级别和格式。后两行把命令行传入的日期文本转换为 Pandas 时间戳，供筛选函数比较。

如果日期文本完全无法解析，`pd.Timestamp(...)` 会抛出异常并停止程序，因为这里没有使用 `errors="coerce"`。

---

## 7. 创建 Excel 并逐产品处理

### 7.1 确保输出目录存在

```python
args.output.parent.mkdir(parents=True, exist_ok=True)
```

例如输出路径为：

```text
output/ppa_raw_measurements_202607.xlsx
```

那么 `args.output.parent` 就是 `output/`。如果目录不存在，这行代码会创建它：

- `parents=True`：所需的上级目录也一并创建；
- `exist_ok=True`：目录已经存在时不报错。

### 7.2 打开 Excel 写入器

```python
with pd.ExcelWriter(args.output, engine="openpyxl") as writer:
```

`ExcelWriter` 负责创建 `.xlsx` 文件。`engine="openpyxl"` 表示使用 OpenPyXL 写入。

`with` 是上下文管理器。离开代码块时，写入器会自动保存并关闭 Excel 文件。默认写入模式会覆盖同路径的已有文件。

### 7.3 遍历产品

```python
for prod_code in args.products:
```

每次循环只处理一个产品。例如第一次可能是 `M626`。

### 7.4 拼接产品快照路径

```python
snapshot_path = (
    args.data_dir / prod_code / f"inline_measurements_{prod_code}.parquet"
)
```

假设：

```text
args.data_dir = data
prod_code     = M626
```

最终路径就是：

```text
data/M626/inline_measurements_M626.parquet
```

### 7.5 快照不存在时跳过

```python
if not snapshot_path.is_file():
    logger.warning(...)
    continue
```

`is_file()` 检查路径是否对应一个真实文件。如果文件不存在：

1. 输出警告日志；
2. 使用 `continue` 结束当前产品的循环；
3. 继续处理下一个产品。

因此一个产品缺少快照，不会阻止其他产品继续处理。

### 7.6 调用核心筛选函数

```python
extracted = extract_ppa_measurements(snapshot_path, start, end)
```

这一行把当前产品的文件路径和公共时间窗口传给前面介绍的核心函数。返回的 `extracted` 已经只包含参数名和时间都满足条件的行。

### 7.7 写入对应产品工作表

```python
extracted.to_excel(writer, sheet_name=prod_code, index=False)
```

参数含义：

| 参数 | 含义 |
|---|---|
| `writer` | 写入当前正在创建的 Excel 文件 |
| `sheet_name=prod_code` | 工作表名使用产品代码，如 `M626` |
| `index=False` | 不将 Pandas 行号写入 Excel |

即使某个产品的筛选结果是 0 行，只要快照文件存在，通常仍会创建一个带列标题的空工作表。

随后日志记录该产品提取了多少行。循环结束后，所有产品结果都已写入同一个工作簿。

---

## 8. 程序入口

```python
if __name__ == "__main__":
    raise SystemExit(main())
```

只有直接运行该脚本时，`main()` 才会执行：

```powershell
python tools/extract_ppa_raw_measurements.py
```

如果其他 Python 文件只是导入其中的 `extract_ppa_measurements` 函数，则不会自动开始读取和导出数据。

`main()` 成功时返回 `0`，`SystemExit(0)` 将这个成功状态传递给操作系统或调用脚本。

---

## 9. 完整运行流程

按照默认参数执行时，程序逻辑可以概括为：

```text
启动脚本
  ↓
读取命令行参数；未提供的参数使用默认值
  ↓
将开始日期和结束日期转换为 Timestamp
  ↓
创建输出目录并打开 ExcelWriter
  ↓
依次遍历 M626、M673、M678、Z571、Z517
  ↓
拼接当前产品的 Parquet 路径
  ├─ 文件不存在 → 记录警告 → 处理下一个产品
  └─ 文件存在
       ↓
     pd.read_parquet 读取整个快照
       ↓
     将 start_time 转为日期时间
       ↓
     逐行计算三个条件
       ├─ param_name 忽略大小写包含 PPA
       ├─ start_time >= start
       └─ start_time < end
       ↓
     用 & 合并为布尔掩码 mask
       ↓
     snapshot.loc[mask] 保留 True 对应的行
       ↓
     reset_index(drop=True) 重建行号
       ↓
     写入以产品代码命名的 Excel 工作表
  ↓
退出 with 块，保存并关闭 Excel
  ↓
返回成功状态 0
```

---

## 10. 运行示例

### 使用脚本默认参数

```powershell
python tools/extract_ppa_raw_measurements.py
```

### 提取完整的 2026 年 7 月

因为结束时间不包含在窗口中，应将结束日期设为 8 月 1 日：

```powershell
python tools/extract_ppa_raw_measurements.py `
  --start-date 2026-07-01 `
  --end-date 2026-08-01
```

### 只处理两个产品并指定输出文件

```powershell
python tools/extract_ppa_raw_measurements.py `
  --products M626 M673 `
  --start-date 2026-07-01 `
  --end-date 2026-08-01 `
  --output output/ppa_M626_M673_202607.xlsx
```

---

## 11. 需要特别注意的行为

1. **脚本不执行 SQL**：它读取的是已经生成好的 Parquet 快照。
2. **筛选发生在内存中**：`pd.read_parquet` 先读取快照，随后由 Pandas 创建 `mask` 并通过 `.loc[mask]` 筛选。
3. **关键字匹配忽略大小写**：`PPA`、`ppa` 和 `Ppa` 都能命中。
4. **时间窗口是左闭右开**：开始时间包含，结束时间不包含。
5. **默认值不包含 2026 年 7 月 31 日**：若需要完整 7 月，应把 `--end-date` 设置为 `2026-08-01`。
6. **无法解析的快照时间会被排除**：它们被转换为 `NaT`，不会通过时间比较。
7. **缺少单个产品快照时只跳过该产品**：其他产品仍继续处理。
8. **已有的同名输出文件会被覆盖**：运行前应确认输出路径是否需要保留旧文件。

最核心的一句代码解释是：

```python
snapshot.loc[mask]
```

它表示“从 `snapshot` 中，只取出 `mask` 对应位置为 `True` 的行”；而 `mask` 的 `True` 又要求参数名条件、开始时间条件和结束时间条件同时成立。

# Skill S001：加密 Excel 的 COM 透明解密方案

> **Skill ID**: S001  
> **问题域**: 文件处理 / 企业加密环境  
> **发现日期**: 2026-05-14  
> **最后验证**: 2026-05-14

---

## 问题描述

企业加密软件对所有 `.xlsx` 文件进行透明加密，导致：

1. `openpyxl` 打开时报 `BadZipFile: File is not a zip file`（文件头部不再是 `PK\x03\x04`）
2. `pd.read_excel(engine='openpyxl')` 全部失败
3. 加密后的 xlsx 文件**无法被任何纯 Python 库直接读取**（包括 `openpyxl`、`xlrd`、`xlrd` 等）
4. 第一次通过 `pd.read_excel()` 转换得到 CSV 后，文件内容**可能因加密状态不同而变化**——已转换的 CSV 可以被读取，但再次读取原始 xlsx 时可能得到不同（错误）内容

## 根因分析

### 加密机制

企业部署了 Active+X 之类的透明加密软件，工作原理为：

- 文件在磁盘上始终处于加密状态（header 为 `00000000 04070702...` 而非 `PK\x03\x04`）
- 只有通过**白名单进程**（如 Excel.exe）打开时，加密软件在内核层透明解密
- Python 解释器（python.exe）**不在白名单中**，因此所有 `open()` / `pd.read_excel()` 调用读取到的都是密文

### 为什么 CSV 有时能读有时不能

早期通过 `pd.read_excel()` 转换出的 CSV 文件是**明文**（因为当时 Python 进程可能通过某种方式触发了解密），但后续文件可能被加密软件重新锁定。因此：

- **不能信任 xlsx 文件的直接读取**（任何时间点都可能被重加密）
- **CSV 文件也可能被重加密**（取决于加密软件的写回策略）

## 解决方案

### 方案选型

| 方案 | 评价 |
|------|------|
| `openpyxl` / `xlrd` | ❌ 无法读取加密文件 |
| `msoffcrypto-tool` | ❌ 针对的是 Office 文档自身密码加密，非企业透明加密 |
| **Win32 COM（本地 Excel.Application）** | ✅ 唯一可行方案 |

### 最终方案：COM 透明解密

利用 Windows 本地安装的 Excel 应用程序，通过 COM 接口打开加密文件——Excel 在白名单中，加密软件会在内存中透明解密。

### 实现代码

详见 [`src/shared_kernel/utils/excel_tools.py`](../src/shared_kernel/utils/excel_tools.py:12) 中的 `_read_encrypted_xlsx_via_com()` 函数。

```python
def _read_encrypted_xlsx_via_com(xlsx_path: Path, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """
    通过 Windows COM 接口（本地 Excel.Application）读取企业加密的 xlsx 文件。
    Excel 进程在企业加密软件的白名单中，因此能透明解密。
    """
    try:
        import win32com.client
        import pythoncom  # Streamlit 多线程环境必须
    except ImportError:
        raise ImportError("需要安装 pywin32: pip install pywin32")

    # 关键：Streamlit 多线程环境下必须先初始化 COM
    try:
        pythoncom.CoInitialize()
    except Exception:
        pass  # 已初始化则静默忽略

    excel = win32com.client.Dispatch('Excel.Application')
    excel.Visible = False  # 后台运行，不显示窗口
    excel.DisplayAlerts = False  # 不弹出警告对话框

    try:
        wb = excel.Workbooks.Open(str(xlsx_path.resolve()))
        ws = wb.Worksheets(sheet_name or 1)
        
        # 读取 UsedRange（已使用区域）的全部数据
        data = ws.UsedRange.Value
        
        if data is None:
            return pd.DataFrame()
        
        # COM 返回二维元组，转换为 DataFrame
        rows = list(data)
        df = pd.DataFrame(rows[1:], columns=rows[0])
        return df
    finally:
        wb.Close(SaveChanges=False)
        excel.Quit()
```

### 关键注意事项

1. **`pythoncom.CoInitialize()`** — Streamlit 的多线程架构（每个请求在独立线程执行）要求在使用 COM 前必须调用此函数，否则抛出 `"尚未调用 CoInitialize"` 异常
2. **`excel.Quit()`** — 必须在 `finally` 块中确保 Excel 进程退出，否则会造成进程泄漏
3. **`DisplayAlerts = False`** — 防止解密过程中弹出的任何对话框卡住进程
4. **导入检查** — `pywin32` 可能安装在系统 Python 而非项目 venv 中，需要显式 import 检查

## 验证方法

```python
# 验证点1：xlsx 文件头不是标准 ZIP
with open('file.xlsx', 'rb') as f:
    header = f.read(4)
    assert header != b'PK\x03\x04'  # 确认为加密文件

# 验证点2：COM 可以读取
df = _read_encrypted_xlsx_via_com(Path('file.xlsx'))
assert len(df) > 0  # 成功读取到数据
```

## 相关文件

- [`src/shared_kernel/utils/excel_tools.py:12`](../src/shared_kernel/utils/excel_tools.py:12) — `_read_encrypted_xlsx_via_com()` 主实现
- [`src/shared_kernel/utils/excel_tools.py:82`](../src/shared_kernel/utils/excel_tools.py:82) — `xlsx_to_csv()` COM→CSV 转换入口
- [`src/spc_domain/infrastructure/repositories/spc_repository.py:323`](../src/spc_domain/infrastructure/repositories/spc_repository.py:323) — `_apply_outlier_filters()` 中调用 COM 读取规则文件
- [`src/yield_domain/core/sheet_lot_processor.py:5`](../src/yield_domain/core/sheet_lot_processor.py:5) — 使用 `comtypes` 读取加密 Override 文件
- [`scripts/regenerate_csv.py`](../scripts/regenerate_csv.py) — 独立脚本批量验证 COM 解密

## 回滚指南

如需回退 COM 方案，恢复为纯 Pandas 读取：

1. 移除 `excel_tools.py` 中的 `_read_encrypted_xlsx_via_com()` 函数
2. 在所有调用处恢复为 `pd.read_excel(engine='openpyxl')`
3. 删除 `import win32com.client` 和 `import pythoncom` 相关行
4. 如果 CSV 备选方案不可用，需要提供其他数据源

> **注意**：回滚后，所有企业加密的 xlsx 文件将**完全不可读取**。仅在加密策略变更（如解密软件移除）后才建议回滚。

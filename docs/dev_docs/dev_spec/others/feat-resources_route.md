# 配置文件整理
当前`resources`路径下的文件太过复杂，By产品区分。每个产品下有多个文件，这样不方便维护，需要进行汇总。

## Task1：基础整理
> ✅ 已完成（2026-08-13）。ctq 输出路径上移至产品目录，ctq 文件重命名为 `ctq_` 前缀；`analysis_files` / `project_files` 已汇总至 resources 根目录，两个专项资料页面已移除 page_header。
1. 将ctq文件的输出路径转移至每个产品的文件夹下。
    - 例如从`resources\M626\ctq`转移至`resources\M626\`
    - 完成后，请修正相应程序中的路径：`src\inline_domain\application\ctq\ctq_data_decoration.py`
2. 请转移相应的ctq文件：根目录下已有重名文件，因此你需要先将它们重命名为以“ctq”而不是“spc”开头
3. 将每个文件的产品下的解析文件和项目文汇总到一起，并存放到resources文件夹下。
    - 例如，将`resources\M626\analysis_files`和`resources\M626\project_files`，分别汇总至`resources\analysis_files`和`resources\project_files`
    - 完成后，请删除以下两个页面中的page_header，因为它们不再需要区分产品：`app\pages\专项资料-台账周报.py`、`app\pages\专项资料-解析报告.py`

## Task2：深入整理
> ✅ 已完成（2026-08-13）。汇总范围经确认为 4 类用户配置（codebaseline / 入库不良率规格 / 趋势图人工修正 / override_rates）+ 3 类 decoration（spc_cpk / spc_sheet_oos / ctq_sheet_oos），detail 明细文件保留在各产品目录。sheet 命名规则：主数据 sheet（原 Sheet1）→ 产品号，辅助 sheet → `<产品号>_<原sheet名>`；decoration 单 sheet 文件统一命名为产品号。迁移与逐 sheet 校验由 `tools/consolidate_resources.py` 完成，源文件已删除。路径解析改为 resources 根目录共享工作簿 + 按产品 sheet 读写（`excel_tools.read_workbook_sheet` / `replace_workbook_sheet`），涉及 yaml `paths`、code_baseline、excel_service、yield_service、sheet_lot_processor、spc/ctq decoration、spc_dashboard 与 file_uploader。

1. 将resources下，每个产品的配置文件都汇总到一个文件中，每个产品对应一个单独的sheet页
    - 例如，将`resources\M626\M626_codebaseline.xlsx`汇总至`resources\codebaseline.xlsx`，By产品分为不同的sheet页。
2. 如果当前一个配置文件中有多个sheet，那就By Sheet进行汇总。
    - 例如，`resources\M626\M626_趋势图人工修正.xlsx`中有两个sheet页：`Group级`、`Code级`，则分别汇总为一个文件
3. 完成后，请修正对应的程序中的路径。以M626为例，各配置文件对应的程序如下（并非精确对应，具体代码位置请分析后锁定）：
    - `resources\M626\M626_趋势图人工修正.xlsx`：`src\yield_domain\core\mwd_trend\mwd_trend_processor.py`
    - `resources\M626\M626_入库不良率规格.xlsx`:`src\yield_domain\core\mwd_trend\mwd_trend_processor.py`
    - `resources\M626\M626_codebaseline.xlsx`:`src\yield_domain\core\mwd_trend\mwd_trend_processor.py`
    - `resources\M626\spc_cpk_decoration.xlsx`:`src\inline_domain\core\spc\cpk_decoration.py`
    - `resources\M626\spc_sheet_oos_decoration.xlsx`:`src\inline_domain\application\spc\spc_data_decoration.py`
    - `resources\M626\ctq_sheet_oos_decoration.xlsx`:`src\inline_domain\application\ctq\ctq_data_decoration.py`

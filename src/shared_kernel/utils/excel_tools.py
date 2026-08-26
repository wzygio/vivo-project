# src/vivo_project/utils/utils.py
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st  # [新增] 引入 streamlit

from src.shared_kernel.config import ConfigLoader


def read_workbook_sheet(xlsx_path: Path, sheet_name: str) -> pd.DataFrame:
    """读取共享工作簿中的指定 sheet。

    文件不存在或 sheet 不存在时返回空 DataFrame（与“文件缺失”语义一致）；
    企业加密等 openpyxl 无法读取的工作簿自动回退到 Excel COM 透明解密读取，
    COM 也失败时抛出异常交由调用方决定（告警或包装为业务异常）。

    Args:
        xlsx_path: 共享工作簿路径
        sheet_name: 目标 sheet 名（通常为产品号或 <产品号>_<原sheet名>）

    Returns:
        该 sheet 的数据；文件/sheet 缺失时为空 DataFrame
    """
    if not xlsx_path.exists():
        return pd.DataFrame()
    try:
        return pd.read_excel(xlsx_path, sheet_name=sheet_name)
    except ValueError:
        # Worksheet named 'X' not found —— sheet 缺失视为无数据
        return pd.DataFrame()
    except Exception as openpyxl_error:
        logging.warning(
            "[excel_tools] 标准读取 %s [%s] 失败，尝试 Excel COM: %s",
            xlsx_path.name, sheet_name, openpyxl_error,
        )
        return _read_encrypted_xlsx_via_com(xlsx_path, sheet_name)


def _read_all_sheets_via_com(xlsx_path: Path) -> dict[str, pd.DataFrame]:
    """通过 Excel COM 读出工作簿的全部 sheets（用于加密工作簿的整体重写回退）。"""
    try:
        import win32com.client
        import pythoncom
    except ImportError:
        raise ImportError("win32com 未安装，无法读取加密工作簿。")

    com_initialized = False
    try:
        pythoncom.CoInitialize()
        com_initialized = True
    except Exception:
        pass

    excel = None
    wb = None
    try:
        excel = win32com.client.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        wb = excel.Workbooks.Open(str(xlsx_path.resolve()), ReadOnly=True)
        sheets: dict[str, pd.DataFrame] = {}
        for ws in wb.Worksheets:
            data = ws.UsedRange.Value
            if data is None or len(data) < 1:
                sheets[ws.Name] = pd.DataFrame()
                continue
            headers = list(data[0])
            rows = [list(row) for row in data[1:]]
            sheets[ws.Name] = pd.DataFrame(rows, columns=headers)
        return sheets
    finally:
        if wb is not None:
            try:
                wb.Close(SaveChanges=False)
            except Exception:
                pass
        wb = None
        try:
            if excel is not None:
                excel.Quit()
        except Exception:
            pass
        excel = None
        if com_initialized:
            try:
                pythoncom.CoUninitialize()
            except Exception:
                pass


def _new_staging_path(xlsx_path: Path, purpose: str) -> Path:
    """在目标同目录创建临时文件，保证最终替换可使用同卷原子操作。"""
    handle = tempfile.NamedTemporaryFile(
        dir=xlsx_path.parent,
        prefix=f".{xlsx_path.name}.{purpose}.",
        suffix=".xlsx",
        delete=False,
    )
    handle.close()
    return Path(handle.name)


def _validate_staged_workbook(
    staged_path: Path,
    sheet_name: str,
    df: pd.DataFrame,
) -> None:
    """确认临时工作簿可打开，且目标 Sheet 的表头和行列数完整。"""
    import openpyxl

    workbook = openpyxl.load_workbook(staged_path, read_only=True, data_only=True)
    try:
        if sheet_name not in workbook.sheetnames:
            raise ValueError(f"临时工作簿缺少目标 Sheet: {sheet_name}")
        worksheet = workbook[sheet_name]
        headers = [cell.value for cell in next(worksheet.iter_rows(max_row=1))]
        if headers != list(df.columns):
            raise ValueError(f"临时工作簿 {sheet_name} 表头校验失败")
        if (
            worksheet.max_row != len(df.index) + 1
            or worksheet.max_column != len(df.columns)
        ):
            raise ValueError(f"临时工作簿 {sheet_name} 行列数校验失败")
    finally:
        workbook.close()


def replace_workbook_sheet(
    xlsx_path: Path,
    sheet_name: str,
    df: pd.DataFrame,
) -> bool:
    """在共享工作簿中替换（或新建）指定 sheet，保留其他 sheet 的内容与格式。

    - 先在同目录临时文件中完成整本写入与校验，再原子替换源文件；
    - 替换前将源文件保存为 `<文件名>.bak`，供失败恢复；
    - 优先 openpyxl 替换目标 sheet，其他 sheet 原样保留；
    - 企业加密等 openpyxl 无法打开的文件回退为：COM 读出全部 sheets，
      替换目标 sheet 后整体重写为明文工作簿（与既有 to_excel 覆盖行为等价）；
    - 返回明确成功状态；文件被占用时返回 False 并保留源文件与旧签名重试条件。

    Args:
        xlsx_path: 共享工作簿路径（不存在则新建单 sheet 工作簿）
        sheet_name: 目标 sheet 名
        df: 写入的数据

    Returns:
        目标 Sheet 已持久化时为 True；文件占用导致未写入时为 False。
    """
    from openpyxl.utils.dataframe import dataframe_to_rows

    xlsx_path = Path(xlsx_path)
    xlsx_path.parent.mkdir(parents=True, exist_ok=True)
    staged_path = _new_staging_path(xlsx_path, "write")
    backup_staged_path: Path | None = None
    try:
        if not xlsx_path.exists():
            df.to_excel(staged_path, index=False, sheet_name=sheet_name)
        else:
            try:
                import openpyxl

                workbook = openpyxl.load_workbook(xlsx_path)
            except Exception:
                workbook = None

            if workbook is not None:
                try:
                    if sheet_name in workbook.sheetnames:
                        del workbook[sheet_name]
                    worksheet = workbook.create_sheet(sheet_name)
                    for row in dataframe_to_rows(df, index=False, header=True):
                        worksheet.append(row)
                    workbook.save(staged_path)
                finally:
                    workbook.close()
            else:
                logging.warning(
                    "[excel_tools] openpyxl 无法打开 %s，回退为整体重写。",
                    xlsx_path.name,
                )
                sheets = _read_all_sheets_via_com(xlsx_path)
                sheets[sheet_name] = df
                with pd.ExcelWriter(staged_path, engine="openpyxl") as writer:
                    for name, sheet_df in sheets.items():
                        sheet_df.to_excel(writer, index=False, sheet_name=name)

        _validate_staged_workbook(staged_path, sheet_name, df)

        if xlsx_path.exists():
            backup_path = xlsx_path.with_suffix(f"{xlsx_path.suffix}.bak")
            backup_staged_path = _new_staging_path(xlsx_path, "backup")
            shutil.copy2(xlsx_path, backup_staged_path)
            os.replace(backup_staged_path, backup_path)
            backup_staged_path = None

        os.replace(staged_path, xlsx_path)
        return True
    except PermissionError as exc:
        logging.warning(
            "[excel_tools] 工作簿被占用，未写入 %s [%s]；源文件保持不变: %s",
            xlsx_path, sheet_name, exc,
        )
        return False
    finally:
        staged_path.unlink(missing_ok=True)
        if backup_staged_path is not None:
            backup_staged_path.unlink(missing_ok=True)


def _read_encrypted_xlsx_via_com(xlsx_path: Path, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """
    [COM fallback] 通过 Windows Excel.Application COM 接口读取加密/受保护 xlsx 文件。
    
    企业加密软件会锁定 xlsx 文件使 openpyxl/xlrd 无法直接读取，
    但 Windows 本地 Excel 应用程序能够透明解密并打开这些文件。
    
    Args:
        xlsx_path: 加密的 xlsx 文件路径
        sheet_name: 可选的 sheet 名称（默认读取第一个 sheet）
    
    Returns:
        包含解密后数据的 DataFrame
    
    Raises:
        ImportError: win32com 未安装
        Exception: Excel 打开或读取失败
    """
    try:
        import win32com.client
        import pythoncom
    except ImportError:
        raise ImportError(
            "win32com 未安装。请执行 `pip install pywin32` 后重试，"
            "或确保在解密环境中运行 xlsx_to_csv。"
        )
    
    com_initialized = False
    try:
        pythoncom.CoInitialize()
        com_initialized = True
    except Exception:
        pass

    excel = None
    wb = None
    ws = None
    used_range = None
    try:
        excel = win32com.client.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        wb = excel.Workbooks.Open(str(xlsx_path.resolve()), ReadOnly=True)
        if sheet_name:
            ws = wb.Worksheets(sheet_name)
        else:
            ws = wb.Worksheets(1)
        
        used_range = ws.UsedRange
        data = used_range.Value

        if data is None or len(data) < 1:
            return pd.DataFrame()

        # COM 返回 tuple of tuples，首行为表头
        headers = list(data[0])
        rows = [list(row) for row in data[1:]]
        df = pd.DataFrame(rows, columns=headers)

        logging.info(f"[xlsx_to_csv] 使用 COM (Excel.Application) 读取加密文件 {xlsx_path.name} 成功, shape={df.shape}")
        return df

    except Exception as e:
        logging.error(f"[xlsx_to_csv] COM 读取加密文件失败: {e}")
        raise
    finally:
        if wb is not None:
            try:
                wb.Close(SaveChanges=False)
            except Exception:
                pass
        used_range = None
        ws = None
        wb = None
        try:
            if excel is not None:
                excel.Quit()
        except Exception:
            pass
        excel = None
        if com_initialized:
            try:
                pythoncom.CoUninitialize()
            except Exception:
                pass


def xlsx_to_csv(
    xlsx_path: Path,
    csv_dir: Path,
    sheet_name: Optional[str] = None,
    encoding: str = "utf-8-sig",
    **read_excel_kwargs,
) -> Path:
    """
    [通用工具] 将 xlsx 文件转换为 csv 格式。

    用途：在企业加密环境中，xlsx 可能被加密导致 openpyxl 无法读取，
          提前在解密环境中将其转为 csv，供生产代码作为 fallback 读取。
          
          [增强] 当 openpyxl/xlrd 均无法读取时（如企业加密软件锁定），
          自动尝试 Windows COM (Excel.Application) 接口进行透明解密读取。

    Args:
        xlsx_path: 源 xlsx 文件路径
        csv_dir:   输出 csv 目录
        sheet_name: 指定 sheet 名（单 sheet 文件可留空）
        encoding:  csv 编码，默认 utf-8-sig（Excel 兼容）
        **read_excel_kwargs: 透传给 pd.read_excel 的额外参数

    Returns:
        生成的 csv 文件路径
    """
    if not xlsx_path.exists():
        raise FileNotFoundError(f"源文件不存在: {xlsx_path}")

    csv_dir.mkdir(parents=True, exist_ok=True)
    csv_path = csv_dir / xlsx_path.with_suffix(".csv").name

    # 读取 xlsx：优先用 openpyxl，失败时尝试 xlrd
    df = pd.DataFrame()
    engines = ["openpyxl", "xlrd"]
    last_err = None
    for engine in engines:
        try:
            kwargs = dict(read_excel_kwargs)
            if sheet_name is not None:
                kwargs["sheet_name"] = sheet_name
            df = pd.read_excel(xlsx_path, engine=engine, **kwargs)
            logging.info(f"[xlsx_to_csv] 使用引擎 {engine} 读取 {xlsx_path.name} 成功, shape={df.shape}")
            break
        except Exception as e:
            last_err = e
            logging.warning(f"[xlsx_to_csv] 引擎 {engine} 读取失败: {e}")
            continue

    # [增强] 引擎全部失败时，尝试 COM fallback（应对企业加密软件锁定）
    if df.empty and last_err is not None:
        try:
            df = _read_encrypted_xlsx_via_com(xlsx_path, sheet_name)
            logging.info(f"[xlsx_to_csv] COM fallback 读取成功, shape={df.shape}")
        except Exception as com_err:
            logging.error(f"[xlsx_to_csv] COM fallback 也失败: {com_err}")
            raise last_err from com_err

    if df.empty:
        raise ValueError(f"无法从 {xlsx_path.name} 读取到任何数据")

    # 写入 csv（不写入 index，保留原始表头）
    df.to_csv(csv_path, index=False, encoding=encoding)
    logging.info(f"[xlsx_to_csv] 成功导出: {csv_path}")
    return csv_path


def batch_export_spc_rules_to_csv(
    resource_dir: Optional[Path] = None,
    csv_subdir: str = "xlsx_to_csv",
) -> list[Path]:
    """
    [批量导出] 将 SPC 相关的加密 xlsx 规则文件批量导出为 csv。
    目标文件：spc_outlier_filters.xlsx

    应在能透明解密企业加密文件的环境中执行一次，
    之后生产代码即可通过 csv fallback 读取规则。

    Returns:
        生成的 csv 文件路径列表
    """
    if resource_dir is None:
        resource_dir = ConfigLoader.get_project_root() / "resources"

    target_files = ["spc_outlier_filters.xlsx"]
    csv_dir = resource_dir / csv_subdir
    exported: list[Path] = []

    for fname in target_files:
        xlsx_path = resource_dir / fname
        if not xlsx_path.exists():
            logging.warning(f"[batch_export_spc_rules_to_csv] 跳过不存在的文件: {xlsx_path}")
            continue
        try:
            csv_path = xlsx_to_csv(xlsx_path, csv_dir)
            exported.append(csv_path)
        except Exception as e:
            logging.error(f"[batch_export_spc_rules_to_csv] 导出 {fname} 失败: {e}")

    return exported


def save_dict_to_excel(data_dict: dict, output_dir: Path, filename: str):
    """
    [通用工具] 将包含 DataFrame 的字典保存到 Excel。
    (此函数保持原样，无需修改)
    """
    if not isinstance(data_dict, dict) or not data_dict:
        logging.error(f"[调试] 无法保存 {filename}：输入不是有效的字典或字典为空！")
        return

    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        file_path = output_dir / filename

        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            logging.info(f"[调试探针] 正在将数据写入 {file_path}...")
            saved_sheets_count = 0

            for key, value in data_dict.items():
                if isinstance(value, pd.DataFrame) and not value.empty:
                    sheet_name = str(key)
                    clean_sheet_name = sheet_name.replace(':', '_').replace('/', '_').replace('?', '_').replace('*', '_').replace('[', '_').replace(']', '_')
                    if len(clean_sheet_name) > 31: clean_sheet_name = clean_sheet_name[:31]

                    try:
                        value.to_excel(writer, sheet_name=clean_sheet_name, index=False)
                        saved_sheets_count += 1
                        logging.debug(f"Saved top-level DataFrame '{key}' to sheet '{clean_sheet_name}'.")
                    except Exception as sheet_error:
                        logging.error(f"[调试] 写入 Sheet 页 '{clean_sheet_name}' (来自顶层键 '{key}') 时出错: {sheet_error}")

                elif key == 'code_level_details' and isinstance(value, dict):
                    logging.debug("Found 'code_level_details', iterating inner dictionary...")
                    for group_name, group_df in value.items():
                        if isinstance(group_df, pd.DataFrame) and not group_df.empty:
                            sheet_name = str(group_name)
                            clean_sheet_name = sheet_name.replace(':', '_').replace('/', '_').replace('?', '_').replace('*', '_').replace('[', '_').replace(']', '_')
                            if len(clean_sheet_name) > 31: clean_sheet_name = clean_sheet_name[:31]

                            try:
                                group_df.to_excel(writer, sheet_name=clean_sheet_name, index=False)
                                saved_sheets_count += 1
                                logging.debug(f"Saved inner DataFrame '{group_name}' to sheet '{clean_sheet_name}'.")
                            except Exception as sheet_error:
                                logging.error(f"[调试] 写入 Sheet 页 '{clean_sheet_name}' (来自 code_level_details['{group_name}']) 时出错: {sheet_error}")

            if saved_sheets_count > 0:
                logging.info(f"[调试探针] 成功将 {saved_sheets_count} 个 DataFrame 保存到: {file_path}")
            else:
                logging.warning(f"[调试] 未能在字典中找到有效的 DataFrame 以保存到 {filename}。")

    except Exception as e:
        logging.error(f"[调试] 保存调试 Excel 文件 '{filename}' 时发生错误: {e}", exc_info=True)

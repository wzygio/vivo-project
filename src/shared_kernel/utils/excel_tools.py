# src/vivo_project/utils/utils.py
import pandas as pd
import logging
import sys
from pathlib import Path
from typing import Optional
import streamlit as st  # [新增] 引入 streamlit

from src.shared_kernel.config import ConfigLoader


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
    
    # [修复] Streamlit 等多线程环境中 COM 可能未初始化
    try:
        pythoncom.CoInitialize()
    except Exception:
        pass
    
    excel = win32com.client.Dispatch('Excel.Application')
    excel.Visible = False
    excel.DisplayAlerts = False
    
    try:
        wb = excel.Workbooks.Open(str(xlsx_path.resolve()))
        if sheet_name:
            ws = wb.Worksheets(sheet_name)
        else:
            ws = wb.Worksheets(1)
        
        used_range = ws.UsedRange
        data = used_range.Value
        
        if data is None or len(data) < 1:
            wb.Close(SaveChanges=False)
            return pd.DataFrame()
        
        # COM 返回 tuple of tuples，首行为表头
        headers = list(data[0])
        rows = [list(row) for row in data[1:]]
        df = pd.DataFrame(rows, columns=headers)
        
        wb.Close(SaveChanges=False)
        logging.info(f"[xlsx_to_csv] 使用 COM (Excel.Application) 读取加密文件 {xlsx_path.name} 成功, shape={df.shape}")
        return df
    
    except Exception as e:
        logging.error(f"[xlsx_to_csv] COM 读取加密文件失败: {e}")
        raise
    finally:
        try:
            excel.Quit()
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
    目标文件：spc_outlier_filters.xlsx、spc_probe_targets.xlsx

    应在能透明解密企业加密文件的环境中执行一次，
    之后生产代码即可通过 csv fallback 读取规则。

    Returns:
        生成的 csv 文件路径列表
    """
    if resource_dir is None:
        resource_dir = ConfigLoader.get_project_root() / "resources"

    target_files = ["spc_outlier_filters.xlsx", "spc_probe_targets.xlsx"]
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
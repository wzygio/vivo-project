# src/vivo_project/utils/utils.py
import pandas as pd
import logging
import sys
from pathlib import Path
import streamlit as st  # [新增] 引入 streamlit

from vivo_project.config import ConfigLoader

# [双保险 1] 使用 @st.cache_resource 确保 Handler 永驻内存，且全生命周期只初始化一次
@st.cache_resource
def setup_logging(log_filename: str = "app.log"):
    """
    初始化日志系统 (单例模式)。
    """
    # 1. 路径计算
    project_root = ConfigLoader.get_project_root()
    log_dir = project_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_filepath = log_dir / log_filename

    log_format = '%(asctime)s - %(levelname)s - [%(module)s] - %(message)s'
    log_date_format = '%Y-%m-%d %H:%M:%S'

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # 2. 清理逻辑：虽然有 cache_resource，但为了健壮性，防止异常情况下的 Handler 堆积
    # 注意：在 cache_resource 保护下，这段代码通常只会在服务器启动时运行一次
    if root_logger.hasHandlers():
        for handler in root_logger.handlers[:]:
            handler.close()
            root_logger.removeHandler(handler)

    # 3. [双保险 2] 强制使用 mode='a' (追加模式)
    try:
        file_handler = logging.FileHandler(log_filepath, mode='w', encoding='utf-8')
        file_handler.setFormatter(logging.Formatter(log_format, datefmt=log_date_format))
        root_logger.addHandler(file_handler)
    except Exception as e:
        # 如果无法写入文件，至少保证控制台能看到
        print(f"❌ 严重错误：无法初始化日志文件 Handler: {e}")

    # 4. 控制台 Handler
    if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(log_format, datefmt=log_date_format))
        root_logger.addHandler(console_handler)

    # 这一行日志非常关键，证明初始化成功
    logging.info(f"✅ 日志系统已启动 (单例模式 | 追加写入): {log_filepath}")
    
    # 返回 logger 实例，虽然这里不需要接收，但符合 cache_resource 规范
    return root_logger

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
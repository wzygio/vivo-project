# 在 src/vivo_project/utils/utils.py 文件中
import pandas as pd
import logging
from pathlib import Path
import sys

from vivo_project.config import LOG_DIR, PROJECT_ROOT 

class Utils:
    """
    一个静态工具类，提供项目范围内的通用辅助功能。
    """

    @staticmethod
    def setup_logging(log_filename: str = "app.log"):
        """初始化日志系统，输出到文件(覆盖模式)和控制台。"""
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_filepath = LOG_DIR / log_filename

        log_format = '%(asctime)s - %(levelname)s - [%(module)s] - %(message)s'
        log_date_format = '%Y-%m-%d %H:%M:%S'

        root_logger = logging.getLogger()
        # --- [可选但推荐] 设置日志级别应尽早 ---
        #     防止在清除 handlers 之前记录了不需要的低级别日志
        root_logger.setLevel(logging.INFO) # 或 DEBUG

        # 清除旧处理器 (保持不变)
        if root_logger.hasHandlers():
            # 移除处理器并关闭它们，确保文件句柄被释放
            for handler in root_logger.handlers[:]:
                handler.close()
                root_logger.removeHandler(handler)

        # --- [核心修改] 文件处理器，使用 mode='w' ---
        try:
            file_handler = logging.FileHandler(log_filepath, mode='w', encoding='utf-8') # <-- 添加 mode='w'
            file_handler.setFormatter(logging.Formatter(log_format, datefmt=log_date_format))
            root_logger.addHandler(file_handler)
        except Exception as e:
             # 如果文件无法以写入模式打开（例如权限问题），至少还能输出到控制台
             logging.error(f"无法以写入模式打开日志文件 '{log_filepath}': {e}")


        # --- 控制台处理器 (保持不变) ---
        # 避免重复添加控制台处理器（如果之前被清除了）
        if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
             console_handler = logging.StreamHandler(sys.stdout)
             console_handler.setFormatter(logging.Formatter(log_format, datefmt=log_date_format))
             root_logger.addHandler(console_handler)

        logging.info(f"日志系统已初始化，将同时输出到文件 '{log_filepath}' (覆盖模式) 和控制台。")

    @staticmethod
    def save_dict_to_excel(data_dict: dict, output_dir: Path, filename: str):
        """
        [通用工具 V1.1 - 支持 code_level_details] 将包含 DataFrame 的字典保存到 Excel。
        - 顶层键值对中的 DataFrame 会被保存。
        - 如果遇到键名为 'code_level_details' 且值为字典，会将其内部的 Group DataFrame 保存为单独 Sheet 页。
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

                # --- 迭代顶层键 ---
                for key, value in data_dict.items():

                    # --- 情况 1: 顶层值是 DataFrame ---
                    if isinstance(value, pd.DataFrame) and not value.empty:
                        sheet_name = str(key) # 使用顶层键作为 Sheet 名
                        # 清理 Sheet 名称中的非法字符 (Excel 不允许 : / ? * [ ])
                        clean_sheet_name = sheet_name.replace(':', '_').replace('/', '_').replace('?', '_').replace('*', '_').replace('[', '_').replace(']', '_')
                        # 限制 Sheet 名称长度 (Excel 限制 31 字符)
                        if len(clean_sheet_name) > 31: clean_sheet_name = clean_sheet_name[:31]

                        try:
                            value.to_excel(writer, sheet_name=clean_sheet_name, index=False)
                            saved_sheets_count += 1
                            logging.debug(f"Saved top-level DataFrame '{key}' to sheet '{clean_sheet_name}'.")
                        except Exception as sheet_error:
                            logging.error(f"[调试] 写入 Sheet 页 '{clean_sheet_name}' (来自顶层键 '{key}') 时出错: {sheet_error}")

                    # --- 情况 2: 键是 'code_level_details' 且值是字典 ---
                    elif key == 'code_level_details' and isinstance(value, dict):
                        logging.debug("Found 'code_level_details', iterating inner dictionary...")
                        # --- 再次迭代内部字典 ---
                        for group_name, group_df in value.items():
                            if isinstance(group_df, pd.DataFrame) and not group_df.empty:
                                sheet_name = str(group_name) # 使用内部键 (Group Name) 作为 Sheet 名
                                # 清理 Sheet 名称
                                clean_sheet_name = sheet_name.replace(':', '_').replace('/', '_').replace('?', '_').replace('*', '_').replace('[', '_').replace(']', '_')
                                if len(clean_sheet_name) > 31: clean_sheet_name = clean_sheet_name[:31]

                                try:
                                    group_df.to_excel(writer, sheet_name=clean_sheet_name, index=False)
                                    saved_sheets_count += 1
                                    logging.debug(f"Saved inner DataFrame '{group_name}' to sheet '{clean_sheet_name}'.")
                                except Exception as sheet_error:
                                    logging.error(f"[调试] 写入 Sheet 页 '{clean_sheet_name}' (来自 code_level_details['{group_name}']) 时出错: {sheet_error}")
                            else:
                                 logging.warning(f"[调试] 'code_level_details' 中的键 '{group_name}' 不是有效的 DataFrame 或为空，已跳过。")

                # --- 报告结果 ---
                if saved_sheets_count > 0:
                     logging.info(f"[调试探针] 成功将 {saved_sheets_count} 个 DataFrame 保存到: {file_path}")
                else:
                     logging.warning(f"[调试] 未能在字典中找到有效的 DataFrame (包括 code_level_details 内部) 以保存到 {filename}。")

        except Exception as e:
            logging.error(f"[调试] 保存调试 Excel 文件 '{filename}' 时发生错误: {e}", exc_info=True)

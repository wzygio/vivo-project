import pandas as pd
import os, logging, time  # 导入日志模块
import streamlit as st  # 导入 streamlit 库
from pathlib import Path
from typing import Dict

from src.shared_kernel.config_model import AppConfig

class ExcelService:
    @staticmethod
    def load_and_clean_data(file_path: str, sheet_name: str = "Sheet1") -> pd.DataFrame:
        """
        智能加载 Excel：自动寻找表头、清洗空列、填充合并单元格
        [修改] 增加 sheet_name 参数，默认为 'Sheet1'
        """
        if not os.path.exists(file_path):  # 检查文件是否存在
            return pd.DataFrame()  # 如果不存在，返回空的 DataFrame

        try:
            # 1. 智能寻找表头行
            # [修改] 显式指定读取 Sheet1，避免读取到错误的隐藏 Sheet
            df_preview = pd.read_excel(
                file_path, 
                header=None, 
                nrows=10, 
                engine='openpyxl', 
                sheet_name=sheet_name # 显式指定 Sheet
            )
            
            header_row_idx = 0  # 初始化表头行索引为 0
            
            for i, row in df_preview.iterrows():  # 遍历预读取的每一行
                row_str = row.astype(str).values  # 将行数据转换为字符串数组
                # 关键词匹配，只要命中一个即可认为是表头
                if any(k in s for k in ["Issue名称", "Issue描述", "北极星指标", "序号", "No."] for s in row_str):
                    header_row_idx = i  # 记录表头行
                    break
            
            # 2. 正式读取
            # [修改] 显式指定读取 Sheet1
            df = pd.read_excel(
                file_path, 
                header=header_row_idx, # type: ignore
                engine='openpyxl', 
                sheet_name=sheet_name # 显式指定 Sheet
            ) # type: ignore

            # 3. 清洗列名 (去除 Unnamed 空列)
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            
            # 4. 去除全空行
            df.dropna(how='all', inplace=True)

            # 5. 处理合并单元格 (向下填充)
            target_cols = ['Issue名称', '工艺段', '发现方', '型号', '北极星指标']
            for col in target_cols:
                if col in df.columns:
                    df[col] = df[col].ffill()

            # 6. 格式化日期
            if '发生日期' in df.columns:
                # 仅转换为 datetime 对象，严禁转换为字符串
                df['发生日期'] = pd.to_datetime(df['发生日期'], errors='coerce')

            return df

        except ValueError as ve:
            # 专门捕获 Sheet 不存在的错误
            logging.error(f"Excel 读取失败: {ve}")
            st.error(f"读取失败：文件中未找到名为 '{sheet_name}' 的工作表。请检查 Excel 文件。")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Excel 读取失败: {e}")
            st.error(f"无法读取 Excel 文件: {e}")
            return pd.DataFrame()

    @staticmethod
    def highlight_status(val):
        """Pandas Styler 样式函数"""
        if val == 'Open':
            return 'background-color: #ffcdd2; color: #b71c1c; font-weight: bold'
        elif val == 'Close':
            return 'background-color: #c8e6c9; color: #1b5e20; font-weight: bold'
        return ''

    @staticmethod
    def get_file_timestamp(file_path: str) -> float:
        """获取文件的最后修改时间戳"""
        if os.path.exists(file_path):
            return os.path.getmtime(file_path)
        return 0.0

    @staticmethod
    def save_data_with_lock(file_path: str, df: pd.DataFrame, expected_timestamp: float, sheet_name: str = "Sheet1") -> tuple[bool, str]:
        """
        带乐观锁和文件锁的安全保存
        [修改] 增加 sheet_name 参数，默认为 'Sheet1'
        """
        lock_file = file_path + ".lock"
        
        try:
            # 1. 乐观锁检查
            current_timestamp = ExcelService.get_file_timestamp(file_path)
            if current_timestamp != expected_timestamp and expected_timestamp != 0.0:
                return False, "保存失败：数据已过期！\n有同事在您编辑期间提交了新版本。\n请刷新页面获取最新数据后再试。"

            # 2. 获取文件互斥锁
            max_retries = 5
            for _ in range(max_retries):
                if not os.path.exists(lock_file):
                    try:
                        with open(lock_file, 'w') as f:
                            f.write("LOCKED")
                        break
                    except Exception:
                        time.sleep(0.1)
                else:
                    time.sleep(0.1)
            else:
                return False, "系统繁忙：当前文件正在被写入，请稍后重试。"

            # 3. 执行写入
            # [修改] 显式指定写入 Sheet1
            # 注意：这将完全重写文件。如果原文件有其他 Sheet，将会丢失！
            # 如果需要保留其他 Sheet，需要改用 pd.ExcelWriter(mode='a')，但那会更复杂且容易出错。
            # 目前逻辑假设每个文件只服务于这一个台账业务。
            df.to_excel(
                file_path, 
                index=False, 
                sheet_name=sheet_name # 显式写入 Sheet1
            )
            
            return True, "保存成功！"

        except Exception as e:
            logging.error(f"保存 Excel 失败: {e}")
            return False, f"保存发生未知错误: {e}"
        
        finally:
            # 4. 释放锁
            if os.path.exists(lock_file):
                try:
                    os.remove(lock_file)
                except Exception as e:
                    logging.error(f"无法移除锁文件: {e}")

    # ==============================================================================
    #                      Excel 覆盖适配器 (Adapter)
    # ==============================================================================
    @staticmethod
    def _parse_override_excel(excel_path: Path) -> Dict[str, Dict[str, Dict[str, float]]]:
        """解析双Sheet页的覆盖配置Excel为嵌套字典格式"""
        overrides = {
            'group_monthly_values': {}, 'group_weekly_values': {}, 'group_daily_values': {},
            'code_monthly_values': {}, 'code_weekly_values': {}, 'code_daily_values': {}
        }
        if not excel_path.exists():
            return overrides

        try:
            xls = pd.read_excel(excel_path, sheet_name=None, engine="openpyxl")
            
            def _parse_sheet(df, level_prefix):
                if df.empty: return
                for _, row in df.iterrows():
                    target = str(row.get('目标名称', '')).strip()
                    period_cn = str(row.get('周期类型', '')).strip()
                    time_key = str(row.get('时间标签', '')).strip()
                    rate_val = row.get('期望不良率', 0.0)
                    
                    if not target or target == 'nan' or not period_cn or not time_key or pd.isna(rate_val):
                        continue
                        
                    # 智能兼容百分比字符串 (如 "1.03%") 和 小数 (如 0.0103)
                    if isinstance(rate_val, str) and '%' in rate_val:
                        rate_val = float(rate_val.replace('%', '')) / 100.0
                    else:
                        rate_val = float(rate_val)
                        # 防呆设计：如果业务人员手滑输入了 1.5 但没加 %，强制转换为 0.015
                        if rate_val > 1.0: 
                            rate_val = rate_val / 100.0

                    period_map = {'月度': 'monthly', '周度': 'weekly', '日度': 'daily'}
                    period_en = period_map.get(period_cn)
                    if not period_en: continue
                    
                    dict_key = f"{level_prefix}_{period_en}_values"
                    if target not in overrides[dict_key]:
                        overrides[dict_key][target] = {}
                    overrides[dict_key][target][time_key] = rate_val

            if 'Group级' in xls:
                _parse_sheet(xls['Group级'], 'group')
            if 'Code级' in xls:
                _parse_sheet(xls['Code级'], 'code')
                
        except Exception as e:
            logging.error(f"解析覆盖Excel失败: {e}", exc_info=True)
            
        return overrides

    @staticmethod
    def inject_excel_overrides_to_config(config: AppConfig, product_dir: Path):
        """
        [核心] 在数据进入底层运算前，拦截并用 Excel 的数据覆盖 YAML 的配置。
        底层 mwd_trend_processor.py 完全无感知。
        """
        override_res = config.paths.get('mwd_override_config')
        if not override_res: 
            return
        
        excel_path = product_dir / override_res.file_name
        excel_overrides = ExcelService._parse_override_excel(excel_path)
        
        # 将解析出的字典注入到 config.processing 中，完美替换原有的 YAML 节点
        for key, value_dict in excel_overrides.items():
            if value_dict:  # 如果 Excel 中有配置，则覆盖
                config.processing[key] = value_dict
import pandas as pd
import os
import logging
import streamlit as st

class ExcelService:
    @staticmethod
    def load_and_clean_data(file_path: str) -> pd.DataFrame:
        """
        智能加载 Excel：自动寻找表头、清洗空列、填充合并单元格
        """
        if not os.path.exists(file_path):
            return pd.DataFrame()

        try:
            # 1. 智能寻找表头行
            # 先读前10行，寻找包含 "Issue" 或 "No." 等关键词的行
            df_preview = pd.read_excel(file_path, header=None, nrows=10, engine='openpyxl')
            header_row_idx = 0 # 默认第0行
            
            for i, row in df_preview.iterrows():
                row_str = row.astype(str).values
                # 关键词匹配，只要命中一个即可认为是表头
                if any(k in s for k in ["Issue名称", "Issue描述", "北极星指标", "序号", "No."] for s in row_str):
                    header_row_idx = i
                    break
            
            # 2. 正式读取
            df = pd.read_excel(file_path, header=header_row_idx, engine='openpyxl')

            # 3. 清洗列名 (去除 Unnamed 空列)
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            
            # 4. 去除全空行
            df.dropna(how='all', inplace=True)

            # 5. 处理合并单元格 (向下填充)
            # 即使是普通 Excel，执行这个也没坏处
            target_cols = ['Issue名称', '工艺段', '发现方', '型号', '北极星指标']
            for col in target_cols:
                if col in df.columns:
                    df[col] = df[col].ffill()

            # 6. 格式化日期
            if '发生日期' in df.columns:
                df['发生日期'] = pd.to_datetime(df['发生日期'], errors='coerce').dt.strftime('%Y-%m-%d')

            return df

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
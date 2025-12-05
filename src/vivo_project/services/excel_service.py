import streamlit as st
import os
from pathlib import Path
import logging
import pandas as pd
import time 


class ExcelService:
    @staticmethod
    def load_and_clean_data(file_path: str) -> pd.DataFrame:
        """
        智能加载 Excel：自动寻找表头、清洗空列、填充合并单元格
        """
        if not os.path.exists(file_path):  # 检查文件是否存在
            return pd.DataFrame()  # 如果不存在，返回空的 DataFrame

        try:
            # 1. 智能寻找表头行
            # 先读前10行，寻找包含 "Issue" 或 "No." 等关键词的行
            df_preview = pd.read_excel(file_path, header=None, nrows=10, engine='openpyxl')  # 预读取前10行，不指定表头
            header_row_idx = 0  # 初始化表头行索引为 0
            
            for i, row in df_preview.iterrows():  # 遍历预读取的每一行
                row_str = row.astype(str).values  # 将行数据转换为字符串数组
                # 关键词匹配，只要命中一个即可认为是表头
                if any(k in s for k in ["Issue名称", "Issue描述", "北极星指标", "序号", "No."] for s in row_str):  # 检查是否包含关键词
                    header_row_idx = i  # 如果包含，记录当前行号为表头索引
                    break  # 找到后跳出循环
            
            # 2. 正式读取
            df = pd.read_excel(file_path, header=header_row_idx, engine='openpyxl')  # type: ignore

            # 3. 清洗列名 (去除 Unnamed 空列)
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]  # 过滤掉列名以 'Unnamed' 开头的列
            
            # 4. 去除全空行
            df.dropna(how='all', inplace=True)  # 删除所有值都为空的行

            # 5. 处理合并单元格 (向下填充)
            # 即使是普通 Excel，执行这个也没坏处
            target_cols = ['Issue名称', '工艺段', '发现方', '型号', '北极星指标']  # 定义需要向下填充的列
            for col in target_cols:  # 遍历目标列
                if col in df.columns:  # 如果列存在于 DataFrame 中
                    df[col] = df[col].ffill()  # 执行向下填充操作

            # 6. 格式化日期
            if '发生日期' in df.columns:
                # [修改] 仅转换为 datetime 对象，严禁转换为字符串 (.strftime)，否则 st.column_config.DateColumn 无法编辑
                df['发生日期'] = pd.to_datetime(df['发生日期'], errors='coerce')

            return df  # 返回处理后的 DataFrame

        except Exception as e:
            logging.error(f"Excel 读取失败: {e}")  # 记录错误日志
            st.error(f"无法读取 Excel 文件: {e}")  # 在界面显示错误信息
            return pd.DataFrame()  # 出错时返回空 DataFrame

    @staticmethod
    def highlight_status(val):
        """Pandas Styler 样式函数"""
        if val == 'Open':  # 如果值为 'Open'
            return 'background-color: #ffcdd2; color: #b71c1c; font-weight: bold'  # 返回红色背景样式
        elif val == 'Close':  # 如果值为 'Close'
            return 'background-color: #c8e6c9; color: #1b5e20; font-weight: bold'  # 返回绿色背景样式
        return ''  # 其他情况不应用样式

    # --- 以下为新增功能：并发安全保存支持 ---

    @staticmethod
    def get_file_timestamp(file_path: str) -> float:
        """获取文件的最后修改时间戳"""
        if os.path.exists(file_path):  # 如果文件存在
            return os.path.getmtime(file_path)  # 返回文件的修改时间戳
        return 0.0  # 如果文件不存在，返回 0.0

    @staticmethod
    def save_data_with_lock(file_path: str, df: pd.DataFrame, expected_timestamp: float) -> tuple[bool, str]:
        """
        带乐观锁和文件锁的安全保存
        :param file_path: 文件路径
        :param df: 要保存的数据
        :param expected_timestamp: 加载数据时记录的时间戳 (用于乐观锁校验)
        :return: (是否成功, 信息消息)
        """
        lock_file = file_path + ".lock"  # 定义锁文件路径
        
        try:
            # 1. 乐观锁检查 (Optimistic Locking)
            current_timestamp = ExcelService.get_file_timestamp(file_path)  # 获取当前文件的实际修改时间
            if current_timestamp != expected_timestamp and expected_timestamp != 0.0:  # 如果时间不一致且不是新文件
                # 时间不匹配，说明有人在你编辑期间修改了文件
                return False, "保存失败：数据已过期！\n有同事在您编辑期间提交了新版本。\n请刷新页面获取最新数据后再试。"  # 返回失败信息

            # 2. 获取文件互斥锁 (File Lock) - 防止写入时的瞬间冲突
            # 尝试创建锁文件，如果已存在则等待重试 (简单自旋锁)
            max_retries = 5  # 最大重试次数
            for _ in range(max_retries):  # 循环尝试
                if not os.path.exists(lock_file):  # 如果锁文件不存在
                    try:
                        with open(lock_file, 'w') as f:  # 创建锁文件
                            f.write("LOCKED")  # 写入标识
                        break  # 成功获取锁，跳出循环
                    except Exception:  # 创建失败（可能被抢占）
                        time.sleep(0.1)  # 等待 0.1 秒
                else:
                    time.sleep(0.1)  # 如果锁文件已存在，等待 0.1 秒
            else:
                # 循环结束仍未获取锁
                return False, "系统繁忙：当前文件正在被写入，请稍后重试。"  # 返回忙碌信息

            # 3. 执行写入
            # 注意：这将覆盖原文件。如果原文件有复杂的表头样式，to_excel 可能会丢失样式。
            # 但为了保证数据一致性，这是必要的权衡。
            df.to_excel(file_path, index=False)  # 将 DataFrame 写入 Excel，不包含索引列
            
            return True, "保存成功！"  # 返回成功信息

        except Exception as e:
            logging.error(f"保存 Excel 失败: {e}")  # 记录保存错误日志
            return False, f"保存发生未知错误: {e}"  # 返回异常信息
        
        finally:
            # 4. 释放锁
            if os.path.exists(lock_file):  # 无论成功失败，都要清理锁文件
                try:
                    os.remove(lock_file)  # 删除锁文件
                except Exception as e:  # 如果删除失败（极少见）
                    logging.error(f"无法移除锁文件: {e}")  # 记录日志，不影响主流程
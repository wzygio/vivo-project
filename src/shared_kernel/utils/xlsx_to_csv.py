#%%
import sys
import os

# 获取项目根目录的更可靠方式
def get_project_root():
    """获取项目根目录"""
    # 从当前文件开始向上查找，直到找到包含特定标记的目录
    current_path = os.path.abspath(__file__)
    while True:
        parent_path = os.path.dirname(current_path)
        # 如果到达了文件系统的根目录，停止查找
        if parent_path == current_path:
            return current_path
        # 检查是否是项目根目录（可以根据实际情况调整判断条件）
        if os.path.exists(os.path.join(parent_path, 'src')) and \
           os.path.exists(os.path.join(parent_path, 'pyproject.toml')):
            return parent_path
        current_path = parent_path

# 将项目根目录添加到 sys.path
project_root = get_project_root()
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import pandas as pd
import os
from pathlib import Path
from src.config import load_config

def convert_xlsx_to_csv(input_path, output_path):
    # 1. 检查文件是否存在
    if not os.path.exists(input_path):
        print(f"错误：找不到文件 {input_path}")
        return

    try:
        # 2. 读取 Excel 文件
        # sheet_name=None 表示读取所有 Sheet，返回一个字典 {sheet_name: DataFrame}
        print(f"正在读取: {input_path} ...")
        all_sheets = pd.read_excel(input_path, sheet_name=None, engine='openpyxl')
        
        # 如果 output_path 是一个文件路径（包含.csv），我们提取其目录和基础文件名
        # 如果 output_path 是一个目录，我们提取输入文件的基础文件名
        if os.path.splitext(output_path)[1].lower() == '.csv':
            # 假设 output_path 是一个完整文件路径，例如 'data/output.csv'
            output_dir = os.path.dirname(output_path)
            base_filename = os.path.splitext(os.path.basename(output_path))[0]
        else:
            # 假设 output_path 是一个目录，例如 'data/output/'
            output_dir = output_path
            base_filename = os.path.splitext(os.path.basename(input_path))[0]
            
        # 确保输出目录存在
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # 3. 遍历所有 Sheet 并保存为 CSV
        count = 0
        for sheet_name, df in all_sheets.items():
            # 构造新的文件名，例如: target_file_Sheet1.csv
            # 处理 sheet_name 中可能包含的非法文件名字符（可选，视需求而定）
            safe_sheet_name = "".join([c for c in sheet_name if c.isalnum() or c in (' ', '-', '_')]).strip()
            output_file = os.path.join(output_dir, f"{base_filename}_{safe_sheet_name}.csv")
            
            # 保存为 CSV
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"成功！Sheet [{sheet_name}] 已保存至: {output_file}")
            count += 1
            
        print(f"转换完成！共转换了 {count} 个工作表。")

    except Exception as e:
        print(f"转换失败: {e}")


def split_csv_by_rows(file_path, output_dir, rows_per_file=50):
    """
    将指定的 CSV 文件按行数拆分为多个小 CSV 文件。
    
    参数:
    file_path (str): 源 CSV 文件路径
    rows_per_file (int): 每个小文件包含的行数，默   认为 50
    """
    try:
        # 1. 读取原始 CSV 文件
        # 使用 utf-8-sig 可以确保包含中文的文件被正确读取和写入
        df = pd.read_csv(file_path)
        total_rows = len(df)
        # 创建output_dir目录
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        # 2. 获取文件名并创建存放拆分文件的文件夹
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        
        print(f"开始拆分文件，总行数: {total_rows}")
        
        # 3. 循环切割并保存
        count = 0
        for i in range(0, total_rows, rows_per_file):
            # 切取指定范围的数据
            chunk = df.iloc[i : i + rows_per_file]
            
            # 构造新的文件名（例如: 原文件名_part_1.csv）
            chunk_num = (i // rows_per_file) + 1
            output_file = os.path.join(output_dir, f"{base_name}_part_{chunk_num}.csv")
            
            # 保存为 CSV
            chunk.to_csv(output_file, index=False, encoding='utf-8-sig')
            count += 1
            
        print(f"拆分完成！共生成 {count} 个文件，存放在目录: {output_dir}")
        return output_dir

    except Exception as e:
        print(f"拆分过程中出现错误: {e}")
        return None

def batch_convert_folder(folder_path):
    # 获取文件夹内所有文件
    if not os.path.exists(folder_path):
        print("文件夹不存在！")
        return

    files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]
    
    if not files:
        print("该文件夹内没有找到 .xlsx 文件")
        return

    print(f"找到 {len(files)} 个 Excel 文件，开始转换...\n")

    for file_name in files:
        # 构建完整路径
        input_full_path = os.path.join(folder_path, file_name)
        
        # 构建输出文件名 (将 .xlsx 替换为 .csv)
        output_file_name = file_name.replace('.xlsx', '.csv')
        output_full_path = os.path.join(folder_path, output_file_name)

        try:
            # 读取并转换
            df = pd.read_excel(input_full_path, engine='openpyxl')
            df.to_csv(output_full_path, index=False, encoding='utf-8-sig')
            print(f"[√] 转换成功: {file_name} -> {output_file_name}")
        except Exception as e:
            print(f"[x] 转换失败 {file_name}: {e}")

    print("\n所有任务完成！")

if __name__ == "__main__":
    config = load_config()

    # === 在这里修改您的文件名 ===
    source_file = "target-file.xlsx"   # 您的 Excel 文件名
    target_file = "target-file.csv"    # 您想要生成的 CSV 文件名
    resources_dir = config.paths.resources_dir
    output_dir = config.paths.output_dir

    convert_xlsx_to_csv(resources_dir/source_file, output_dir/'xlsx_to_csv_sheet')
    # split_csv_by_rows(Path(output_dir / target_file), Path(output_dir / 'xlsx_to_csv_split'), 20)

    # target_folder = '.' 
    # batch_convert_folder(target_folder)
# %%

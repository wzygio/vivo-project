#%%
# main.py
import logging
from dotenv import load_dotenv
from pathlib import Path
import sys

# --- 1. 初始化与配置 ---

# 确保项目根目录在搜索路径中
project_root = Path(__file__).resolve().parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# 导入我们的工具类和工作流
from vivo_project.utils.utils import Utils
from vivo_project.services.workflow_handler import WorkflowHandler

# 设置日志系统
Utils.setup_logging("main_test_run.log")

# 加载环境变量
# 注意: 确保你的.env文件在/config目录下
env_path = project_root / 'config' / '.env'
load_dotenv(dotenv_path=env_path)

def main():
    """
    项目主入口函数，用于测试后台工作流。
    """
    logging.info("--- [测试运行] OLED不良率分析项目启动 ---")
    
    # --- 2. 直接调用核心工作流 ---
    # 这个函数现在会返回一个包含多个DataFrame的字典
    result_data = WorkflowHandler.run_sheet_defect_rate_workflow()
    
    # --- 3. 检查并打印结果 ---
    if result_data:
        logging.info("工作流执行成功，返回了数据字典。")
        
        # 打印主表 (Group Level Summary)
        print("\n" + "="*20 + " 主表 (Group Level Summary) 预览: " + "="*20)
        group_summary_df = result_data.get("group_level_summary")
        if group_summary_df is not None and not group_summary_df.empty:
            print(group_summary_df.head())
        else:
            print("未能获取到Group级别总览表。")

        # 打印明细表 (Code Level Details)
        print("\n" + "="*20 + " 明细表 (Code Level Details) 预览: " + "="*20)
        code_details_dict = result_data.get("code_level_details")
        if code_details_dict:
            for group_name, detail_df in code_details_dict.items():
                print(f"\n--- {group_name} ---")
                if detail_df is not None and not detail_df.empty:
                    print(detail_df.head())
                else:
                    print(f"(该Group下未发现缺陷数据)")
        else:
            print("未能获取到Code级别明细表。")

    else:
        logging.error("工作流执行失败，未返回任何数据。")
        
    logging.info("--- [测试运行] 项目流程结束 ---")


if __name__ == '__main__':
    main()
# %%

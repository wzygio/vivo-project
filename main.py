#%%
# main.py (V2.0 - 全工作流测试版)
import logging
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
import sys

# --- [核心修改] 恢复路径计算和 sys.path 添加 ---
# a. 获取项目根目录 (main.py 所在的目录)
project_root = Path(__file__).resolve().parent
# b. 计算 src 目录路径
src_root = project_root / 'src'
# c. [关键] 将 src 目录添加到 Python 搜索路径的最前面
if str(src_root) not in sys.path:
    sys.path.insert(0, str(src_root))

# 导入我们的工具类和工作流
from vivo_project.config import CONFIG
from vivo_project.utils.app_setup import AppSetup
from vivo_project.utils.utils import Utils
from vivo_project.services.yield_service import YieldAnalysisService
from vivo_project.config import PROJECT_ROOT, DATA_DIR

def main():
    """
    项目主入口函数，用于【一键测试】所有后台工作流的健全性。
    它会依次调用每个工作流，并记录成功或失败，但不会打印数据内容。
    """
    logging.info("--- [测试运行] 全工作流健全性检查启动 ---")

    Utils.setup_logging("main.log")

    # --- 2. 定义所有需要测试的工作流函数名称 ---
    workflows_to_test = [
        "run_sheet_defect_rate_workflow",
        "run_lot_defect_rate_workflow",
        # "run_mwd_trend_workflow",
        # "run_code_level_mwd_trend_workflow",
        # "run_current_month_trend_workflow",
        # "run_mapping_data_workflow",
    ]

    success_count = 0
    failure_count = 0

    output_debug_dir = DATA_DIR / "processed"
    # --- 3. 循环测试每个工作流 ---
    for workflow_name in workflows_to_test:
        logging.info(f"--- 正在测试: {workflow_name} ---")
        try:
            # 使用getattr动态获取要调用的函数
            workflow_func = getattr(WorkflowHandler, workflow_name)
            result = workflow_func()

            # --- [核心修改 2] 使用 Utils.save_dict_to_excel ---
            if workflow_name == 'run_sheet_defect_rate_workflow' and result:
                Utils.save_dict_to_excel( # <-- 修改调用
                    data_dict=result,
                    output_dir=output_debug_dir,
                    filename="debug_SHEET_results.xlsx"
                )
            if workflow_name == 'run_lot_defect_rate_workflow' and result:
                Utils.save_dict_to_excel( # <-- 修改调用
                    data_dict=result,
                    output_dir=output_debug_dir,
                    filename="debug_LOT_results.xlsx"
                )
            if workflow_name == 'run_code_level_mwd_trend_workflow' and result:
                 # Utils.save_dict_to_excel 也能处理 mwd 结果字典
                Utils.save_dict_to_excel( # <-- 修改调用
                    data_dict=result,
                    output_dir=output_debug_dir,
                    filename="debug_MWD_CODE_results.xlsx"
                )
                
            # 检查结果（即使不打印，也确认它没有因逻辑错误而返回意外的None）
            if result is not None:
                logging.info(f"[成功] {workflow_name} 执行完毕。")
                success_count += 1
            else:
                # 某些函数在特定条件下（如无数据）返回None是正常的，我们将其标记为警告
                logging.warning(f"[警告] {workflow_name} 执行完毕，但返回了 None。")
                success_count += 1 # 只要没崩溃，就算执行成功
                
        except Exception as e:
            # 如果函数执行过程中发生任何崩溃，记录错误并继续
            logging.error(f"[失败] {workflow_name} 执行时发生异常: {e}", exc_info=True)
            failure_count += 1

    # --- 4. 打印最终总结 ---
    logging.info("--- [测试运行] 全工作流健全性检查结束 ---")
    print("\n" + "="*30 + " 测试总结 " + "="*30)
    print(f"  成功: {success_count} 个")
    print(f"  失败: {failure_count} 个")
    print("="*72)

    if failure_count > 0:
        logging.error("测试运行中发现错误，请检查上面的日志详情 (main_test_run.log)。")
    else:
        logging.info("所有工作流均已成功执行，未发生崩溃。")


if __name__ == '__main__':
    main()
# %%
# 🎯 Target File: src/shared_kernel/utils/data_inspector.py
# 🛠️ Action: 替换为单文件多 Sheet 动态覆盖探针

import pandas as pd
import logging
import re
from pathlib import Path
from src.shared_kernel.config import ConfigLoader

def export_probed_details(df: pd.DataFrame, probe_name: str) -> None:
    """
    [全链路数据探针 - 单文件多 Sheet 模式]
    读取本地名单拦截目标数据，并将不同探针的数据输出到 logs/spc_probe_results.xlsx 
    的不同 Sheet 中。永远不带时间戳，同名 Sheet 自动覆盖保留最新。
    """
    if df is None or df.empty:
        return

    try:
        root_dir = ConfigLoader.get_project_root()
        target_file = root_dir / "resources" / "spc_probe_targets.xlsx"
        
        # 1. 探针名单不存在则静默放行
        if not target_file.exists():
            return
            
        # 2. 读取目标名单
        targets_df = pd.read_excel(target_file, dtype=str).fillna("")
        
        required_cols = ['prod_code', 'sheet_id', 'step_id', 'param_name']
        if not all(col in targets_df.columns for col in required_cols):
            logging.warning(f"🚨 [{probe_name}] 探针目标表缺少必要字段，必须包含: {required_cols}")
            return
            
        if targets_df.empty:
            return

        # 3. 构建多重精确捕获网
        final_mask = pd.Series(False, index=df.index)
        df_clean = df.copy()
        
        for col in required_cols:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.strip()

        for _, target in targets_df.iterrows():
            mask = pd.Series(True, index=df_clean.index)
            for col in required_cols:
                if col in df_clean.columns:
                    mask &= (df_clean[col] == str(target[col]).strip())
            final_mask |= mask

        # 4. 执行捕获与单文件多 Sheet 落盘
        if final_mask.any():
            hit_data = df[final_mask]
            
            logs_dir = root_dir / "logs"
            logs_dir.mkdir(parents=True, exist_ok=True)
            
            # [核心改动 1] 固定输出文件名称，去除时间戳
            export_path = logs_dir / "spc_probe_results.xlsx"
            
            # [核心改动 2] 净化 Sheet 名称 (Excel 规定 Sheet 名不能超过31字符，且不能包含特殊符号)
            safe_sheet_name = re.sub(r'[\\/*?:\[\]]', '', probe_name).strip()[:31]
            
            # [核心改动 3] 智能追加与覆盖写入
            if export_path.exists():
                # 如果文件已存在，使用追加模式 (a)，遇到同名 Sheet 直接替换 (replace)
                with pd.ExcelWriter(export_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
                    hit_data.to_excel(writer, sheet_name=safe_sheet_name, index=False)
            else:
                # 如果是第一次创建文件，使用写入模式 (w)
                with pd.ExcelWriter(export_path, engine='openpyxl', mode='w') as writer:
                    hit_data.to_excel(writer, sheet_name=safe_sheet_name, index=False)
            
            logging.warning(f"🚨 [{probe_name}] 成功捕获 {len(hit_data)} 条明细！已覆盖更新至 logs/spc_probe_results.xlsx (Sheet: {safe_sheet_name})")

    except Exception as e:
        logging.error(f"🚨 [{probe_name}] 探针导出异常: {e}")
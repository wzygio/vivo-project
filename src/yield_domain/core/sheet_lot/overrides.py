# src/vivo_project/core/sheet_lot/overrides.py
import logging
from collections import defaultdict
from pathlib import Path
from typing import Dict, Optional

import comtypes
import comtypes.client
import numpy as np
import pandas as pd

def _load_override_excel(
    override_file_path: Optional[Path],
    override_sheet_name: str
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """
    [Refactor] 接收完整的 Path 对象
    """
    if not override_file_path or not override_sheet_name:
        return None, None
        
    logging.info(f"--- [COM Loader] 开始加载覆盖数据 (文件: '{override_file_path.name}') ---")
    abs_path = str(override_file_path.resolve())

    if not override_file_path.exists():
        logging.error(f"[COM] 文件不存在: {abs_path}")
        return None, None

    # --- COM 初始化 ---
    try:
        comtypes.CoInitialize()
    except:
        pass 

    excel_app = None
    workbook = None
    
    try:
        # [逻辑保持不变，仅路径来源变了]
        logging.info("[COM] 正在启动 Excel 应用程序实例...")
        excel_app = comtypes.client.CreateObject("Excel.Application")
        excel_app.Visible = False
        excel_app.DisplayAlerts = False 

        logging.info(f"[COM] 正在打开工作簿: {abs_path}")
        workbook = excel_app.Workbooks.Open(abs_path)

        try:
            sheet = workbook.Sheets(override_sheet_name)
        except Exception:
            logging.error(f"[COM] 找不到名为 '{override_sheet_name}' 的 Sheet 页。")
            return None, None

        raw_data = sheet.UsedRange.Value()
        
        if not raw_data or len(raw_data) < 2:
            logging.warning("[COM] Excel 数据为空或只有表头。")
            return None, None

        logging.info(f"[COM] 成功通过 Excel 提取数据，共 {len(raw_data)} 行。")

        header = raw_data[0]
        rows = raw_data[1:]
        
        rows_cleaned = []
        for row in rows:
            rows_cleaned.append(list(row) if row else [None]*len(header))

        df = pd.DataFrame(rows_cleaned, columns=list(header))

        expected_cols = ['lot_id', 'sheet_id', 'override_rate', 'defect_desc']
        
        df.columns = [str(c).strip() for c in df.columns]
        
        missing_cols = [col for col in expected_cols if col not in df.columns]
        if missing_cols:
            logging.error(f"[COM] 缺少必需列: {missing_cols}。实际列: {df.columns.to_list()}")
            return None, None

        if df['override_rate'].dtype == 'object':
             df['override_rate'] = df['override_rate'].astype(str).str.rstrip('%')
             df['override_rate'] = pd.to_numeric(df['override_rate'], errors='coerce')
             if df['override_rate'].mean() > 1.0:
                 df['override_rate'] = df['override_rate'] / 100.0

        df['defect_desc'] = df['defect_desc'].astype(str).str.strip()
        df.dropna(subset=expected_cols, inplace=True)
        
        lot_override_df = df.groupby(['lot_id', 'defect_desc'])['override_rate'].mean().reset_index()
        lot_override_df.rename(columns={'override_rate': 'override_rate_avg'}, inplace=True)

        return df[expected_cols], lot_override_df[['lot_id', 'defect_desc', 'override_rate_avg']]

    except Exception as e:
        logging.error(f"[COM] Excel 读取失败: {e}", exc_info=True)
        return None, None

    finally:
        if workbook:
            try:
                workbook.Close(False)
            except: pass
        if excel_app:
            try:
                excel_app.Quit()
            except: pass
        try:
            comtypes.CoUninitialize()
        except: pass

def _calculate_lot_override_rate_heuristic(
    override_df: pd.DataFrame,
    lot_base_info_df: pd.DataFrame,
    mwd_code_data: Dict[str, pd.DataFrame] | None
) -> pd.DataFrame:
    """
    [新增 V1.0 - 启发式公式] 计算 Lot 级覆盖良损。
    公式: LotRate = 当月良损 + (同卡Sheet良损之和) / (30 + 同卡Sheet数)
    """
    logging.info("开始使用启发式公式计算 Lot 级覆盖不良率...")
    
    if override_df is None or override_df.empty:
        return pd.DataFrame()

    try:
        # 1. 计算 "同卡Sheet良损和" (Sum) 和 "同卡Sheet数" (Count)
        # -----------------------------------------------------------
        # 按 Lot 和 缺陷描述分组聚合
        lot_stats = override_df.groupby(['lot_id', 'defect_desc'])['override_rate'].agg(
            rate_sum='sum',
            sheet_count='count'
        ).reset_index()
        
        # 2. 准备 "当月良损" (Base Rate)
        # -----------------------------------------------------------
        # 需要先获取每个 Lot 的时间，以便匹配月度数据
        if lot_base_info_df is not None and not lot_base_info_df.empty:
            # 仅保留需要的列
            lot_dates = lot_base_info_df[['lot_id', 'warehousing_time']].drop_duplicates()
            # 合并时间信息到统计表
            lot_stats = pd.merge(lot_stats, lot_dates, on='lot_id', how='left')
        else:
            lot_stats['warehousing_time'] = pd.NaT
            logging.warning("缺少 Lot 基础信息，无法匹配当月良损，将默认当月良损为 0。")

        # 将时间转换为 YYYY-MM 格式以匹配 mwd_code_data
        lot_stats['time_period'] = pd.to_datetime(
            lot_stats['warehousing_time'], format='%Y%m%d', errors='coerce'
        ).dt.strftime('%Y-%m月')
        
        # 从 mwd_code_data 中提取月度基准
        monthly_map = {}
        if mwd_code_data and 'monthly' in mwd_code_data:
            df_monthly = mwd_code_data['monthly']
            if not df_monthly.empty and {'time_period', 'defect_desc', 'defect_rate'}.issubset(df_monthly.columns):
                # 构建查找字典: (时间, 描述) -> 率
                # 预处理：确保 rate 是 float
                df_monthly['defect_rate'] = pd.to_numeric(df_monthly['defect_rate'], errors='coerce').fillna(0)
                monthly_map = df_monthly.set_index(['time_period', 'defect_desc'])['defect_rate'].to_dict()
            else:
                logging.warning("月度趋势数据格式不正确或为空。")

        # 定义查找函数
        def get_base_rate(row):
            key = (row.get('time_period'), row['defect_desc'])
            return monthly_map.get(key, 0.0)

        # 应用查找
        lot_stats['base_rate'] = lot_stats.apply(get_base_rate, axis=1)
        
        # 3. 应用最终公式
        # -----------------------------------------------------------
        # 公式: Base + Sum / (30 + Count)
        # 注意: 平滑因子 30 是硬编码的经验值
        smoothing_factor = 30
        lot_stats['override_rate_avg'] = lot_stats['base_rate'] + (
            lot_stats['rate_sum'] / (float(smoothing_factor) + lot_stats['sheet_count'])
        )
        
        logging.info(f"Lot 级覆盖率计算完成，共计算 {len(lot_stats)} 条记录。")
        
        # 返回符合 _override_rates 预期的格式: [lot_id, defect_desc, override_rate_avg]
        return lot_stats[['lot_id', 'defect_desc', 'override_rate_avg']]

    except Exception as e:
        logging.error(f"使用启发式公式计算 Lot 覆盖率时出错: {e}", exc_info=True)
        # 出错时返回空 DF，避免中断主流程
        return pd.DataFrame()

def _override_rates(
        simulated_code_details_dict: Dict[str, pd.DataFrame],
        override_data_df: pd.DataFrame | None,
        entity_id_col: str,
        desc_to_group_map: dict
    ) -> Dict[str, pd.DataFrame]:
        """
        [核心函数 V1.8 - 物理常识防呆版] 使用外部数据覆盖模拟的不良率。
        彻底移除了无脑的“暴力插入”。现在系统会严格审查实体是否在当前时间窗口的物理基座中存活。
        """
        if override_data_df is None or override_data_df.empty:
            logging.info(f"无覆盖数据提供 ({entity_id_col} 级别)，跳过覆盖步骤。")
            return simulated_code_details_dict

        # --- 动态定义必需列 ---
        rate_col_name = 'override_rate' if entity_id_col == 'sheet_id' else 'override_rate_avg'
        required_cols = ['lot_id', 'defect_desc', rate_col_name]
        if entity_id_col == 'sheet_id': required_cols.append('sheet_id')
        required_cols = list(dict.fromkeys(required_cols)) 

        missing_cols = [col for col in required_cols if col not in override_data_df.columns]
        if missing_cols:
            logging.error(f"覆盖数据 DataFrame ({entity_id_col}) 缺少必需列: {missing_cols}，无法执行覆盖。")
            return simulated_code_details_dict

        logging.info(f"开始使用外部数据覆盖 {entity_id_col} 级别的不良率 (严格审查模式)...")
        
        # --- [审计准备] ---
        all_config_ids = set(override_data_df[entity_id_col].astype(str).str.strip().unique())
        processed_ids = set()
        watchlist = ['L3MR5A0B023', 'L3MR5A0B026']

        final_results_dict = {group: df.copy() for group, df in simulated_code_details_dict.items() if df is not None}
        total_replaced_count = 0
        total_inserted_count = 0

        # --- 准备模板与物理花名册 ---
        all_sim_df_list = [df for df in final_results_dict.values() if not df.empty]
        if not all_sim_df_list:
            logging.error("无法执行任何操作，因为当前时间窗口内无任何物理基底数据。")
            return simulated_code_details_dict
        
        all_sim_df = pd.concat(all_sim_df_list, ignore_index=True)
        
        # 🛑 [核心防御: 提取当前时间窗口的合法实体名册]
        valid_entity_ids = set(all_sim_df[entity_id_col].astype(str).str.strip().unique())
        
        generic_template_row = all_sim_df.iloc[0]
        lot_specific_templates = all_sim_df.drop_duplicates(subset=['lot_id']).set_index('lot_id')
        
        new_rows_to_add_by_group = defaultdict(list)
        processed_indices = set()

        # --- 遍历覆盖 DataFrame ---
        for index, override_row in override_data_df.iterrows():
            target_desc = str(override_row['defect_desc']).strip()
            target_entity_id = str(override_row[entity_id_col]).strip() 
            target_lot_id = override_row['lot_id']
            override_rate = override_row[rate_col_name]

            is_target_trace = target_entity_id in watchlist
            if is_target_trace:
                logging.warning(f"!!! [追踪] 发现 Excel 指令 ID: {target_entity_id}")
                logging.warning(f"    - 缺陷描述: '{target_desc}'")

            # =================================================================
            # 🛑 [防呆拦截拦截机制生效]
            # 拒绝跨时空的幽灵实体插入，捍卫物质守恒定律！
            # =================================================================
            if target_entity_id not in valid_entity_ids:
                if is_target_trace:
                    logging.warning(f"    -> [拦截] 实体 '{target_entity_id}' 在当前数据底座中物理不存在！拒绝凭空捏造。")
                else:
                    logging.debug(f"跳过覆盖: 实体 '{target_entity_id}' 在当前窗口内不存在或已被过滤。")
                continue # 直接跳过，不计入 processed_ids

            # 1. 查找目标 Group
            target_group = desc_to_group_map.get(target_desc)
            if not target_group:
                continue

            # 2. 查找目标 Group 的 DataFrame
            target_df = final_results_dict.get(target_group)
            if target_df is None:
                continue 

            # 3. 匹配行
            match_mask = pd.Series(False, index=target_df.index)
            if not target_df.empty:
                match_mask = (target_df[entity_id_col].astype(str).str.strip() == target_entity_id) & \
                             (target_df['defect_desc'] == target_desc)

            matched_indices = target_df.index[match_mask]

            # 4. 执行
            if not matched_indices.empty:
                # --- 替换 ---
                if is_target_trace: logging.warning(f"    -> [动作] 找到匹配缺陷记录，执行替换。")
                target_df.loc[matched_indices, 'defect_rate'] = override_rate
                if 'total_panels' in target_df.columns:
                    panels = target_df.loc[matched_indices, 'total_panels']
                    new_counts = np.maximum(0, np.round(override_rate * panels)).astype(int)
                    target_df.loc[matched_indices, 'defect_panel_count'] = new_counts
                    if index not in processed_indices:
                        total_replaced_count += len(matched_indices)
                        processed_indices.add(index)
                        processed_ids.add(target_entity_id) 
            else:
                # --- 插入 (仅针对合法存在的实体插入它原先没有的缺陷) ---
                if is_target_trace: logging.warning(f"    -> [动作] 该合法实体未包含该缺陷，准备插入新缺陷记录。")
                
                # [优化]: 寻找最精确的模板行，优先抓取该实体自身的物理基础信息(如时间、过货率)
                entity_rows = all_sim_df[all_sim_df[entity_id_col].astype(str).str.strip() == target_entity_id]
                if not entity_rows.empty:
                    template_row = entity_rows.iloc[0]
                elif target_lot_id in lot_specific_templates.index:
                    template_row = lot_specific_templates.loc[target_lot_id]
                else:
                    template_row = generic_template_row

                try:
                    template_panels = float(template_row.get('total_panels', 1))
                    if template_panels == 0: template_panels = 1.0

                    new_row = {
                        'sheet_id': target_entity_id if entity_id_col == 'sheet_id' else (template_row.get('sheet_id', '') if 'sheet_id' in template_row else ''),
                        'lot_id': target_lot_id,
                        'defect_desc': target_desc,
                        'defect_rate': override_rate,
                        'defect_group': target_group,
                        'total_panels': template_panels,
                        'defect_panel_count': np.maximum(0, np.round(override_rate * template_panels)).astype(int),
                        'warehousing_time': template_row.get('warehousing_time', ''),
                        'array_input_time': template_row.get('array_input_time', pd.NaT),
                        'pass_rate': template_row.get('pass_rate', 0.0)
                    }
                    
                    if entity_id_col == 'lot_id': new_row['lot_id'] = target_entity_id 
                    
                    target_df_cols = target_df.columns.to_list()
                    if not target_df_cols and (target_group not in new_rows_to_add_by_group): 
                         target_df_cols = [col for col in ['sheet_id', 'lot_id', 'warehousing_time', 'array_input_time', 'defect_group', 'defect_desc', 'defect_panel_count', 'defect_rate', 'total_panels', 'pass_rate'] if col in new_row]
                         if final_results_dict.get(target_group) is None or final_results_dict.get(target_group).empty: # type: ignore
                             final_results_dict[target_group] = pd.DataFrame(columns=target_df_cols)
                    
                    new_row_filtered = {k: v for k, v in new_row.items() if k in target_df_cols}
                    new_rows_to_add_by_group[target_group].append(new_row_filtered)
                    
                    if index not in processed_indices:
                        total_inserted_count += 1
                        processed_indices.add(index)
                        processed_ids.add(target_entity_id) 
                        
                except Exception as insert_err:
                    logging.error(f"构建插入行失败 (ID: {target_entity_id}): {insert_err}", exc_info=True)

        # --- 合并新行 ---
        if new_rows_to_add_by_group:
            for group, new_rows in new_rows_to_add_by_group.items():
                if new_rows:
                    df_new = pd.DataFrame(new_rows)
                    target_df = final_results_dict.get(group)
                    if target_df is None: target_df = pd.DataFrame(columns=df_new.columns)
                    final_results_dict[group] = pd.concat([target_df, df_new], ignore_index=True).where(pd.notna, None) # type: ignore

        # --- [最终审计报告] ---
        failed_ids = all_config_ids - processed_ids
        
        logging.info(f"覆盖审计完成: Excel中共配置 {len(all_config_ids)} 个ID，成功应用 {len(processed_ids)} 个，拦截防呆 {len(failed_ids)} 个。")
        
        if failed_ids:
            failed_list = sorted(list(failed_ids))
            logging.warning("========== [物理防呆拦截名单] ==========")
            logging.warning(f"以下 ID 由于在当前数据底座中不存在，已被系统拒绝凭空捏造: {failed_list[:20]}")
            logging.warning("=======================================")

        return final_results_dict


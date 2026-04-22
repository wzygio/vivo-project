# 🎯 Target File: src/spc_domain/application/spc_service.py
# 🛠️ Action: 全文件替换 (修复 Pydantic 赋值 Bug 与 ALL 扫描逻辑)

import logging
import hashlib
import pandas as pd
import numpy as np
import streamlit as st
from typing import TYPE_CHECKING, Tuple, Optional, List
from datetime import datetime
from pathlib import Path
from dateutil.relativedelta import relativedelta
from dataclasses import dataclass

# 引入底层配置与仓储层
from src.shared_kernel.config_model import AppConfig
from src.shared_kernel.utils.data_inspector import export_probed_details
from src.spc_domain.infrastructure.data_loader import SpcQueryConfig
from src.spc_domain.infrastructure.repositories.spc_repository import SpcRepository

# 引入核心计算引擎
from src.spc_domain.core.spc_calculator import (
    preprocess_sheet_features, 
    apply_spc_rules, 
    aggregate_spc_metrics,
    sanitize_to_compliant
)

if TYPE_CHECKING:
    from src.shared_kernel.infrastructure.db_handler import DatabaseManager

# =========================================================================
# [核心修复] 将 Pydantic 的 BaseModel 替换为标准库的 @dataclass
# =========================================================================
@dataclass
class SpcDashboardViewModel:
    """SPC 看板视图模型 (支持 st.cache_data 原生序列化)"""
    global_summary_df: pd.DataFrame
    detail_df: pd.DataFrame
    station_detail_df: pd.DataFrame = None  # type: ignore

class SpcAnalysisService:
    _custom_end_date: Optional[datetime] = None

    @classmethod
    def set_analysis_end_date(cls, end_date: Optional[datetime] = None):
        cls._custom_end_date = end_date

    @classmethod
    def get_time_window(cls) -> Tuple[datetime, datetime]:
        current_end = cls._custom_end_date or datetime.now()
        # 1. 往前推3个月
        three_months_ago = current_end - relativedelta(months=3)
        # 2. [核心修复]：强制将起始日期对齐到该月的 1 号的 0点0分0秒，确保自然月的完整性
        current_start = three_months_ago.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return current_start, current_end

    @staticmethod
    def _apply_time_bucket_mapping(df: pd.DataFrame, time_type: str, end_dt: datetime) -> pd.DataFrame:
        """
        [内部辅助] 极速时间桶映射引擎
        V4.6 终极版：重叠数据魔方 + 强力防呆补0基座 + 自定义工厂自然周(周三至周三)
        """
        if not df.empty and 'sheet_start_time' in df.columns:
            df['sheet_start_time'] = pd.to_datetime(df['sheet_start_time'], errors='coerce') 

        # 非混合模式的快速降级兜底
        if time_type != 'MIXED':
            if df.empty or 'sheet_start_time' not in df.columns:
                return df
            day_str = df['sheet_start_time'].dt.strftime('%Y%m%d')
            day_sort = "3_" + day_str
            df['time_group'], df['sort_index'] = day_str, day_sort
            return df

        if time_type == 'MIXED':
            # 绝对锚点：永远以请求看板的 end_dt(今天) 为准
            end_dt_ts = pd.to_datetime(end_dt).normalize()
            
            # =======================================================
            # 步骤 1：定义目标时间桶的绝对标签 (即前端 X 轴必然显示的坐标)
            # =======================================================
            # 1.1 天级 (今日及往前6天，共7天)
            target_days = [(end_dt_ts - pd.Timedelta(days=i)) for i in range(7)]
            target_days_strs = [d.strftime('%Y%m%d') for d in target_days]
            target_days_sorts = ["3_" + d for d in target_days_strs]

            # 1.2 周级 (工厂自定义：加一天使得周日变为周一，从而完美借用 iso 标准提取3周)
            shifted_end = end_dt_ts + pd.Timedelta(days=1)
            w0_iso = shifted_end.isocalendar()
            w1_iso = (shifted_end - pd.Timedelta(days=7)).isocalendar()
            w2_iso = (shifted_end - pd.Timedelta(days=14)).isocalendar()

            def format_factory_week(iso):
                return f"{iso[0]}W{str(iso[1]).zfill(2)}"

            target_weeks_strs = [format_factory_week(w0_iso), format_factory_week(w1_iso), format_factory_week(w2_iso)]
            target_weeks_sorts = ["2_" + w for w in target_weeks_strs]

            # 1.3 月级 (最近3个月)
            m0 = end_dt_ts
            m1 = m0 - pd.DateOffset(months=1)
            m2 = m0 - pd.DateOffset(months=2)
            target_months_strs = [m0.strftime('%YM%m'), m1.strftime('%YM%m'), m2.strftime('%YM%m')]
            target_months_sorts = ["1_" + m for m in target_months_strs]

            all_time_groups = target_months_strs + target_weeks_strs + target_days_strs
            all_sort_indices = target_months_sorts + target_weeks_sorts + target_days_sorts

            # =======================================================
            # 步骤 2：切分真实的业务数据 (如果数据库有数据的话)
            # =======================================================
            df_real = pd.DataFrame()
            if not df.empty and 'sheet_start_time' in df.columns:
                # 真实天级
                day_bound = target_days[-1]
                mask_day = df['sheet_start_time'] >= day_bound
                df_day = df[mask_day].copy()
                df_day['time_group'] = df_day['sheet_start_time'].dt.strftime('%Y%m%d')
                df_day['sort_index'] = "3_" + df_day['time_group']

                # 真实周级 (对齐工厂自定义周，全量减2天以匹配前方的 target_weeks)
                df_shifted = df['sheet_start_time'] + pd.Timedelta(days=1)
                df_iso = df_shifted.dt.isocalendar()
                df_week_str = df_iso.year.astype(str) + "W" + df_iso.week.astype(str).str.zfill(2)
                mask_week = df_week_str.isin(target_weeks_strs)
                df_week = df[mask_week].copy()
                df_week['time_group'] = df_week_str[mask_week]
                df_week['sort_index'] = "2_" + df_week['time_group']

                # 真实月级
                df_month_str = df['sheet_start_time'].dt.strftime('%Y') + 'M' + df['sheet_start_time'].dt.strftime('%m')
                mask_month = df_month_str.isin(target_months_strs)
                df_month = df[mask_month].copy()
                df_month['time_group'] = df_month_str[mask_month]
                df_month['sort_index'] = "1_" + df_month['time_group']

                df_real = pd.concat([df_month, df_week, df_day], ignore_index=True)

            # =======================================================
            # 步骤 3：构建强力防呆补 0 基座 (Scaffolding)
            # =======================================================
            if not df.empty and 'prod_code' in df.columns and 'factory' in df.columns:
                unique_dims = df[['prod_code', 'factory']].drop_duplicates()
            else:
                unique_dims = pd.DataFrame([{'prod_code': 'UNKNOWN', 'factory': 'UNKNOWN'}])

            dummy_dfs = []
            for tg, si in zip(all_time_groups, all_sort_indices):
                temp_dummy = unique_dims.copy()
                temp_dummy['time_group'] = tg
                temp_dummy['sort_index'] = si
                
                temp_dummy['sheet_id'] = None 
                # [遵照 UI 指示]：移除 param_value = np.nan，替换为 data_type
                temp_dummy['data_type'] = None
                temp_dummy['spc_status'] = None
                dummy_dfs.append(temp_dummy)

            df_dummy = pd.concat(dummy_dfs, ignore_index=True)

            # 虚实结合：真实的明细 + 用于占位的空壳维度
            df_final = pd.concat([df_real, df_dummy], ignore_index=True)
            return df_final
            
        return df

    # =========================================================================
    # 1. 内部缓存引擎 (绝对安全地缓存原生数据)
    # [架构亮点] 去掉了下划线前缀，确保能被前端的 extract_cached_funcs 工具自动抓取清空
    # =========================================================================
    @staticmethod
    def compute_snapshot_signature(data_root: Path, target_prod: str) -> str:
        """
        [企业级缓存签名] 计算 SPC 快照目录的聚合签名。
        当任意产品的快照被删除、重建或修改时，签名改变，触发 L1 Cache Miss。
        """
        hash_md5 = hashlib.md5()
        ignore_dirs = {'doc_cache', 'processed', 'raw', 'spc_cache', 'yield_cache'}
        
        if target_prod.upper() == "ALL":
            if not data_root.exists():
                return "NOT_EXISTS"
            dirs = [d for d in data_root.iterdir() if d.is_dir() and not d.name.startswith(('.', '__')) and d.name not in ignore_dirs]
        else:
            dirs = [data_root / target_prod]
        
        for d in sorted(dirs, key=lambda x: x.name):
            for f in sorted(d.glob("*.parquet"), key=lambda x: x.name):
                stat = f.stat()
                hash_md5.update(f"{f.name}_{stat.st_mtime}_{stat.st_size}".encode())
        
        return hash_md5.hexdigest()[:8]

    @staticmethod
    @st.cache_data(show_spinner=False, ttl=3600)
    def fetch_dashboard_data_dict(
        _db_manager: 'DatabaseManager', 
        query_config_json: str, 
        time_type: str = 'MIXED',
        force_compliant: bool = False,
        data_type_filter: str = 'SPC',
        snapshot_signature: str = ""
    ) -> dict:
        """
        [内部缓存层] 负责所有重负载的查询与计算，返回原生字典以完美规避 Pickle 序列化陷阱。
        """
        try:
            config_instance = SpcQueryConfig.model_validate_json(query_config_json)
            config_instance.data_type_filter = data_type_filter
        except Exception as e:
            logging.error(f"Config 解析失败: {e}")
            return {"global_summary_df": pd.DataFrame(), "detail_df": pd.DataFrame(), "station_detail_df": pd.DataFrame()}
        
        target_prod = config_instance.prod_code
        start_dt, end_dt = SpcAnalysisService.get_time_window()
        
        search_prods: List[str] = []
        data_root = Path("data")
        
        ignore_dirs = {'doc_cache', 'processed', 'raw', 'spc_cache', 'yield_cache'}
        if target_prod.upper() == "ALL":
            if data_root.exists():
                for d in data_root.iterdir():
                    if d.is_dir() and not d.name.startswith(('.', '__')) and d.name not in ignore_dirs:
                        search_prods.append(d.name)
        else:
            search_prods = [target_prod]

        all_status_dfs = []

        for prod in search_prods:
            prod_snapshot_dir = data_root / prod 
            prod_snapshot_dir.mkdir(parents=True, exist_ok=True)

            repo = SpcRepository(snapshot_dir=prod_snapshot_dir, use_snapshot=True, db_manager=_db_manager)
            
            current_fetch_config = config_instance.model_copy()
            current_fetch_config.prod_code = prod
            current_fetch_config.start_date = start_dt.strftime("%Y-%m-%d")
            current_fetch_config.end_date = end_dt.strftime("%Y-%m-%d")
            current_fetch_config.data_type_filter = data_type_filter

            m_df = repo.get_spc_measurements(current_fetch_config)
            s_df = repo.get_spc_spec_limits(prod)
            
            if not m_df.empty:
                features = preprocess_sheet_features(measure_df=m_df, spec_df=s_df)
                enable_soos = data_type_filter.upper() != 'AOI'
                status = apply_spc_rules(sheet_features=features, enable_soos=enable_soos)
                
                if force_compliant:
                    status = sanitize_to_compliant(status)
                all_status_dfs.append(status)

        if not all_status_dfs:
            return {"global_summary_df": pd.DataFrame(), "detail_df": pd.DataFrame(), "station_detail_df": pd.DataFrame()}

        # 合并所有工厂/产品原始状态
        raw_status_df = pd.concat(all_status_dfs, ignore_index=True)
        
        # =========================================================================
        # 🛑 [核心修复 B] 在合并完成、聚合发生之前，对全维度物理表执行【唯一一次】洗白！
        # 此时数据含有厂别、型号等所有字段，修饰规则 100% 精准命中！
        # =========================================================================
        raw_status_df = sanitize_to_compliant(raw_status_df, add_tag=True)

        # 🚨 [关键探针 A] 记录原始物理报警数
        ooc_count_raw = raw_status_df[raw_status_df['is_ooc'] == 1].shape[0] if 'is_ooc' in raw_status_df.columns else 0
        logging.info(f"📊 [Service] 原始物理 OOC 总数: {ooc_count_raw}")

        # =========================================================================
        # 🛑 [核心修复 1]：在“三倍扩充”之前，先进行站点聚合！
        # 此时 raw_status_df 是 1:1 的真实物理数据，绝无重复
        # =========================================================================
        enable_soos = data_type_filter.upper() != 'AOI'
        station_detail_df = aggregate_spc_metrics(
            spc_status_df=raw_status_df, 
            group_cols=['prod_code', 'factory', 'step_id'], 
            time_group_col='step_id', # 站点维度不需要时间组
            enable_soos=enable_soos
        )

        # 🚨 [关键探针 B] 记录聚合后的站点报警数
        ooc_count_station = station_detail_df['OOC片数'].sum() if 'OOC片数' in station_detail_df.columns else 0
        logging.info(f"📊 [Service] 站点维度 OOC 汇总数: {ooc_count_station}")

        # =========================================================================
        # [执行扩充]：此处开始，数据将变为 3 份副本，仅用于趋势展示
        # =========================================================================
        full_status_df = SpcAnalysisService._apply_time_bucket_mapping(raw_status_df, time_type.upper(), end_dt)
        
        global_summary_df = aggregate_spc_metrics(
            spc_status_df=full_status_df, 
            group_cols=['sort_index', 'time_group'],
            time_group_col='time_group',
            enable_soos=enable_soos
        ) 
        
        detail_df = aggregate_spc_metrics(
            spc_status_df=full_status_df, 
            group_cols=['sort_index', 'time_group', 'prod_code', 'factory'],
            time_group_col='time_group',
            enable_soos=enable_soos
        ) 
        
        if not global_summary_df.empty:
            global_summary_df = global_summary_df.sort_values('sort_index').drop(columns=['sort_index'])
        if not detail_df.empty:
            detail_df = detail_df.sort_values(['sort_index', 'factory']).drop(columns=['sort_index'])

        if not station_detail_df.empty:
            check_cols = ['OOS片数', 'OOC片数', 'SOOS片数']
            check_cols = [c for c in check_cols if c in station_detail_df.columns]
            if check_cols:
                station_detail_df['total_err'] = station_detail_df[check_cols].sum(axis=1)
                station_detail_df = station_detail_df[station_detail_df['total_err'] > 0].drop(columns=['total_err'])

        # 返回绝对安全的字典
        return {
            "global_summary_df": global_summary_df,
            "detail_df": detail_df,
            "station_detail_df": station_detail_df
        }

    # =========================================================================
    # 2. 对外标准契约 (无缓存装饰器，实时组装强模型)
    # =========================================================================
    @staticmethod
    def get_spc_dashboard_data(
        _db_manager: 'DatabaseManager', 
        query_config_json: str, 
        time_type: str = 'MIXED',
        force_compliant: bool = False,
        data_type_filter: str = 'SPC',
        snapshot_signature: str = ""
    ) -> SpcDashboardViewModel:
        """
        [企业级标准接口] 实时从底层缓存引擎拉取字典，安全组装为 SpcDashboardViewModel 对象。
        """
        # 1. 穿透调用内部缓存引擎，获取字典
        raw_data = SpcAnalysisService.fetch_dashboard_data_dict(
            _db_manager, 
            query_config_json, 
            time_type, 
            force_compliant, 
            data_type_filter,
            snapshot_signature
        )
        
        # 2. 实时实例化数据类，彻底消灭热重载时的序列化报错！
        return SpcDashboardViewModel(
            global_summary_df=raw_data.get("global_summary_df", pd.DataFrame()),
            detail_df=raw_data.get("detail_df", pd.DataFrame()),
            station_detail_df=raw_data.get("station_detail_df", pd.DataFrame())
        )

    @staticmethod
    def get_spc_defect_details(
        _db_manager: 'DatabaseManager', 
        query_config_json: str, 
        time_group: str, 
        defect_type: str,
        time_type: str = 'MIXED',
        force_compliant: bool = False,
        data_type_filter: str = 'SPC'
    ) -> pd.DataFrame:
        """
        [企业级下钻 API] 针对前端大盘数字点击事件，提供精准的明细级数据下钻。
        利用 Parquet 缓存极速响应，避免回表查询导致的性能损耗。
        
        :param time_group: 目标时间组 (如 '2026M01', '2026W11', '20260319')
        :param defect_type: 报警类型 (如 'OOS', 'SOOS', 'OOC')
        """
        logging.info(f"==> [Drill-down API] 开始钻取明细 | 时间节点: {time_group} | 类型: {defect_type} <==")
        
        try:
            config_instance = SpcQueryConfig.model_validate_json(query_config_json)
            # [新增] 注入 data_type_filter 参数
            config_instance.data_type_filter = data_type_filter
        except Exception as e:
            logging.error(f"Config 解析失败: {e}")
            return pd.DataFrame()

        target_prod = config_instance.prod_code
        start_dt, end_dt = SpcAnalysisService.get_time_window()
        
        # 1. 智能探测产品目录 (复用 ALL 模式的探测逻辑)
        search_prods: List[str] = []
        data_root = Path("data")
        
        ignore_dirs = {'doc_cache', 'processed', 'raw', 'spc_cache', 'yield_cache'}
        if target_prod.upper() == "ALL":
            if data_root.exists():
                for d in data_root.iterdir():
                    if d.is_dir() and not d.name.startswith(('.', '__')) and d.name not in ignore_dirs:
                        search_prods.append(d.name)
        else:
            search_prods = [target_prod]

        all_status_dfs = []

        # 2. 从本地缓存极速加载数据并还原状态
        for prod in search_prods:
            prod_snapshot_dir = data_root / prod 
            
            # 使用 use_snapshot=True 强制走缓存，保护数据库
            repo = SpcRepository(snapshot_dir=prod_snapshot_dir, use_snapshot=True, db_manager=_db_manager)
            
            current_fetch_config = config_instance.model_copy()
            current_fetch_config.prod_code = prod
            current_fetch_config.start_date = start_dt.strftime("%Y-%m-%d")
            current_fetch_config.end_date = end_dt.strftime("%Y-%m-%d")

            m_df = repo.get_spc_measurements(current_fetch_config)
            s_df = repo.get_spc_spec_limits(prod)
            
            if not m_df.empty:
                features = preprocess_sheet_features(measure_df=m_df, spec_df=s_df)
                
                # [企业级优化] 根据数据类型决定是否启用 SOOS 判定
                enable_soos = data_type_filter.upper() != 'AOI'
                status = apply_spc_rules(sheet_features=features, enable_soos=enable_soos)
                
                # [可选] 合规修饰
                if force_compliant:
                    status = sanitize_to_compliant(status)
                all_status_dfs.append(status)

        if not all_status_dfs:
            logging.warning("下钻查询未命中任何底层数据。")
            return pd.DataFrame()

        # 3. 合并全量数据并应用相同的重叠魔方时间切割规则
        raw_status_df = pd.concat(all_status_dfs, ignore_index=True)
        if time_group == "ALL":
            # 直接使用最底层的 1:1 物理真实数据
            filtered_df = raw_status_df.copy()
        else:
            # 兼容旧逻辑：应用重叠魔方时间切割规则，并精确过滤
            full_status_df = SpcAnalysisService._apply_time_bucket_mapping(raw_status_df, time_type.upper(), end_dt)
            filtered_df = full_status_df[full_status_df['time_group'] == time_group].copy()

        if filtered_df.empty:
            return pd.DataFrame()

        # 4. 精准拦截过滤
        # 过滤缺陷类型 (兼容 Core 层状态字段可能的不同命名体系)
        # 这里假设您的规则引擎核心层输出的状态列名叫 'spc_status' 或 'status'
        if 'spc_status' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['spc_status'] == defect_type]
        elif 'status' in filtered_df.columns:
            filtered_df = filtered_df[filtered_df['status'] == defect_type]
        else:
            # 兼容布尔值标记的情况 (如 is_oos == True)
            bool_col = f"is_{defect_type.lower()}"
            if bool_col in filtered_df.columns:
                filtered_df = filtered_df[filtered_df[bool_col] == True]
            else:
                logging.warning(f"无法在数据集中找到与 {defect_type} 对应的状态列，返回全量时间片数据。")

        # 5. [核心修改] 清理内部计算列，保持明细表纯净
        columns_to_drop = ['sort_index', 'time_group', 'param_value']
        filtered_df = filtered_df.drop(columns=[c for c in columns_to_drop if c in filtered_df.columns])

        logging.info(f"钻取成功，共捕获 {len(filtered_df)} 条明细数据。")
        return filtered_df
    

    @staticmethod
    def safe_refresh_snapshots(_db_manager: 'DatabaseManager', query_config_json: str) -> bool:
        """
        [生命周期钩子] 代理 UI 的强刷指令，触发底层的安全覆写 (Safe Overwrite)。
        返回 True 表示刷新调度成功；返回 False 仅表示底层可能挂了，但不影响前端读取旧数据。
        """
        try:
            config_instance = SpcQueryConfig.model_validate_json(query_config_json)
            target_prod = config_instance.prod_code
            data_root = Path("data")

            search_prods = []
            
            ignore_dirs = {'doc_cache', 'processed', 'raw', 'spc_cache', 'yield_cache'}
            if target_prod.upper() == "ALL":
                if data_root.exists():
                    for d in data_root.iterdir():
                        if d.is_dir() and not d.name.startswith(('.', '__')) and d.name not in ignore_dirs:
                            search_prods.append(d.name)
            else:
                search_prods = [target_prod]

            start_dt, end_dt = SpcAnalysisService.get_time_window()
            success_flag = True

            for prod in search_prods:
                prod_snapshot_dir = data_root / prod
                # 实例化仓储
                repo = SpcRepository(snapshot_dir=prod_snapshot_dir, use_snapshot=True, db_manager=_db_manager)

                current_fetch_config = config_instance.model_copy()
                current_fetch_config.prod_code = prod
                current_fetch_config.start_date = start_dt.strftime("%Y-%m-%d")
                current_fetch_config.end_date = end_dt.strftime("%Y-%m-%d")

                logging.info(f"🔄 [Service] 向底层下发 {prod} 强刷指令 (Force Refresh)...")
                
                # [核心联动] 强刷指令穿透！Repository 内部会安全地尝试覆盖
                df = repo.get_spc_measurements(current_fetch_config, force_refresh=True)

            return success_flag
            
        except Exception as e:
            logging.error(f"❌ 安全覆写代理调度失败: {e}")
            return False
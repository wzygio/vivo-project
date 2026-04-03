import logging # 引入日志模块用于记录计算过程的状态
import pandas as pd # 引入 Pandas 用于全量向量化内存计算
import numpy as np # 引入 Numpy 用于处理极值与 NaN 数学防呆

def preprocess_sheet_features(
    measure_df: pd.DataFrame, 
    spec_df: pd.DataFrame,
    filter_keys: list = None,  # [新增] 用于过滤数据的去重键（含 site_name）
    group_keys: list = None,   # [修改] 用于计算特征的聚合键（不含 site_name）
    join_keys: list = None
) -> pd.DataFrame:
    """
    [Phase 1] Sheet 级特征提取与规格线绑定 (Feature Engineering & Spec Binding)
    
    [核心修复] 分两阶段处理：
    1. 过滤阶段：按 filter_keys（含 site_name）去重，保留每个点位最新记录
    2. 计算阶段：按 group_keys（不含 site_name）聚合，计算整个 sheet 的统计值
    """
    logging.info("开始执行 [Phase 1] Sheet 级特征降维与规格关联...") 

    if measure_df.empty: 
        logging.warning("输入量测数据为空，跳过特征降维。") 
        return pd.DataFrame() 

    # [核心修复] 定义两阶段键
    # filter_keys: 用于去重过滤，包含 site_name（点位级）
    # group_keys: 用于聚合计算，不包含 site_name（sheet 级）
    if filter_keys is None:
        filter_keys = ['factory', 'prod_code', 'sheet_id', 'step_id', 'param_name', 'site_name']
    if group_keys is None:
        group_keys = ['factory', 'prod_code', 'sheet_id', 'step_id', 'param_name']
    
    try:
        # ===================================================================
        # 阶段 1: 按 filter_keys 去重，保留每个点位最新记录
        # ===================================================================
        logging.info(f"[Phase 1.1] 按点位去重: {filter_keys}")
        
        # 先排序，确保最新记录在最后
        df_sorted = measure_df.sort_values(by='sheet_start_time', ascending=True)
        
        # 按 filter_keys 去重，保留最后一条（即最新的）
        df_deduplicated = df_sorted.drop_duplicates(subset=filter_keys, keep='last')
        
        logging.info(f"去重完成: {len(measure_df)} 条 -> {len(df_deduplicated)} 条")
        
        # ===================================================================
        # 阶段 2: 按 group_keys 聚合，计算整个 sheet 的统计值
        # ===================================================================
        logging.info(f"[Phase 1.2] 按 Sheet 聚合计算: {group_keys}")
        
        # 执行命名聚合 - 一个 sheet 下所有点位的统计值
        sheet_features = df_deduplicated.groupby(group_keys, as_index=False).agg(
            sheet_mean=('param_value', 'mean'),     # 所有点位的均值
            sheet_max=('param_value', 'max'),       # 所有点位的最大值
            sheet_min=('param_value', 'min'),       # 所有点位最小值
            sheet_start_time=('sheet_start_time', 'min')  # 该 sheet 的最早时间
        ) 
        
        logging.info(f"特征降维完成，单点数据已压缩为 {len(sheet_features)} 条 Sheet 级特征。")

        # 2. 规格绑定逻辑 (Specification Binding)
        if spec_df.empty: 
            logging.warning("输入规格数据为空，将生成无基准线的特征表。")
            for col in ['usl', 'lsl', 'ucl', 'lcl']: 
                sheet_features[col] = np.nan 
            return sheet_features 

        # [安全验证] join_keys 依然是 ['prod_code', 'step_id', 'param_name']，不加 factory。
        # 因为同产品的同一站点参数规格在所有工厂应该是一致的（或规格表本就没分工厂）。
        if join_keys is None:
            join_keys = ['prod_code', 'step_id', 'param_name']
            
        # 此时的 Left Join 绝对精准，不会因 eqp_type 的缺失而导致笛卡尔积膨胀
        merged_df = pd.merge( 
            sheet_features, 
            spec_df, 
            on=join_keys, 
            how='left' 
        ) 

        logging.info("[Phase 1] 规格绑定成功。")
        return merged_df 

    except Exception as e: 
        logging.error(f"[Phase 1] 特征降维与绑定时发生致命错误: {e}") 
        return pd.DataFrame()

def apply_spc_rules( # 定义 Phase 2 核心计算函数：执行 SPC 判定引擎
    sheet_features: pd.DataFrame, # 入参：经过 Phase 1 降维并绑定规格线的数据表
    enable_soos: bool = True # [新增] 是否启用 SOOS 判定，默认为 True
) -> pd.DataFrame: # 返回：附带判定状态 (OOS/SOOS/OOC) 与 One-Hot 统计列的 DataFrame
    """
    [Phase 2] 向量化状态判定与优先级裁决 (Vectorized Status Routing)
    严格遵循 OOS > SOOS > OOC 的优先级，针对单边/双边规格安全执行判定，并输出布尔特征列。
    
    [企业级扩展] 通过 enable_soos 参数支持 AOI 等无需 SOOS 判定的业务场景，避免无效计算。
    """
    logging.info("开始执行 [Phase 2] 向量化规则判定矩阵计算...") # 记录核心引擎启动

    if sheet_features.empty: # 顶层防呆防御
        return pd.DataFrame() # 如果无数据输入，短路拦截

    df = sheet_features.copy() # 原则：纯函数不污染外部入参，拷贝一份用于矩阵运算

    try:
        # ---------------------------------------------------------
        # 矩阵计算 1：OOS (超规) - 基于均值 (Mean)
        # ---------------------------------------------------------
        # Pandas 特性：如果 usl/lsl 是 NaN (单边管控)， `> NaN` 永远返回 False，完美规避报错且符合业务直觉
        mask_oos_upper = df['sheet_mean'] > df['usl'] # 建立均值触碰上限的布尔掩码矩阵
        mask_oos_lower = df['sheet_mean'] < df['lsl'] # 建立均值触碰下限的布尔掩码矩阵
        mask_oos = mask_oos_upper | mask_oos_lower # 逻辑或 (|) 操作，合成最终的 OOS 触碰矩阵

        # ---------------------------------------------------------
        # 矩阵计算 2：SOOS (超极值) - 基于极值 (Max/Min)
        # [企业级优化] 根据 enable_soos 参数决定是否计算，AOI 场景可跳过以提升性能
        # ---------------------------------------------------------
        if enable_soos:
            mask_soos_upper = df['sheet_max'] > df['usl'] # 建立最大值触碰上限的布尔掩码矩阵 (只要一个点超规，max必然超规)
            mask_soos_lower = df['sheet_min'] < df['lsl'] # 建立最小值触碰下限的布尔掩码矩阵
            mask_soos = mask_soos_upper | mask_soos_lower # 逻辑或 (|) 操作，合成最终的 SOOS 触碰矩阵
        else:
            mask_soos = pd.Series(False, index=df.index) # AOI 场景：SOOS 永远为 False

        # ---------------------------------------------------------
        # 矩阵计算 3：OOC (失控) - 基于均值 (Mean) 对比管控线 (UCL/LCL)
        # ---------------------------------------------------------
        mask_ooc_upper = df['sheet_mean'] > df['ucl'] # 建立均值触碰管控上限的掩码
        mask_ooc_lower = df['sheet_mean'] < df['lcl'] # 建立均值触碰管控下限的掩码
        mask_ooc = mask_ooc_upper | mask_ooc_lower # 逻辑或，合成最终的 OOC 触碰矩阵

        # ---------------------------------------------------------
        # 优先级裁决引擎 (Priority Routing)
        # 业务铁律：每个 Sheet 的该参数只能有一个状态，OOS > SOOS > OOC
        # [企业级优化] 动态构建 conditions 和 choices，适配 AOI 等简化场景
        # ---------------------------------------------------------
        conditions = [ # 按绝对优先级降序排列条件矩阵列表
            mask_oos,  # 优先级 1：最高级别报警 (超规)
            mask_ooc   # 优先级 2：低级别报警 (均值偏移失控)
        ]
        choices = [ # 对应条件的输出标签列表
            'OOS', 
            'OOC'
        ]
        
        # 如果启用 SOOS，插入优先级 2
        if enable_soos:
            conditions.insert(1, mask_soos)
            choices.insert(1, 'SOOS')
        
        # 核心：使用 numpy.select 一次性将三维掩码压缩为单一状态列，未命中任何条件则默认输出 'OK'
        df['spc_status'] = np.select(conditions, choices, default='OK') 

        # ---------------------------------------------------------
        # 独热编码映射 (One-Hot Encoding) -> 为 Phase 3 聚合求和做准备
        # [企业级优化] 根据 enable_soos 动态生成列，AOI 场景不生成 is_soos
        # ---------------------------------------------------------
        df['is_oos'] = (df['spc_status'] == 'OOS').astype(int)   # 映射 OOS，命中为 1，未命中为 0
        df['is_ooc'] = (df['spc_status'] == 'OOC').astype(int)   # 映射 OOC，命中为 1，未命中为 0
        if enable_soos:
            df['is_soos'] = (df['spc_status'] == 'SOOS').astype(int) # 映射 SOOS，命中为 1，未命中为 0

        logging.info(f"[Phase 2] 规则判定完成，共生成 {len(df)} 条状态裁决。") # 记录完成状态
        return df # 返回附带状态标识的极简事实表

    except Exception as e: # 拦截极低概率的矩阵计算崩溃
        logging.error(f"[Phase 2] 执行向量化规则判定时发生错误: {e}") # 输出错误栈
        return pd.DataFrame() # 抛出空表触发兜底
    

def sanitize_to_compliant(
    spc_status_df: pd.DataFrame,
    add_tag: bool = True
) -> pd.DataFrame:
    """
    [合规修饰器] 将所有 Sheet 的 SPC 状态强制修正为合规 (OK)。
    
    典型使用场景：
    - 演示/测试环境需要展示"理想状态"
    - 数据脱敏场景下隐藏真实报警信息
    - A/B 测试时作为对照组基准线
    
    Args:
        spc_status_df: 经过 apply_spc_rules 判定后的 DataFrame
        add_tag: 是否添加 compliance_tag 列标记数据已被修饰（推荐开启，防止误导）
    
    Returns:
        状态全部被修正为 'OK' 的 DataFrame，保留原始计算值
    """
    if spc_status_df.empty:
        return spc_status_df
    
    df = spc_status_df.copy()
    
    # 1. 强制重置状态列为 OK
    df['spc_status'] = 'OK'
    
    # 2. 重置 One-Hot 标记列为 0
    for col in ['is_oos', 'is_soos', 'is_ooc']:
        if col in df.columns:
            df[col] = 0
    
    # 3. [防呆设计] 添加修饰标记列，避免下游误用
    if add_tag:
        df['compliance_tag'] = 'SANITIZED'
    
    logging.info(f"[合规修饰器] 已修正 {len(df)} 条记录状态为合规 (OK)。")
    return df


def aggregate_spc_metrics( # 定义 Phase 3 核心聚合函数：生成最终报表指标
    spc_status_df: pd.DataFrame, # 入参：经过 Phase 2 状态路由并附带 One-Hot 标签的数据表
    time_group_col: str, # 入参：时间桶的列名 (例如 'time_group'，由调用方在传入前基于时间戳生成)
    group_cols: list | str,
    enable_soos: bool = True # [新增] 是否包含 SOOS 列，默认为 True
) -> pd.DataFrame: # 返回：严格包含前端报表所需所有中英文列名的最终汇总表
    """
    [Phase 3] 报表颗粒度聚合与复合指标计算 (Report Aggregation & Metric Calculation)
    
    [方案B] 分母改为 Sheet+Step+Param 组合粒度：
    - 抽检数 = 不重复的 (sheet_id + step_id + param_name) 组合数量
    - 分子保持不变：按 is_oos/is_ooc/is_soos 求和
    - 确保报警率不超过 100%
    
    [企业级扩展] 通过 enable_soos 参数支持 AOI 等无需 SOOS 的业务场景。
    """
    logging.info(f"开始执行 [Phase 3] 报表指标聚合 (按维度: {time_group_col}, 方案B: Sheet+Step+Param 组合粒度)...")

    if spc_status_df.empty:
        logging.warning("输入的状态数据为空，返回空聚合表。")
        return pd.DataFrame()

    try:
        # [方案B 核心修改] 创建组合键用于统计分母
        # 分母 = 不重复的 (sheet_id + step_id + param_name) 组合
        df = spc_status_df.copy()
        df['_sample_key'] = df['sheet_id'].astype(str) + '|' + df['step_id'].astype(str) + '|' + df['param_name'].astype(str)
        
        # 1. 核心折叠逻辑：定义聚合策略字典
        agg_funcs = {
            '_sample_key': 'nunique',  # [方案B] 分母：统计组合键的唯一数量
            'is_oos': 'sum',           # 分子 1：对 OOS 的独热列求和
            'is_ooc': 'sum'            # 分子 2：对 OOC 的独热列求和
        }
        if enable_soos:
            agg_funcs['is_soos'] = 'sum'  # 分子 3：对 SOOS 的独热列求和

        # 2. 执行向量化聚合计算
        report_df = df.groupby(group_cols, as_index=False).agg(agg_funcs)

        # 3. 字段映射：重命名为前端严格要求的报表字段
        rename_map = {
            '_sample_key': '抽检数',  # [方案B] 映射分母列为组合键数量
            'is_oos': 'OOS片数',
            'is_ooc': 'OOC片数'
        }
        if enable_soos:
            rename_map['is_soos'] = 'SOOS片数'
        report_df.rename(columns=rename_map, inplace=True)

        # 4. 复合比率运算与物理底线防御 (Zero-Division Protection)
        total = report_df['抽检数']

        # 基础报警率计算
        report_df['OOS'] = np.where(total == 0, np.nan, report_df['OOS片数'] / total)
        report_df['OOC'] = np.where(total == 0, np.nan, report_df['OOC片数'] / total)
        if enable_soos:
            report_df['SOOS'] = np.where(total == 0, np.nan, report_df['SOOS片数'] / total)

        # 复合报警率计算
        if enable_soos:
            report_df['OOS+SOOS'] = np.where(
                total == 0, np.nan,
                (report_df['OOS片数'] + report_df['SOOS片数']) / total
            )
            report_df['OOS+SOOS+OOC'] = np.where(
                total == 0, np.nan,
                (report_df['OOS片数'] + report_df['SOOS片数'] + report_df['OOC片数']) / total
            )
        else:
            # AOI 场景：简化为 OOS+OOC
            report_df['OOS+OOC'] = np.where(
                total == 0, np.nan,
                (report_df['OOS片数'] + report_df['OOC片数']) / total
            )

        logging.info(f"[Phase 3] 指标聚合完成(方案B)，成功生成 {len(report_df)} 个时间桶的报表数据。")
        return report_df

    except Exception as e:
        logging.error(f"[Phase 3] 指标聚合时发生致命错误: {e}")
        return pd.DataFrame()

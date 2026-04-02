import logging # 引入日志模块用于记录计算过程的状态
import pandas as pd # 引入 Pandas 用于全量向量化内存计算
import numpy as np # 引入 Numpy 用于处理极值与 NaN 数学防呆

def preprocess_sheet_features(
    measure_df: pd.DataFrame, 
    spec_df: pd.DataFrame,
    group_keys: list = None, # 将硬编码的键表提取为入参，实现与底层数据结构的彻底解耦
    join_keys: list = None
) -> pd.DataFrame:
    """
    [Phase 1] Sheet 级特征提取与规格线绑定 (Feature Engineering & Spec Binding)
    """
    logging.info("开始执行 [Phase 1] Sheet 级特征降维与规格关联...") 

    if measure_df.empty: 
        logging.warning("输入量测数据为空，跳过特征降维。") 
        return pd.DataFrame() 

    # 1. 核心降维逻辑 (Feature Aggregation)
    # [修改] 分组键移除 'sheet_start_time'，按 sheet 唯一标识分组
    # [更新] 增加 site_name 字段，支持更细粒度的分组
    if group_keys is None:
        group_keys = ['factory', 'prod_code', 'sheet_id', 'step_id', 'param_name', 'site_name']
    
    try: 
        # 执行命名聚合
        # [修改] 增加 sheet_start_time 取最小值，作为该 sheet 的时间依据
        sheet_features = measure_df.groupby(group_keys, as_index=False).agg(
            sheet_mean=('param_value', 'mean'), 
            sheet_max=('param_value', 'max'), 
            sheet_min=('param_value', 'min'),
            sheet_start_time=('sheet_start_time', 'min')
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
    sheet_features: pd.DataFrame # 入参：经过 Phase 1 降维并绑定规格线的数据表
) -> pd.DataFrame: # 返回：附带判定状态 (OOS/SOOS/OOC) 与 One-Hot 统计列的 DataFrame
    """
    [Phase 2] 向量化状态判定与优先级裁决 (Vectorized Status Routing)
    严格遵循 OOS > SOOS > OOC 的优先级，针对单边/双边规格安全执行判定，并输出布尔特征列。
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
        # ---------------------------------------------------------
        mask_soos_upper = df['sheet_max'] > df['usl'] # 建立最大值触碰上限的布尔掩码矩阵 (只要一个点超规，max必然超规)
        mask_soos_lower = df['sheet_min'] < df['lsl'] # 建立最小值触碰下限的布尔掩码矩阵
        mask_soos = mask_soos_upper | mask_soos_lower # 逻辑或 (|) 操作，合成最终的 SOOS 触碰矩阵

        # ---------------------------------------------------------
        # 矩阵计算 3：OOC (失控) - 基于均值 (Mean) 对比管控线 (UCL/LCL)
        # ---------------------------------------------------------
        mask_ooc_upper = df['sheet_mean'] > df['ucl'] # 建立均值触碰管控上限的掩码
        mask_ooc_lower = df['sheet_mean'] < df['lcl'] # 建立均值触碰管控下限的掩码
        mask_ooc = mask_ooc_upper | mask_ooc_lower # 逻辑或，合成最终的 OOC 触碰矩阵

        # ---------------------------------------------------------
        # 优先级裁决引擎 (Priority Routing)
        # 业务铁律：每个 Sheet 的该参数只能有一个状态，OOS > SOOS > OOC
        # ---------------------------------------------------------
        conditions = [ # 按绝对优先级降序排列条件矩阵列表
            mask_oos,  # 优先级 1：最高级别报警 (超规)
            mask_soos, # 优先级 2：次高级别报警 (单点超规)
            mask_ooc   # 优先级 3：低级别报警 (均值偏移失控)
        ]
        choices = [ # 对应条件的输出标签列表
            'OOS', 
            'SOOS', 
            'OOC'
        ]
        
        # 核心：使用 numpy.select 一次性将三维掩码压缩为单一状态列，未命中任何条件则默认输出 'OK'
        df['spc_status'] = np.select(conditions, choices, default='OK') 

        # ---------------------------------------------------------
        # 独热编码映射 (One-Hot Encoding) -> 为 Phase 3 聚合求和做准备
        # ---------------------------------------------------------
        df['is_oos'] = (df['spc_status'] == 'OOS').astype(int)   # 映射 OOS，命中为 1，未命中为 0
        df['is_soos'] = (df['spc_status'] == 'SOOS').astype(int) # 映射 SOOS，命中为 1，未命中为 0
        df['is_ooc'] = (df['spc_status'] == 'OOC').astype(int)   # 映射 OOC，命中为 1，未命中为 0

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
    group_cols: list | str
) -> pd.DataFrame: # 返回：严格包含前端报表所需所有中英文列名的最终汇总表
    """
    [Phase 3] 报表颗粒度聚合与复合指标计算 (Report Aggregation & Metric Calculation)
    按时间桶折叠数据，计算抽检数分母与各报警分子，并安全推演复合报警率。
    """
    logging.info(f"开始执行 [Phase 3] 报表指标聚合 (按维度: {time_group_col})...") # 记录聚合启动

    if spc_status_df.empty: # 顶层防呆防御：检查无数据输入的情况
        logging.warning("输入的状态数据为空，返回空聚合表。") # 记录空数据警告
        return pd.DataFrame() # 安全兜底返回

    try: # 开启防宕机聚合计算块
        # 1. 核心折叠逻辑：定义聚合策略字典
        agg_funcs = { # 构造字典以指导 Pandas 的列级聚合行为
            'sheet_id': 'nunique', # 分母防线：统计去重后的独特 Panel 数量，作为真实的“抽检/抽检数”
            'is_oos': 'sum',       # 分子 1：对 OOS 的独热列求和，得出 OOS 总片数
            'is_soos': 'sum',      # 分子 2：对 SOOS 的独热列求和，得出 SOOS 总片数
            'is_ooc': 'sum'        # 分子 3：对 OOC 的独热列求和，得出 OOC 总片数
        }

        # 2. 执行向量化聚合计算
        report_df = spc_status_df.groupby(group_cols, as_index=False).agg(agg_funcs) # 以时间维度为轴进行分组并应用上述聚合策略

        # 3. 字段映射：重命名为前端严格要求的报表字段
        rename_map = { # 建立从底层列名到业务指标列名的映射关系
            'sheet_id': '抽检数', # 映射分母列
            'is_oos': 'OOS片数', # 映射 OOS 报警数量
            'is_soos': 'SOOS片数', # 映射 SOOS 报警数量
            'is_ooc': 'OOC片数' # 映射 OOC 报警数量
        }
        report_df.rename(columns=rename_map, inplace=True) # 原地更新列名，节省内存切片操作

        # 4. 复合比率运算与物理底线防御 (Zero-Division Protection)
        total = report_df['抽检数'] # 提取分母列引用，提高代码可读性与后续运算速度

        # 基础报警率计算：利用 np.where 拦截除零错误。如果抽检数为0，强制返回纯净的 np.nan
        report_df['OOS'] = np.where(total == 0, np.nan, report_df['OOS片数'] / total) # 向量化计算 OOS 率
        report_df['SOOS'] = np.where(total == 0, np.nan, report_df['SOOS片数'] / total) # 向量化计算 SOOS 率
        report_df['OOC'] = np.where(total == 0, np.nan, report_df['OOC片数'] / total) # 向量化计算 OOC 率

        # 复合报警率计算：严格使用 (分子A + 分子B) / 分母，避免比率直接相加带来的浮点精度坍塌
        report_df['OOS+SOOS'] = np.where( # 防呆计算：OOS 与 SOOS 的复合不良率
            total == 0, np.nan, # 拦截条件：分母为 0 抛出 NaN
            (report_df['OOS片数'] + report_df['SOOS片数']) / total # 逻辑：两分子之和除以总数
        )
        report_df['OOS+SOOS+OOC'] = np.where( # 防呆计算：总体不良率 (三者全包)
            total == 0, np.nan, # 拦截条件：分母为 0 抛出 NaN
            (report_df['OOS片数'] + report_df['SOOS片数'] + report_df['OOC片数']) / total # 逻辑：三分母之和除以总数
        )

        logging.info(f"[Phase 3] 指标聚合完成，成功生成 {len(report_df)} 个时间桶的报表数据。") # 记录处理成果
        return report_df # 将承载最终指标的 DataFrame 交付给外部系统

    except Exception as e: # 拦截极速聚合中可能因类型导致的底层崩溃
        logging.error(f"[Phase 3] 指标聚合时发生致命错误: {e}") # 记录异常源追踪
        return pd.DataFrame() # 抛出空表触发顶层架构的降级响应
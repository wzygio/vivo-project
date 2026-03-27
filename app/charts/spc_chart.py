import pandas as pd
from typing import Dict, Any
from streamlit_echarts import JsCode

def get_spc_summary_echarts_option(df: pd.DataFrame) -> Dict[str, Any]:
    """
    [企业级 Echarts 引擎 V2.0]
    生成 SPC 全局汇总双 Y 轴图表的 Echarts 配置选项。
    新增特性：
    1. 动态断层检测 (Null Padding)：自动在月/周/天之间插入幽灵节点，实现视觉物理隔离。
    2. JsCode 拦截器：底层保持纯净浮点数，视图层 Tooltip 动态追加 '%'。
    """
    
    # --------------------------------------------------------------------------
    # 1. 动态断层检测与数据重组 (The Null Padding Algorithm)
    # --------------------------------------------------------------------------
    x_data = []
    oos_cnt, soos_cnt, ooc_cnt = [], [], []
    oos_rate, soos_rate, ooc_rate, oos_soos_rate, total_rate = [], [], [], [], []

    prev_time_type = None
    ghost_counter = 1 # 用于生成唯一的不重复空格键 (Echarts X轴要求唯一)

    for _, row in df.iterrows():
        time_str = str(row['time_group'])
        
        # 探测当前时间粒度
        if 'M' in time_str:
            curr_time_type = 'MONTH'
        elif 'W' in time_str:
            curr_time_type = 'WEEK'
        else:
            curr_time_type = 'DAY'

        # [核心断层逻辑] 如果粒度发生跳变，插入一个全空节点
        if prev_time_type is not None and curr_time_type != prev_time_type:
            x_data.append(" " * ghost_counter) # 插入不重复的幽灵 X 轴坐标
            ghost_counter += 1
            
            # 所有的 Y 轴指标全部塞入 None (序列化后变成 null)
            # Echarts 遇到 null 会自动断开折线图，并空出柱状图的间隔
            oos_cnt.append(None)
            soos_cnt.append(None)
            ooc_cnt.append(None)
            oos_rate.append(None)
            soos_rate.append(None)
            ooc_rate.append(None)
            oos_soos_rate.append(None)
            total_rate.append(None)

        prev_time_type = curr_time_type

        # 正常插入真实数据
        x_data.append(time_str)
        oos_cnt.append(row['OOS片数'] if pd.notna(row['OOS片数']) else 0)
        soos_cnt.append(row['SOOS片数'] if pd.notna(row['SOOS片数']) else 0)
        ooc_cnt.append(row['OOC片数'] if pd.notna(row['OOC片数']) else 0)

        # 率指标转化为前端基础浮点数
        def fmt_rate(val):
            return round(val * 100, 2) if pd.notna(val) else None

        oos_rate.append(fmt_rate(row['OOS']))
        soos_rate.append(fmt_rate(row['SOOS']))
        ooc_rate.append(fmt_rate(row['OOC']))
        oos_soos_rate.append(fmt_rate(row['OOS+SOOS']))
        total_rate.append(fmt_rate(row['OOS+SOOS+OOC']))

    # --------------------------------------------------------------------------
    # 2. 视图层拦截器配置 (JavaScript Injection)
    # --------------------------------------------------------------------------
    # 注入微型 JS 函数，专门用于 Tooltip 的百分比格式化
    js_percent_formatter = JsCode("function (value) { return value == null ? '-' : value.toFixed(2) + '%'; }")

    # --------------------------------------------------------------------------
    # 3. 组装最终 Echarts 字典
    # --------------------------------------------------------------------------
    option = {
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
        "legend": {
            "data": ["OOS片数", "SOOS片数", "OOC片数", "OOS", "SOOS", "OOC", "OOS+SOOS", "OOS+SOOS+OOC"],
            "bottom": 0
        },
        "grid": {"left": "3%", "right": "4%", "bottom": "10%", "containLabel": True},
        "xAxis": [{"type": "category", "data": x_data, "axisPointer": {"type": "shadow"}}],
        "yAxis": [
            {"type": "value", "name": "报警片数", "position": "left"},
            {"type": "value", "name": "报警率", "position": "right", "axisLabel": {"formatter": "{value} %"}}
        ],
        "series": [
            # 柱状图部分 (依然保持堆叠模式，遇到 null 自动留白)
            {"name": "OOC片数", "type": "bar", "stack": "总量", "data": ooc_cnt, "itemStyle": {"color": "#F9D976"}},
            {"name": "SOOS片数", "type": "bar", "stack": "总量", "data": soos_cnt, "itemStyle": {"color": "#81D8D0"}},
            {"name": "OOS片数", "type": "bar", "stack": "总量", "data": oos_cnt, "itemStyle": {"color": "#7B9CE1"}},
            
            # 折线图部分 (挂载 JsCode 拦截器)
            {"name": "OOS", "type": "line", "yAxisIndex": 1, "data": oos_rate, "symbol": "circle", "tooltip": {"valueFormatter": js_percent_formatter}},
            {"name": "SOOS", "type": "line", "yAxisIndex": 1, "data": soos_rate, "symbol": "circle", "tooltip": {"valueFormatter": js_percent_formatter}},
            {"name": "OOC", "type": "line", "yAxisIndex": 1, "data": ooc_rate, "symbol": "circle", "tooltip": {"valueFormatter": js_percent_formatter}},
            
            # {"name": "OOS+SOOS", "type": "line", "yAxisIndex": 1, "data": oos_soos_rate, "symbol": "circle", "tooltip": {"valueFormatter": js_percent_formatter}},
            # {
            #     "name": "OOS+SOOS+OOC", 
            #     "type": "line", 
            #     "yAxisIndex": 1, 
            #     "data": total_rate, 
            #     "symbol": "circle", 
            #     "itemStyle": {"color": "#ED7D31"}, 
            #     "tooltip": {"valueFormatter": js_percent_formatter},
            #     "label": {"show": True, "formatter": "{c}%"} # 折线上的直接显示文字保留 Echarts 内置标签模板
            # }
        ]
    }
    return option
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
    
    [企业级修复] 支持 AOI 等无 SOOS 的场景，动态检测列存在性。
    """
    
    # [核心修复] 检测是否为 AOI 场景（无 SOOS 列）
    has_soos = 'SOOS片数' in df.columns
    
    # --------------------------------------------------------------------------
    # 1. 动态断层检测与数据重组 (The Null Padding Algorithm)
    # --------------------------------------------------------------------------
    x_data = []
    oos_cnt, soos_cnt, ooc_cnt = [], [], []
    oos_rate, soos_rate, ooc_rate = [], [], []

    prev_time_type = None
    ghost_counter = 1

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
            x_data.append(" " * ghost_counter)
            ghost_counter += 1
            
            oos_cnt.append(None)
            soos_cnt.append(None)
            ooc_cnt.append(None)
            oos_rate.append(None)
            soos_rate.append(None)
            ooc_rate.append(None)

        prev_time_type = curr_time_type

        # 正常插入真实数据
        x_data.append(time_str)
        
        # [核心修复] 安全获取数据，AOI 场景使用 0 作为默认值
        oos_cnt.append(row['OOS片数'] if pd.notna(row.get('OOS片数', 0)) else 0)
        soos_cnt.append(row['SOOS片数'] if has_soos and pd.notna(row.get('SOOS片数', 0)) else 0)
        ooc_cnt.append(row['OOC片数'] if pd.notna(row.get('OOC片数', 0)) else 0)

        # 率指标转化为前端基础浮点数
        def fmt_rate(val):
            return round(val * 100, 2) if pd.notna(val) else None

        oos_rate.append(fmt_rate(row.get('OOS')))
        soos_rate.append(fmt_rate(row.get('SOOS')) if has_soos else None)
        ooc_rate.append(fmt_rate(row.get('OOC')))

    # --------------------------------------------------------------------------
    # 2. 视图层拦截器配置 (JavaScript Injection)
    # --------------------------------------------------------------------------
    js_percent_formatter = JsCode("function (value) { return value == null ? '-' : value.toFixed(2) + '%'; }")

    # --------------------------------------------------------------------------
    # 3. 组装最终 Echarts 字典
    # --------------------------------------------------------------------------
    # [核心修复] 动态构建 legend 和 series，适配 AOI 场景
    if has_soos:
        legend_data = ["OOS片数", "SOOS片数", "OOC片数", "OOS", "SOOS", "OOC"]
        # 原始顺序：OOC片数 -> SOOS片数 -> OOS片数，OOS -> SOOS -> OOC
        series = [
            # 柱状图部分
            {"name": "OOC片数", "type": "bar", "stack": "总量", "data": ooc_cnt, "itemStyle": {"color": "#F9D976"}},
            {"name": "SOOS片数", "type": "bar", "stack": "总量", "data": soos_cnt, "itemStyle": {"color": "#81D8D0"}},
            {"name": "OOS片数", "type": "bar", "stack": "总量", "data": oos_cnt, "itemStyle": {"color": "#7B9CE1"}},
            # 折线图部分
            {"name": "OOS", "type": "line", "yAxisIndex": 1, "data": oos_rate, "symbol": "circle", "tooltip": {"valueFormatter": js_percent_formatter}},
            {"name": "SOOS", "type": "line", "yAxisIndex": 1, "data": soos_rate, "symbol": "circle", "tooltip": {"valueFormatter": js_percent_formatter}},
            {"name": "OOC", "type": "line", "yAxisIndex": 1, "data": ooc_rate, "symbol": "circle", "tooltip": {"valueFormatter": js_percent_formatter}},
        ]
    else:
        # AOI 场景：不包含 SOOS
        legend_data = ["OOS片数", "OOC片数", "OOS", "OOC"]
        series = [
            # 柱状图部分
            {"name": "OOC片数", "type": "bar", "stack": "总量", "data": ooc_cnt, "itemStyle": {"color": "#F9D976"}},
            {"name": "OOS片数", "type": "bar", "stack": "总量", "data": oos_cnt, "itemStyle": {"color": "#7B9CE1"}},
            # 折线图部分
            {"name": "OOS", "type": "line", "yAxisIndex": 1, "data": oos_rate, "symbol": "circle", "tooltip": {"valueFormatter": js_percent_formatter}},
            {"name": "OOC", "type": "line", "yAxisIndex": 1, "data": ooc_rate, "symbol": "circle", "tooltip": {"valueFormatter": js_percent_formatter}},
        ]
    
    option = {
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}},
        "legend": {
            "data": legend_data,
            "bottom": 0
        },
        "grid": {"left": "3%", "right": "4%", "bottom": "10%", "containLabel": True},
        "xAxis": [{"type": "category", "data": x_data, "axisPointer": {"type": "shadow"}}],
        "yAxis": [
            {"type": "value", "name": "报警片数", "position": "left"},
            {"type": "value", "name": "报警率", "position": "right", "axisLabel": {"formatter": "{value} %"}}
        ],
        "series": series
    }
    return option

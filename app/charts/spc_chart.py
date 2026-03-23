import pandas as pd
from typing import Dict, Any

def get_spc_summary_echarts_option(df: pd.DataFrame) -> Dict[str, Any]:
    """
    生成 SPC 全局汇总双 Y 轴图表的 Echarts 配置选项。 # 隔离绘图逻辑，保持调用端整洁
    采用堆叠柱状图 (左Y轴) + 多折线图 (右Y轴) 组合。 # 1:1 还原视觉稿设计
    """
    # 提取 X 轴数据
    x_data = df['time_group'].tolist() # 时间轴节点
    
    # 提取并处理 Y 轴数据 (处理 NaN 为 0，避免绘图断层)
    oos_cnt = df['OOS片数'].fillna(0).tolist() # OOS 报警片数
    soos_cnt = df['SOOS片数'].fillna(0).tolist() # SOOS 报警片数
    ooc_cnt = df['OOC片数'].fillna(0).tolist() # OOC 报警片数
    
    # 提取率数据 (为了在图表上显示为 1.29，需要将 0.0129 乘以 100，并处理 NaN 为 '-')
    def format_rate(series: pd.Series) -> list:
        return [round(val * 100, 2) if pd.notna(val) else '-' for val in series] # 转化为前端易读的百分比浮点数

    oos_rate = format_rate(df['OOS']) # OOS 率
    soos_rate = format_rate(df['SOOS']) # SOOS 率
    ooc_rate = format_rate(df['OOC']) # OOC 率
    oos_soos_rate = format_rate(df['OOS+SOOS']) # 联合报警率
    total_rate = format_rate(df['OOS+SOOS+OOC']) # 总报警率

    # 构建 Echarts Option 字典
    option = {
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "cross"}}, # 开启十字准星提示框
        "legend": {
            "data": ["OOS片数", "SOOS片数", "OOC片数", "OOS", "SOOS", "OOC", "OOS+SOOS", "OOS+SOOS+OOC"], # 图例清单
            "bottom": 0 # 图例放置在底部
        },
        "grid": {"left": "3%", "right": "4%", "bottom": "10%", "containLabel": True}, # 调整网格边距
        "xAxis": [{"type": "category", "data": x_data, "axisPointer": {"type": "shadow"}}], # X轴绑定时间节点
        "yAxis": [
            {"type": "value", "name": "报警片数", "position": "left"}, # 左侧 Y 轴 (绝对值)
            {"type": "value", "name": "报警率", "position": "right", "axisLabel": {"formatter": "{value} %"}} # 右侧 Y 轴 (百分比)
        ],
        "series": [
            # 柱状图部分 (barmode='stack')
            {"name": "OOC片数", "type": "bar", "stack": "总量", "data": ooc_cnt, "itemStyle": {"color": "#F9D976"}}, # 堆叠柱状图
            {"name": "SOOS片数", "type": "bar", "stack": "总量", "data": soos_cnt, "itemStyle": {"color": "#81D8D0"}}, # 堆叠柱状图
            {"name": "OOS片数", "type": "bar", "stack": "总量", "data": oos_cnt, "itemStyle": {"color": "#7B9CE1"}}, # 堆叠柱状图
            # 折线图部分 (yAxisIndex=1 绑定右侧 Y 轴)
            {"name": "OOS", "type": "line", "yAxisIndex": 1, "data": oos_rate, "symbol": "circle"}, # 右轴折线
            {"name": "SOOS", "type": "line", "yAxisIndex": 1, "data": soos_rate, "symbol": "circle"}, # 右轴折线
            {"name": "OOC", "type": "line", "yAxisIndex": 1, "data": ooc_rate, "symbol": "circle"}, # 右轴折线
            {"name": "OOS+SOOS", "type": "line", "yAxisIndex": 1, "data": oos_soos_rate, "symbol": "circle"}, # 右轴折线
            {"name": "OOS+SOOS+OOC", "type": "line", "yAxisIndex": 1, "data": total_rate, "symbol": "circle", "itemStyle": {"color": "#ED7D31"}, "label": {"show": True, "formatter": "{c}%"}} # 主力折线带 Label
        ]
    }
    return option # 返回标准 Echarts 字典对象
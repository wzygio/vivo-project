import streamlit as st
import pandas as pd
import numpy as np
from streamlit_echarts import st_echarts
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode

st.set_page_config(page_title="厂别站点 Top10 测试", layout="wide")

st.title("🧪 跨厂区异常站点 Top 10 (垂直堆叠柱状图)")

# =========================================================================
# 1. 模拟底层数据生成 (刻意制造稀疏报警，测试不足 10 个的场景)
# =========================================================================
np.random.seed(42) # 固定种子以便复现
factories = ["一厂", "二厂", "三厂"]
stations = [f"ST_{i:02d}" for i in range(1, 21)]

# 模拟 1000 条制程数据，但 90% 以上的数据是 0 报警
df = pd.DataFrame({
    "factory": np.random.choice(factories, 1000),
    "step_id": np.random.choice(stations, 1000),
    # 极低概率触发报警
    "OOS片数": np.random.choice([0]*50 + [1, 2], 1000),   
    "SOOS片数": np.random.choice([0]*80 + [1], 1000),
    "OOC片数": np.random.choice([0]*95 + [1], 1000)
})

# =========================================================================
# 2. 后端聚合与过滤引擎 (核心防呆逻辑)
# =========================================================================
# 【核心修改1】联合 'factory' 和 'step_id' 进行跨厂区聚合
agg_df = df.groupby(['factory', 'step_id'])[['OOS片数', 'SOOS片数', 'OOC片数']].sum()

# 计算总报警数
agg_df['Total_Alarms'] = agg_df['OOS片数'] + agg_df['SOOS片数'] + agg_df['OOC片数']

# 【核心修改2】暴力剔除总报警数为 0 的健康站点 (解决不足 10 个的问题)
agg_df = agg_df[agg_df['Total_Alarms'] > 0]

if agg_df.empty:
    st.success("🎉 当前所选时间段内，所有厂区均无任何超规报警站点！")
    st.stop()

# 【核心修改3】降序排列 (从高到低)，并截取 Top 10
# 注意：垂直柱状图从左往右画，所以必须从大到小排 (ascending=False)
top10_df = agg_df.sort_values('Total_Alarms', ascending=False).head(10)

# 重置索引，以便后续提取
top10_df = top10_df.reset_index()
# 拼接 X 轴显示的标签：工厂名 \n 站点名
top10_df['display_name'] = top10_df['factory'] + "\n" + top10_df['step_id']

# =========================================================================
# 3. 前端 Echarts 渲染逻辑 (垂直堆叠)
# =========================================================================
x_data = top10_df['display_name'].tolist()
oos_data = top10_df['OOS片数'].tolist()
soos_data = top10_df['SOOS片数'].tolist()
ooc_data = top10_df['OOC片数'].tolist()

option = {
    "tooltip": {
        "trigger": "axis",
        "axisPointer": {"type": "shadow"}
    },
    "legend": {
        "data": ["OOC片数", "SOOS片数", "OOS片数"],
        "bottom": 0
    },
    "grid": {
        "left": "3%",
        "right": "3%",
        "bottom": "15%", # 留出空间给 X 轴的两行文字
        "containLabel": True
    },
    "xAxis": {
        "type": "category",  # 【核心修改4】X 轴变为类目轴
        "data": x_data,
        "axisLabel": {
            "interval": 0,   # 强制显示所有标签
            "fontWeight": "bold"
        }
    },
    "yAxis": {
        "type": "value",     # 【核心修改4】Y 轴变为数值轴
        "name": "报警总片数"
    },
    "series": [
        {
            "name": "OOC片数",
            "type": "bar",
            "stack": "总量",
            "barMaxWidth": 80, # 限制柱子最大宽度，防止站点过少时柱子撑得太粗
            "itemStyle": {"color": "#F9D976"},
            "data": ooc_data
        },
        {
            "name": "SOOS片数",
            "type": "bar",
            "stack": "总量",
            "itemStyle": {"color": "#81D8D0"},
            "data": soos_data
        },
        {
            "name": "OOS片数",
            "type": "bar",
            "stack": "总量",
            "itemStyle": {"color": "#7B9CE1"},
            "label": {
                "show": True,
                "position": "top", # 【核心修改5】总计数值显示在柱子正上方
                "fontWeight": "bold",
                "formatter": """function(params) {
                    let total = params.value + %s[params.dataIndex] + %s[params.dataIndex];
                    return total > 0 ? total : '';
                }""" % (soos_data, ooc_data)
            },
            "data": oos_data
        }
    ]
}

st_echarts(option, height="450px")

st.divider()
import streamlit as st
import pandas as pd
import numpy as np
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

st.set_page_config(page_title="Top10 站点表格测试", layout="wide")

st.title("🧪 Top 10 站点汇总与明细表渲染测试")

# =========================================================================
# 1. 模拟底层数据生成 (刻意制造多产品、多站点的随机报警)
# =========================================================================
np.random.seed(42)
products = ["M626", "M678", "M888"]
stations = [f"ST_{i:02d}" for i in range(1, 30)]

# 模拟底层 2000 条制程数据
df = pd.DataFrame({
    "prod_code": np.random.choice(products, 2000),
    "step_id": np.random.choice(stations, 2000),
    "OOS片数": np.random.choice([0]*10 + [1, 2], 2000),   
    "SOOS片数": np.random.choice([0]*20 + [1], 2000),
    "OOC片数": np.random.choice([0]*30 + [1], 2000)
})

# =========================================================================
# 2. 核心聚合逻辑：提取全局 Top 10
# =========================================================================
# 全局按站点聚合
agg_df = df.groupby('step_id')[['OOS片数', 'SOOS片数', 'OOC片数']].sum()
agg_df['报警总数'] = agg_df['OOS片数'] + agg_df['SOOS片数'] + agg_df['OOC片数']

# 剔除 0 报警，按总数降序，截取 Top 10
top10_df = agg_df[agg_df['报警总数'] > 0].sort_values('报警总数', ascending=False).head(10)
top10_stations_list = top10_df.index.tolist()

if top10_df.empty:
    st.success("🎉 当前所选时间段内，无任何报警站点！")
    st.stop()

# =========================================================================
# 3. 汇总表 (Summary Table): 转置矩阵
# =========================================================================
st.markdown("#### 📊 Top 10 异常站点汇总表")

sum_view = top10_df.copy()

for col in ['OOS', 'SOOS', 'OOC']:
    ratio = np.where(sum_view['报警总数'] > 0, sum_view[f'{col}片数'] / sum_view['报警总数'], 0)
    sum_view[f'{col}占比'] = [f"{x * 100:.2f}%" for x in ratio]
    sum_view[f'{col}片数'] = sum_view[f'{col}片数'].astype(str)

sum_view['报警总数'] = sum_view['报警总数'].astype(str)

ordered_metrics = ['报警总数', 'OOS片数', 'OOS占比', 'SOOS片数', 'SOOS占比', 'OOC片数', 'OOC占比']
sum_view = sum_view[ordered_metrics]

# 转置
view_df = sum_view.T.reset_index().rename(columns={'index': '统计维度'})

# 渲染配置
gb_sum = GridOptionsBuilder.from_dataframe(view_df)
gb_sum.configure_selection(selection_mode="single", use_checkbox=False)
gb_sum.configure_column("统计维度", pinned="left", width=120, cellStyle={'fontWeight': 'bold', 'backgroundColor': '#f8f9fa'})

for col in top10_stations_list:
    gb_sum.configure_column(col, cellStyle={'backgroundColor': 'transparent'})

grid_options_sum = gb_sum.build()

# [核心修复 2]：使用 JsCode 编写真实的 JS 函数来控制行样式
row_style_jscode = JsCode("""
function(params) {
    if (params.data && params.data['统计维度'] && params.data['统计维度'].includes('占比')) {
        return {'backgroundColor': 'rgba(230, 240, 255, 0.4)'};
    }
    return null;
}
""")
grid_options_sum['getRowStyle'] = row_style_jscode

AgGrid(
    view_df,
    gridOptions=grid_options_sum,
    update_mode=GridUpdateMode.SELECTION_CHANGED,
    theme='streamlit',
    height=280,
    allow_unsafe_jscode=True,  # 必须开启，否则 JsCode 无法执行
    key="ag_top10_summary"
)

st.divider()

# =========================================================================
# 4. 明细表 (Detail Table)
# =========================================================================
st.markdown("#### 📑 By产品 - Top 10 站点报警明细")

detail_raw = df[df['step_id'].isin(top10_stations_list)].copy()
agg_det = detail_raw.groupby(['prod_code', 'step_id'])[['OOS片数', 'SOOS片数', 'OOC片数']].sum()

ordered_metrics_det = ['OOS片数', 'SOOS片数', 'OOC片数']
pivot_df = agg_det.unstack(level='step_id', fill_value=0)

stacked_df = pivot_df.stack(level=0, dropna=False)
stacked_df.index.names = ['品名', '报警类型']
stacked_df = stacked_df.reindex(ordered_metrics_det, level='报警类型')

flat_df = stacked_df.reset_index()
available_stations = [s for s in top10_stations_list if s in flat_df.columns]
flat_df = flat_df[['品名', '报警类型'] + available_stations]

gb_det = GridOptionsBuilder.from_dataframe(flat_df)
gb_det.configure_selection(selection_mode="single", use_checkbox=False)
gb_det.configure_column("品名", rowGroup=True, hide=True)
gb_det.configure_column("报警类型", pinned="left", width=120, cellStyle={'fontWeight': 'bold', 'backgroundColor': '#f8f9fa'})

grid_options_det = gb_det.build()
grid_options_det['groupDefaultExpanded'] = -1 
grid_options_det['autoGroupColumnDef'] = {
    'headerName': '📦 产品型号', 
    'width': 160, 
    'pinned': 'left', 
    'cellRendererParams': {'suppressCount': True}
}

AgGrid(
    flat_df,
    gridOptions=grid_options_det,
    enable_enterprise_modules=True,
    update_mode=GridUpdateMode.SELECTION_CHANGED,
    theme='streamlit',
    height=450,
    key="ag_top10_detail"
)
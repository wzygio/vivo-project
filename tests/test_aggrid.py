import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

st.set_page_config(page_title="前端状态稳定性测试")

# 1. 初始化纯净的状态机
if "ag_key" not in st.session_state: st.session_state.ag_key = 0
if "ag_intent" not in st.session_state: st.session_state.ag_intent = None

@st.dialog("🔍 稳定的测试弹窗", width="large")
def mock_modal(defect):
    st.success(f"当前钻取: {defect}。下面是固定的 110 行静态数据。")
    # 生成绝对固定的 110 条静态数据，绝不含随机数
    df = pd.DataFrame({"报警类型": [defect]*110, "测试编号": range(1, 111)})
    st.dataframe(df, use_container_width=True, height=300)
    st.info("💡 请尝试关闭弹窗并重新点击。如果表格依然齐齐整整是 110 行，证明前端 Intent 机制坚如磐石，并未破坏任何数据提取。")

st.title("🧪 前端 Intent 机制稳定性论证")

df = pd.DataFrame({"报警类型": ["OOS片数", "SOOS片数", "OOC片数"], "2026M03": [28, 110, 10]})
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_selection(selection_mode="single", use_checkbox=False)

grid_response = AgGrid(
    df, gridOptions=gb.build(), 
    update_mode=GridUpdateMode.SELECTION_CHANGED, 
    key=f"ag_test_{st.session_state.ag_key}"
)

# 2. 意图捕获与重载
selected = grid_response.get("selected_rows")
if selected is not None and len(selected) > 0:
    row_dict = selected.iloc[0].to_dict() if isinstance(selected, pd.DataFrame) else selected[0]
    st.session_state.ag_intent = {"defect": row_dict["报警类型"]}
    st.session_state.ag_key += 1
    st.rerun()

# 3. 拦截意图并弹窗
if st.session_state.ag_intent:
    intent = st.session_state.ag_intent
    st.session_state.ag_intent = None
    mock_modal(intent["defect"])
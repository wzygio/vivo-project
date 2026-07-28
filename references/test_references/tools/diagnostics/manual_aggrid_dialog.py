import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode

st.set_page_config(page_title="伪装关闭按钮测试", layout="wide")

if "ag_key" not in st.session_state: st.session_state.ag_key = 0
if "spc_lock" not in st.session_state: st.session_state.spc_lock = None

# ==========================================
# 弹窗定义 (注入 CSS 魔法)
# ==========================================
# 注意：标题留空，为了给我们的自定义 Header 腾出空间
@st.dialog(" ", width="large")
def stealth_dialog(defect):
    # 1. 注入 CSS：干掉原生的关闭按钮，并消除默认 Header 的占位空白
    st.markdown(
        """
        <style>
        /* 隐藏原生的右上角关闭按钮 */
        [data-testid="stDialog"] button[aria-label="Close"] { 
            display: none !important; 
        }
        /* 紧凑化自定义 Header 的顶部边距 */
        [data-testid="stDialog"] div[data-testid="stVerticalBlock"] {
            gap: 0.5rem;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

    # 2. 构造伪装的 Header (左侧标题，右侧按钮)
    header_col1, header_col2 = st.columns([10, 1])
    with header_col1:
        st.markdown(f"### 🔍 报警明细 - {defect}")
    with header_col2:
        # 伪装的退出按钮
        if st.button("✖", key="fake_close_btn", use_container_width=True, help="关闭并释放图表状态"):
            st.session_state.spc_lock = None
            st.session_state.ag_key += 1
            st.rerun()

    st.divider()

    # 3. 弹窗常规内容
    st.success(f"成功锁定并钻取: **{defect}**")
    tabs = ["2026M02", "2026M03 (110 片)", "20260401 (23 片)"]
    sel = st.segmented_control("时间切换测试", options=tabs, default=tabs[-1])
    st.write(f"当前展示: **{sel}** 的明细...")


# ==========================================
# 主程序
# ==========================================
st.title("🧪 UI 伪装魔法：自定义弹窗关闭事件")
st.info("💡 请点击 AgGrid 触发弹窗，然后点击弹窗右上角我们伪装的【✖】按钮关闭，看看背后的表格是否完美失忆。")

df = pd.DataFrame({"报警类型": ["OOS", "SOOS", "OOC"], "数值": ["28片", "110片", "10片"]})
gb = GridOptionsBuilder.from_dataframe(df)
gb.configure_selection(selection_mode="single", use_checkbox=False)

grid = AgGrid(
    df, 
    gridOptions=gb.build(), 
    update_mode=GridUpdateMode.SELECTION_CHANGED, 
    key=f"ag_test_{st.session_state.ag_key}"
)

# 边缘触发逻辑
selected = grid.get("selected_rows")
if selected is not None and len(selected) > 0:
    row_dict = selected.iloc[0].to_dict() if isinstance(selected, pd.DataFrame) else selected[0]
    defect = row_dict["报警类型"]
    
    if st.session_state.spc_lock != defect:
        st.session_state.spc_lock = defect
        stealth_dialog(defect)
else:
    st.session_state.spc_lock = None
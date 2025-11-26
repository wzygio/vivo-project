import streamlit as st
import os
from pathlib import Path

# --- 1. 初始化与配置 ---
from vivo_project.config import CONFIG
from vivo_project.app.app_setup import AppSetup
AppSetup.initialize_app()

from vivo_project.services.yield_service import YieldAnalysisService
from vivo_project.app.components.components import create_code_selection_ui

# --- 2. 定义常量与路径 ---
# 假设所有 PPT 都放在这个目录下
PPT_SOURCE_DIR = "resources/analysis_ppt" 
IMG_OUTPUT_DIR = "resources/ppt_images"

# 确保源目录存在，避免报错
if not os.path.exists(PPT_SOURCE_DIR):
    os.makedirs(PPT_SOURCE_DIR)

# --- 3. 状态管理 (Session State) ---
# 我们需要记住当前正在查看哪个文件
if 'viewing_ppt' not in st.session_state:
    st.session_state.viewing_ppt = None # 存储当前正在查看的文件名

# --- 4. 实例化 Service ---
ppt_service = PPTService(output_dir=IMG_OUTPUT_DIR)

# ==============================================================================
#  界面区域 A: 标题与文件列表
# ==============================================================================
st.title("🗂️ 资料解析中心")
st.caption("浏览、下载或在线预览服务器上的分析报告。")

# 获取文件列表
ppt_files = [f for f in os.listdir(PPT_SOURCE_DIR) if f.endswith(('.pptx', '.ppt'))]

if not ppt_files:
    st.warning(f"文件夹 `{PPT_SOURCE_DIR}` 为空，请先上传 PPT 文件。")
else:
    st.markdown("### 📄 报告列表")
    
    # 遍历文件列表，生成每一行
    for ppt_file in ppt_files:
        file_path = os.path.join(PPT_SOURCE_DIR, ppt_file)
        
        # --- 容器化设计：每一行一个带边框的卡片 ---
        with st.container(border=True):
            # 使用列布局：名称占大头，按钮占小头
            # [图标+名称 (70%)]  [下载 (15%)]  [查看 (15%)]
            col_name, col_dl, col_view = st.columns([7, 1.5, 1.5])
            
            with col_name:
                # 垂直居中显示文件名，加个小图标美化
                st.markdown(f"**📊 {ppt_file}**")
            
            with col_dl:
                # 读取文件二进制流用于下载
                # 注意：这里只是读取流，不耗时
                with open(file_path, "rb") as f:
                    st.download_button(
                        label="⬇️ 下载",
                        data=f,
                        file_name=ppt_file,
                        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                        key=f"dl_{ppt_file}", # 唯一Key
                        use_container_width=True # 按钮填满列宽，显得整齐
                    )
            
            with col_view:
                # 查看按钮
                # 点击后，更新 session_state，并触发转换
                if st.button("👁️ 查看", key=f"view_{ppt_file}", use_container_width=True):
                    # 1. 更新状态
                    st.session_state.viewing_ppt = ppt_file
                    
                    # 2. 立即触发转换 (带 Loading 效果)
                    # 注意：传入 ppt_service 的是相对路径
                    rel_path = os.path.join(PPT_SOURCE_DIR, ppt_file)
                    with st.spinner(f"正在解析 {ppt_file}，请稍候..."):
                        success = ppt_service.convert_to_images(rel_path)
                        if not success:
                            st.error("解析失败，请检查后台日志。")
                            st.session_state.viewing_ppt = None # 重置状态
                        else:
                            st.rerun() # 强制刷新页面以展示下方预览区

# ==============================================================================
#  界面区域 B: 预览窗口 (仅当选择了文件时显示)
# ==============================================================================
if st.session_state.viewing_ppt:
    st.markdown("---") # 分割线
    
    # 标题栏：显示当前文件名 + 关闭按钮
    col_title, col_close = st.columns([9, 1])
    with col_title:
        st.subheader(f"📖 正在预览: {st.session_state.viewing_ppt}")
    with col_close:
        if st.button("❌ 关闭", type="primary"):
            st.session_state.viewing_ppt = None
            st.rerun()

    # 获取图片并展示
    images = ppt_service.get_images()
    
    if images:
        # 优化体验：可以在这里加一个滑动条或者分页，但垂直滚动最直观
        for idx, img_path in enumerate(images):
            st.image(img_path, caption=f"Slide {idx+1}", use_container_width=True)
    else:
        st.warning("未找到解析后的图片，请尝试重新点击“查看”。")
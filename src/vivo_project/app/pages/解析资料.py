# src/vivo_project/app/pages/解析资料.py
import streamlit as st
from vivo_project.services.ppt_service import PPTService # 导入服务

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")

# 初始化服务
ppt_service = PPTService(output_dir="resources/ppt_images")
ppt_file = "resources/example.pptx"

st.title("📊 PPT 报告查看器")

if st.button("🔄 更新/加载 PPT"):
    with st.spinner("正在处理..."):
        ppt_service.convert_to_images(ppt_file)
        st.success("加载完成")

# 获取图片并显示
images = ppt_service.get_images()
for img in images:
    st.image(img, use_container_width=True)
import streamlit as st
import os
import ppt_utils  # 导入上面写的模块
from src.vivo_project.config import RESOURCE_DIR

# 定义路径
PPT_FILE = RESOURCE_DIR / "大数据科日报.pptx"         # 你的源文件
IMG_OUTPUT_DIR = RESOURCE_DIR / "ppt_images"    # 图片输出文件夹

st.set_page_config(
    page_title="Visionox 报表预览",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("📊 报表图片预览模式 (高清版)")

with st.sidebar:
    st.write("### 操作面板")
    if st.button("🔄 更新/转换 PPT"):
        with st.spinner("正在清理旧文件并生成高清图片..."):
            if os.path.exists(PPT_FILE):
                success = ppt_utils.ppt_to_images(PPT_FILE, IMG_OUTPUT_DIR)
                if success:
                    st.success("转换成功！")
                else:
                    st.error("转换失败。")
            else:
                st.error(f"找不到文件: {PPT_FILE}")

st.write("---")

if os.path.exists(IMG_OUTPUT_DIR):
    images = ppt_utils.get_sorted_images(IMG_OUTPUT_DIR)
    
    if not images:
        st.info("暂无图片，请点击左侧按钮进行转换。")
    else:
        st.write(f"共找到 {len(images)} 页幻灯片：")
        
        for idx, img_path in enumerate(images):
            # 使用 container_width 填满宽屏布局
            st.image(img_path, caption=f"第 {idx+1} 页", use_container_width=True)
            st.write("---") 
else:
    st.info("请先进行转换。")
import streamlit as st
import os
from pathlib import Path
import logging
import pandas as pd
import time


# --- 1. 基础配置与导入 ---
from vivo_project.config import CONFIG, PROJECT_ROOT  # 导入项目配置和根路径
from vivo_project.utils.app_setup import AppSetup  # 导入应用初始化工具
from vivo_project.app.components.components import render_page_header  # 导入页头渲染组件

# 引入所有 Service
from vivo_project.services.file_manager_service import FileManagerService  # 文件管理服务
from vivo_project.services.excel_service import ExcelService  # Excel 服务
from vivo_project.services.ppt_service import PPTService  # PPT 服务
from vivo_project.services.pdf_service import PDFService  # PDF 服务

# 使用 cache_resource 避免重复初始化
@st.cache_resource
def init_global_resources():
    AppSetup.initialize_app()  # 初始化应用配置
init_global_resources()  # 执行初始化

st.set_page_config(layout="wide", initial_sidebar_state="collapsed")  # 设置页面布局为宽屏
render_page_header("📋 专项资料")  # 渲染统一页头

# --- 2. 路径定义 ---
DOC_SOURCE_DIR = "resources/project_files"  # 文档源目录
IMG_CACHE_DIR = "data/doc_cache"  # 图片缓存目录

if not os.path.exists(DOC_SOURCE_DIR):  # 如果源目录不存在
    os.makedirs(DOC_SOURCE_DIR)  # 创建源目录

# --- 3. 状态管理初始化 ---
# 初始化用于文件浏览的状态
if 'viewing_file' not in st.session_state:
    st.session_state.viewing_file = None  # 当前查看的文件名
if 'viewing_type' not in st.session_state:
    st.session_state.viewing_type = None  # 当前查看的文件类型

# 初始化用于 Excel 编辑的专用状态
# 我们需要把数据和时间戳存在 Session 中，防止每次交互都被重置
if 'editor_data' not in st.session_state:
    st.session_state.editor_data = None  # 缓存的 DataFrame 数据
if 'editor_timestamp' not in st.session_state:
    st.session_state.editor_timestamp = 0.0  # 缓存的文件加载时间戳（用于乐观锁）
if 'editor_file_ref' not in st.session_state:
    st.session_state.editor_file_ref = None  # 记录当前缓存的是哪个文件，防止切文件时数据错乱

# --- 4. 获取并分类文件 ---
classified_files = FileManagerService.get_classified_files(DOC_SOURCE_DIR)  # 获取分类文件列表

# 定义 UI 上显示的分类映射
category_map = [
    ("📂 北极星台账", classified_files['ledger'], True),   # 标题, 文件列表, 默认展开
    ("📅 北极星周报", classified_files['weekly'], False),
    ("🗃️ 其他归档",     classified_files['others'], False)
]

# ==============================================================================
#  界面区域 A: 分类折叠列表
# ==============================================================================
st.caption("浏览、下载或在线预览各类分析报告与台账。")  # 页面说明

# 遍历三个分类，生成 UI
for title, files, default_expanded in category_map:
    if not files: continue # 如果该分类没文件，就不显示
    
    with st.expander(f"{title} ({len(files)})", expanded=default_expanded):  # 创建折叠面板
        for doc_file in files:
            # 获取类型
            f_type = FileManagerService.get_file_type(doc_file)  # 识别文件类型
            file_path = os.path.join(DOC_SOURCE_DIR, doc_file)  # 拼接完整路径
            
            # 设定图标和 MIME
            if f_type == 'EXCEL':
                icon, mime = "📗", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif f_type == 'PDF':
                icon, mime = "📕", "application/pdf"
            else: # PPT
                icon, mime = "📊", "application/vnd.openxmlformats-officedocument.presentationml.presentation"

            # 渲染单行卡片
            with st.container(border=True):  # 创建带边框的容器
                c_name, c_dl, c_view = st.columns([8, 1, 1])  # 分列布局
                
                with c_name:
                    st.markdown(f"**{icon} {doc_file}**")  # 显示文件名
                
                with c_dl:
                    with open(file_path, "rb") as f:  # 读取文件用于下载
                        st.download_button("⬇️ 下载", f, file_name=doc_file, mime=mime, key=f"dl_{doc_file}", use_container_width=True)
                
                with c_view:
                    # 查看按钮逻辑
                    if st.button("👁️ 查看", key=f"view_{doc_file}", use_container_width=True):
                        st.session_state.viewing_file = doc_file  # 更新当前查看文件
                        st.session_state.viewing_type = f_type  # 更新当前查看类型
                        
                        # --- 切换文件时，必须重置编辑器状态 ---
                        # 否则会显示上一个文件的缓存数据
                        st.session_state.editor_data = None
                        st.session_state.editor_timestamp = 0.0
                        st.session_state.editor_file_ref = None
                        
                        # --- 特殊处理: PDF/PPT 需要预处理转图片 ---
                        if f_type in ['PDF', 'PPT']:
                            # 根据类型选择服务
                            service = PDFService(IMG_CACHE_DIR) if f_type == 'PDF' else PPTService(IMG_CACHE_DIR)
                            
                            with st.spinner(f"正在启动 {f_type} 引擎解析，请稍候..."):
                                success = service.convert_to_images(os.path.join(DOC_SOURCE_DIR, doc_file))  # 执行转换
                            
                            if success:
                                st.rerun() # 成功后刷新以显示图片
                            else:
                                st.error("解析失败，请检查日志。")
                                st.session_state.viewing_file = None
                        
                        else:
                            # Excel 不需要预处理，直接刷新显示
                            st.rerun()

# ==============================================================================
#  界面区域 B: 统一预览/编辑窗口
# ==============================================================================
if st.session_state.viewing_file:  # 如果有正在查看的文件
    curr_file = st.session_state.viewing_file  # 获取文件名
    curr_type = st.session_state.viewing_type  # 获取文件类型
    
    st.markdown("---")  # 分割线
    
    # 预览头
    c_head, c_close = st.columns([9, 1])
    with c_head:
        if curr_type == 'EXCEL':
            st.subheader(f"📝 正在编辑: {curr_file}")  # Excel 显示为编辑模式
        else:
            st.subheader(f"📖 正在预览: {curr_file}")  # 其他文件显示为预览模式
            
    with c_close:
        if st.button("❌ 关闭", type="primary"):  # 关闭按钮
            st.session_state.viewing_file = None
            st.rerun()  # 刷新页面

    # --- 多态渲染逻辑 ---
    try:
        # Case 1: Excel 编辑器 (核心修改部分)
        if curr_type == 'EXCEL':
            file_path = os.path.join(DOC_SOURCE_DIR, curr_file)
            
            # 1. 首次加载或重新加载逻辑
            # 如果缓存为空，或者缓存的文件不是当前文件，则从磁盘读取
            if st.session_state.editor_data is None or st.session_state.editor_file_ref != curr_file:
                with st.spinner("正在加载最新数据并校验版本..."):
                    # 读取数据
                    df = ExcelService.load_and_clean_data(file_path)
                    # 读取时间戳 (用于乐观锁)
                    ts = ExcelService.get_file_timestamp(file_path)
                    
                    # 存入 Session State
                    st.session_state.editor_data = df
                    st.session_state.editor_timestamp = ts
                    st.session_state.editor_file_ref = curr_file
            
            # 2. 渲染可编辑表格 (Data Editor)
            # 注意：data_editor 不支持 style.map 颜色，只能用 column_config 配置格式
            if st.session_state.editor_data is not None and not st.session_state.editor_data.empty:
                
                # 定义列配置 (美化版)
                column_cfg = {
                    "No.": st.column_config.NumberColumn(
                        "序号", 
                        format="%d", 
                        width="small",
                        help="Issue 唯一编号"
                    ),
                    
                    # 1. 针对长文本：虽然无法自动换行，但强制设为 large 宽度，最大化可视区域
                    "Issue描述": st.column_config.TextColumn(
                        "Issue 描述", 
                        width="large",  # 关键：加宽
                        required=True,
                        help="详细的异常描述信息"
                    ),
                    "原因分析": st.column_config.TextColumn(
                        "原因分析", 
                        width="large", # 关键：加宽
                        help="以及排查出的根本原因"
                    ),
                    
                    # 2. 针对日期：使用原生日期控件，解决 '1970-01-01' 丑陋问题
                    "发生日期": st.column_config.DateColumn(
                        "发生日期",
                        format="YYYY-MM-DD",
                        width="medium"
                    ),
                    
                    # 3. 针对状态：加入 Emoji 视觉符号，提升高级感
                    # 注意：保存到 Excel 时也会包含这些 Emoji，这在现代系统中通常是兼容的
                    "状态": st.column_config.SelectboxColumn(
                        "当前状态",
                        width="medium",
                        options=[
                            "🔴 Open", 
                            "🟢 Close", 
                            "🟡 Monitor", 
                            "⚪ Pending"
                        ],
                        required=True,
                        help="红色:未结案 | 绿色:已结案"
                    ),
                    
                    # 4. 针对数值/指标：如果有良率或数值列，可以格式化
                    # 这里假设 '北极星指标' 可能是文本，如果是数字可以用 NumberColumn
                    "北极星指标": st.column_config.TextColumn(
                        "北极星指标",
                        width="medium"
                    ),
                    
                    # 5. 其他辅助列优化
                    "工艺段": st.column_config.TextColumn("工艺段", width="small"),
                    "发现方": st.column_config.TextColumn("发现方", width="small"),
                    "型号": st.column_config.TextColumn("型号", width="small"),
                }

                # --- 数据清洗：将旧的纯文本状态映射为新的 Emoji 状态，确保 UI 显示正确 ---
                status_mapping = {
                    "Open": "🔴 Open",
                    "Close": "🟢 Close",
                    "Monitor": "🟡 Monitor"
                }
                # 如果 '状态' 列存在，则应用映射；如果映射字典里找不到(如空值)，保持原样
                if "状态" in st.session_state.editor_data.columns:
                     st.session_state.editor_data["状态"] = st.session_state.editor_data["状态"].replace(status_mapping)
                
                # 渲染编辑器，返回值是用户修改后的新 DataFrame
                edited_df = st.data_editor(
                    st.session_state.editor_data,  # 传入缓存的数据
                    height=600,
                    use_container_width=True,
                    hide_index=True,
                    column_config=column_cfg,
                    num_rows="dynamic", # 允许增删行
                    key=f"editor_{curr_file}" # 确保唯一 Key
                )
                
                # 3. 保存按钮区
                st.info("💡 提示：支持多人并发编辑。若保存时提示冲突，请刷新页面获取最新版后重试。")
                
                col_save, col_info = st.columns([1, 5])
                with col_save:
                    # 使用一个容器来存放保存后的反馈信息，避免UI跳动
                    save_status_container = st.empty()
                    
                    if st.button("💾 保存修改", type="primary", use_container_width=True):
                        with st.spinner("正在安全写入..."):
                            # 1. 尝试保存
                            success, msg = ExcelService.save_data_with_lock(
                                file_path, 
                                edited_df, 
                                st.session_state.editor_timestamp
                            )
                            
                            if success:
                                # A. 保存成功：正常刷新
                                st.success(msg)
                                logging.info(f"User saved file: {curr_file}")
                                st.session_state.editor_data = edited_df
                                st.session_state.editor_timestamp = ExcelService.get_file_timestamp(file_path)
                                time.sleep(1) 
                                st.rerun() 
                            else:
                                # B. 保存失败（冲突）：启动“逃生舱”
                                # 注意：这里绝对不要 rerun，否则数据就丢了！
                                save_status_container.error(f"⚠️ {msg}")
                                
                                # 在错误信息下方，直接提供当前数据的下载备份
                                # 将当前的 DataFrame 转为 Excel 字节流
                                import io
                                buffer = io.BytesIO()
                                with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                                    edited_df.to_excel(writer, index=False)
                                
                                # 显示备份下载按钮
                                st.warning("您的修改尚未保存！请先下载您的修改版本作为备份，然后刷新页面。")
                                st.download_button(
                                    label="📥 下载我的修改备份 (防丢失)",
                                    data=buffer,
                                    file_name=f"冲突备份_{curr_file}",
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    key="backup_download_btn"
                                )
                                logging.warning(f"Save conflict for {curr_file}, user offered backup.")

            else:
                st.warning("Excel 内容为空或读取失败。")

        # Case 2: PDF / PPT 渲染 (读取缓存图片)
        elif curr_type in ['PDF', 'PPT']:
            # 重新实例化服务以获取图片列表
            service = PDFService(IMG_CACHE_DIR) if curr_type == 'PDF' else PPTService(IMG_CACHE_DIR)
            images = service.get_images()  # 获取转换后的图片列表
            
            if images:
                st.info(f"共加载 {len(images)} 页内容")
                for idx, img_path in enumerate(images):
                    abs_path = str(Path(PROJECT_ROOT) / img_path)  # 转换为绝对路径
                    st.image(abs_path, caption=f"Page {idx+1}", use_container_width=True)  # 渲染图片
            else:
                st.warning("缓存图片丢失，请重新点击“查看”按钮。")
                
    except Exception as e:
        st.error(f"预览渲染出错: {e}")  # 捕获并显示全局异常
        logging.error(f"Render error: {e}")
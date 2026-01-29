# tests/diagnose_streamlit_env.py
import streamlit as st
import os
import sys
import logging
from pathlib import Path

# 引入项目配置
from vivo_project.config import ConfigLoader
from vivo_project.utils.utils import setup_logging

st.set_page_config(page_title="🔍 日志环境显影剂", layout="wide")

st.title("🕵️‍♂️ Streamlit 日志环境显影剂")

# --- 1. 路径侦查 ---
st.header("1. 路径侦查")
col1, col2 = st.columns(2)

cwd = Path.cwd()
project_root = ConfigLoader.get_project_root()
log_dir = project_root / "logs"
log_file = log_dir / "app.log"

with col1:
    st.info("**当前工作目录 (CWD)**")
    st.code(str(cwd))
    
    st.info("**计算出的 Project Root**")
    st.code(str(project_root))

with col2:
    st.info("**期望的日志目录**")
    st.code(str(log_dir))
    
    st.info("**期望的日志文件**")
    st.code(str(log_file))

# --- 2. 写入权限暴力测试 ---
st.header("2. 写入权限暴力测试")
st.write("尝试绕过 logging 模块，直接用 Python `open()` 函数写入文件...")

if st.button("🧨 暴力写入测试文件"):
    try:
        # 1. 确保目录存在
        if not log_dir.exists():
            st.warning(f"目录不存在，尝试创建: {log_dir}")
            log_dir.mkdir(parents=True, exist_ok=True)
            st.success("✅ 目录创建成功")
        else:
            st.success("✅ 目录已存在")
            
        # 2. 写入文件
        test_file = log_dir / "streamlit_debug.txt"
        with open(test_file, "w", encoding="utf-8") as f:
            f.write("Streamlit 环境写入测试成功！")
        
        if test_file.exists():
            st.success(f"🎉 写入成功！文件位置: {test_file}")
            st.balloons()
        else:
            st.error("❌ 写入操作未报错，但文件未找到！(非常诡异)")
            
    except Exception as e:
        st.error(f"❌ 写入失败，捕获异常: {e}")
        st.exception(e)

# --- 3. Logger 状态透视 ---
st.header("3. Logger 状态透视")

# 尝试调用 setup_logging (带缓存)
st.write("正在调用 `setup_logging`...")
try:
    setup_logging("app.log")
    st.success("`setup_logging` 调用完成 (未报错)")
except Exception as e:
    st.error(f"`setup_logging` 调用崩溃: {e}")

# 检查 Root Logger
root = logging.getLogger()
st.write(f"Root Logger Level: `{logging.getLevelName(root.level)}`")
st.write(f"Handlers ({len(root.handlers)}):")

if not root.handlers:
    st.error("⚠️ Root Logger 没有任何 Handler！日志当然写不进去！")
else:
    for i, h in enumerate(root.handlers):
        st.write(f"**Handler {i+1}:** `{h}`")
        if isinstance(h, logging.FileHandler):
            st.success(f"   -> 发现 FileHandler! 目标路径: `{h.baseFilename}`")
            # 检查这个路径是否真的存在
            if os.path.exists(h.baseFilename):
                st.caption("      ✅ 文件物理存在")
            else:
                st.error("      ❌ 文件物理不存在！(Handler 指向了一个虚空路径)")
        elif isinstance(h, logging.StreamHandler):
             st.info("   -> StreamHandler (控制台输出)")

# --- 4. 强制清理缓存 ---
st.header("4. 核弹级重置")
if st.button("🧹 清除 st.cache_resource (强制重运行 setup)"):
    st.cache_resource.clear()
    st.rerun()
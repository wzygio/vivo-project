# tests/diagnose_logging_standalone.py
import logging
import time
import sys
from pathlib import Path

# 引入项目模块
from yield_domain.utils.utils import setup_logging
from config import ConfigLoader

def test_logging():
    print("="*60)
    print("📋 日志系统独立诊断 (Standalone Mode)")
    print("="*60)

    # 1. 初始化日志
    print("[Step 1] 初始化日志系统...")
    try:
        # 强制指定一个测试文件名，避免污染生产日志
        test_log_file = "test_diagnosis.log"
        setup_logging(test_log_file)
        
        # 获取日志路径
        root = ConfigLoader.get_project_root()
        log_path = root / "logs" / test_log_file
        print(f"   -> 日志文件路径: {log_path}")
        
    except Exception as e:
        print(f"❌ 初始化失败: {e}")
        return

    # 2. 尝试写入不同级别的日志
    print("\n[Step 2] 尝试写入测试日志...")
    logger = logging.getLogger()
    
    # 打印当前的 Handlers
    print(f"   -> 当前 Handlers: {logger.handlers}")
    
    msgs = [
        "INFO: 这是一条测试信息",
        "WARNING: 这是一条警告信息",
        "ERROR: 这是一条错误信息"
    ]
    
    for msg in msgs:
        if "INFO" in msg: logger.info(msg)
        elif "WARNING" in msg: logger.warning(msg)
        elif "ERROR" in msg: logger.error(msg)
        print(f"   -> 已发送: {msg}")

    # 3. 立即回读文件验证
    print("\n[Step 3] 立即回读文件验证...")
    if not log_path.exists():
        print("❌ 失败: 日志文件未创建！")
        return

    with open(log_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    print("-" * 20 + " 文件内容 " + "-" * 20)
    print(content)
    print("-" * 50)

    if "这是一条测试信息" in content:
        print("✅ 诊断通过：在非 Streamlit 环境下，日志写入功能正常。")
        print("👉 结论：问题出在 Streamlit 的生命周期管理（重置或覆盖）上。")
    else:
        print("❌ 诊断失败：虽然代码运行了，但文件里没有内容。")
        print("👉 可能原因：文件流被缓冲(Buffer)未刷新，或者 Handler 配置被意外修改。")

if __name__ == "__main__":
    test_logging()
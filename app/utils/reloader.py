import sys, os
import logging
import hashlib
from pathlib import Path

def deep_reload_modules(root_package_names=("src", "app", "spc_domain", "yield_domain", "shared_kernel")):
    """
    [V2.0 DDD 适配版] 强制热重载指定包名下的所有子模块。
    允许传入元组，覆盖 sys.modules 中可能出现的所有顶层包名。
    """
    unloaded_count = 0  # 用于统计被卸载模块的数量
    
    # 遍历当前内存中已加载的所有模块
    for module_name in list(sys.modules.keys()):
        # 1. 匹配所有可能的核心包名前缀
        # 2. 排除 app.pages，因为 Streamlit 原生会管理页面级的热更
        if module_name.startswith(root_package_names) and "app.pages" not in module_name:
            del sys.modules[module_name]
            unloaded_count += 1
            
    if unloaded_count > 0:
        logging.info(f"🔥 [Hot Reload] 已强制卸载 {unloaded_count} 个后端模块，下次 import 将读取最新代码。")


def get_project_revision(project_root: Path) -> str:
    """
    [V2.0 DDD 适配版] 计算整个项目的代码指纹。
    抛弃硬编码子目录，直接监控顶层 src 和 app，实现真正的全覆盖。
    """
    hash_md5 = hashlib.md5()
    
    # 核心修复：直接监控项目根目录下的 src 和 app 两个大类
    # 无论 Infrastructure 藏在哪个 Domain 文件夹里，全部都能扫到！
    target_dirs = [project_root / "src", project_root / "app"]
    
    for target_path in target_dirs:
        if not target_path.exists(): 
            continue
        
        # 深度遍历目录下所有 .py 和 .yaml 文件
        for root, _, files in os.walk(target_path):
            for file in sorted(files):  # 排序保证哈希顺序一致
                if file.endswith(".py") or file.endswith(".yaml"):
                    file_path = os.path.join(root, file)
                    mtime = os.path.getmtime(file_path)
                    hash_md5.update(f"{file}_{mtime}".encode('utf-8'))
                    
    return hash_md5.hexdigest()
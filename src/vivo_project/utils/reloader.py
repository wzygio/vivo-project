# src/vivo_project/utils/reloader.py
import sys, os
import importlib
import logging
import hashlib
from pathlib import Path

def deep_reload_modules(root_package_name="vivo_project"):
    """
    强制热重载指定包名下的所有子模块。
    这解决了修改 backend 代码后需要重启 Streamlit 的问题。
    """
    # 获取所有已加载的、属于该项目的模块
    # 使用 list() 锁定 keys，防止迭代时字典大小变化
    modules_to_reload = [
        name for name in sys.modules.keys() 
        if name.startswith(root_package_name) 
        and name not in sys.modules 
    ]
    
    # 策略：简单的 reload 可能无法处理复杂的依赖关系。
    # 对于 Streamlit 开发环境，最暴力的有效方法是：
    # 将业务模块从 sys.modules 中移除，迫使 Python 重新从磁盘读取。
    
    unloaded_count = 0
    for module_name in list(sys.modules.keys()):
        # 排除 strict frontend (app pages)，因为 Streamlit 会自己管
        # 主要针对 core, application, infrastructure, domain
        if module_name.startswith(root_package_name) and "app" not in module_name:
            del sys.modules[module_name]
            unloaded_count += 1
            
    if unloaded_count > 0:
        logging.info(f"🔥 [Hot Reload] 已强制卸载 {unloaded_count} 个后端模块，下次 import 将读取最新代码。")

def get_project_revision(project_root: Path) -> str:
        """
        计算整个项目的代码指纹。
        扫描 core, application, config 等所有关键目录。
        """
        hash_md5 = hashlib.md5()
        # 监控这些关键目录
        target_dirs = ["core", "application", "config", "app", "infrastructure"]
        
        src_path = project_root / "src" / "vivo_project"
        
        for subdir in target_dirs:
            target_path = src_path / subdir
            if not target_path.exists(): continue
            
            # 遍历目录下所有 .py 和 .yaml 文件
            for root, _, files in os.walk(target_path):
                for file in sorted(files):  # 排序保证顺序一致
                    if file.endswith(".py") or file.endswith(".yaml"):
                        file_path = os.path.join(root, file)
                        # 获取修改时间戳
                        mtime = os.path.getmtime(file_path)
                        # 将文件名和时间戳加入哈希计算
                        hash_md5.update(f"{file}_{mtime}".encode('utf-8'))
                        
        return hash_md5.hexdigest()
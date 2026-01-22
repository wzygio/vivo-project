# src/vivo_project/utils/reloader.py
import sys
import importlib
import logging

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
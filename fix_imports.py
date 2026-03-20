# fix_imports.py
import os
from pathlib import Path
import logging

def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(message)s')

# 🎯 核心替换规则字典 (从最长/最精确的开始替换，防止误伤)
REPLACEMENTS = {
    # 1. 修复前端 (app) 被错误加上 yield_domain 前缀的问题
    "yield_domain.app.": "app.",
    "yield_domain.utils.session_manager": "app.utils.session_manager",
    "yield_domain.utils.reloader": "app.utils.reloader",
    "yield_domain.utils.app_setup": "app.utils.app_setup",
    
    # 2. 修复共享内核 (shared_kernel) 的配置模型引用
    "yield_domain.config_model": "shared_kernel.config_model",
    "vivo_project.config_model": "shared_kernel.config_model",
    "from yield_domain.config import": "from shared_kernel.config import",
    "from vivo_project.config import": "from shared_kernel.config import",
    
    # 3. 修复 utils.py 拆分后的引用错位
    "from yield_domain.utils.utils import setup_logging": "from app.utils.logger_setup import setup_logging",
    "from vivo_project.utils.utils import setup_logging": "from app.utils.logger_setup import setup_logging",
    "from yield_domain.utils.utils import save_dict_to_excel": "from shared_kernel.utils.excel_tools import save_dict_to_excel",
    "from vivo_project.utils.utils import save_dict_to_excel": "from shared_kernel.utils.excel_tools import save_dict_to_excel",
    "yield_domain.utils.utils": "shared_kernel.utils.excel_tools", # 兜底
    "vivo_project.utils.utils": "shared_kernel.utils.excel_tools", # 兜底
    
    # 4. 修复遗留的 vivo_project (它们现在属于 yield_domain)
    "vivo_project.infrastructure": "yield_domain.infrastructure",
    "vivo_project.core": "yield_domain.core",
    "vivo_project.application": "yield_domain.application",
}

def run_auto_fix(base_path: str = "."):
    root = Path(base_path)
    target_dirs = ["app", "src", "tests"] # 只扫描这三个存有代码的目录
    changed_files_count = 0
    
    logging.info("🚀 开始全盘扫描并自动修复导入路径...\n")
    
    for d in target_dirs:
        dir_path = root / d
        if not dir_path.exists():
            continue
            
        # 递归查找所有的 .py 文件
        for file_path in dir_path.rglob("*.py"):
            with open(file_path, "r", encoding="utf-8") as f:
                original_content = f.read()
            
            new_content = original_content
            # 遍历替换规则
            for old_text, new_text in REPLACEMENTS.items():
                new_content = new_content.replace(old_text, new_text)
                
            # 如果内容发生了改变，则写回文件
            if new_content != original_content:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(new_content)
                logging.info(f"✅ 已修复: {file_path.relative_to(root)}")
                changed_files_count += 1
                
    logging.info(f"\n🎉 自动修复完成！共修改了 {changed_files_count} 个文件。")

if __name__ == "__main__":
    setup_logging()
    # 运行前强烈建议确认 VS Code 中左侧源代码管理 (Git) 已经 commit 或暂存了旧代码，以防万一
    run_auto_fix()
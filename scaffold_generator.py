import logging
from pathlib import Path

def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format='%(message)s')

def _create_init_file(directory: Path) -> None:
    """内部辅助：安全创建 __init__.py"""
    init_file = directory / "__init__.py"
    if not init_file.exists():
        init_file.touch()
        logging.info(f"    └── 📄 添加: __init__.py")

def build_enterprise_structure(base_path: str = ".") -> None:
    root_dir = Path(base_path)
    logging.info(f"🚀 开始在 {root_dir.absolute()} 补全企业级结构...")

    # ==========================================
    # 1. 补全 Src 领域后端结构 (DDD)
    # ==========================================
    src_dir = root_dir / "src"
    src_dir.mkdir(exist_ok=True)
    
    # 定义各领域所需的标准分层
    domain_structure: dict[str, list[str]] = {
        "yield_domain": ["application", "core", "infrastructure"],
        "spc_domain": ["application", "core", "infrastructure"],
        "shared_kernel": ["utils"]
    }

    logging.info("\n📦 [后端领域层] src/ ...")
    for domain, subdirs in domain_structure.items():
        domain_path = src_dir / domain
        domain_path.mkdir(parents=True, exist_ok=True)
        _create_init_file(domain_path) # 领域根目录需要 __init__.py
        logging.info(f"  ├── 📁 {domain}/")

        for subdir in subdirs:
            sub_path = domain_path / subdir
            sub_path.mkdir(parents=True, exist_ok=True)
            _create_init_file(sub_path) # 领域子模块需要 __init__.py

    # ==========================================
    # 2. 补全 App 前端结构
    # ==========================================
    app_dir = root_dir / "app"
    app_dir.mkdir(exist_ok=True)
    _create_init_file(app_dir) # app 根目录需要 __init__.py

    app_subdirs = ["charts", "components", "pages", "utils"]
    
    logging.info("\n🎨 [前端展示层] app/ ...")
    for subdir in app_subdirs:
        sub_path = app_dir / subdir
        sub_path.mkdir(parents=True, exist_ok=True)
        logging.info(f"  ├── 📁 {subdir}/")
        
        # 🚨 架构铁律：Streamlit 的 pages 文件夹绝对不能有 __init__.py
        if subdir == "pages":
            logging.info("    └── 🚫 绕过: pages/ 不生成 __init__.py (Streamlit 规范)")
        else:
            _create_init_file(sub_path)

    logging.info("\n🎉 企业级目录与 Python 包标记 (Init) 补全完成！")

if __name__ == "__main__":
    setup_logging()
    build_enterprise_structure()
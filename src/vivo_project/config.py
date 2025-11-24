# src/vivo_project/config.py
import yaml
import logging
import sys
import os                 
from dotenv import load_dotenv 
from pathlib import Path
from typing import Dict, Any

# --- 核心路径定义 (保持不变) ---
try:
    SRC_ROOT = Path(__file__).resolve().parent.parent
    PROJECT_ROOT = SRC_ROOT.parent
except NameError:
    PROJECT_ROOT = Path.cwd()
    SRC_ROOT = PROJECT_ROOT / "src"
    logging.warning(f"__file__ 未定义，假定项目根目录为: {PROJECT_ROOT}")

# --- 将 SRC_ROOT 添加到 sys.path (保持不变) ---
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))
    logging.debug(f"将 SRC_ROOT 添加到 sys.path: {SRC_ROOT}")

# --- 其他常用路径常量 (保持不变) ---
LOG_DIR = PROJECT_ROOT / "logs"
DATA_DIR = PROJECT_ROOT / "data"
RESOURCE_DIR = PROJECT_ROOT / "resources"
CONFIG_FILE = PROJECT_ROOT / "config" / "config.yaml"
ENV_FILE = PROJECT_ROOT / ".env" # <--- 定义 .env 文件路径

# --- 全局配置变量 ---
CONFIG: Dict[str, Any] = {}

# --- 配置加载函数 (修改以包含 .env 加载) ---
def load_config(
    config_path: Path = CONFIG_FILE,
    env_path: Path = ENV_FILE # <--- 添加 env_path 参数
    ) -> Dict[str, Any]:
    """
    加载 YAML 配置文件和 .env 文件中的环境变量。
    .env 文件中的数据库配置将填充到 'database' 节。
    """
    loaded_config = {} # 初始化为空字典

    # 1. 加载 YAML 文件 (如果存在)
    try:
        if config_path.is_file():
            with open(config_path, 'r', encoding='utf-8') as f:
                yaml_config = yaml.safe_load(f)
                if isinstance(yaml_config, dict):
                     loaded_config.update(yaml_config) # 合并 YAML 配置
                logging.info(f"YAML 配置文件已成功加载: {config_path}")
        else:
            logging.warning(f"YAML 配置文件未找到: {config_path}")
    except Exception as e:
        logging.error(f"加载或解析 YAML 文件 {config_path} 时出错: {e}")

    try:
        if env_path.is_file():
            load_dotenv(dotenv_path=env_path, override=True)
            logging.info(f".env 文件已成功加载到环境变量: {env_path}")
        else:
            # 修改为 Error 级别，因为 db_handler 强依赖 .env
            logging.error(f".env 文件未找到: {env_path}，数据库连接将失败！")

    except Exception as e:
        logging.error(f"加载 .env 文件 {env_path} 时出错: {e}", exc_info=True)

    return loaded_config

# --- 执行加载 ---
CONFIG = load_config()

# --- 可以在这里添加日志记录定义的路径 ---
logging.debug(f"PROJECT_ROOT 定义为: {PROJECT_ROOT}")
logging.debug(f"SRC_ROOT 定义为: {SRC_ROOT}")
logging.debug(f"LOG_DIR 定义为: {LOG_DIR}")

# --- [可选] 确保 LOG_DIR 存在 ---
LOG_DIR.mkdir(parents=True, exist_ok=True) # 可以取消注释，让 config.py 负责创建日志目录

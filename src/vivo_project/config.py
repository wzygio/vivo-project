import yaml
import logging
from pathlib import Path
from typing import Dict, Any

# --- 全局配置变量 ---
# 将 CONFIG 初始化为空字典，以便即使加载失败，导入它的模块也不会出错
CONFIG: Dict[str, Any] = {}

# --- 配置加载函数 ---
def load_config(config_filename: str = "config/config.yaml") -> Dict[str, Any]:
    """
    加载指定名称的 YAML 配置文件。
    配置文件预期位于项目根目录 (config.py 文件向上两级的目录)。
    """
    try:
        # 1. 确定 config.yaml 的路径
        project_root = Path(__file__).resolve().parent.parent.parent
        config_path = project_root / config_filename

        # 2. 检查文件是否存在
        if not config_path.is_file():
            logging.error(f"配置文件未找到: {config_path}")
            return {} # 返回空字典

        # 3. 读取并解析 YAML 文件
        with open(config_path, 'r', encoding='utf-8') as f:
            # 使用 safe_load 防止执行任意代码
            loaded_config = yaml.safe_load(f)
            logging.info(f"配置文件已成功加载: {config_path}")
            # 确保返回的是字典，即使 YAML 文件为空
            return loaded_config if isinstance(loaded_config, dict) else {}

    except FileNotFoundError:
        logging.error(f"配置文件未找到: {config_path}")
        return {}
    except yaml.YAMLError as e:
        logging.error(f"解析配置文件 {config_path} 时出错: {e}")
        return {}
    except Exception as e:
        logging.error(f"加载配置文件时发生未知错误: {e}", exc_info=True)
        return {}

CONFIG = load_config()


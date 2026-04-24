import logging
from logging.handlers import TimedRotatingFileHandler # 引入企业级的时间轮转 Handler
import sys
from pathlib import Path
import streamlit as st

from src.shared_kernel.config import ConfigLoader

@st.cache_resource
def setup_logging(base_filename: str = "app"):
    """
    [企业级日志架构] 初始化日志系统 (单例模式)。
    支持按天自动切分、过期日志自动清理、按级别物理隔离。
    """
    project_root = ConfigLoader.get_project_root()
    log_dir = project_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    log_format = '%(asctime)s - %(levelname)s - [%(module)s] - %(message)s'
    log_date_format = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(log_format, datefmt=log_date_format) # 提前实例化 Formatter 以复用

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO) # 根 Logger 拦截 INFO 及以上的所有级别

    if root_logger.hasHandlers():
        for handler in root_logger.handlers[:]:
            handler.close()
            root_logger.removeHandler(handler)

    # =========================================================
    #  [Phase 2] 领域分流 Filter（零侵入业务代码）
    # =========================================================
    class DomainFilter(logging.Filter):
        """根据文件路径判断日志所属领域，兼容 Windows / Linux 路径"""
        def __init__(self, domain_marker: str):
            self.domain_marker = domain_marker
        
        def filter(self, record):
            path = record.pathname.replace("\\", "/")
            return self.domain_marker in path

    class ExcludeDomainsFilter(logging.Filter):
        """排除已分类的领域日志，仅保留未分类日志（如 app/ 目录）"""
        def __init__(self, domain_markers: list[str]):
            self.domain_markers = domain_markers
        
        def filter(self, record):
            path = record.pathname.replace("\\", "/")
            return not any(m in path for m in self.domain_markers)

    DOMAIN_MARKERS = ["spc_domain", "yield_domain", "shared_kernel"]

    try:
        # =========================================================
        #  通道 1：全量流水日志 (按天轮转)
        # =========================================================
        info_log_path = log_dir / f"{base_filename}_info.log"
        info_handler = TimedRotatingFileHandler(
            filename=info_log_path,
            when="midnight",    # 每天午夜零点自动触发切分
            interval=1,         # 间隔 1 天
            backupCount=30,     # 自动清理：最多保留最近 30 天的日志文件
            encoding='utf-8'
        )
        info_handler.setLevel(logging.INFO) # 拦截 INFO 级别
        info_handler.setFormatter(formatter)
        info_handler.addFilter(ExcludeDomainsFilter(DOMAIN_MARKERS))  # 排除已分类的领域日志
        root_logger.addHandler(info_handler)

        # =========================================================
        #  [Phase 2] 通道 1b/1c/1d：按领域自动分流 (按天轮转)
        # =========================================================
        for domain in DOMAIN_MARKERS:
            domain_log_path = log_dir / f"{base_filename}_{domain.split('_')[0]}.log"
            domain_handler = TimedRotatingFileHandler(
                filename=domain_log_path,
                when="midnight",
                interval=1,
                backupCount=30,
                encoding='utf-8'
            )
            domain_handler.setLevel(logging.INFO)
            domain_handler.setFormatter(formatter)
            domain_handler.addFilter(DomainFilter(domain))
            root_logger.addHandler(domain_handler)

        # =========================================================
        #  通道 2：高优报警日志 (按天轮转，专供快速排查)
        # =========================================================
        error_log_path = log_dir / f"{base_filename}_error.log"
        error_handler = TimedRotatingFileHandler(
            filename=error_log_path,
            when="midnight",    # 每天午夜零点自动触发切分
            interval=1,         # 间隔 1 天
            backupCount=90,     # 错误日志往往需要长期追溯，保留 90 天
            encoding='utf-8'
        )
        error_handler.setLevel(logging.WARNING) # 核心隔离：只放行 WARNING, ERROR, CRITICAL
        error_handler.setFormatter(formatter)
        root_logger.addHandler(error_handler)

        # =========================================================
        #  通道 3：调试追踪日志 (按天轮转，短期保留)
        # =========================================================
        trace_logger = logging.getLogger("trace")
        trace_logger.setLevel(logging.DEBUG)
        trace_logger.propagate = False  # 防止 trace 日志重复进入根 Logger 的业务通道

        trace_log_path = log_dir / f"{base_filename}_trace.log"
        trace_handler = TimedRotatingFileHandler(
            filename=trace_log_path,
            when="midnight",    # 每天午夜零点自动触发切分
            interval=1,         # 间隔 1 天
            backupCount=7,      # 探针日志只需短期保留
            encoding='utf-8'
        )
        trace_handler.setLevel(logging.DEBUG)
        trace_handler.setFormatter(formatter)
        trace_logger.addHandler(trace_handler)

    except Exception as e:
        print(f"❌ 严重错误：无法初始化企业级日志 Handler: {e}")

    # 控制台 Handler (供开发者本地实时观测)
    if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    logging.info("✅ 企业级日志系统已启动 (用途 × 领域 二维隔离 | 午夜自动轮转)")
    
    return root_logger
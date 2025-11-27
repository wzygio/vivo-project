# 文件路径: src/vivo_project/utils/create_snapshot.py
import sys
import logging
from pathlib import Path
from datetime import datetime, timedelta

# --- [1. 路径引导] ---
current_file = Path(__file__).resolve()
# 向上找3层：utils -> vivo_project -> src
src_root = current_file.parent.parent.parent
if str(src_root) not in sys.path:
    sys.path.insert(0, str(src_root))

# --- [2. 导入模块] ---
from vivo_project.utils.app_setup import AppSetup
from vivo_project.config import CONFIG
# [核心修改] 直接引用 Repository，不再经过 Service
from vivo_project.infrastructure.repositories.panel_repository import PanelRepository 
from vivo_project.utils.utils import setup_logging

# 初始化日志
setup_logging()

def record_data():
    logging.info("📸 [Snapshot] 开始录制数据快照 (调用 PanelRepository)...")
    
    # 1. 准备查询参数 (通常快照需要覆盖较长的时间范围，例如最近60天或90天)
    end_date_obj = datetime.now()
    start_date_obj = end_date_obj - timedelta(days=90) # 默认拉取90天
    
    start_date_str = start_date_obj.strftime('%Y-%m-%d')
    end_date_str = end_date_obj.strftime('%Y-%m-%d')
    
    # 获取配置中的默认筛选条件 (如果有的话)，否则使用通配符
    # 假设我们需要所有 Product 和所有工单类型
    target_product = "ALL" 
    target_wo_types = [] # 空列表通常代表“全部”
    
    # 2. 实例化仓库
    repo = PanelRepository()
    
    logging.info(f"📡 准备强制拉取数据: {start_date_str} -> {end_date_str}")
    
    try:
        # 3. [核心逻辑] 调用 Repository 的 get_panel_details
        # 关键参数: force_refresh=True
        # 这会迫使 Repository：
        #   a. 跳过读取旧快照
        #   b. 调用 data_loader 去查询数据库
        #   c. 查询成功后，自动覆盖/保存新的快照文件 (这是我们在 PanelRepository 中写好的逻辑)
        df = repo.get_panel_details(
            start_date=start_date_str,
            end_date=end_date_str,
            product_code=target_product,
            work_order_types=target_wo_types,
            force_refresh=True  # <--- 强制刷新，触发“查库+存快照”流程
        )
        
        if not df.empty:
            logging.info(f"✅ 快照更新流程完成。数据行数: {len(df)}")
            # 注意：具体的保存动作已经在 repo.get_panel_details 内部自动完成了
            # 这里只需要确认结果即可
        else:
            logging.warning("⚠️ 数据库返回空数据，快照可能未更新。")
            
    except Exception as e:
        logging.error(f"❌ 快照录制失败: {e}", exc_info=True)

if __name__ == "__main__":
    record_data()
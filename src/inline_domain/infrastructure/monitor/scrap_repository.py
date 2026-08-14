"""Scrap data adapter dedicated to the automatic-warning monitor use case."""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from src.shared_kernel.config import ConfigLoader

# [Phase 1] 调试追踪专用 Logger
trace_logger = logging.getLogger("trace")


class InlineScrapRepository:
    """Load scrap sheets and expose them in the OOC-disguised SPC-compatible format."""

    def get_scrap_data(self, prod_code: str) -> pd.DataFrame:
        """
        [报废数据适配器] 从 resources/scrap_sheets.xlsx 读取报废数据，
        按 prod_code 过滤后转换为与 SPC 管道兼容的格式（OOC 伪装）。

        注意：scrap_sheets.xlsx 是统一文件，内部已包含所有产品的数据，
              通过 '产品型号' 列进行过滤。
        """
        try:
            project_root = ConfigLoader.get_project_root()
            scrap_path = project_root / "resources" / "scrap_sheets.xlsx"
            trace_logger.info(f"🚧 [ScrapTrace][Repo-L1] scrap_path={scrap_path}, exists={scrap_path.exists()}")

            if not scrap_path.exists():
                logging.warning(f"[SpcRepo] 报废数据文件不存在: {scrap_path}")
                return pd.DataFrame()

            # 1. 读取 Excel（尝试多种引擎）
            df = pd.DataFrame()
            engines = ['openpyxl', 'xlrd']
            for engine in engines:
                try:
                    df = pd.read_excel(scrap_path, engine=engine)
                    trace_logger.info(f"🚧 [ScrapTrace][Repo-L2] 使用引擎 {engine} 读取成功, shape={df.shape}, columns={df.columns.tolist()}")
                    break
                except Exception as e:
                    trace_logger.info(f"🚧 [ScrapTrace][Repo-L2] 引擎 {engine} 失败: {e}")
                    continue

            if df.empty:
                logging.info(f"[SpcRepo] 报废数据为空或无法读取: {scrap_path}")
                return pd.DataFrame()

            # 2. 列名标准化（支持中文和英文列名）
            col_mapping = {
                '产品型号': 'prod_code',
                'Sheet_ID': 'sheet_id',
                'sheet_id': 'sheet_id',
                '报废时间': 'sheet_start_time',
                '报废时间(yyyy-mm-dd)': 'sheet_start_time',
                'warehousing_time': 'sheet_start_time',
                '报废站点': 'step_id',
                '报废站点(五位代码)': 'step_id',
                'scrap_step': 'step_id',
            }

            rename_dict = {src: dst for src, dst in col_mapping.items() if src in df.columns}
            trace_logger.info(f"🚧 [ScrapTrace][Repo-L3] rename_dict={rename_dict}")
            if rename_dict:
                df = df.rename(columns=rename_dict)

            # 3. 确保必要列存在
            required_cols = ['prod_code', 'sheet_id', 'sheet_start_time', 'step_id']
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                logging.error(f"[SpcRepo] 报废数据缺少必要列: {missing}，实际列: {df.columns.tolist()}")
                return pd.DataFrame()

            # 4. 按产品型号过滤（统一文件内包含多产品数据）
            df['prod_code'] = df['prod_code'].astype(str).str.strip()
            before_filter = len(df)
            df = df[df['prod_code'].str.upper() == prod_code.upper()].copy()
            trace_logger.info(f"🚧 [ScrapTrace][Repo-L4] 按 prod_code={prod_code} 过滤: {before_filter} -> {len(df)} 条")

            if df.empty:
                logging.info(f"[SpcRepo] 产品 {prod_code} 在报废数据中无记录")
                return pd.DataFrame()

            # 5. 类型转换与清洗
            df['sheet_start_time'] = pd.to_datetime(df['sheet_start_time'], errors='coerce')
            before_dropna = len(df)
            df = df.dropna(subset=['sheet_start_time'])
            trace_logger.info(f"🚧 [ScrapTrace][Repo-L5] dropna 后: {before_dropna} -> {len(df)} 条, 时间样本: {df['sheet_start_time'].head(3).tolist()}")

            # 6. 推断厂别
            df['factory'] = df['step_id'].astype(str).apply(self._infer_factory_from_step)
            trace_logger.info(f"🚧 [ScrapTrace][Repo-L6] 厂别推断: {df['factory'].unique().tolist()}")

            # 7. 状态伪装（伪装成 OOC，使 aggregate_spc_metrics 无感知处理）
            df['is_ooc'] = 1
            df['is_oos'] = 0
            df['is_soos'] = 0
            df['param_name'] = '报废'
            df['site_name'] = '报废'
            df['data_type'] = '报废'
            df['spc_status'] = 'OOC'

            # 8. 添加必要的占位列（与 apply_spc_rules 输出格式兼容）
            for col in ['sheet_mean', 'sheet_max', 'sheet_min', 'usl', 'lsl', 'ucl', 'lcl']:
                if col not in df.columns:
                    df[col] = np.nan

            trace_logger.info(f"🚧 [ScrapTrace][Repo-L7] 最终返回: {len(df)} 条, columns={df.columns.tolist()}")
            return df

        except Exception as e:
            logging.error(f"[SpcRepo] 加载报废数据失败: {e}", exc_info=True)
            return pd.DataFrame()

    @staticmethod
    def _infer_factory_from_step(step_id: str) -> str:
        """
        根据报废站点代码推断厂别。
        优先级：精确映射 > 前缀推断 > UNKNOWN
        """
        try:
            config = ConfigLoader.get_scrap_factory_mapping()
            step = str(step_id).strip().upper()

            # 1. 精确匹配
            mappings = config.get('mappings', {})
            if step in mappings:
                return mappings[step]

            # 2. 前缀推断
            prefix_rules = config.get('default_prefix_rules', {})
            for prefix, factory in prefix_rules.items():
                if step.startswith(str(prefix).upper()):
                    return factory

            return 'UNKNOWN'
        except Exception:
            return 'UNKNOWN'

import pandas as pd
import os, logging, time  # 导入日志模块
import streamlit as st  # 导入 streamlit 库
import yaml
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.shared_kernel.config_model import AppConfig

class ExcelService:
    MAPPING_CONFIG_FILE_NAME = "mapping_config.xlsx"

    @staticmethod
    def load_and_clean_data(file_path: str, sheet_name: str = "Sheet1") -> pd.DataFrame:
        """
        智能加载 Excel：自动寻找表头、清洗空列、填充合并单元格
        [修改] 增加 sheet_name 参数，默认为 'Sheet1'
        """
        if not os.path.exists(file_path):  # 检查文件是否存在
            return pd.DataFrame()  # 如果不存在，返回空的 DataFrame

        try:
            # 1. 智能寻找表头行
            # [修改] 显式指定读取 Sheet1，避免读取到错误的隐藏 Sheet
            df_preview = pd.read_excel(
                file_path, 
                header=None, 
                nrows=10, 
                engine='openpyxl', 
                sheet_name=sheet_name # 显式指定 Sheet
            )
            
            header_row_idx = 0  # 初始化表头行索引为 0
            
            for i, row in df_preview.iterrows():  # 遍历预读取的每一行
                row_str = row.astype(str).values  # 将行数据转换为字符串数组
                # 关键词匹配，只要命中一个即可认为是表头
                if any(k in s for k in ["Issue名称", "Issue描述", "北极星指标", "序号", "No."] for s in row_str):
                    header_row_idx = i  # 记录表头行
                    break
            
            # 2. 正式读取
            # [修改] 显式指定读取 Sheet1
            df = pd.read_excel(
                file_path, 
                header=header_row_idx, # type: ignore
                engine='openpyxl', 
                sheet_name=sheet_name # 显式指定 Sheet
            ) # type: ignore

            # 3. 清洗列名 (去除 Unnamed 空列)
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            
            # 4. 去除全空行
            df.dropna(how='all', inplace=True)

            # 5. 处理合并单元格 (向下填充)
            target_cols = ['Issue名称', '工艺段', '发现方', '型号', '北极星指标']
            for col in target_cols:
                if col in df.columns:
                    df[col] = df[col].ffill()

            # 6. 格式化日期
            if '发生日期' in df.columns:
                # 仅转换为 datetime 对象，严禁转换为字符串
                df['发生日期'] = pd.to_datetime(df['发生日期'], errors='coerce')

            return df

        except ValueError as ve:
            # 专门捕获 Sheet 不存在的错误
            logging.error(f"Excel 读取失败: {ve}")
            st.error(f"读取失败：文件中未找到名为 '{sheet_name}' 的工作表。请检查 Excel 文件。")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Excel 读取失败: {e}")
            st.error(f"无法读取 Excel 文件: {e}")
            return pd.DataFrame()

    @staticmethod
    def highlight_status(val):
        """Pandas Styler 样式函数"""
        if val == 'Open':
            return 'background-color: #ffcdd2; color: #b71c1c; font-weight: bold'
        elif val == 'Close':
            return 'background-color: #c8e6c9; color: #1b5e20; font-weight: bold'
        return ''

    @staticmethod
    def get_file_timestamp(file_path: str) -> float:
        """获取文件的最后修改时间戳"""
        if os.path.exists(file_path):
            return os.path.getmtime(file_path)
        return 0.0

    @staticmethod
    def save_data_with_lock(file_path: str, df: pd.DataFrame, expected_timestamp: float, sheet_name: str = "Sheet1") -> tuple[bool, str]:
        """
        带乐观锁和文件锁的安全保存
        [修改] 增加 sheet_name 参数，默认为 'Sheet1'
        """
        lock_file = file_path + ".lock"
        
        try:
            # 1. 乐观锁检查
            current_timestamp = ExcelService.get_file_timestamp(file_path)
            if current_timestamp != expected_timestamp and expected_timestamp != 0.0:
                return False, "保存失败：数据已过期！\n有同事在您编辑期间提交了新版本。\n请刷新页面获取最新数据后再试。"

            # 2. 获取文件互斥锁
            max_retries = 5
            for _ in range(max_retries):
                if not os.path.exists(lock_file):
                    try:
                        with open(lock_file, 'w') as f:
                            f.write("LOCKED")
                        break
                    except Exception:
                        time.sleep(0.1)
                else:
                    time.sleep(0.1)
            else:
                return False, "系统繁忙：当前文件正在被写入，请稍后重试。"

            # 3. 执行写入
            # [修改] 显式指定写入 Sheet1
            # 注意：这将完全重写文件。如果原文件有其他 Sheet，将会丢失！
            # 如果需要保留其他 Sheet，需要改用 pd.ExcelWriter(mode='a')，但那会更复杂且容易出错。
            # 目前逻辑假设每个文件只服务于这一个台账业务。
            df.to_excel(
                file_path, 
                index=False, 
                sheet_name=sheet_name # 显式写入 Sheet1
            )
            
            return True, "保存成功！"

        except Exception as e:
            logging.error(f"保存 Excel 失败: {e}")
            return False, f"保存发生未知错误: {e}"
        
        finally:
            # 4. 释放锁
            if os.path.exists(lock_file):
                try:
                    os.remove(lock_file)
                except Exception as e:
                    logging.error(f"无法移除锁文件: {e}")

    # ==============================================================================
    #                      Excel 覆盖适配器 (Adapter)
    # ==============================================================================
    @staticmethod
    def _read_override_excel_via_com(
        excel_path: Path,
        sheet_names: tuple[str, str] = ("Group级", "Code级"),
    ) -> Dict[str, pd.DataFrame]:
        """Read the known trend-override sheets through Excel COM."""
        from src.shared_kernel.utils.excel_tools import _read_encrypted_xlsx_via_com

        return {
            sheet_name: _read_encrypted_xlsx_via_com(excel_path, sheet_name=sheet_name)
            for sheet_name in sheet_names
        }

    @staticmethod
    def _parse_override_excel(
        excel_path: Path,
        sheet_names: tuple[str, str] = ("Group级", "Code级"),
    ) -> Dict[str, Dict[str, Dict[str, float]]]:
        """解析双Sheet页的覆盖配置Excel为嵌套字典格式"""
        overrides = {
            'group_monthly_values': {}, 'group_weekly_values': {}, 'group_daily_values': {},
            'code_monthly_values': {}, 'code_weekly_values': {}, 'code_daily_values': {}
        }
        if not excel_path.exists():
            return overrides

        try:
            try:
                xls = pd.read_excel(excel_path, sheet_name=None, engine="openpyxl")
            except Exception as openpyxl_error:
                logging.warning(
                    "读取趋势覆盖 Excel 失败，尝试 Excel COM 解密读取: %s",
                    openpyxl_error,
                )
                xls = ExcelService._read_override_excel_via_com(excel_path, sheet_names)
            
            def _parse_sheet(df, level_prefix):
                if df.empty: return
                for _, row in df.iterrows():
                    target = str(row.get('目标名称', '')).strip()
                    period_cn = str(row.get('周期类型', '')).strip()
                    time_key = str(row.get('时间标签', '')).strip()
                    rate_val = row.get('期望不良率', 0.0)
                    
                    if not target or target == 'nan' or not period_cn or not time_key or pd.isna(rate_val):
                        continue
                        
                    # 智能兼容百分比字符串 (如 "1.03%") 和 小数 (如 0.0103)
                    if isinstance(rate_val, str) and '%' in rate_val:
                        rate_val = float(rate_val.replace('%', '')) / 100.0
                    else:
                        rate_val = float(rate_val)
                        # 防呆设计：如果业务人员手滑输入了 1.5 但没加 %，强制转换为 0.015
                        if rate_val > 1.0: 
                            rate_val = rate_val / 100.0

                    period_map = {'月度': 'monthly', '周度': 'weekly', '日度': 'daily'}
                    period_en = period_map.get(period_cn)
                    if not period_en: continue
                    
                    dict_key = f"{level_prefix}_{period_en}_values"
                    if target not in overrides[dict_key]:
                        overrides[dict_key][target] = {}
                    overrides[dict_key][target][time_key] = rate_val

            if sheet_names[0] in xls:
                _parse_sheet(xls[sheet_names[0]], 'group')
            if sheet_names[1] in xls:
                _parse_sheet(xls[sheet_names[1]], 'code')
                
        except Exception as e:
            logging.error(f"解析覆盖Excel失败: {e}", exc_info=True)
            
        return overrides

    @staticmethod
    def inject_excel_overrides_to_config(config: AppConfig, product_dir: Path):
        """
        [核心] 在数据进入底层运算前，拦截并用 Excel 的数据覆盖 YAML 的配置。
        底层 mwd_trend_processor.py 完全无感知。
        """
        ExcelService.inject_mapping_config_to_config(config)

        override_res = config.paths.get('mwd_override_config')
        if not override_res: 
            return
        
        # 汇总工作簿位于 resources 根目录，按 <产品号>_Group级 / <产品号>_Code级 sheet 区分
        excel_path = product_dir.parent / override_res.file_name
        prod_code = config.data_source.product_code
        excel_overrides = ExcelService._parse_override_excel(
            excel_path,
            sheet_names=(f"{prod_code}_Group级", f"{prod_code}_Code级"),
        )
        
        # 将解析出的字典注入到 config.processing 中，完美替换原有的 YAML 节点
        for key, value_dict in excel_overrides.items():
            if value_dict:  # 如果 Excel 中有配置，则覆盖
                config.processing[key] = value_dict

    @staticmethod
    def parse_mapping_config_excel(
        excel_path: Path,
        product_code: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """解析全产品 Mapping 修饰 Excel 为 mapping_hotspot_script 列表。"""
        if not excel_path.exists():
            return []

        try:
            xls = pd.read_excel(excel_path, sheet_name=None, engine="openpyxl")
            if not xls:
                return []
            df = xls.get("Mapping修饰", next(iter(xls.values())))
        except Exception as e:
            logging.warning(f"openpyxl 解析 Mapping 配置 Excel 失败，尝试 COM 兜底: {e}")
            try:
                df = ExcelService._read_mapping_config_via_com(excel_path)
            except Exception as com_error:
                logging.error(f"解析 Mapping 配置 Excel 失败: {com_error}", exc_info=True)
                return []

        scripts: List[Dict[str, Any]] = []
        grouped_scripts: Dict[tuple, Dict[str, Any]] = {}
        for _, row in df.dropna(how="all").iterrows():
            if not ExcelService._parse_bool(
                ExcelService._get_first_value(row, ["启用", "enable"]),
                default=True,
            ):
                continue

            target_product = ExcelService._normalize_text(
                ExcelService._get_first_value(row, ["产品型号", "产品", "target_product"]),
                default="ALL",
            )
            if product_code and target_product.upper() != "ALL" and target_product != product_code:
                continue

            target_code = ExcelService._normalize_text(
                ExcelService._get_first_value(row, ["Defect Code", "不良代码", "Code", "target_code"]),
                default="ALL",
            )
            target_batch = ExcelService._normalize_text(
                ExcelService._get_first_value(row, ["蒸镀批次", "批次", "target_batch"]),
                default="ALL",
            )
            target_batch = ExcelService._normalize_mapping_batch_text(target_batch)
            mode = ExcelService._normalize_text(
                ExcelService._get_first_value(row, ["修饰模式", "mode"]),
                default="multiplicative",
            ).lower()

            script: Dict[str, Any] = {
                "enable": True,
                "target_product": target_product,
                "target_code": target_code,
                "target_batch": target_batch,
                "mode": mode,
            }

            numeric_fields = {
                "hotspot_multiplier": ["热点倍率", "hotspot_multiplier"],
                "normal_multiplier": ["普通倍率", "normal_multiplier"],
                "hotspot_adder": ["热点加值", "hotspot_adder"],
                "normal_multiplier_in_add_mode": ["加值模式普通倍率", "normal_multiplier_in_add_mode"],
                "random_seed": ["随机种子", "random_seed"],
                "random_variation": ["随机波动", "random_variation"],
            }
            for key, columns in numeric_fields.items():
                value = ExcelService._parse_number(ExcelService._get_first_value(row, columns))
                if value is not None:
                    script[key] = value

            random_method = ExcelService._normalize_text(
                ExcelService._get_first_value(row, ["随机方法", "random_method"]),
                default="",
            )
            if random_method:
                script["random_method"] = random_method

            hotspot_rules = ExcelService._parse_hotspot_rule_fields(
                rule_type=ExcelService._get_first_value(row, ["规则", "rule", "hotspot_rule"]),
                positions=ExcelService._get_first_value(row, ["膜位", "位置", "positions", "hotspot_values"]),
            )
            if not hotspot_rules:
                hotspot_rules = ExcelService._parse_hotspot_rules(
                    ExcelService._get_first_value(row, ["热点规则", "hotspot_rules"])
                )
            if hotspot_rules:
                script["hotspot_rules"] = hotspot_rules

            script_key = ExcelService._mapping_script_key(script)
            if script_key in grouped_scripts:
                existing = grouped_scripts[script_key]
                existing.setdefault("hotspot_rules", [])
                existing["hotspot_rules"].extend(script.get("hotspot_rules", []))
                continue

            grouped_scripts[script_key] = script
            scripts.append(script)

        return scripts

    @staticmethod
    def inject_mapping_config_to_config(
        config: AppConfig,
        excel_path: Optional[Path] = None,
    ) -> None:
        """将全局 Mapping Excel 配置注入到当前产品的 AppConfig。"""
        if excel_path is None:
            excel_path = ExcelService.get_mapping_config_path()
        if not excel_path.exists():
            return

        scripts = ExcelService.parse_mapping_config_excel(
            excel_path,
            product_code=config.data_source.product_code,
        )
        config.processing["mapping_hotspot_script"] = scripts

    @staticmethod
    def get_mapping_config_path() -> Path:
        from src.shared_kernel.config import ConfigLoader

        return ConfigLoader.get_project_root() / "resources" / ExcelService.MAPPING_CONFIG_FILE_NAME

    @staticmethod
    def _read_mapping_config_via_com(excel_path: Path) -> pd.DataFrame:
        from src.shared_kernel.utils.excel_tools import _read_encrypted_xlsx_via_com

        return _read_encrypted_xlsx_via_com(excel_path, sheet_name="Mapping修饰")

    @staticmethod
    def _get_first_value(row: pd.Series, names: List[str]) -> Any:
        for name in names:
            if name in row.index:
                return row.get(name)
        return None

    @staticmethod
    def _is_blank(value: Any) -> bool:
        if value is None:
            return True
        if pd.isna(value):
            return True
        return str(value).strip() == ""

    @staticmethod
    def _normalize_text(value: Any, default: str = "") -> str:
        if ExcelService._is_blank(value):
            return default
        text = str(value).strip()
        if text.lower() == "nan":
            return default
        return text

    @staticmethod
    def _parse_bool(value: Any, default: bool = False) -> bool:
        if ExcelService._is_blank(value):
            return default
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if text in {"true", "1", "yes", "y", "启用", "是"}:
            return True
        if text in {"false", "0", "no", "n", "禁用", "否"}:
            return False
        return default

    @staticmethod
    def _parse_number(value: Any) -> Optional[Any]:
        if ExcelService._is_blank(value):
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        if number.is_integer():
            return int(number)
        return number

    @staticmethod
    def _parse_batch_index(value: Any) -> Optional[Any]:
        if ExcelService._is_blank(value):
            return None
        text = str(value).strip()
        if text.lower() in {"oldest", "latest", "middle", "all"}:
            return text.lower() if text.lower() != "all" else "ALL"
        parts = [part.strip() for part in text.replace("，", ",").split(",") if part.strip()]
        parsed: List[Any] = []
        for part in parts:
            try:
                parsed.append(int(float(part)))
            except ValueError:
                parsed.append(part)
        if len(parsed) == 1:
            return parsed[0]
        return parsed

    @staticmethod
    def _normalize_mapping_batch_text(value: str) -> str:
        if value.upper() == "ALL":
            return "ALL"
        text = value.replace("蒸镀批", "").replace("批次", "").strip()
        return ExcelService._format_mapping_batch_date(text)

    @staticmethod
    def _format_mapping_batch_date(value: str) -> str:
        match = re.fullmatch(r"(\d{2}|\d{4})[/-](\d{1,2})[/-](\d{1,2})", value.strip())
        if not match:
            return value

        year_text, month_text, day_text = match.groups()
        year = int(year_text)
        if len(year_text) == 2:
            year += 2000
        return f"{year:04d}/{int(month_text):02d}/{int(day_text):02d}"

    @staticmethod
    def _parse_hotspot_rules(value: Any) -> List[Dict[str, Any]]:
        if ExcelService._is_blank(value):
            return []
        text = str(value).strip()
        try:
            parsed = yaml.safe_load(text)
            if isinstance(parsed, dict):
                return [parsed]
            if isinstance(parsed, list):
                return parsed
        except Exception:
            logging.debug("热点规则不是 YAML/JSON 结构，尝试按简写语法解析。", exc_info=True)

        return ExcelService._parse_hotspot_rule_shorthand(text)

    @staticmethod
    def _parse_hotspot_rule_fields(rule_type: Any, positions: Any) -> List[Dict[str, Any]]:
        rule_type_text = ExcelService._normalize_text(rule_type, default="").lower()
        if not rule_type_text:
            return []

        values = ExcelService._split_mapping_positions(positions)
        if not values:
            return []

        if rule_type_text == "position":
            position_pairs = []
            for value in values:
                parts = [part.strip() for part in re.split(r"[:：/\-\s]+", value) if part.strip()]
                if len(parts) == 2:
                    position_pairs.append(parts)
            return [{"type": rule_type_text, "value": position_pairs}] if position_pairs else []

        return [{"type": rule_type_text, "value": values}]

    @staticmethod
    def _split_mapping_positions(value: Any) -> List[str]:
        if ExcelService._is_blank(value):
            return []
        return [item.strip() for item in str(value).replace("，", ",").split(",") if item.strip()]

    @staticmethod
    def _mapping_script_key(script: Dict[str, Any]) -> tuple:
        def _freeze(value: Any) -> Any:
            if isinstance(value, dict):
                return tuple(sorted((key, _freeze(item)) for key, item in value.items()))
            if isinstance(value, list):
                return tuple(_freeze(item) for item in value)
            return value

        return tuple(
            sorted(
                (key, _freeze(value))
                for key, value in script.items()
                if key != "hotspot_rules"
            )
        )

    @staticmethod
    def _parse_hotspot_rule_shorthand(text: str) -> List[Dict[str, Any]]:
        rules: List[Dict[str, Any]] = []
        for segment in [part.strip() for part in text.split(";") if part.strip()]:
            if ":" not in segment:
                continue
            rule_type, raw_values = segment.split(":", 1)
            rule_type = rule_type.strip()
            values = [item.strip() for item in raw_values.replace("，", ",").split(",") if item.strip()]
            if rule_type == "position":
                positions = []
                for item in values:
                    parts = [part.strip() for part in item.split(":") if part.strip()]
                    if len(parts) == 2:
                        positions.append(parts)
                rules.append({"type": rule_type, "value": positions})
            else:
                rules.append({"type": rule_type, "value": values})
        return rules

import hashlib
import logging
import re
from typing import Any, Optional

import numpy as np
import pandas as pd


@staticmethod
def apply_hotspot_modification_to_matrix(
    heatmap_matrix: pd.DataFrame,
    batch_no: str,
    code_desc: str,
    batch_position: int,        # [修改] 当前批次在排序列表中的 0-based 位置
    total_batches: int,         # [新增] 批次总数，用于解析负索引
    script_config_list: list,
    product_code: Optional[str] = None,
) -> pd.DataFrame:
    """
    [V4.0 - 产品/Code/批次精确匹配 + 多模式修饰]
    按照"剧本库"(列表)修饰已聚合的Mapping图矩阵。
    新配置支持 target_product / target_code / target_batch，三者均支持 ALL。
    target_batch_index 支持以下格式：
      - 整数: 0=第一个, -1=最后一个, -2=倒数第二个
      - 整数列表: [-1, -2] = 最后两个
      - 字符串(向后兼容): 'oldest'/'latest'/'middle'
    """
    try:
        # --- [核心逻辑 1] 搜索匹配的脚本 ---
        matched_scripts = []
        for script in script_config_list:
            if not _mapping_script_matches(
                script=script,
                product_code=product_code,
                code_desc=code_desc,
                batch_no=batch_no,
                batch_position=batch_position,
                total_batches=total_batches,
            ):
                continue
            matched_scripts.append(script)

        # 如果没有匹配的脚本，则返回原始矩阵
        if not matched_scripts:
            logging.debug(f"未找到 Code '{code_desc}' / 位置({batch_position}/{total_batches}) 的匹配修饰脚本，跳过。")
            return heatmap_matrix

        logging.info(f"为批次 {batch_no} (Code: {code_desc}) 应用匹配的Mapping热点修饰脚本...")

        # --- [核心逻辑 2] 使用匹配到的脚本字典执行操作 ---
        # 1. 加载第一个匹配模式；同一模式的后续脚本可叠加热点规则。
        first_script = matched_scripts[0]
        mode = _normalize_mapping_mode(first_script.get('mode', 'multiplicative'))
        mode_scripts = [
            script for script in matched_scripts
            if _normalize_mapping_mode(script.get('mode', 'multiplicative')) == mode
        ]

        if mode in {'original', 'raw', 'none'}:
            return heatmap_matrix

        if mode == 'random':
            seed = first_script.get('random_seed')
            random_method = first_script.get('random_method', 'poisson')
            random_variation = first_script.get('random_variation', 0.0)
            return _apply_random_mapping_distribution(
                heatmap_matrix=heatmap_matrix,
                product_code=product_code,
                batch_no=batch_no,
                code_desc=code_desc,
                seed=seed,
                method=random_method,
                variation=random_variation,
            )

        # 2. 准备“翻译器” (10行 x 21列) - (保持不变)
        row_name_to_index = {
            '1A': 0, '1B': 1, '1C': 2, '1D': 3, '1E': 4,
            '2A': 5, '2B': 6, '2C': 7, '2D': 8, '2E': 9
        }
        col_name_to_index = {f"{chr(ord('A') + i)}0": i for i in range(21)} # 确认是21列

        # 3. 创建“高发区蒙版” (保持不变)
        def _build_hotspot_mask(rules: list) -> pd.DataFrame:
            hotspot_mask = pd.DataFrame(
                np.full(heatmap_matrix.shape, False),
                index=heatmap_matrix.index,
                columns=heatmap_matrix.columns
            )
            for rule in rules:
                hotspot_type = rule.get('type')
                hotspot_values = rule.get('value', [])
                if hotspot_type == 'row':
                     row_indices = [row_name_to_index.get(name) for name in hotspot_values if name in row_name_to_index]
                     if row_indices: hotspot_mask.iloc[row_indices, :] = True # type: ignore
                elif hotspot_type == 'col':
                     col_indices = [col_name_to_index.get(name) for name in hotspot_values if name in col_name_to_index]
                     if col_indices:
                          # 确保列索引在 DataFrame 范围内
                          valid_col_indices = [idx for idx in col_indices if idx in heatmap_matrix.columns]
                          if valid_col_indices: hotspot_mask.iloc[:, valid_col_indices] = True # type: ignore
                elif hotspot_type == 'position':
                     for pos in hotspot_values:
                          row_idx = row_name_to_index.get(pos[0])
                          col_idx = col_name_to_index.get(pos[1])
                          if row_idx is not None and col_idx is not None and \
                             row_idx in heatmap_matrix.index and col_idx in heatmap_matrix.columns:
                               hotspot_mask.iloc[row_idx, col_idx] = True # 使用 iloc
            return hotspot_mask

        # 5. 根据模式，应用数学逻辑
        if mode == 'additive':
            norm_add = _to_number(first_script.get('normal_multiplier_in_add_mode'), 0)
            logging.info(f"应用“加值”模式: 热点区按配置加值，其他区(+{norm_add})")
            add_matrix = pd.DataFrame(
                np.full(heatmap_matrix.shape, norm_add),
                index=heatmap_matrix.index,
                columns=heatmap_matrix.columns,
                dtype=float,
            )
            assigned_hotspots = pd.DataFrame(
                np.full(heatmap_matrix.shape, False),
                index=heatmap_matrix.index,
                columns=heatmap_matrix.columns,
            )
            for script in mode_scripts:
                hotspot_mask = _build_hotspot_mask(script.get('hotspot_rules', []))
                new_hotspots = hotspot_mask & ~assigned_hotspots
                if new_hotspots.to_numpy().any():
                    hot_add = _to_number(script.get('hotspot_adder'), 0)
                    add_matrix = add_matrix.mask(new_hotspots, hot_add)
                    assigned_hotspots = assigned_hotspots | new_hotspots

            modified_matrix = heatmap_matrix + add_matrix

        else: # 默认为 'multiplicative'
            norm_multi = _to_number(first_script.get('normal_multiplier'), 1.0)
            logging.info(f"应用“倍率”模式: 热点区按配置倍率，其他区(x{norm_multi})")
            multiplier_matrix = pd.DataFrame(
                np.full(heatmap_matrix.shape, norm_multi),
                index=heatmap_matrix.index,
                columns=heatmap_matrix.columns,
                dtype=float,
            )
            assigned_hotspots = pd.DataFrame(
                np.full(heatmap_matrix.shape, False),
                index=heatmap_matrix.index,
                columns=heatmap_matrix.columns,
            )
            for script in mode_scripts:
                hotspot_mask = _build_hotspot_mask(script.get('hotspot_rules', []))
                new_hotspots = hotspot_mask & ~assigned_hotspots
                if new_hotspots.to_numpy().any():
                    hot_multi = _to_number(script.get('hotspot_multiplier'), 1.0)
                    multiplier_matrix = multiplier_matrix.mask(new_hotspots, hot_multi)
                    assigned_hotspots = assigned_hotspots | new_hotspots
            modified_matrix = heatmap_matrix * multiplier_matrix

        # 6. 确保结果为非负整数 (保持不变)
        return modified_matrix.astype(int).clip(lower=0)

    except Exception as e:
        logging.error(f"在应用Mapping矩阵修饰时发生错误: {e}", exc_info=True)
        return heatmap_matrix # 出错时返回原始矩阵


def _normalize_mapping_mode(mode: Any) -> str:
    mode_text = str(mode or 'multiplicative').strip().lower()
    if mode_text == 'addtive':
        return 'additive'
    return mode_text


def _to_number(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _mapping_script_matches(
    script: dict[str, Any],
    product_code: Optional[str],
    code_desc: str,
    batch_no: str,
    batch_position: int,
    total_batches: int,
) -> bool:
    if not script.get('enable', False):
        return False

    if not _matches_target(script.get('target_product'), product_code):
        return False
    if not _matches_target(script.get('target_code'), code_desc):
        return False

    target_batch = script.get('target_batch', script.get('target_batches'))
    batch_matches = True
    if target_batch is not None:
        batch_matches = _matches_batch_target(target_batch, batch_no)

    target_batch_index = script.get('target_batch_index')
    index_matches = True
    if target_batch_index is not None:
        index_matches = _matches_batch_index(target_batch_index, batch_position, total_batches)

    return batch_matches and index_matches


def _matches_target(target: Any, actual: Optional[str]) -> bool:
    if target is None:
        return True
    if isinstance(target, list):
        return any(_matches_target(item, actual) for item in target)
    target_text = str(target).strip()
    if target_text.upper() == 'ALL':
        return True
    if actual is None:
        return False
    return target_text == str(actual).strip()


def _normalize_batch_text(value: Any) -> str:
    text = str(value).strip().upper()
    text = text.replace('批次', '').replace('蒸镀批', '')
    date_match = re.search(r'(\d{2}|\d{4})[/-](\d{1,2})[/-](\d{1,2})', text)
    if date_match:
        year_text, month_text, day_text = date_match.groups()
        year = int(year_text)
        if len(year_text) == 2:
            year += 2000
        return f'{year:04d}{int(month_text):02d}{int(day_text):02d}'
    text = re.sub(r'[\s/_\-]+', '', text)
    return text


def _matches_batch_target(target: Any, batch_no: str) -> bool:
    if isinstance(target, list):
        return any(_matches_batch_target(item, batch_no) for item in target)
    target_text = str(target).strip()
    if target_text.upper() == 'ALL':
        return True

    actual_text = str(batch_no).strip()
    if target_text == actual_text:
        return True

    normalized_target = _normalize_batch_text(target_text)
    normalized_actual = _normalize_batch_text(actual_text)
    if not normalized_target or not normalized_actual:
        return False
    return normalized_target == normalized_actual or normalized_target in normalized_actual


def _matches_batch_index(target_idx: Any, batch_position: int, total_batches: int) -> bool:
    if target_idx is None:
        return True
    if isinstance(target_idx, int):
        normalized = target_idx if target_idx >= 0 else total_batches + target_idx
        return normalized == batch_position
    if isinstance(target_idx, list):
        return any(_matches_batch_index(item, batch_position, total_batches) for item in target_idx)
    if isinstance(target_idx, str):
        normalized_idx = target_idx.strip().lower()
        if normalized_idx == 'all':
            return True
        if normalized_idx == 'oldest':
            return batch_position == 0
        if normalized_idx == 'latest':
            return batch_position == total_batches - 1
        if normalized_idx == 'middle':
            return 0 < batch_position < total_batches - 1
    return False


def _stable_mapping_seed(*parts: Any) -> int:
    seed_text = '|'.join(str(part) for part in parts)
    digest = hashlib.sha256(seed_text.encode('utf-8')).hexdigest()
    return int(digest[:16], 16) % (2**32 - 1)


def _apply_random_mapping_distribution(
    heatmap_matrix: pd.DataFrame,
    product_code: Optional[str],
    batch_no: str,
    code_desc: str,
    seed: Any = None,
    method: Any = 'poisson',
    variation: Any = 0.0,
) -> pd.DataFrame:
    total_defects = int(np.nansum(heatmap_matrix.to_numpy()))
    if total_defects <= 0:
        return heatmap_matrix.astype(int).clip(lower=0)

    row_count, col_count = heatmap_matrix.shape
    cell_count = row_count * col_count
    rng_seed = _stable_mapping_seed(product_code or 'ALL', batch_no, code_desc, seed or 'random')
    rng = np.random.default_rng(rng_seed)

    method_text = str(method or 'poisson').strip().lower()
    if method_text in {'even', 'balanced'}:
        base_count, remainder = divmod(total_defects, cell_count)
        values = np.full(cell_count, base_count, dtype=int)
        if remainder > 0:
            chosen_cells = rng.choice(cell_count, size=remainder, replace=False)
            values[chosen_cells] += 1
    else:
        probabilities = np.full(cell_count, 1.0 / cell_count)
        try:
            variation_value = max(0.0, float(variation or 0.0))
        except (TypeError, ValueError):
            variation_value = 0.0

        if variation_value > 0:
            shape = 1.0 / (variation_value ** 2)
            scale = variation_value ** 2
            weights = rng.gamma(shape=shape, scale=scale, size=cell_count)
            probabilities = weights / weights.sum()

        values = rng.multinomial(total_defects, probabilities)

    randomized = values.reshape((row_count, col_count))
    return pd.DataFrame(randomized, index=heatmap_matrix.index, columns=heatmap_matrix.columns)

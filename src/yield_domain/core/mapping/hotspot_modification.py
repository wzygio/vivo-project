import hashlib
import logging
import re
from dataclasses import dataclass
from typing import Any, Optional

import numpy as np
import pandas as pd

from src.yield_domain.core.mapping.layout import resolve_mapping_layout


_LINE_HOTSPOT_RANDOM_MAX = 2
DEFAULT_POSITION_MODIFICATION_MODE = 'deterministic_position'


@dataclass(frozen=True)
class MappingModificationPlan:
    mode: str
    scripts: tuple[dict[str, Any], ...] = ()

    @property
    def applies_default_position_modification(self) -> bool:
        return self.mode == DEFAULT_POSITION_MODIFICATION_MODE


@staticmethod
def apply_hotspot_modification_to_matrix(
    heatmap_matrix: pd.DataFrame,
    batch_no: str,
    code_desc: str,
    batch_position: int,        # [修改] 当前批次在排序列表中的 0-based 位置
    total_batches: int,         # [新增] 批次总数，用于解析负索引
    script_config_list: list,
    product_code: Optional[str] = None,
    mapping_layout: Optional[dict[str, Any]] = None,
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
        plan = resolve_mapping_modification_plan(
            script_config_list=script_config_list,
            product_code=product_code,
            code_desc=code_desc,
            batch_no=batch_no,
            batch_position=batch_position,
            total_batches=total_batches,
        )

        if plan.applies_default_position_modification:
            logging.debug(
                f"Code '{code_desc}' / 位置({batch_position}/{total_batches}) "
                "使用默认确定性坐标偏移，无需矩阵修饰。"
            )
            return heatmap_matrix

        logging.info(f"为批次 {batch_no} (Code: {code_desc}) 应用匹配的Mapping热点修饰脚本...")

        # --- [核心逻辑 2] 使用匹配到的脚本字典执行操作 ---
        # 1. 修饰方案已锁定唯一模式，并仅保留最高优先级同层的同模式脚本。
        first_script = plan.scripts[0]
        mode = plan.mode
        mode_scripts = list(plan.scripts)

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

        # 2. 使用产品 Mapping 布局建立坐标翻译器
        layout = resolve_mapping_layout(mapping_layout)
        row_name_to_index = {
            label: index
            for index, label in enumerate(layout.row_labels)
        }
        col_name_to_index = {
            label: index
            for index, label in enumerate(layout.column_labels)
        }

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

        line_hotspot_mask = pd.DataFrame(
            np.full(heatmap_matrix.shape, False),
            index=heatmap_matrix.index,
            columns=heatmap_matrix.columns,
        )

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
                hotspot_rules = script.get('hotspot_rules', [])
                hotspot_mask = _build_hotspot_mask(hotspot_rules)
                new_hotspots = hotspot_mask & ~assigned_hotspots
                if new_hotspots.to_numpy().any():
                    hot_add = _to_number(script.get('hotspot_adder'), 0)
                    add_matrix = add_matrix.mask(new_hotspots, hot_add)
                    line_rules = [
                        rule for rule in hotspot_rules
                        if rule.get('type') in {'row', 'col'}
                    ]
                    line_hotspot_mask = line_hotspot_mask | (
                        _build_hotspot_mask(line_rules) & new_hotspots
                    )
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
                hotspot_rules = script.get('hotspot_rules', [])
                hotspot_mask = _build_hotspot_mask(hotspot_rules)
                new_hotspots = hotspot_mask & ~assigned_hotspots
                if new_hotspots.to_numpy().any():
                    hot_multi = _to_number(script.get('hotspot_multiplier'), 1.0)
                    multiplier_matrix = multiplier_matrix.mask(new_hotspots, hot_multi)
                    line_rules = [
                        rule for rule in hotspot_rules
                        if rule.get('type') in {'row', 'col'}
                    ]
                    line_hotspot_mask = line_hotspot_mask | (
                        _build_hotspot_mask(line_rules) & new_hotspots
                    )
                    assigned_hotspots = assigned_hotspots | new_hotspots
            modified_matrix = heatmap_matrix * multiplier_matrix

        modified_matrix = _add_line_hotspot_random_perturbation(
            modified_matrix=modified_matrix,
            line_hotspot_mask=line_hotspot_mask,
            product_code=product_code,
            batch_no=batch_no,
            code_desc=code_desc,
            mode=mode,
            seed=first_script.get('random_seed'),
        )

        # 6. 确保结果为非负整数 (保持不变)
        return modified_matrix.astype(int).clip(lower=0)

    except Exception as e:
        logging.error(f"在应用Mapping矩阵修饰时发生错误: {e}", exc_info=True)
        return heatmap_matrix # 出错时返回原始矩阵


def _normalize_mapping_mode(mode: Any) -> str:
    mode_text = str(mode or 'multiplicative').strip().lower()
    if mode_text == 'addtive':
        return 'additive'
    if mode_text in {
        'default',
        'deterministic',
        'deterministic_position',
        'position_offset',
    }:
        return DEFAULT_POSITION_MODIFICATION_MODE
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


def resolve_mapping_modification_plan(
    script_config_list: list,
    product_code: Optional[str],
    code_desc: str,
    batch_no: str,
    batch_position: int,
    total_batches: int,
) -> MappingModificationPlan:
    matched_scripts = _get_ordered_matching_mapping_scripts(
        script_config_list=script_config_list,
        product_code=product_code,
        code_desc=code_desc,
        batch_no=batch_no,
        batch_position=batch_position,
        total_batches=total_batches,
    )
    if not matched_scripts:
        return MappingModificationPlan(mode=DEFAULT_POSITION_MODIFICATION_MODE)

    highest_priority = _mapping_script_priority(matched_scripts[0])
    highest_priority_scripts = [
        script
        for script in matched_scripts
        if _mapping_script_priority(script) == highest_priority
    ]
    mode = _normalize_mapping_mode(
        highest_priority_scripts[0].get('mode', 'multiplicative')
    )
    mode_scripts = tuple(
        script
        for script in highest_priority_scripts
        if _normalize_mapping_mode(script.get('mode', 'multiplicative')) == mode
    )
    return MappingModificationPlan(mode=mode, scripts=mode_scripts)


def _get_ordered_matching_mapping_scripts(
    script_config_list: list,
    product_code: Optional[str],
    code_desc: str,
    batch_no: str,
    batch_position: int,
    total_batches: int,
) -> list:
    matched_scripts = [
        script
        for script in script_config_list
        if _mapping_script_matches(
            script=script,
            product_code=product_code,
            code_desc=code_desc,
            batch_no=batch_no,
            batch_position=batch_position,
            total_batches=total_batches,
        )
    ]
    matched_scripts.sort(key=_mapping_script_priority, reverse=True)
    return matched_scripts


def _mapping_script_priority(
    script: dict[str, Any],
) -> tuple[int, int, int, int, int]:
    product_specificity = _mapping_target_specificity(
        script.get('target_product')
    )
    code_specificity = _mapping_target_specificity(script.get('target_code'))
    target_batch = script.get('target_batch', script.get('target_batches'))
    batch_specificity = _mapping_target_specificity(target_batch)
    index_specificity = _mapping_target_specificity(
        script.get('target_batch_index')
    )
    return (
        product_specificity
        + code_specificity
        + batch_specificity
        + index_specificity,
        batch_specificity,
        index_specificity,
        product_specificity,
        code_specificity,
    )


def _mapping_target_specificity(target: Any) -> int:
    if target is None:
        return 0
    if isinstance(target, (list, tuple, set)):
        if not target or any(_mapping_target_specificity(item) == 0 for item in target):
            return 0
        return 1
    return 0 if str(target).strip().upper() == 'ALL' else 1


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


def _add_line_hotspot_random_perturbation(
    modified_matrix: pd.DataFrame,
    line_hotspot_mask: pd.DataFrame,
    product_code: Optional[str],
    batch_no: str,
    code_desc: str,
    mode: str,
    seed: Any = None,
) -> pd.DataFrame:
    perturbable_mask = line_hotspot_mask & (modified_matrix > 0)
    if not perturbable_mask.to_numpy().any():
        return modified_matrix

    seed_part = seed if seed is not None else 'line-hotspot'
    rng_seed = _stable_mapping_seed(
        product_code or 'ALL',
        batch_no,
        code_desc,
        mode,
        seed_part,
    )
    rng = np.random.default_rng(rng_seed)
    random_additions = rng.integers(
        0,
        _LINE_HOTSPOT_RANDOM_MAX + 1,
        size=modified_matrix.shape,
    )
    perturbation_matrix = pd.DataFrame(
        np.where(perturbable_mask, random_additions, 0),
        index=modified_matrix.index,
        columns=modified_matrix.columns,
    )
    return modified_matrix + perturbation_matrix


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

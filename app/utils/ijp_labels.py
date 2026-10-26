"""Business CODE annotations shared by IJP selectors, charts and alerts."""

from src.shared_kernel.config import ConfigLoader


def get_ijp_code_descriptions(region: str) -> dict[str, str]:
    section = 'ijp_hole' if region == '孔区' else 'ijp'
    return dict(ConfigLoader.load_domain_config('indicator_domain').get(
        section, {},
    ).get('code_descriptions', {}))


def format_ijp_code(code: str, descriptions: dict[str, str]) -> str:
    description = descriptions.get(code, '').strip()
    return f'{code}：{description}' if description else code

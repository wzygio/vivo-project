from importlib import import_module


def test_indicator_domain_exposes_qtime_and_ijp_as_peer_submodules() -> None:
    module_names = (
        "src.indicator_domain.application.qtime.service",
        "src.indicator_domain.application.ijp.service",
        "src.indicator_domain.core.qtime.shop",
        "src.indicator_domain.core.ijp.overflow",
        "src.indicator_domain.infrastructure.qtime.repository",
        "src.indicator_domain.infrastructure.ijp.repository",
    )

    imported = [import_module(module_name) for module_name in module_names]

    assert all(module is not None for module in imported)

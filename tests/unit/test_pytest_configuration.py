from pathlib import Path
import tomllib


def test_pytest_configuration_exposes_src_and_repository_root() -> None:
    with Path("pyproject.toml").open("rb") as config_file:
        config = tomllib.load(config_file)

    pytest_config = config["tool"]["pytest"]["ini_options"]

    assert pytest_config["pythonpath"] == ["src", "."]

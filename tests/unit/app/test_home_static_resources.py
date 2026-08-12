from pathlib import Path


APP_DIR = Path(__file__).parents[3] / "app"


def test_home_static_bundle_is_owned_by_app() -> None:
    static_dir = APP_DIR / "static"

    assert {path.name for path in static_dir.iterdir() if path.is_file()} == {
        "config.js",
        "index.html",
        "script.js",
        "style.css",
    }
    assert not (APP_DIR.parent / "resources" / "static").exists()

# Project Commands

## Smoke

```powershell
# Explicit fast scopes
uv run --no-sync python tools/smoke.py spc
uv run --no-sync python tools/smoke.py yield
uv run --no-sync python tools/smoke.py equipment

# Conservative complete-unit default
uv run --no-sync python tools/smoke.py
```

Fast scopes select existing tests by domain and print the resolved files. They
are a feedback optimization, not a substitute for the complete suite.

## Complete validation

```powershell
uv run pytest tests/unit/ -v --tb=short
uv run pytest tests/ -v --tb=short
```

## Manual diagnostics

人工诊断脚本不参与 pytest 收集，统一放在 `diagnostics/`。使用
`uv run python references/test_references/tools/diagnostics/<script>.py` 运行；
Streamlit 脚本则使用 `uv run streamlit run <script>.py`。

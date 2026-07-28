# 人工诊断脚本

这里存放需要真实环境、数据库连接或 Streamlit 交互才能执行的诊断脚本；它们不属于 pytest 自动化测试，因此不应放在 `tests/` 中。

从仓库根目录执行 Python 诊断脚本：

```powershell
uv run python references/test_references/tools/diagnostics/diagnose_config.py
```

运行 Streamlit 脚本时，将文件名替换为对应脚本：

```powershell
uv run streamlit run references/test_references/tools/diagnostics/diagnose_streamlit_env.py
```

`_bootstrap.py` 为这些脚本统一定位仓库根目录并加入 `src/`，不要在各脚本中再次硬编码 `sys.path`。

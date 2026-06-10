# 计划：PNL 指标收严分析

## 目标
读取 `resources/project_files` 下的 PNL 指标规格版本，输出版本间和产品间收严规格统计、明细、绘图配置和成果图。

## 涉及文件清单
- `src/spc_domain/core/indicator_spec_comparison.py`: 规格解析与收严比较纯业务逻辑。
- `src/spc_domain/application/indicator_improvement_service.py`: Excel 读取、产品版本映射、结果表和图片输出编排。
- `src/spc_domain/application/indicator_improvement_cli.py`: 可重复运行的任务入口。
- `tests/unit/test_indicator_spec_comparison.py`: 规格解析与比较单元测试。
- `output/task-Indicator_Improvement/`: 任务指定输出目录。

## 执行步骤
1. [x] 识别加密 Excel 并通过 COM fallback 读取。
2. [x] 按 `20251127 -> M678`、`20260205 -> M626` 处理早期通用 sheet。
3. [x] 解析可比较监控规格，复杂不可比规格不计入收严。
4. [x] 输出 Task1/Task2 数据表、Task3 绘图配置表和 PNG 成果图。

## 验收标准
- `uv run pytest tests/unit/test_indicator_spec_comparison.py -q` 通过。
- `uv run python -m src.spc_domain.application.indicator_improvement_cli` 成功生成指定目录产物。
- `indicator_improvement_results.xlsx` 可被 openpyxl 读取。

## 回滚指南
删除本计划涉及的新增源码、脚本、测试文件，并删除 `output/task-Indicator_Improvement` 下本次生成产物即可回退。

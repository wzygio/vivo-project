---
name: refresh-cpm-warning-counts
description: 从项目 SPC 台账按产品重算 CPM 周度预警项目数，再汇总到月、季、年，并按既有总项目数重算达标项目数和达标率。用于北极星 CPM 汇总表的本地维护。
---

# 刷新 CPM 预警项目数

在项目根目录运行。脚本使用项目配置定位两份工作簿，在 Windows 上通过 Excel 原生接口逐格写回。

```powershell
.venv/Scripts/python.exe tools/refresh-cpm-warning-counts/scripts/refresh.py
.venv/Scripts/python.exe tools/refresh-cpm-warning-counts/scripts/refresh.py --apply
```

第一条命令预览。用户已要求写回时，执行第二条命令。可用 `--as-of YYYY-MM-DD` 指定统计日，默认取上海日期。用 `--ledger` 和 `--summary` 指定测试副本。

- 按产品读取 `<产品>_cpm`，只统计已结束 ISO 周中按业务键去重的 `flag=False` 项目。可读台账中缺少对应 Sheet 时，预警数为零。
- 周度归入周日所在自然月，再由月汇总至季、年。有预警但汇总表缺少对应周期行时，脚本会指出缺行并停止写回。
- 按各行既有总项目数减预警项目数得出达标项目数，再以达标数除以总数计算达标率。总项目数为零时，达标率取 100%。预警数超出总数时停止写回，指出产品和周期。
- 仅写入有变化的预警数、达标数和达标率单元格。脚本在副本中验证数值及其余单元格，再替换正式文件并保留一份备份。核对输出的 `changed_fields`、`weekly_after` 和 `applied`。

时间归属见 [CPK/CPM 汇总规则](../../references/domain/inline_domain/monitor/rules-cpk-summary-workbook.md)。

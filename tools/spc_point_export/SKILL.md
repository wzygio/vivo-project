---
name: spc-point-export
description: 在当前 vivo-project 中导出 SPC 应用层最终绘图点位明细，每个产品独立输出 XLSX；也可复用此前导出的点位重新生成文件。
---

# SPC 点位明细按产品导出

仅供本项目使用，复用项目 `.venv`、配置和应用代码，无需安装到全局 skills。

## 执行

1. 在项目根目录读取 `CONTEXT.md`，确认用户的日期和产品范围。
2. 默认选择所有已启用产品，日期为上海时区的上个月一日至当天。先检查参数：

   ```powershell
   tools\spc_point_export\run.bat --check
   ```

3. 执行导出。每个产品独立生成一份 XLSX，包含全部点位字段和导出说明；空产品保留空表。

   ```powershell
   tools\spc_point_export\run.bat
   ```

   按需指定产品、日期和新的输出目录：

   ```powershell
   tools\spc_point_export\run.bat --product M626 --product M678 --start-date 2026-10-01 --end-date 2026-11-10 --output-dir output/reports/spc_selected
   ```

   同等 Python 入口：`uv run --no-sync python -m tools.spc_point_export`。
   全部参数以 `run.bat --help` 为准。

4. 退出码为 0 且输出 `manifest.json` 的 `status` 为 `success` 后交付。提供每个产品的 XLSX 链接、行数、大小和空表情况；部分失败时说明已完成产品及失败原因。

## 数据口径

- 取 `SpcRepository` 的点位准备结果，经 `prepare_decorated_data(persist=False)` 输出 `raw_measurements_df`。保留日期前推、当日截止、厂别/日期排除、最新点去重、参数过滤、主制程追溯、规格覆盖、传统 OOS 与 SPC 专用点位修饰。CPK/CPM 数值修饰不改变点位，导出无需执行能力发布。
- 全部历史点位用于月周日图；Sheet 特征仍按现有服务限定为上周一起。导出保留用户选择的完整点位窗口，不缩短到后两幅图的近期窗口。
- 从配置获取已启用产品和资源位置；保持数据字段的原名、顺序和数值。每个产品始终是一份文件；超过 Excel 单表行数时在该产品文件内分表。
- 优先使用经过版本、覆盖、文件代际和 TTL 校验的现有源快照。缺失、过期或覆盖不足时，只读查询本次所需的源时间窗口，在内存执行既有源偏差校正与仓储输出转换；查询失败即停止，不退回过期数据。本工具不维护源快照。
- 规格、参数目录及主制程履历通过只读事务读取。先查数据库结构和视图底层来源，再为事实查询限制时间、产品和材料范围，为各参考来源添加提取时刻之前（或时间为空）的有效性条件。结构变化时失败，不绕过检查。SQL 超时为 180 秒，不使用临时表。
- 导出关闭 OOS/OOC、CPK/CPM、成员关系和过货台账的附带写入。此工具是操作者明确要求的完整字段导出，不加入普通报表页面的下载入口。

## 复用已提取数据

先前导出目录含 `manifest.json` 和 `points/<产品>.parquet`，可直接重建分产品文件：

```powershell
tools\spc_point_export\run.bat --from-export output/reports/spc_selected --product M626
```

此分支读取保存的最终点位，继承原日期及截止口径，不查询数据库、不重新修饰、不改写日期标签。也兼容此前临时任务的 `manifest.json` 加同目录 `<产品>.parquet` 布局。仅允许选择当前仍启用且在原导出中存在的产品。

## 产物与校验

- 默认输出：`output/reports/spc_point_export/<开始日>_<结束日>/<唯一运行目录>/`；已有目录拒绝覆盖。
- 每个产品为 `SPC_<产品>_<开始日>_<结束日>.xlsx`。`points/` 保存可复用的最终点位；`manifest.json` 记录运行状态、字段、数量、文件及校验摘要。
- 大表逐行写入；写盘前在内存验证 XLSX 压缩包 CRC、所有数据行的序号和数量、完整表头及各表首尾记录的全部字段。字符串按文本写入，保留前导零，避免被解释为公式或链接。
- 企业文件保护可能使落盘后的 XLSX 无法被普通 ZIP/openpyxl 直接打开。因此校验在加密前完成；交付文件通过公司环境的 Excel 使用，无需再打开整个大文件进行 COM 转换。
- 输入的异常点过滤工作簿通过项目已有只读 Excel 适配器读取，沿用原过滤算法；无需另存工作簿进行解密。如果 Excel 正处于单元格编辑或弹窗状态并拒绝自动化调用，先提示操作者完成编辑/关闭弹窗，再重新执行；保留用户已有的 Excel 窗口。
- 临时工作簿文件位于 `output/tmp/spc_point_export/`，正常完成、异常和键盘中断时由上下文清理。已完成产品和提取点位保留，失败状态不会标成成功。强制终止进程后，检查本次临时目录及 `manifest.json` 后再重试；使用新的输出目录。

## 维护验证

```powershell
.venv\Scripts\python.exe -m pytest tests/unit/test_spc_point_export.py -q
tools\spc_point_export\run.bat --check
```

修改数据库来源、时间语义或修饰流程时，同时检查本目录 `source.py` 与 SPC 应用链路。测试和样本导出不等同于本次全产品实时取数验收。

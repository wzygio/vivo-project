---
name: spc-point-export
description: 导出当前项目 SPC 所有点位修饰完成后的完整点位明细，支持精确起止时间，每个产品独立 XLSX；不导出箱线统计、能力指标或图表。
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

   精确到时间（上海时间；开始与结束边界均包含）：

   ```powershell
   tools\spc_point_export\run.bat --start-time "2026-11-11 12:00:00" --end-time "2026-11-16 12:00:00"
   ```

   时间筛选在全部点位修饰完成后执行；当天的结束时间仍服从全局报表截止配置。

   同等 Python 入口：`uv run --no-sync python -m tools.spc_point_export`。
   全部参数以 `run.bat --help` 为准。

4. 退出码为 0 且输出 `manifest.json` 的 `status` 为 `success` 后交付。提供每个产品的 XLSX 链接、行数、大小和空表情况；部分失败时说明已完成产品及失败原因。

## 数据口径

- **出口是最终点位，不是最终图表**：取 `SpcRepository` 的点位准备结果，经 `prepare_decorated_data(scope="spc", persist=False)` 输出 `raw_measurements_df`。保留日期前推、当日截止、厂别/日期排除、最新点去重、参数过滤、主制程追溯、规格覆盖、传统 OOS 与 SPC 专用点位修饰。
- 与报表同源的调用链：`core/spc/spc_point_decoration.py:apply_spc_point_decoration` → `application/shared/decorated_data.py:prepare_decorated_data` → `decorated_features.py` → `spc_service.py` → 报表 `raw_measurements_df`。后两层当前原样传递最终点位；skill 直接调用同一共享修饰函数，避免复制点位修饰公式。默认包含全部已启用产品及全部厂别/站点/参数，不套用页面当前的交互筛选。
- 核查日期为 2026-11-16：有效 `param_value` 的最后一次数值修饰是传统 OOS 修饰之后的 SPC 中央规格区间修饰。后续绘图层不再对有效点位进行缩放、裁剪、四舍五入回写或抽样。悬浮文字 `.4f` 只是显示格式，导出保留底层数值。
- 全部历史点位用于月周日图；Sheet 特征仍按现有服务限定为上周一起。导出保留用户选择的完整点位窗口，不缩短到后两幅图的近期窗口。
- 点位准备起点沿用报表的上个月一日（若请求更早则使用更早的起点），以保留相同的最新点选择和设备／腔室履历追溯上下文。准备和点位修饰完成后，才按用户指定的精确时间截取；额外上下文不会写入交付的点位表，也不因此生成图表或能力结果。
- 从配置获取已启用产品和资源位置；保持数据字段的原名、顺序和数值。每个产品始终是一份文件；超过 Excel 单表行数时在该产品文件内分表。
- 优先使用经过版本、覆盖、文件代际和 TTL 校验的现有源快照。缺失、过期或覆盖不足时，只读查询本次所需的源时间窗口，在内存执行既有源偏差校正与仓储输出转换；查询失败即停止，不退回过期数据。本工具不维护源快照。
- 规格、参数目录及主制程履历通过只读事务读取。先查数据库结构和视图底层来源，再为事实查询限制时间、产品和材料范围，为各参考来源添加提取时刻之前（或时间为空）的有效性条件。结构变化时失败，不绕过检查。SQL 超时为 180 秒，不使用临时表。
- 导出关闭 OOS/OOC、CPK/CPM、成员关系和过货台账的附带写入。此工具是操作者明确要求的完整字段导出，不加入普通报表页面的下载入口。

### 点位出口之后的处理（告知，不纳入 skill）

- CPK/CPM 的计算、台账修饰和能力豁免：改变能力指标或其计算集合，不回写点位的 `param_value`。
- 页面厂别/站点/参数选择、后两幅图的近期时间窗口、无效数值/标识剔除：决定显示哪些记录，不是新一轮有效点位数值修饰。
- 月周日展开、可选 Sheet 均值、箱线图统计、排序、分组、轴标签、规格线、纵轴范围、颜色与悬浮文字格式：是聚合或展示处理；不导出其结果，不生成 Figure、图表 JSON 或图像。

永久边界约束由 [CONTEXT.md：SPC Final Point Data Boundary](../../CONTEXT.md#spc-final-point-data-boundary) 统一维护。修改 SPC 后端、前端或导出时先遵循该约束，保持页面和 skill 消费同一最终点位出口；新的点位修饰必须放在该出口之前。此 skill 只交付最终点位，不混入后续指标或图表结果；另一系统的箱线算法、图形布局和样式仍由其绘图实现决定。

## 复用已提取数据

先前导出目录含 `manifest.json` 和 `points/<产品>.parquet`，可直接重建分产品文件：

```powershell
tools\spc_point_export\run.bat --from-export output/reports/spc_selected --product M626
```

此分支读取保存的最终点位，继承原日期、精确时间及截止口径，不查询数据库、不重新修饰、不改写时间标签。也兼容此前临时任务的 `manifest.json` 加同目录 `<产品>.parquet` 布局。仅允许选择当前仍启用且在原导出中存在的产品。

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

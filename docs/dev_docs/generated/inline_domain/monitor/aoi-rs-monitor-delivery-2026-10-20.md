# AOI_RS 监控看板交付证据

日期：2026-10-20。运行模式：development-flow general。原始需求为 `docs/dev_docs/dev_spec/inline_domain/monitor/feat-AOI_TT_center.md`，正文 AOI_RS 是本次范围。

## 已实现行为

超规预警看板新增独立 AOI_RS 模块。默认 ARRAY、全部启用产品、全部站点/Code、当前年已开始的半年/季度/月，以及包含本周的最近5个ISO周。首次查询建立站点/Code选项，随后可多选；查询条件变化收起旧结果。每产品一张原生 st.dataframe，每产品一个 Excel Sheet，导出与公开表格一致。

密度按整段分子和整段去重过货分母计算；有过货无缺陷为0，无过货为缺失值。复用已有显示时间/日期排除/截止与 AOI_RS规则，通过独立无L1适配器按产品、厂别、月份读取。存储为独立可配置的周期汇总工作簿，含成功空覆盖，失败不发布；已闭合周期复用，当前受影响周期更新。政策、决策、规格和历史身份进入相应失效签名。

## 最终专项回归

命令：

```powershell
.venv/Scripts/python.exe -m pytest tests/architecture tests/unit/inline_domain/core/monitor/test_aoi_rs_periods.py tests/unit/inline_domain/core/aoi_rs tests/unit/inline_domain/application/aoi_rs tests/unit/inline_domain/infrastructure/aoi_rs tests/unit/inline_domain/application/monitor/test_aoi_rs_monitor_service.py tests/unit/inline_domain/infrastructure/monitor/test_aoi_rs_monitor_source.py tests/unit/app/pages/test_auto_warning_page.py tests/unit/app/pages/test_warning_page_split.py tests/e2e/test_aoi_rs_monitor_apptest.py -q --tb=short
```

结果：**216 passed，19 warnings，33.23秒**。现有告警投影中的 pandas dtype警告保留，本次新历史拼接的警告已处理。日志：`output/test-results/aoi-rs-monitor/focused-regression.txt`。

覆盖：跨年ISO周、半年季度月边界、整段分母去重、零缺陷/零分母、旧报表轴兼容、严格SQL失败、日期前推/排除/截止、成功空覆盖复用、闭合月份不误刷、当前月依赖周更新及窗口外不误依赖、规则失效、两产品线程并发保留、查询门控、普通空/失败、公开导出完全一致、政策/文件变更后收起旧结果。

## 浏览器 E2E

命令：`.venv/Scripts/python.exe tests/e2e/run_aoi_rs_monitor_browser.py`。

使用实际页面和隔离源/工作簿，数据库构造被禁止。最终结果：**AOI_RS browser E2E passed**。

- 首次打开无结果，查询后有双产品表。
- 原生按钮完成 Excel 下载；AppTest另外逐表核对下载字节内容。
- 厂别变化使旧结果消失，重新查询恢复。
- 1440/768/375宽度页面无横向溢出；窄屏实际操作周数和查询。
- 针对 Streamlit内部滚动，分别滚动捕获周期控件、产品表格及下载按钮。
- 普通用户失败不泄漏内部异常，真实空结果无残留表格，无浏览器 pageerror。

证据目录：`output/test-results/aoi-rs-monitor/`。`browser-result.json`记录通过项；`normal-1440.png`、`normal-768.png`、`normal-375.png`、`controls-*.png`、`download-*.png`、`empty.png`和`failure.png`记录页面。`processes.json`最终为 `cleaned=true`、`alive=[]`、`listening=false`。未修改或重启正式服务。

## 全量回归与基线归因

执行 `.venv/Scripts/python.exe -m pytest tests/unit tests/architecture -q --tb=short`，首轮 **2251 passed，22 failed，108 warnings，297.05秒**。不能宣称全量通过。

- 4个页面结构断言仍只期待原有三个看板，已随新增AOI_RS更新；相应页面与产品标签测试单独复测 **18 passed**，最终专项也包含页面结构测试。
- 1个产品注释 AppTest在全量负载下触发3秒默认超时，单独复测通过，未修改无关测试的时间阈值。
- 余17个失败涉及 QTime标题、CTQ/管线及共享管理测试缺少临时全局配置、CPK/CPM规则预期、AOI_TT豁免、未登记的已有资源备份以及测量夹具缺少factory字段。
- 使用只读导入覆盖，将本次修改的既有生产模块换为任务起点 `81e6f8c67f29bdf0ad18d6dec380511ab26ffcd1` 内容，17个残留节点仍全部失败。该方法未切换工作区、未覆盖用户改动，也不是完整干净工作树的基准运行。日志：`output/test-results/aoi-rs-monitor/baseline-residual.txt`。
- 原有 `test_monitor_excel_apptest.py` 的3个失败也在任务起点页面代码下复现，包含计数预期与企业Excel COM/文件占用问题；证据为 `baseline-regression.txt`。这些测试不计入新增看板E2E通过声明。

## 评审与限制

Standards与Spec独立评审已完成；旧会话展示签名问题已修复并补测，复审无未关闭的功能阻断发现。窄屏证据缺口也已补充实际滚动截图。

未查询生产数据库，未做生产数据验收；未安装覆盖率/静态类型检查工具，未生成覆盖率或类型检查通过声明。跨进程锁复用现有公开实现，新增并发测试验证线程发布；未单独执行跨进程竞争或强制杀死控制器的恢复实验。

已有工作簿及并行任务改动保持独立，不纳入本次提交。开发分支为 `feat/aoi-rs-monitor`；合并及正式ADR审批单独等待用户确认，不能宣称完整development-flow的合并/沉淀阶段已通过。

## 文档与追踪

- 规则：`references/domain/inline_domain/monitor/rules-aoi-rs-period-monitor.md`。
- 本地规格及tickets：`.scratch/aoi-rs-monitor/PRD.md`和`issues/`。
- ADR审阅草案：`.scratch/aoi-rs-monitor/ADR-draft.md`，尚未作为已接受ADR写入 `docs/ADR/`。

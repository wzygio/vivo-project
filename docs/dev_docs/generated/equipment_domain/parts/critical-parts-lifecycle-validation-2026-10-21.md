# 关键备件寿命规则修改验证

日期：2026-10-21。范围：常规备件与CVD的98%更换阈值、旧展示截断删除、CVD数值及年限周期回绕，以及“已更换”行的测量值显示本次报告更换日期。业务规则由[关键备件数据流](../../../../../references/domain/equipment_domain/parts/data-flow-critical-parts.md)维护，本文仅记录验证证据。

## 执行结果

日期展示修复前，截图对应的1.5年规格用例及常规/CVD混合服务用例均失败：已更换的测量值仍为旧日期或0。修复后核心回归78项通过，完整相关回归 **121项通过**。

```powershell
.\.venv\Scripts\python.exe -m pytest tests/unit/test_equipment_parts.py tests/unit/test_cvd_parts.py tests/unit/test_cvd_parts_loader.py tests/unit/test_parts_service_cache.py tests/unit/test_equipment_parts_dashboard.py tests/unit/test_equipment_data_fabricator.py tests/unit/test_equipment_data_updater.py tests/unit/test_equipment_data_commands.py tests/unit/test_equipment_snapshot_latest.py tests/unit/app/pages/test_parts_page_cvd.py tests/unit/app/pages/test_parts_page_global_cache.py tests/integration/test_equipment_fake_dataset.py::test_report_service_bootstraps_fabrication_without_an_operator_command tests/architecture -q --tb=short
```

覆盖：98%边界与98.01%；源值未变时重复计算仍标记更换；CVD数值达到完整寿命、跨多个周期、回绕后的月增量；年限周期的自然月、闰日、月末、未来日期；已更换行在常规、CVD数值、CVD年限三类中统一显示报告日期，进度保持0%，正常行保留数值或周期日期；截图对应2025-04-26/24/22源日期在2026-10-21报告中均显示新日期；源输入不变；常规备件回绕后维持TTL增量；服务汇总、公开字段投影及CSV导出；页面普通模式的正常、空数据和失败分支；缓存重载及架构依赖。

另外通过：

```powershell
.\.venv\Scripts\python.exe -m compileall -q src/equipment_domain app/charts/equipment_domain app/pages/关键备件报表.py
git diff --check -- src/equipment_domain app/pages/关键备件报表.py app/charts/equipment_domain config/domain/equipment_domain.yaml tests/unit/test_equipment_parts.py tests/unit/test_cvd_parts.py tests/unit/test_equipment_data_updater.py tests/unit/app/pages/test_parts_page_cvd.py tests/integration/test_equipment_fake_dataset.py tests/integration/test_equipment_parts_db.py references/domain/equipment_domain/parts/data-flow-critical-parts.md
```

## 范围限制

未连接生产数据库，未运行使用生产数据库或维护基线的集成用例，也未通过真实数据页面触发加载。离线集成使用临时基线、临时快照和无数据库对象；页面检查使用Streamlit AppTest与注入数据。未启动服务或重启已有服务。未改写维护工作簿和现有源快照。

页面缓存签名升级为 `parts_report_replacement_dates_v7`，不复用仍含0或旧更换日期的L2载荷；源快照生命周期保留原实现。

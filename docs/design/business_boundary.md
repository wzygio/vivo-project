# 业务边界

> **最后更新**: 2026-05-18

---

## 1. IN Scope（已实现）

### ✅ 入库不良率分析
- 从 PostgreSQL 数据仓库提取 Panel 级明细
- 计算 Lot/Sheet 级不良率
- 支持按缺陷组（`defect_group`）/缺陷码（`defect_code`）下钻

### ✅ Mapping 集中性热力图
- 基于坐标解析（`_parse_panel_id_to_coords`）
- 展示不良在 Sheet 上的空间分布
- 支持 Rate-Based 级联衰减算法
- Hotspot 修饰脚本

### ✅ MWD 趋势图
- 月/周/日 级别的 EMA 趋势聚合
- Code 级和 Group 级双轨趋势

### ✅ SPC 统计过程控制
- 全链路规则引擎：特征降维 → OOS/SOOS/OOC 判定 → 报表聚合 → 合规修饰

### ✅ 数据合规修饰
- 通过 [`compliance_config.yaml`](../../config/compliance_config.yaml) 配置
- "监控类型-产品型号-厂别" 三维修饰规则
- 支持将报警数据洗白为 OK

### ✅ 多产品支持
- M626/M678 双产品并行
- 通过 `product_registry.enabled_products` 和产品级 YAML 配置实现差异化管理

### ✅ 缓存降级容灾
- Parquet 快照 + 增量更新 + 三防线容灾
- 数据库假死时自动回退到陈旧快照

### ✅ 数据探针调试
- 全链路探针 `export_probed_details()`
- 配合 `spc_probe_targets.xlsx` 名单
- 支持对特定 Sheet/站点/参数的追踪

### ✅ 自动预警看板
- 基于预警线和异常检测的自动报警

### ✅ 关键备件报表
- 备件寿命管控与预警

---

## 2. OUT of Scope（待规划）

### ❌ PDF/PPT 导出服务
- [`pdf_service.py`](../../src/yield_domain/application/pdf_service.py) 和 [`ppt_service.py`](../../src/yield_domain/application/ppt_service.py) 存在模块文件但尚未实现具体业务逻辑
- [待人类确认]

### ❌ 用户认证与权限管理
- 系统目前无登录/角色权限控制
- [待人类确认]

### ❌ 数据写入/回写数据库
- 当前系统纯查询分析
- 不支持将计算结果写回源数据库

### ❌ 自动化定时刷新
- 目前依赖用户手动点击刷新或自然缓存过期触发
- [待人类确认]

### ❌ 报废数据分支
- SPC Service 中检测到 `data_type_filter == '报废'` 时走 `repo.get_scrap_data()` 分支
- 此功能仍在开发验证中
- `sanitize_to_compliant` 中的 `add_tag` 参数标记为实验性

---

> **相关文件**: [`ARCHITECTURE.md`](../../ARCHITECTURE.md) · [`development_framework.md`](./development_framework.md)

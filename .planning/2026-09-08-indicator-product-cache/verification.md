# 验证记录

日期：2026-09-08；分支：feat/indicator-product-cache-isolation。

## 通过项

- 定向回归：128 passed，1 deselected（已知门户字符串断言）。包含矩阵、page header、七个产品页接线、compliance、真实 RenderGate 版本隔离以及矩阵集成。
- 浏览器 E2E：PASS。运行 tests/e2e/indicator_product_cache.js，真实 Streamlit 矩阵和缓存，临时工作簿与模拟领域输入。验证 CPK 红灯详情读 Excel、修改 Excel 后变绿、compliance 达标短路、普通用户无定向刷新控件、管理员仅刷新 Yield Lot/M626、其他指标和产品计数不变、1920/768 视口无横向溢出。
- Standards/Spec 双轴审查：发现的 Yield/SPC 图像旧 memo 键已修复，复审无阻塞。
- compileall、git diff --check 通过；环境未安装 Black/Ruff，未擅自安装。

产物：output/test-results/indicator-product-cache/targeted.xml、regular.png、admin.png、admin-768.png。

## 全仓单元测试边界

pytest tests/unit：1242 passed，6 failed，80 warnings，78.11 秒。不是全仓全绿。

六项均涉及本次未改变的既存断言／策略：

1. test_every_streamlit_page_uses_the_shared_page_header：硬编码 13 页面，HEAD 已有 15 页面。
2. test_portal_navigation_points_aoi_rs_to_the_streamlit_page：门户节点精确字符串已不匹配，config.js 未改动。
3. test_all_project_cache_data_decorators_use_the_global_ttl_accessor：既存 compliance_manager.py:45 小表缓存，文件未改动。
4. test_compact_mapping_defaults_to_penultimate_batch：期望倒数第二批次，既存实现选最新批次；本次仅修改该模块图像缓存签名。
5. test_yield_data_policy_is_defined_once_in_global_config：现有配置多两个 TP 缺陷组。
6. test_yield_data_policy_is_built_once_from_validated_app_config：同上；config/global.yaml 未改动。

全仓运行期间出现 Windows COM 0x80010108 诊断，但 pytest 继续完成并输出以上汇总。未为通过本次任务而改动这些不相关业务策略／测试。新功能与隔离 E2E 均通过。

## 交付边界

生产 resources/data 无 Git 改动；未运行生产数据刷新。测试使用自己的 8518 端口和浏览器会话，结束后已停止；测试产物保留于 output。流程停在开发分支交付，尚未获本轮合并／推送授权，不进入合并后 ADR 沉淀阶段。

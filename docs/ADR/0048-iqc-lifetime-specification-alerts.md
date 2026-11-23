# ADR-0048 北极星寿命规格点与巡检预警

日期 2026-11-23。适用范围为 IQC 寿命测试报表、自动预警看板与每日巡检。

## 背景

北极星 800nit 规格按产品和 WRGB 画面给出小时数。规格时刻与实际测试时刻常有差异，同一结果需要在折线图、报表预警中心和自动预警看板上解释。

## 决定

1. 产品型号与画面共同定位维护规格，复用屏体的产品各有一行。详情及样品编号沿用 [ADR-0030](0030-iqc-lifetime-anonymous-m3-boundary.md) 的匿名投影。
2. Core 负责四点回归与规格点评估。每个规格点记录实测、插值或预测的取值方式，效率衰减与亮度衰减独立判定。完整规则见[寿命监控规则](../../references/domain/iqc_domain/lifetime/rules-lifetime-monitoring.md)。
3. 页面图表直接消费规格点评估。每日巡检通过应用服务对全部启用产品评估，将低于阈值的记录及发布范围原子写入工作簿。预警中心和看板复用[发布结果读取和预热机制](0044-unified-automatic-alert-inspection.md)。

## 取舍与后果

直接使用回归直线的截距可能使预测段偏离 400H 实测点，因此采用经过该实测点的平行线。只接受精确测试时刻会遗漏 535H、665H 等规格，故在已有数据覆盖区间内插值，之后才外推。估算方法随数值一起展示。

打开报表时重新发布预警会让共享结果依赖交互时机和手动筛选。由巡检发布可保证全产品使用同一批结果，代价是测点或规格更新后，图表与最新一次巡检之间可能存在时间差。工作簿读取、有效期及巡检失败沿用已有发布契约。

## 验证

对应需求位于 `.scratch/lifetime-spec-alerts/PRD.md`。领域测试覆盖七型号规格、非共线回归及 0.95 边界。页面、看板与巡检验证入口为 `tests/e2e/run_iqc_lifetime_specs_browser.py`、`tests/unit/app/sections/monitor/test_published_alert_snapshots.py` 和 `tests/e2e/test_inspection_e2e.py`。

# AOI_TT 趋势预警

AOI_TT 复用 [AOI_RS 趋势预警规则](../aoi_rs/rules-aoi-rs-trend-alerts.md)，使用同一纯计算程序和精简预警中心。TT 的测量来源、检测片数分母及 Particle Size 聚合继续遵循 ADR-0008、ADR-0024、ADR-0025。

## 日期与判定

- 报表结束日为 R，固定以昨天 `T=R−1` 为当日。昨天缺测时不回退到最新观测日。
- P 是 `[T−7,T)` 内最近有效记录，允许跨日；Q 为同一区间内再前一次有效记录，仅供 T-2 展示。真实零密度可参与比较，缺测不补零。
- 基线 M 是 `[T−6,T−2]` 五个自然日中正密度的算术平均，跳过缺测、无效值和零密度，不延长窗口凑数。
- 当日超规：`D(T)>1.2×D(P)` 且 `D(P)>1.2×M`；要求 T/P 有效、八天计算窗至少三个有效日、M 为正。均值仍为各有效日等权算术平均，不按检测片数加权。
- 独立的连续两日上升判定已删除；Q 不参与判定，输出不再含 `rising_evaluable` 或 `rising_alert`。基线缺失时数据不足，不再依赖另一规则单独报警，预警原因仅为“当日超规”。
- 严格大于，恰好 1.2 倍不预警；共享程序以 `1e-12` 相对容差处理浮点边界。有限非负密度且有限正检测片数才是有效记录。

## TT 适配口径

产品内部仅取 `particle_size == "Total"` 的趋势，按 `factory + step_id + tt_name` 独立评估。S、M、L、H 不参与预警，也不生成对应评估记录；仅某粒径上涨而 Total 稳定时不报警。Total 缺测时不使用其他粒径替代，不累加粒径重构 Total。

密度沿用报表当前 Total 投影：同厂别、站点、日期、TT 参数的 Total `SUM(tt_qty)`，除以同厂别、站点、日期的 TT 检测 `distinct sheet_id` 数。先按完整当前明细计算既有聚合，再选择 Total 序列进行判定，保留既有站点分母。使用 TT 测量自身分母，不接入 RS 过货视图。修饰、日期排除和粒径生成/来源选择均先于趋势计算，预警与报表采用同一份当前明细。

预警通过 `build_period_trend_df(daily_start_date=T−7, daily_end_date=T)` 构建八个自然日的显式日轴。缺失指标日期仍缺测；普通报表不传这两个参数，保留现有展示日轴和月周聚合截止日 R。

## 分层与缓存

- `src/inline_domain/core/shared/density_trend_alerts.py` 是 RS/TT 共用的判定实现，无 I/O。完整程序随 [RS 规则文档](../aoi_rs/rules-aoi-rs-trend-alerts.md#8-实现程序与接入位置) 同步维护。
- `src/inline_domain/core/aoi_tt/trend_alerts.py` 先筛选 Total，再绑定 TT 三个指标键和输出列；评估结果不含 `particle_size`。
- `application/aoi_tt/aoi_tt_service.py` 在现有明细修饰及粒径展开后，以 Total 指标集计算 `trend_evaluation_df`，缓存原生 DataFrame payload，在缓存外构建 ViewModel。策略版本、查询日期、产品 revision、决策签名、粒径配置及日期排除均参与现有缓存身份。TT 策略版本带 `tt-total-only-v1` 后缀，使原逐粒径预警缓存失效。
- 本机制不写入单片报警历史、周报警率或全指标矩阵。既有单片异常独立运行。

## 页面与图像

趋势中心在手动查询门控之前展示。命中时仅显示业务表格，不附规则说明、计数提示或解释横幅。表格共十一列：厂别、站点、TT名称、当日、当日密度、T-1有效日、T-1密度、T-2有效日、T-2密度、基线5日均值、预警原因。不向前端传入 Particle Size、检测片数、有效天数及内部评估字段。

当日标签指 T；T-1/T-2 标签表示实际选中的 P/Q，其日期可能不相邻。没有命中时保留简短的无数据、数据不足或可评估项无预警状态。

命中图像按厂别、站点、TT名称精确过滤，保留该指标已有的 Total 及各粒径趋势、Lot、Sheet 图像；表格取消粒径判定不删除图像的粒径拆分。聚合仍使用完整当前报表明细，筛选仅控制绘图输出，避免不同 TT 的有效片集合不同时缩小站点分母或改变日轴。图表 key 使用 `aoi_tt_trend_alert`、`aoi_tt_oos_alert` 和 `aoi_tt_report` 的独立区域前缀，允许同指标同时出现于趋势、单片异常和手动报表。

## 验证入口

- 核心：`tests/unit/inline_domain/core/aoi_tt/test_aoi_tt_trend_alerts.py`；共享边界由原 RS 规则用例覆盖。
- 应用及缓存：`tests/unit/inline_domain/application/aoi_tt/test_aoi_tt_service.py`。
- 页面十一列、状态、忽略粒径单独上涨、保留粒径图像及图表 key：`tests/unit/app/sections/aoi_tt/test_aoi_tt_trend_warning_ui.py`。
- 自动展示门控：`tests/unit/app/pages/test_aoi_tt_page.py`。
- 无数据库浏览器夹具：`tests/e2e/fixtures/aoi_tt_trend_app.py`。

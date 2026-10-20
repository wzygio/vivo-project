# AOI_TT 周度 PPT 报告

核验日期：2026-10-20。入口为 AOI_TT 页“TT 周报导出（PPT）”，普通模式即可使用。

## 周次与统计口径

- 固定取参考日的上一完整 ISO 周，即上周一 00:00 至本周一 00:00（右侧不含）；不采用折线轴跳过空周后的最近可用周。
- 数值为当前应用服务产物对应的“平均每片 TT 个数”。直接复用页面的 `build_period_trend_df`，分子为各站点、TT 项目、粒径的 `tt_qty` 合计，分母为同厂别、站点、周内 TT 测量明细的 distinct Sheet/Glass。
- 不使用 RS 过货视图；不先把明细筛成所选 TT 或粒径后另算分母。含零值的测量记录也属于检测片。
- 同站点该周无检测记录时填“无过货”；对应 TT/粒径有零值测量时填 `0`；站点有检测但缺少该 TT/粒径测量值时填“数据不足”，不把缺少数据当成零。
- 使用当前服务已处理的明细，不另取原始事实或重新执行一套处理。PPA 始终排除在 TT 外。
- 即使整个查询窗口没有 TT 明细，应用服务也保留该产品的规格项目。规格不含厂别；厂别优先由所选产品实际站点事实确定，已知 AOI 站点可用于无明细项目的归类。
- 平均值通常保留四位小数并去末尾零；很小的非零值使用四位有效数字，避免错误显示为零。

## 筛选与表格

- 产品使用全局启用列表与配置顺序，默认全部；空选代表全部。厂别默认 ARRAY，空选代表 ARRAY/OLED/TP。
- 每页最多八款产品；七至八款时使用较小字号。超过八款再分批，项目行较多时按高度分页，每页保留表头和模板资产。
- Array 增加层别列，按下表映射；站点筛选只作用于 Array，空选表示全部，不影响 OLED/TP。

| AOI TT 站点 | 层别 |
|---|---|
| 11620 | 3layer |
| 12620 | M1 |
| 13620 | M2 |
| 15620 | M3 |
| 18620 | M4 |

- Particle Size 默认 `Total`，可选当前 TT 支持的 `Total/S/M/L/H`，空选代表全部。OLED 始终只输出 Total。
- 项目行按“TT 名称（Particle Size）”标记；Total 与各子粒径分别输出，不能混加。非 Array 同一 TT 参数跨站时，在项目名后追加站点以消除歧义。
- 配置或粒径来源不支持某个粒径时，有检测的对应单元格显示“数据不足”，不能复制 Total 值或假定为零。

## 模板、共享与交互

- 模板由全局 `resources.inline_domain.files.aoi_tt_weekly_ppt_template` 配置，当前沿用用户提供的 `references/template/template-PPT.pptx`。保留原文件、母版、Logo、水印，生成可编辑表格。
- 与 RS 共用 `application/shared/weekly_summary.py` 的原生表格结构及数字格式、`app/charts/inline_domain/weekly_summary_ppt.py` 的 PPT 写入，以及 `infrastructure/shared/ppt_template.py` 的受保护模板读取。两个模板资源指向同一路径时，表现层按路径/mtime/size 复用模板原生 bytes 缓存。
- TT 与 RS 的产品查询、业务计算、缓存签名、生成状态及下载文件名保持各自归属。TT 报告分类为 `TT Defect`、单位为“平均每片 TT 个数”，文件名为 `AOI_TT_周报_<ISO 周>.pptx`。
- 用户点击生成后再下载，可重复下载。失败或异常中断会解除忙状态，并保留上次完整报告；筛选变化时提示旧下载尚未更新，禁用产品会使包含该产品的旧下载失效。
- 普通模式提示和下载仅包含业务内容；技术异常记录在日志，不显示内部路径、标志字段或其他数据版本。
- 企业保护模板只读打开并保存临时副本，在 Office 关闭前捕获可读 Open XML 字节。临时文件写入 `output/tmp` 并在成功/失败/中断后清理；保留已有用户 PowerPoint 会话。普通 PPTX 无需 Office。

## 验证

- 数据规则与真实服务空明细：`tests/unit/inline_domain/application/aoi_tt/test_aoi_tt_weekly_report.py`。
- 普通模式生成、下载、筛选、错误、中断、缓存：`tests/unit/app/sections/aoi_tt/test_aoi_tt_weekly_export.py`。
- 下载文件的八产品列、数值、类别及单位：`tests/unit/app/sections/aoi_tt/test_aoi_tt_weekly_ppt.py`。
- 模板读取及生命周期：`tests/unit/inline_domain/infrastructure/shared/test_ppt_template.py`。
- 2026-10-20 使用实际模板及合成数据生成 Array/OLED/TP 三页、每页八产品，由 PowerPoint 重新打开和导出预览通过。样例及测试日志位于 `output/test-results/aoi_tt_weekly/`，并非生产周报。

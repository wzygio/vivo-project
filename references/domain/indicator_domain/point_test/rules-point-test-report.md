# 密集点测试报表规则

## 来源与产品

`config/global.yaml` 的 `resources.indicator_domain.files.point_test_workbook` 是来源路径唯一配置，默认指向用户提供的 `inline密集点测试数据.xlsx`。通过 ConfigLoader 和 Indicator composition 组装，不查询数据库。

识别同一工作簿中的 `<产品>-detail` / `<产品>-summary` 配对。展示前按全局 enabled_products 取交集并保留全局顺序，选择器标签复用产品注释，已失效选择清理；当前来源只有 Z576。其它辅助页不展示；缺少配对、非法表头和不完整统计数据为读取失败，不当作“无数据”。仅有空配对或没有可用产品时显示业务空态。

## 明细与汇总

明细首行为字段名，保留全部来源列与记录。Excel 序列日期与日期时间值统一为日期时间（微秒精度），空值保留，不按 SITE_NAME 推断膜位。

汇总每块为三组横向七列，中间隔一空列；首列业务标题为调整前、调整后、修正后。各组 ID 仅在当前块中向下补全，遇到标题或组内空行重置。每条记录保留产品、阶段、SHEET_ID、膜位、MAX、MIN、AVE、STD、Δ/（MAX-MIN)。同一阶段/大板/膜位重复身份拒绝读取，不合并或覆盖。

summary 是用户维护的统计结果，不能从当前 detail 重算：调整前大板不在 detail 中。三个阶段均为用户要求的业务分类，与管理员展示模式无关。页面在产品选择后分别呈现三类汇总和明细，原生表格支持排序、搜索、列菜单及 CSV 下载，导出值来自当前产品完整 DataFrame。

这个静态测试资源没有贯穿三阶段的时间字段，因此不应用实时查询的日期前推、当日截止和日期排除；保留来源明细和汇总的一致性。

## 读取、缓存与失败

标准 XLSX 按 `header=None` 读取，避免 pandas 对重复汇总表头自动加后缀。企业加密输入复制到 `output/decrypted_files/` 的唯一临时目录，通过 fr_file_decryption 的 excel-values 策略读取；调用方保留外层 COM apartment，待包返回并释放内部代理后卸载。立即读取转换字节，再清理全部临时副本；原始资源不改写。

页面缓存只保存原生容器与 DataFrame，完整来源路径、mtime_ns、大小构成键，TTL=1小时，max_entries=8。手动页头刷新清除此缓存；文件更新和路径变更自动重读。先成功加载再展示，不保留上一次成功数据作为失败后的报表。

普通页面只展示业务错误；详细异常写服务器日志。图片字段仅显示其来源值，不主动请求图片链接。读取和展示均不触发数据处理、写回或数据库回退。

## 验证与追踪

应用服务、标准/加密 adapter 隔离、生产 section 的 AppTest、缓存键和架构守卫由 point_test 对应测试覆盖。显式设置 `VIVO_POINT_TEST_LIVE=1` 才进行维护工作簿核对。
浏览器入口 `tests/e2e/run_point_test_browser.py` 运行实际薄页，使用无数据库夹具验证切换、空态和失败，另用 live 场景走真实 composition 和工作簿；产物在 `output/test-results/point-test/`。runner 记录进程身份及端口并清理本次进程树，恢复时只清理身份匹配的残留。
需求和票位于 `.scratch/inline-point-test/`；数据分析与交付记录在 `docs/dev_docs/generated/indicator_domain/point_test/`。

# Findings

- SPC/CTQ 工作簿键：`prod_code, step_id, param_name, sheet_id`；事件时间为 `sheet_start_time`。
- AOI_TT 字段含 `tt_name, sheet_id, lot_id, start_time, tt_qty, usl`。
- AOI_RS 字段含 `rs_code, chart_kind, point_id, value, spec, sheet_start_time`。
- 工作簿只含异常事实，无正常样本分母和控制限，不能恢复旧告警率/OOC/SOOS。
- 当前 Excel 产品 sheet 会被最新生成结果替换；长期历史必须用覆盖窗口增量替换而非 append-only。
- 现有 monitor 独立执行全量量测/规格读取和规则计算；目标主路径必须移除该依赖。
- 用户确认刷新状态仅 `?admin=true` 可见。
- AOI 查询支持 factory/step/指标过滤；这类局部查询不得替换产品×scope 的完整日期窗口，
  仅无维度过滤的完整查询维护长期历史。
- 决策工作簿通过单次全 sheet 读取复用，缓存以 mtime/size 签名失效，避免产品循环触发
  多次企业加密 COM 读取。

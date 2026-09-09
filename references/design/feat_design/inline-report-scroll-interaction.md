# Inline 报表滚动与图表交互设计

## 1. 背景与范围

SPC 报表出现数据已加载、鼠标位于图表区域时无法继续向下滚动的现象。检查时主内容容器允许滚动，内容高度大于视口高度；图表渲染入口未覆盖 Streamlit 默认启用的 Plotly 滚轮缩放，存在图表缩放与页面滚动争用滚轮的问题。

同样的配置缺口也存在于 CTQ、AOI_TT、AOI_RS 报表。本设计覆盖这四个报表的普通指标图和复用相同渲染入口的预警指标图，不扩展到其他业务页面。

## 2. 交互原则

**长报表中的普通滚轮操作优先用于页面纵向滚动。** 鼠标位于图表绘图区时，也应能够继续浏览后续指标。

- 关闭图表滚轮缩放，避免滚轮在图表区域被用于修改坐标范围。
- 保留图表工具栏的放大、缩小、重置坐标与平移，以及原有拖拽交互。
- 保留原有图表尺寸、三列布局、折叠面板和图表标识。
- 不通过隐藏滚动条、固定页面高度、禁用全部鼠标事件或将图表静态化处理该冲突。
- 普通报表与预警图使用同一滚轮策略。

这项策略针对 Plotly 图表。表格自身的滚动、浏览器缩放及未来的独立全屏分析工具，应按各自交互需求单独处理。

## 3. 实现归属

滚轮行为属于前端展示层，由 `app/sections/inline_domain/` 在调用 `st.plotly_chart` 时显式传入：

```python
st.plotly_chart(
    figure,
    width="stretch",
    config={"scrollZoom": False},
)
```

已有 `key` 等参数继续保留。配置应放在渲染入口，而不是 Plotly Figure 的业务布局、后端 application/core/infrastructure 或数据缓存中。当前配置只有一项，直接在各渲染入口声明，不额外增加包装层。

| 报表 | 图表范围 | 渲染入口 |
|---|---|---|
| SPC | 月周日分布、主站点设备/腔室分布、过货时间分布 | [spc_dashboard.py](../../../app/sections/inline_domain/spc/spc_dashboard.py) 的 `_render_indicator_payload` |
| CTQ | 月周日分布、主站点设备/腔室分布、过货时间分布 | [ctq_dashboard.py](../../../app/sections/inline_domain/ctq/ctq_dashboard.py) 的 `_render_ctq_indicator_payload` |
| AOI_TT | 月周日趋势、By Lot、By Sheet；覆盖不同 Particle Size | [aoi_tt_dashboard.py](../../../app/sections/inline_domain/aoi_tt/aoi_tt_dashboard.py) 的三类图表调用 |
| AOI_RS | 月周日趋势、By Lot、By Sheet | [aoi_rs_dashboard.py](../../../app/sections/inline_domain/aoi_rs/aoi_rs_dashboard.py) 的三类图表调用 |

页面文件负责调用这些区块，不需要重复设置滚动样式。修复不改变查询、异常点过滤、规格、统计或修饰结果。

## 4. 后续维护约束

新增或迁移上述报表的 Plotly 图表时，应显式保留 `scrollZoom=False`。新增其他 Plotly 配置项时合并到同一个 `config`，避免覆盖滚轮策略。

若仍有“无法滚动”，应分别检查：主滚动容器的实际高度和 overflow、图表的滚轮处理、覆盖层拦截事件，以及浏览器是否因大量绘图无响应。不能仅凭外层 `.stApp` 的 `overflow: hidden` 判断页面被锁定；Streamlit 可由内部主容器承担滚动。

## 5. 验证方式与边界

自动化回归检查实际渲染调用，确认各类图表均收到 `scrollZoom=False`，并保留已有布局与数据传递断言。测试位置为 `tests/unit/app/sections/` 下的 `spc`、`ctq`、`aoi_tt`、`aoi_rs` 对应测试。

浏览器验收应覆盖：

1. 加载多个指标，确保内容高度超过视口。
2. 将鼠标分别放在趋势、Lot/设备、Sheet/时间图的绘图区，向下滚动，确认页面位置变化且坐标范围不因滚轮改变。
3. 滚动到最后一个指标，再向上返回；展开预警图后重复检查。
4. 验证工具栏缩放、坐标重置与平移仍可使用。

单元测试可确认配置和调用覆盖，不能替代真实浏览器滚动验收。2026-09-09 的 SPC 检查已读取主容器尺寸；修复后的现场滚动验收因浏览器调试连接中断尚未完成。其余三页本次通过代码核对与渲染回归测试验证，不将其描述为已完成现场滚动验收。

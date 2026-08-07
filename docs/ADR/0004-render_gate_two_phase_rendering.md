# RenderGate 两阶段渲染设计

对应程序：`app/manager/render_gate.py`

参考实现：`app/sections/spc/spc_dashboard.py`（`_build_indicator_render_payload` /
`_render_indicator_payload` / `render_spc_indicator_sections`）

## 1. 问题：图表阶段式显示

Streamlit 的执行模型是"脚本自上而下、元素即建即推"：每个 `st.*` 调用一旦执行，
对应的 delta 立即推送浏览器，前端收到即渲染。

报表页面的数据通常在脚本开头一次性加载完成（`st.cache_data` 缓存的 payload），
因此阶段式显示**不是"数据未就绪就渲染"**，而是：

```text
构建图1(重计算) -> 推送图1 -> 构建图2(重计算) -> 推送图2 -> ...
```

重计算（Plotly Figure 构建、ECharts option 组装、大表整形）与渲染调用交错，
每张图算完一张弹一张，页面持续抖动、操作卡顿。

## 2. 方案：计算与渲染两阶段分离

```text
阶段 1 collect: RenderGate 在统一 spinner 下集中执行全部纯计算任务
阶段 2 flush:   调用方遍历结果，集中执行 st.* 渲染调用
```

渲染阶段各元素之间没有计算间隔，所有图表在极短时间内连续到达浏览器，
用户观感为"等待数秒 → 一次性铺满"，消除逐张跳出与页面抖动。

## 3. API 与使用约定

```python
from functools import partial
from app.manager.render_gate import RenderGate

gate = RenderGate()                      # spinner 文案可自定义
for item in items:
    gate.stage(partial(build_payload, item))   # 纯计算，禁止 st.*
for payload in gate.collect():                 # spinner 内批量执行
    render_payload(payload)                    # 仅 st.* 调用
```

- `stage(job)`：注册纯计算任务。任务内**禁止任何 `st.*` 渲染调用**
  （`st.plotly_chart`、`st.expander`、`st.metric`、`st.dataframe` 等）。
- `collect()`：在统一 spinner 下按注册顺序执行全部任务，返回结果列表，
  队列随即清空；空队列直接返回 `[]`，不显示 spinner。
- 渲染阶段的 `st.*` 调用顺序、布局结构、widget key、交互行为
  （`on_select="rerun"`、echarts 事件、AgGrid 选择等）必须与改造前完全一致。

## 4. 改造模式

对"循环内边算图边渲染"的函数，拆成两个助手：

- `_build_xxx_payload(...) -> dict`：纯计算。返回渲染所需的全部材料
  （Figure / option / 表格 DataFrame / 格式化后的指标文本）。
- `_render_xxx_payload(payload) -> None`：纯渲染。只执行 `st.*` 调用。

原函数保留签名不变：先循环 `gate.stage(...)`，再 `for payload in gate.collect(): _render(...)`。
页面文件无需改动。

## 5. 适用范围

- 适用：循环渲染多图表的 section（SPC/CTQ 指标区、预警看板、Yield 看板、备件报表等）。
- 不适用：纯表格页面、文件管理页面（无重计算图表，无阶段式显示问题）。
- 注意：构建全部图表后再渲染会短暂持有全部 Figure 对象，内存峰值略升；
  对单页几十张图的规模可忽略。

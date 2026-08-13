# ADR-0012：共享 Inline 原始测量快照与报表派生适配器

- Status: Accepted
- Date: 2026-08-13
- Scope: `src/inline_domain/{application,infrastructure}/`、`src/inline_domain/composition.py`、SPC/CTQ/AOI_TT 页面

## Context

SPC、CTQ 和 AOI_TT 均从 `eda.spc_tzbjx_array`、`eda.spc_tzbjx_oled`、
`eda.spc_tzbjx_tsp` 读取测量事实，并通过 `mdw.dwr_mes_productspec` 映射产品。
此前 SPC/CTQ 共用一份已经过 SPC 规则处理的快照，AOI_TT 则重复执行三厂
UNION 查询；应用服务还直接构造基础设施仓储。结果是数据库读取重复、快照语义
混合，并且应用层依赖具体 SQL/Parquet 实现。

三类报表虽然同源，但派生口径不同：SPC/CTQ 需要参数分类、异常值过滤和主制程
追溯，AOI_TT 需要保留 lot 并按规格中的 step+param 识别 TT。因此不能共享一份
已经按某一报表口径处理过的数据集。

## Decision

1. `infrastructure/measurement/` 作为共享技术适配器，拥有三厂测量 DAO、参数元数据
   DAO、主制程履历 DAO 和产品级原始快照。
2. 原始快照稳定字段为 `factory, prod_code, start_time, sheet_id, lot_id, step_id,
   param_name, site_name, unit_id, param_value`。一次参数化 UNION 获取三厂数据，
   产品和时间过滤均下推数据库。
3. 快照按产品隔离，使用三个月滚动提取窗口、8 小时 TTL 和显式策略版本；写入
   采用同目录临时文件原子替换。同产品并发首次读取使用进程内锁合并；刷新失败
   时仅在历史快照可读时降级。
4. 应用层定义消费方拥有的 Protocol 端口。SPC、CTQ、AOI_TT 服务接收端口，
   不导入数据加载器、具体仓储或 Parquet。`src/inline_domain/composition.py` 负责
   在页面边界组装数据库、共享快照和派生适配器。
5. `infrastructure/spc/` 负责 SPC 派生预处理；`infrastructure/ctq/` 固定选择
   CTQ 投影；`infrastructure/aoi_tt/` 负责 TT step+param 过滤、字段映射和规格
   投影。任何派生规则均不回写原始快照。
6. 主制程 OUT 履历查询同样归 `infrastructure/measurement/`；SPC 只组合履历端口并
   完成纯 DataFrame 路由与最近前序记录匹配。
7. 删除 SPC/AOI_TT 旧 `data_loader.py`、重复 DTO 和 SPC 专用快照兼容分支。
   `SpcRepository` 的 raw、metadata、history 三个端口均为必填，缺失时立即失败。
8. `MonitorAnalysisService` 接收 `MonitorSpcRepositoryFactory`，由 Streamlit 页面组合根
   注入具体仓储；application/monitor 不得导入 infrastructure。
9. `infrastructure/monitor/` 只保留 `InlineMonitorRepository`，作为自动预警用例的
   仓储门面；跨 SPC、CTQ、AOI_TT 复用的 DAO 和快照实现不得放入该目录。

## Alternatives considered

- 让三类报表直接读取同一份 SPC 处理后快照：拒绝。SPC 的 LOSS、异常值、参数
  分类和追溯规则会改变 AOI_TT 的事实口径，且旧快照不稳定保留 lot。
- 把共享逻辑放入 application service：拒绝。SQL、Parquet、TTL 和失败降级均
  是出站适配器职责，会造成依赖方向反转。
- 只抽取 SQL 函数、各报表继续维护自己的快照：拒绝。仍会重复存储、重复刷新，
  并可能在同一产品上形成不同时间窗口和新鲜度语义。
- 建立一个同时返回 SPC/CTQ/AOI_TT ViewModel 的大仓储：拒绝。它会把三个消费
  方的规则耦合到共享层，扩大修改半径。

## Consequences

- 正面：同一产品和 TTL 窗口内三类报表复用一次数据库提取；共享快照是可审计的
  原始事实；应用服务可用 fake ports 独立测试；报表规则仍可分别演进。
- 代价：参数元数据和主制程追溯仍会由 measurement 适配器执行各自的小型查询；进程内锁不提供跨进程
  去重，多个 Streamlit worker 同时冷启动仍可能各执行一次提取。
- 约束：共享快照字段或语义变更必须升级策略版本；不得把 SPC/CTQ/AOI_TT 专属
  过滤下沉到共享 DAO；新页面必须通过端口和组合根消费共享事实。

## Verification

- measurement DAO/snapshot/metadata/history 与三派生适配器定向测试：50 passed。
- Inline application/infrastructure 扩大定向回归：154 passed；AST 边界测试同时约束
  SPC/AOI_TT 无 SQL 执行能力以及 monitor application 无 infrastructure 导入。
- SPC smoke：161 passed。
- 全量 pytest：374 passed、7 个与本改动无关的既有失败（加密 xlsx 诊断假设、
  Yield 配置预期和 Code selector 新参数不一致）。
- Playwright E2E（localhost:8503）：SPC/CTQ 数据筛选正常，AOI_TT 查询渲染
  3 charts；三页均无 traceback。
- 自动预警页面在正常 Home 入口下完成启动、依赖装配和筛选区渲染且无 traceback；
  真实 ALL 查询在 90 秒观察窗口内仍在运行，未将其记为完整查询通过。

## Traceability

- Issue: `.scratch/inline-measurement-snapshot/issues/01-centralize-inline-measurement-snapshot.md`
- Plan: `.planning/2026-08-13-inline-measurement-snapshot/`
- Supersedes ADR-0008 中 AOI_TT 独立读取三厂明细的实现选择；TT 参数识别和分母
  口径仍由 ADR-0008 管理。

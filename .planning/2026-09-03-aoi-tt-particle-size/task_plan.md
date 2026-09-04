# Task Plan: AOI_TT Particle Size

## Goal

在保持 AOI_TT Total 口径和现有预警/修饰行为兼容的前提下，为 ARRAY/TDSUM 增加 O、L Particle Size 数据、筛选和三类图像，并以自动化测试和浏览器证据完成交付。

## Source

- Issue: `.scratch/aoi-tt-report/issues/02-distinguish-particle-size.md`
- Requirement: `docs/dev_docs/dev_spec/Inline_domain/feat-AOI_TT.md#task1-optaoi_tt报表区分particle_size`
- Approval: 用户要求“分析并完成”Task1-Opt，且原规格明确“直接执行到底完成开发”，视为对本计划、接口扩展和测试重点的批准。

## Current Phase

Complete

## Phases

### Phase 1: Requirements and triage

- [x] 固化 enhancement issue、验收标准与范围边界。
- [x] 确认参考 SQL 计数放大的原因是未去重 Sheet→产品映射造成连接乘法。
- [x] 确认 Particle Size 数据源只覆盖 ARRAY，OLED/TP 保持 Total。
- **Status:** complete

### Phase 2: TDD tracer bullet

- [x] RED：数据访问契约证明只统计 AOI + O/L，并在 Sheet→产品连接前去重。
- [x] RED：领域组合证明 Total 不变、ARRAY/TDSUM 补 O/L（缺失补零）、其他厂别/参数不扩展。
- [x] RED：聚合证明 particle_size 是月周天、Lot、Sheet 的独立分组维度。
- [x] RED：页面筛选默认 Total/O/L 全选，同一 Expander 按所选等级生成三图。
- [x] GREEN：实现最小垂直切片，使上述测试通过。
- **Status:** complete

### Phase 3: Compatibility and failure paths

- [x] Particle Size 数据为空或读取失败时只保留 Total，验证服务不会整体降级为空。
- [x] Total 的三态修饰、规格线、单片异常预警和既有筛选保持不变。
- [x] OLED、TP 及非 TDSUM 指标不生成伪 O/L 数据。
- [x] O/L 每个 Total Sheet 补零，确保趋势与 Lot 分母覆盖全部检测片。
- **Status:** complete

### Phase 4: Verification and browser QA

- [x] 运行 AOI_TT infrastructure/core/application/app 定向单元测试。
- [x] 运行 AOI_TT 邻接回归与覆盖率，新增/变更逻辑达到至少 80%（实际 85%）。
- [x] 运行项目规定的编译/静态检查与 `git diff --check`（用户规格文件的既有尾随空格除外）。
- [x] 启动锁定环境 Streamlit，执行 AOI_TT E2E：筛选选项、默认全选、单等级三图、同 Expander、多等级隔离。
- [x] 浏览器视觉检查：桌面 viewport 下筛选器、Expander 和三列图无溢出/遮挡；执行一次探索性组合筛选。
- **Status:** complete

### Phase 5: Documentation and delivery

- [x] 更新 AOI_TT 领域数据链路和稳定术语/口径说明。
- [x] 记录 ADR，包含去重原因、数据权威来源、分层与兼容性决策。
- [x] 对照 Issue acceptance criteria 逐项关闭并记录测试证据。
- [x] 复核工作区差异，保留并未覆盖用户既有改动。
- **Status:** complete

## Acceptance Criteria Checklist

- [x] AC1 AOI/O/L 过滤：仓储测试 + SQL 契约断言。
- [x] AC2 Sheet→产品去重：仓储测试 + 查询结构断言。
- [x] AC3 Total 保持 SPC `param_value`：服务/领域回归。
- [x] AC4 ARRAY/TDSUM 扩展 Total/O/L 且缺失补 0：领域单测。
- [x] AC5 OLED/TP/非 TDSUM 只保留 Total：领域单测。
- [x] AC6 三类聚合按 Particle Size 隔离：core 单测。
- [x] AC7 多选默认全选且不改变现有级联：app 单测 + E2E。
- [x] AC8 同一 Expander 内每选中等级三图：app 单测 + E2E。
- [x] AC9 Particle 数据失败保留 Total：application 单测。
- [x] AC10 定向回归、覆盖率、静态检查与 E2E 全绿：命令日志。

## Public Interface Changes

- AOI_TT 数据端口新增 Particle Size 计数读取能力。
- AOI_TT 报表明细新增 `particle_size` 列，值域为 `Total | O | L`。
- AOI_TT 页面筛选结果新增已选 Particle Size 集合。

## Decisions Made

| Decision | Rationale |
|---|---|
| 先独立聚合缺陷明细并使用唯一 Sheet→产品映射 | 避免 SPC 多记录与 defect 多记录形成连接乘法 |
| Total 先按既有逻辑修饰，再以剩余 Total Sheet 作为 O/L 基准集合 | 保持 Total 三态行为；Delete 同时排除对应 Sheet 的分项图 |
| O/L 未命中时补 0 | 使 O/L 平均值分母代表全部 AOI 检测片，而不是仅有该粒径缺陷的片 |
| O/L 仅扩展 ARRAY/TDSUM | 唯一已授权数据源和当前业务目标均限定该范围 |
| O/L 沿用现有 USL/UCL 画线但不参与 Total 超规修饰 | 需求未提供粒径专属规格；防止 Total 规格篡改分项真实计数 |
| Particle 源失败退化为 Total-only | 新增能力不应破坏既有报表可用性 |

## Errors Encountered

| Error | Attempt | Resolution |
|---|---:|---|
| `rtk rg` 的 PowerShell 双引号模式被拆词 | 1 | 后续小范围搜索改用 `Select-String` 或单引号安全模式 |

## Scope Guard

- 不修改 AOI_RS/SPC/CTQ。
- 不新增 OLED/TP Particle 数据源。
- 不新增粒径专属规格或改变 Total 预警口径。
- 不要求 O+L 与 Total 相等。

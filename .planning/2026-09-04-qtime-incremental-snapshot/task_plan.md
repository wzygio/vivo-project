# Task Plan: Q-Time 共享增量快照

## Goal

交付每日 07:00 刷新、24 小时有效、被 Q-Time 报表与自动预警看板共用的单套增量 L1 快照，并通过 E2E 验证。

## Next Step

已完成，等待交付。

## Current Phase

Phase 4: Project record — complete

## Phases

### Phase 1: Requirements design

- [x] 核验 tracker、领域词汇、现有共享入口与快照逻辑。
- [x] 完成时序、快照粒度、增量窗口和失败语义的设计审批。
- [x] 发布 ready-for-agent PRD 与 ticket 图。
- **Status:** complete

### Phase 2: Planning

- [x] 执行顺序：01 统一快照 → 02 调度 adapters → 03 E2E/运维闭环。
- [x] 确认不改动两页的 `get_cached_shop_monitoring` 共享 seam。
- [x] 记录单元、CLI 集成、页面回归与隔离目录 E2E 验证门。
- [x] 用户已批准完成开发，不存在新的范围分支。
- **Status:** complete

### Phase 3: Development and testing

- [x] Ticket 01：统一厂别快照、两日重叠替换、月窗口修剪、降级读取。
- [x] Ticket 02：幂等刷新 CLI 与 Windows 07:00 任务注册 adapter。
- [x] Ticket 03：共享复用、页面回归、CLI 到快照 E2E。
- [x] 执行代码评审并修复发现。
- **Status:** complete

### Phase 4: Project record

- [x] 更新稳定运行流/领域约束。
- [x] 修订 ADR-0023，连接 PRD、tickets 与验证证据。
- **Status:** complete

## Ticket Frontier

| Ticket | Status | Blocking edge | Primary evidence |
|---|---|---|---|
| 01 unified shop snapshot | complete | none | 13 focused tests + 63 domain tests |
| 02 scheduled refresh adapters | complete | 01 | 3 CLI tests + PowerShell syntax check |
| 03 E2E and operations | complete | 01, 02 | isolated flow + two browser journeys passed |

## Decisions Made

| Decision | Rationale |
|---|---|
| complex mode | 新建共享持久化 module/interface 并横跨 application、infrastructure、CLI 和 UI 消费者 |
| 每厂别一份原始事实快照 | 允许任意产品/站点在内存过滤，两页共享，不固化人工修饰结果 |
| 删除旧签名和 option L1 生成逻辑 | 用户明确要求仅保留新 L1 方案 |
| 两日覆盖刷新 | 支持迟到/修正记录，不依赖源表稳定唯一键 |
| 失败时旧快照降级 | 保留看板可用性，同时以日志显式报警 |

## Errors Encountered

| Error | Attempt | Resolution |
|---|---:|---|
| RTK 扫描中 Windows `README*` glob 不可用 | 1 | 改用明确路径/文件扫描，不重复该 glob |
| Ruff 未安装 | 1 | 不自动安装；使用 compileall、pytest、现有类型配置与人工评审 |
| Q-Time 浏览器 E2E 查询按钮因站点默认值为空而超时 | 1 | 调整 E2E 为在按钮 disabled 时显式选择首个站点，不改生产 UI 契约 |

## Approval

- 2026-09-04：用户批准四项推荐设计，并明确要求去掉旧 L1 生成逻辑后完成开发。

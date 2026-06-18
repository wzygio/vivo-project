# Design Index

Use this file as the entrypoint for design knowledge in the 天柱专项报表系统 Harness.

## Start Here

- [Project context](../../CONTEXT.md): project purpose, boundaries, and fast routing.
- [System architecture](../../ARCHITECTURE.md): technology stack, runtime flow, cache strategy, and module map.
- [Development framework](development_framework.md): EPCC flow, TDD expectations, and red-line constraints.

## Domain Design

- [Yield Domain](yield_domain.md): 入库不良率、Mapping、Sheet/Lot、MWD 趋势相关设计。
- [SPC Domain](spc_domain.md): SPC/AOI/CTQ/OOC/OOS/SOOS、合规修饰和参数分类设计。
- [Shared Kernel](shared_kernel.md): 配置、数据库、日志、Excel/文件处理等共享基础设施。

## Update Rules

- Update the relevant design doc when module responsibilities, public contracts, data flow, cache semantics, or product rules change.
- Keep detailed business rules out of `AGENTS.md`; route from `AGENTS.md` to this index and then to the specific design document.
- If a referenced design file is removed or superseded, update this index in the same change.

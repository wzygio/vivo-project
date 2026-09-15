# ADR-0031：IJP 大孔独立数据口径与同页展示

- Status: Implemented; production data validation pending
- Date: 2026-09-15
- Trace: [任务书](../dev_docs/dev_spec/indicator_domain/task-IJP溢流报表开发-孔区.md)、[数据来源分析](../dev_docs/dev_spec/indicator_domain/datasource-IJP溢流报表分析-孔区.md)、`.scratch/ijp-hole/PRD.md`

## Context

大孔 C3RA 与旧边框 C3DM 的缺陷源、分类和分母不同，但页面筛选、查询门控、产品/线体/打印机布局可以复用。原 FineReport 日统计对腔室履历多追溯七天，玻璃和 Total 没有此追溯。

## Decision

1. 在现有 IJP 页面增加区域切换，保留旧入口行为。新增 `indicator_domain` 下 application/core/infrastructure 的同名 `ijp_hole` 子模块及对应 section/chart。
2. 独立只读 port 只输出聚合计数与批次选项；组合根绑定 SQL adapter。复用现有查询 DTO 和固定日期窗口的公开 API，不继承旧 adapter 的私有方法或旧 C3DM 统计逻辑。
3. 大孔判定必须在 SQL 源头完成；使用 PostgreSQL `split_part`，SQLite 集成测试注册其等价 SQL 函数。此处明确区别于旧 IJP 将路径派生放在 Python 的选择，因为本任务要求在数据提取前排除小孔。
4. 去重事实包含窗口内履历标记，保持日统计与其它视图差异。全组 CODE 计数形成分母后再筛选展示序列，Total 汇总计数后求比。
5. 应用服务强制启用产品范围；空配置/完全失效选择返回空而不查询全部产品。SQL 参数绑定、EXISTS 防关联倍增，时间转换仅在仓储边界。
6. 前端接收原生 DataFrame/容器形式的公开结果，内部路径和履历标记不进入图形。数据读取失败清除上次结果并显示业务文案。

## Alternatives

- 新建页面：布局高度相同，用户优先要求融合，因此不采用。
- 合并进旧 C3DM 计算：不同来源和分母会隐式影响已有行为，因此不采用。
- 将两类时间窗统一成七天：改变原 Total/玻璃范围，因此不采用。

## Consequences and risks

同页复用展示习惯，独立业务口径便于测试。代价是两个区域各有 section/filter 状态，不能自动复用彼此的结果。新模块的 SQL 依赖 PostgreSQL 路径函数，测试方言须保持语义一致。

读取权限检查发现缺陷表 SELECT 被拒绝（42501），真实数据对账与性能验证仍待完成；不提供替代数据降级。不得将隔离 E2E 宣称为生产联调成功。

## Verification

聚焦单元/SQL 集成/架构测试和孔区、旧边框浏览器 E2E 通过。执行细节与未核实项见来源分析。实现位于 `feat/ijp-hole` 工作分支；尚未提交、合并或发布。

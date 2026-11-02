# 异常片／Lot 只读 API

Status: ready-for-agent
Date: 2026-11-02
Source: docs/dev_docs/dev_spec/inline_domain/monitor/feat-API.md

## Approved decisions

用户已确认共同读取／HTTP测试边界、SPC→其余Inline→Q-Time/Yield→集成交付的执行顺序。
同时提供OOS/OOC，以事件区分；Yield/Q-Time仅有OOS。包含RS的Sheet/Lot及Yield/Q-Time Lot，不推断Lot内各片。
必要字段必须完整，资料不足的来源返回不可用；用户随后修订：Yield厂别必须由维护资料提供，站点可NULL。Q-Time站点与层别均采用起点f_step。

## Contract

GET /api/v1/sheet-anomalies；一产品一分类，上一完整ISO周，事件默认OOS。支持factory/entity_type/entity_id过滤、limit(1..500)及身份／查询绑定短期游标。
业务字段：分类、发生站点、部门、层别、监控参数、异常事件；另含对象类型、对象编号、产品型号、发生时间、起始站点、命中边界。ID为字符串，时间显式上海时区。Yield发生站点唯一可空例外；无站点时需维护描述以获得层别，不能伪造完整性。
仅明确False释放；True/Delete/空/非法均不释放。Q-Time保留原决策台账；Yield补持久人工决定，默认True、历史跨发布保留。
API只读本地发布结果及维护元数据，不查询数据库、不重算、不补文件。工序字典通过独立有界同步命令发布；Yield厂别/可选站点/描述由维护表提供。
公开政策在读取及计数分页前应用，所有响应不含flag、路径、原始状态、决策和异常堆栈。政策失效关闭输出。
鉴权、启用产品与调用方范围交集、限流、并发／缓存上限、安全错误。缺失/损坏/覆盖不足/现有规则过期/必需字段缺失503；有效空200；401/403/422/429；快照过期410。
不将Inline缓存TTL伪装成数据时效。QTime/Yield保留各自原有有效性。

## Verification seams

已批准：现有文件端口的临时工作簿、应用公开服务、进程内HTTP客户端。验证源文件字节保留、禁止SQL/生产替身、政策/授权变化、稳定分页、Flag保留、周边界、OOC/RS粒度、QTime起点、Yield厂别必需站点可空。
保留现有报表回归、后端依赖守卫及适用完整回归。对外只有API，不新增业务UI。

## Out of scope

趋势/能力/IJP比例/备件/IQC、图像/原始量测、任意历史、HTTP写入/强刷、直接改业务资产、公网部署及未经确认合并。

## Delivery

可运行代码、隔离测试、依赖与环境模板、字典同步及维护命令、references/domain/inline_domain/monitor下使用说明、架构与知识路由、ADR草案、提交及证据。合并是development-flow独立确认点。

## Tickets

issues/01-spc-http.md → issues/02-inline-events.md → issues/03-lot-release.md → issues/04-delivery.md

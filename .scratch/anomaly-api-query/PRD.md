# API2.2 查询与返回字段

Status: ready-for-agent

Approved 2026-11-02: inclusive Shanghai calendar dates; return available sources and explicitly mark failed sources; execute01 ->02 ->03 at the existing HTTP and real temporary-workbook seams. This approval supersedes the draft's pending date/failure paragraphs below.

Partial-response contract: meta.complete=false and meta.failed_sources list authorized product/sub_domain/event_type and a constant result_unavailable code. No paths, internal flags or exception text. A successful empty source counts as available. If every selected source fails, retain503 error result_unavailable and include the safe failed_sources list in error. A request-level timeout/overload/configuration failure remains an ordinary safe error. No new business record fields.
Source: 用户2026-11-02指定feat-API2.2，原文件保留在原工作区，包含用户未提交改动。

## Problem Statement

调用方必须逐个指定产品和来源才能取数，不能直接查询全部授权异常，也缺少发生站点、层别、监控参数和日期范围筛选。默认100行需分页，不符合默认完整取数要求；起始站点与QTime发生站点重复。

## Solution

所有查询参数可省略，不带参数时读取上海时区上一完整ISO周、全部当前授权且启用产品／来源的OOS明细，不限制返回条数。可指定过滤字段及时间范围；保留显式分页并解释cursor。删除起始站点，仅提出其他返回字段建议，不实施扩展。

## User Stories

1. 调用方不传筛选即可获取全部授权OOS。
2. 调用方可限定产品、分类或厂别。
3. 调用方可按发生站点、层别、监控参数精确筛选。
4. 调用方可查询包含首尾的自定义日期。
5. 调用方可单独填写时间边界并使用另一端默认值。
6. 调用方默认获取全部行，也可显式分页。
7. 调用方能区分部分结果和完整空结果。
8. 调用方能定位失败的产品与分类。
9. 调用方遇到范围或公开结果变化可重新分页。
10. 维护者保留本地只读、释放及来源有效性边界。

## Contract

- 可选product、sub_domain、factory、step_id、layer、parameter_name、entity_type、entity_id，省略表示当前授权范围全部，普通文本精确匹配；event_type默认OOS。
- 时间边界可选，默认上一ISO周周一／周日；上海时区YYYY-MM-DD日期，包含首尾两天。本地发布覆盖不足不得补算或查询数据库。
- limit省略表示无条数截断，显式正整数时分页，保持既有500最大分页大小；既有字节／缓存／超时／并发上限不被取消。cursor仅与显式limit及相同筛选条件一起使用。
- 默认全来源的OOC只选择支持OOC的四Inline；显式Yield／QTime+OOC仍422。认证保持必需，未授权显式请求403。
- 跨来源返回可用部分，meta.complete=false并列出安全失败来源；全部失败503。不会隐瞒缺失来源后伪装为完整结果。
- Yield发生站点／层别仍NULL；删除起始站点字段。其他返回字段建议另作说明，不添加实测值、阈值、单位、分组、异常ID等字段。

## Testing decisions

既有HTTP seam验证可选范围、授权／禁用产品、无limit超过100行、显式分页、日期／筛选参数、字段移除、错误与游标失效。真实临时工作簿贯通各域公开读取器，时间窗口不能在固定周过滤后再次筛选导致漏行。源字节保持不变；DB／Office／维护目录写入护栏保护两个工作区。

## Execution view

三个纵向ticket：01零参数授权聚合与完整取数；02时间窗口和新增字段筛选；03全局游标、安全回归、文档及已授权服务更新。01→02→03。沿用现有测试边界；本次内部扩展将在API上层协调，域仍各自拥有发布有效性和选择逻辑。

## Out of scope

新增异常引擎、源数据刷新、工作簿改写、数据库查询、授权扩大、任意资源路径或绕过人工Flag、实现未来返回字段、推送远端、操作原报表服务。

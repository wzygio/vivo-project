# Task：Pipeline复用

## 待决策点
1. D2（CTQ 修饰口径，关键）：切换。但你要注意文件路径已经发生了改变，所有产品的ctq数据都已经被汇总到了一个文件中
2. D3（AOI 归属）：monitor 中的AOI，其实对应的就是aoi_tt。因此，aoi_rs 报表不在本次范围。但aoi_tt在。

## 修正点
针对您提出的将public pipline拆分为两段的方案，我基本同意，但有几处需要修正：
1. 段1：“白名单过滤 + data_type 注入”和“异常点过滤 + 主制程追溯”都写入了`src\inline_domain\infrastructure\spc\spc_repository.py`中：这绝不可接受，spc应该是与其它业务模块平行的模块，所有可以复用的逻辑都应放入`src\inline_domain\infrastructure\measurement`中
2. 段2：按照标准的DDD设计，各个模块的repository，应该直接对应各个模块的application层。然而按照你的设计，我们必须先把各个模块的repository汇总到你设计的`SheetFeaturesService`层中，然后再提供给各个模块的service。你确定这样是符合逻辑的吗？

## Workflow
1. 对于段1：请直接执行。然后将当前`src\inline_domain\infrastructure`的架构规范总结为一篇技术文档，写入：`references\domain\Inline_domain`
2. 对于段2：请您最后审核一下您的方案，如果需要修正请进行修正。无论如何，请按照最终方案完成改造`docs\dev_docs\generated\Inline_domain\monitor_data_reuse_evaluation_and_design.md`

## Goal
请按照`developement-flow`这一skill进行开发，并不断优化直至E2E测试通过。


# Yield Lot 自动归属与API空层别

Status: ready-for-agent
Mode: development-flow general
Source: 用户2026-11-02指定feat-API需求2.1，明确要求完成自动厂别归属、停止手工Yield归属、层别NULL。该指令修订原ADR-0037中Yield必须维护描述／层别的约定。

## Problem Statement

Yield已在Lot计算中掌握Defect Code对应的defect_group，但发布结果遗漏分组与厂别，导致API依赖重复人工维护，且为无法定位的入库不良填写层别会制造错误精度。

## Solution

原Lot计算及发布路径保存defect_group与factory。三类Array分组映射ARRAY，OLED_Mura映射OLED，两类TP分组映射TP。API消费已发布厂别，Yield站点与层别均NULL，不访问维护字典或Yield归属表。

## User Stories

1. 作为发布者，我希望每条Lot/Code超规自动带出已知分组及厂别，无需重复手工登记。
2. 作为调用方，我希望按真实厂别授权／筛选Lot，并收到NULL层别，而不是推测定位。
3. 作为维护者，我希望原Flag决定、日期、规格、其他产品及已有工作表保持原约定。
4. 作为调用方，我希望未知分组／缺厂别的已释放记录明确不可用，避免缺记录却声称完整。

## Implementation Decisions

- 复用已有Lot纯计算、产品工作簿发布／读取和HTTP公开投影，不新增seam、数据库查询或独立预警引擎。
- 用户补充确认：映射由global.yaml的data_source.defect_group_factory_mapping配置，配置加载器验证后由既有composition注入发布流程，纯计算不读取YAML。该配置属于现有Yield策略签名覆盖的data_source，修改后旧发布按既有规则失效，需要正常重发。
- 新发布Lot结果增加defect_group、factory；未知分组保留原超规，factory为空，不猜厂别、不删除异常。
- 保留旧Lot发布结果对报表／矩阵的读取兼容性；API缺厂别的释放记录503，正常重发后按新字段可用。HTTP不迁移、不重算文件。
- Yield API从发布记录读取factory，发生站点／层别固定NULL。其它五类来源仍必须有真实站点及层别。
- 新建元数据模板只含站点字典。同步仅验证／更新站点字典，历史Yield归属及其他工作表原样保留，不删除维护资料。
- 用户已批准的HTTP/本地文件及共同查询测试边界继续适用；一个端到端ticket即可覆盖本次明确范围。开发实现不默认获得新的服务重启或生产工作簿改写授权。

## Testing Decisions

- 沿用既有纯Lot计算和真实临时发布文件→HTTP seam，六条映射使用需求独立给定的期望值。
- 验证真实文件列、leading-zero身份、Flag持久历史、厂别过滤、空元数据路径、旧无厂别503及正常再发布后可用。
- 非Yield NULL层别必须被拒绝；同步不依赖／修改历史Yield归属。
- 只用临时工作簿，禁止数据库、真实Office及生产文件写入；不为本次测试启动正式或临时服务。

## Out of Scope

重写现有生产工作簿、重算生产数据、修改人工Flag决定、增改授权、推送远端、重启已确认的正式服务。

## Execution view

Ticket01无阻塞：先最小六组Lot计算RED/GREEN，再贯通临时发布→HTTP及兼容性；针对受影响集合回归和静态检查，Standards/Spec独立评审，提交专用分支。沿用既有seams和用户2.1明确合同，不新增需求分支。合并／正式服务更新沿用本会话已明确授权范围；如需要新的运行操作，在具体结果可审阅后提出。

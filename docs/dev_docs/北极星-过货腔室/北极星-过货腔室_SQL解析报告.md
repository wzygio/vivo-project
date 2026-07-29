# 北极星「过货腔室」FineReport SQL 解析报告

> 解析日期：2026-07-29  
> 解析对象：本目录下 5 个 `long_text_*.txt`  
> 核验方式：静态 SQL 拆解、当前 `inline_domain` SPC 实现对照、通过
> `DatabaseManager` 对 PostgreSQL 数据源执行只读元数据与聚合查询。  
> 安全说明：未在本报告中记录连接地址、账号、口令、Sheet/Glass ID 等敏感信息。

## 1. 结论摘要

这 5 个文件不是 5 套独立取数逻辑，而是：

1. 一段完全相同的公共 CTE（第 1～119 行）；
2. 在公共数据集后拼接 5 个不同的 FineReport 图表/告警数据集。

公共 CTE 把 ARRAY、OLED、TP 三张 SPC 时序表统一为同一结构，随后各文件分别生成：

| 文件 | 数据集用途 | 最终粒度 |
|---|---|---|
| [`long_text_0373…`](./long_text_0373FD1D-4E5D-4E7A-98E5-67C77CADDA4D.txt) | 超规 Sheet/Glass 清单 | 产品 × 站点 × 参数 × 告警类型 |
| [`long_text_193F…`](./long_text_193F78B3-0CEA-4E4B-BE1F-0D15E175573C.txt) | 按过货时间排列的单片点位箱线图 | 物料 × 站点 × 参数 |
| [`long_text_64F4…`](./long_text_64F468F2-007B-476F-8981-9FFB57BC5F70.txt) | 全查询区间规格线、均值、标准差、CPK | 产品 × 站点 × 参数 |
| [`long_text_8B68…`](./long_text_8B684237-155E-4F41-8ECA-E22189A2BEC0.txt) | 月/周/日点位分布箱线图 | 周期 × 厂别 × 产品 × 站点 × 参数 |
| [`long_text_BA37…`](./long_text_BA373569-2FFE-4848-AC4D-363C2B462531.txt) | 按设备/腔室排列的单片点位箱线图 | 设备/腔室 × 物料 × 站点 × 参数 |

最重要的技术与业务结论如下：

- SQL 中的大量 `CASE WHEN` 确实承载业务规则，主要包括 MQC 产品重命名、
  OLED 特定参数物理异常清洗、OOS/OOC/SOOS 分类、周期窗口选择以及
  “站点参数应追溯主设备还是腔室”的路由。
- SQL 具有明显的人工维护和复制痕迹。FineReport 很可能自动保存/导出了模板数据集，
  `long_text_<UUID>` 文件名也可能是提取工具生成的；但 SQL 主体不像统一查询生成器的产物，
  更像人工编写后在多个数据集中复制、局部改尾部。
- 原 SQL 存在可实质改变报表结果的风险：白名单 JOIN 放大量测行、点位去重键不完整、
  OOC/OOS 优先级与编号设计矛盾、物理清洗范围可能提前删除真实 OOS 点、
  设备履历 JOIN 可能产生多对多放大。
- 当前 Python SPC 实现已经在点位去重、分层、缓存和规则可测试性方面明显优于原 SQL，
  但没有完整复刻 MQC 映射、`299.99` 哨兵过滤、工序类型过滤以及主设备/腔室履历追溯。
  当前仓储层的结束日期过滤也与 FineReport 不一致，需要单独确认。

## 2. 五个数据集的公共数据流

```text
FineReport 参数
  ├─ 产品 cmcbProdid / MQC 子产品 cmcbMqcProd
  ├─ 开始、结束日期 dtStartDate / dtEndDate
  └─ 厂别、站点、参数 cmcbFactory / cmcbStep / cmcbParamname
        │
        ▼
prod_code_data
  IMP_SPC_TZBJX + DWR_MES_PRODUCTSPEC_V
  得到允许查询的 product_spec ↔ product_code
        │
        ▼
spc_raw_data
  ├─ ARRAY: eda.spc_tzbjx_array
  ├─ OLED : eda.spc_tzbjx_oled
  └─ TP   : eda.spc_tzbjx_tsp
        │
        ├─ 统一物料 ID、时间、产品、站点、参数、点位和值
        ├─ 关联工序/产品字典
        ├─ 通过 IMP_SPC_TZBJX 限定 data_type = SPC
        ├─ 排除 VIEW/AOI 工序、±299.99 和 OLED 特定物理异常值
        └─ dense_rank 标记“最新记录”
        │
        ▼
五个报表尾部
  ├─ 告警清单
  ├─ 单片箱线图
  ├─ CPK/规格线
  ├─ 月周日箱线图
  └─ 设备/腔室箱线图
```

五个文件第 1～119 行经空白归一化后的内容哈希完全一致，说明它们来自同一公共片段的复制。

## 3. FineReport 参数层

SQL 中的 `${...}` 不是 PostgreSQL 语法，而是 FineReport 在执行前处理的模板表达式。

| 参数 | 作用 | SQL 中的处理 |
|---|---|---|
| `dtStartDate` | 查询开始日期 | `>= start::timestamp` |
| `dtEndDate` | 查询结束日期 | `<= end::timestamp + interval '1day'` |
| `cmcbProdid` | 产品多选/单选 | 动态拼接 `IN (...)` |
| `cmcbMqcProd` | MQC 二级产品 | OLED MQC 分支精确匹配 |
| `cmcbFactory` | 厂别 | 动态拼接工序字典的 `B.factory IN (...)` |
| `cmcbStep` | 站点 | 动态拼接 `step_id IN (...)` |
| `cmcbParamname` | 参数 | 动态拼接 `param_name IN (...)` |
| `daycnt` | 保留最近 N 个日周期 | 周期数据集中的 `rn1 <= daycnt` |
| `weekcnt` | 保留最近 N 个周周期 | 周期数据集中的 `rn1 <= weekcnt` |
| `monthcnt` | 保留最近 N 个月周期 | 周期数据集中的 `rn1 <= monthcnt` |

### 3.1 日期边界

原 SQL 使用：

```sql
start_time >= dtStartDate
and start_time <= dtEndDate + interval '1day'
```

这会额外包含“结束日期下一天 00:00:00”这一瞬间。更稳定的写法应为半开区间：

```sql
start_time >= :start_time
and start_time < :end_date + interval '1day'
```

### 3.2 参数注入方式

`${IF(..., "AND ... IN ('" + value + "')")}` 是字符串级 SQL 拼装，不是数据库绑定参数。
它依赖 FineReport 对多选值和引号的正确转义。若参数来源可被外部控制，存在 SQL 注入和
特殊字符破坏语句的风险。

## 4. 公共 CTE 逐层拆解

### 4.1 `prod_code_data`：产品范围

来源：

- `eda.IMP_SPC_TZBJX`：参数白名单/分类配置；
- `DWR_MES_PRODUCTSPEC_V`：产品字典，`prod_id → product_code`。

输出：

- `product_code`：展示/筛选用产品代码；
- `productspecname`：三厂 SPC 明细表中的物理产品规格键。

`SELECT DISTINCT` 的作用是把白名单表中的多参数记录压缩为产品规格列表。

### 4.2 `spc_raw_data`：三厂结构归一化

| 统一字段 | ARRAY | OLED | TP |
|---|---|---|---|
| `factory` | 常量 `ARRAY` | 常量 `OLED` | 常量 `TP` |
| `mat_id` | `sheet_id` | `glass_id` | `glass_id` |
| `start_time` | `sheet_start_time` | `glass_start_time` | `glass_start_time` |
| `product_spec` | 原字段 | 原字段 | 原字段 |
| `equip_id` | 原字段 | 原字段 | 原字段 |
| `pre_sub_unit_id` | 原字段 | 原字段 | 原字段 |
| `step_id` / `param_name` / `site_name` / `param_value` | 同名 | 同名 | 同名 |

三个分支通过 `UNION ALL` 合并。此处选择 `UNION ALL` 是合理的，因为厂别字段已能区分来源，
没有必要支付全行去重成本。

### 4.3 工序字典

`dwr_mes_processoperationspec_v` 提供：

- `description`，拼成 `step_id-description`；
- `factory`，响应厂别筛选；
- `detail_oper_code_type`，排除 `VIEW` 和 `AOI`。

虽然写的是 `LEFT JOIN`，但后续：

```sql
and B.detail_oper_code_type not in ('VIEW', 'AOI')
```

会同时排除 `B` 不匹配产生的 NULL，因此语义上已经接近 `INNER JOIN`。

### 4.4 SPC 白名单

三个分支都使用：

```sql
inner join EDA.IMP_SPC_TZBJX x
  on a.step_id = x.step_id
 and a.param_name = x.parmtername
 and x.data_type = 'SPC'
```

它的业务意图是：只保留被配置为 SPC 的监控指标。

但 JOIN 缺少以下产品约束之一：

```sql
x.productspecname = a.product_spec
```

或：

```sql
x.productspecname = s.productspecname
```

只读数据库核验显示，SPC 白名单中有 141 组 `step_id + parmtername` 重复键，
重复组平均约 9.45 行、最大 11 行。因此当前 JOIN 会把一条量测复制多次。

更符合意图且不会放大数据的写法是：

```sql
where exists (
    select 1
    from eda.imp_spc_tzbjx x
    where x.productspecname = a.product_spec
      and x.step_id = a.step_id
      and x.parmtername = a.param_name
      and x.data_type = 'SPC'
)
```

### 4.5 MQC 产品重命名

OLED 分支将基础产品 `MQC` 改写为：

```text
MQC-<IMP_SPC_TZBJX_MQC.PRODCODE>
```

映射键为 `PPID`。这是一个业务上的“基础产品 + MQC 子产品”复合标识。
只有 OLED 分支应用该规则，说明 MQC 子产品语义属于 OLED 工序数据。

当 `cmcbProdid = 'MQC'` 时，又通过第二个 `CASE WHEN` 强制匹配
`cmcbMqcProd`；非 MQC 查询则条件恒真。

### 4.6 哨兵值与物理异常清洗

三厂共同排除：

```sql
abs(param_value) != 299.99
```

这表明 `±299.99` 被当作设备错误码、缺测占位值或无效量测值，而不是正常数据。

OLED 还对站点 21230 和 21200 应用硬编码范围。其性质不是统一的 USL/LSL：

| 站点 | 参数 | SQL 保留范围 | 业务判断 |
|---|---|---:|---|
| 21230 | `B_0_CIE_Y` | `(0.0415, 0.0555)` | 色度/光学物理清洗 |
| 21230 | `Bi_0` | `(114, 210)` | 亮度类物理清洗 |
| 21230 | `G_0_CIE_X` | `(0.2161, 0.2761)` | 色度物理清洗 |
| 21230 | `G_0_E` | `(127, 223)` | 光学物理清洗 |
| 21230 | `JNCD_30/45/60` | `(0, 100)` | 极宽防呆范围，不是实际规格线 |
| 21230 | `R_0_CIE_X` | `(0.6721, 0.6861)` | 色度物理清洗 |
| 21230 | `R_0_E` | `(40, 100)` | 光学物理清洗 |
| 21200 | 10 个 `PPA_*_X/Y` | `[-6, 6]` | PPA 偏移清洗 |

数据库核验表明，这些范围与 `dwd_imp_dv_param_spec` 中的规格并不一致。例如：

- PPA 的数据库规格可为 `[-6.5, 6.5]`，SQL 却先删掉 `< -6` 或 `> 6` 的点；
- `JNCD_30/45/60` 数据库上限约为 6～6.5，SQL 的 `< 100` 显然只是在排除物理坏值；
- `Bi_0`、`G_0_CIE_X` 等参数的 SQL 清洗边界也不等于所有产品的规格边界。

因此这些条件应该被定义为“原始数据有效域”，不能与 SPC 规格判定混为一谈。
其中 PPA 等范围比规格更窄，会在告警计算前删除真实接近或超过规格的点，使结果偏乐观。

### 4.7 “最新记录”标记

原 SQL 使用：

```sql
dense_rank() over (
  partition by mat_id, step_id, param_name
  order by start_time desc
) as rn
```

后续统一过滤 `rn = 1`。

问题在于 `site_name` 不在分区键中。若同一 Sheet 的不同点位写入时间不完全一致，
只有等于整片最大时间戳的点位会留下，较早写入的其他合法点位会丢失。

如果目标是“每个点位保留最新一次记录”，应使用：

```sql
row_number() over (
  partition by product, mat_id, step_id, param_name, site_name
  order by start_time desc, update_time desc
)
```

当前 Python DAO 已采用包含 `site_name` 的 `ROW_NUMBER`，方向正确。

## 5. 告警业务逻辑：OOS、SOOS、OOC

两个文件复用了同一状态判断：

- `long_text_0373...txt` 第 138～141 行；
- `long_text_193F...txt` 第 156～159 行。

规则含义：

| 状态 | SQL 条件 | 业务含义 |
|---|---|---|
| OOC | Sheet 点位均值 `< LCL` 或 `> UCL` | 均值越过管控线，过程失控 |
| OOS | Sheet 点位均值 `< LSL` 或 `> USL` | Sheet 均值越过规格线 |
| SOOS | 任一点位值 `< LSL` 或 `> USL` | 均值可能正常，但存在单点超规 |
| 正常波动 | 以上均未命中 | 当前规则下正常 |

### 5.1 实际优先级存在矛盾

字符串被编码为：

```text
1-OOS
2-SOOS
3-OOC
4-正常波动
```

随后用 `min(chaogui_flag)` 选择最小编号。这显示设计者想表达：

```text
OOS > SOOS > OOC > 正常
```

但内层 `CASE` 实际先判断 OOC，再判断 OOS、SOOS：

```sql
case
  when mean outside LCL/UCL then '3-OOC'
  when mean outside LSL/USL then '1-OOS'
  when point outside LSL/USL then '2-SOOS'
  else '4-正常波动'
end
```

在通常的 `LSL < LCL < UCL < USL` 关系下，均值越过规格线时也已经越过管控线，
因此会先被判成 OOC，OOS 分支可能不可达。`min()` 无法修复已经被前置 CASE 吞掉的 OOS。

当前 Python `apply_spc_rules` 明确采用：

```text
OOS > SOOS > OOC > OK
```

这与编号所表达的原始设计意图一致，但与 FineReport SQL 的实际运行结果不一致。

### 5.2 空规格线

比较 NULL 会得到 UNKNOWN，`CASE WHEN` 不会命中。因此：

- 单边规格可以自然工作；
- 完全缺失规格时会落入“正常波动”，而不是“无规格/不可判定”。

后者会掩盖规格配置缺失。建议增加独立的 `NO_SPEC` 状态。

## 6. 五个 SQL 尾部逐文件解析

### 6.1 `long_text_0373...txt`：告警物料清单

范围：第 120～154 行。

处理过程：

1. `spc_raw_data` 关联规格表；
2. 只保留 `rn = 1`；
3. 按物料、站点、参数计算 Sheet 均值并判断状态；
4. `min('1-OOS', '2-SOOS', ...)` 选择每片最严重状态；
5. 删除“正常波动”；
6. 按产品、站点、参数、状态聚合；
7. `string_agg(distinct mat_id, ', ')` 拼接异常物料清单。

最终输出适合 FineReport 的异常汇总表或钻取入口，不是明细事实表。

风险：

- 物料 ID 被拼成一个长字符串，无法分页、排序或高效下钻；
- 告警优先级受前述 CASE 顺序问题影响；
- 白名单 JOIN 放大会增加窗口函数和字符串聚合成本。

### 6.2 `long_text_193F...txt`：按过货时间的单片箱线图

范围：第 120～174 行。

每个物料、站点、参数计算：

- Q1：`percentile_cont(0.25)`；
- 中位数：`percentile_cont(0.5)`；
- Q3：`percentile_cont(0.75)`；
- 原始最小值、最大值；
- 物料状态。

外层把上下须裁到 Tukey 1.5 IQR 范围：

```text
upper whisker = min(max_value, Q3 + 1.5 × IQR)
lower whisker = max(min_value, Q1 - 1.5 × IQR)
```

这只是输出箱体和须，没有输出离群点明细。X 轴标签为
`mat_id + start_time`，并按过货时间排序。

`dwd_calendar` 在本数据集中没有提供最终字段或筛选条件，属于无效 JOIN。

### 6.3 `long_text_64F4...txt`：规格线、均值、标准差与 CPK

范围：第 120～147 行。

聚合粒度：

```text
prod_code × step_id × param_name
```

输出：

- `LSL/TARGET/USL/LCL/CL/UCL`；
- 全查询区间点位均值；
- 点位样本标准差 `STDDEV`；
- CPK；
- 包含数据和所有规格线的 Y 轴最小/最大范围。

CPK 公式为：

```text
min((USL - mean) / (3σ), (mean - LSL) / (3σ))
```

与标准双边 CPK 公式一致，但有两个实现差异：

- `σ = 0` 时 SQL 返回 0；当前 Python `calculate_cpk` 在均值位于规格内时返回正无穷；
- SQL 使用全区间原始点位标准差，不是 Sheet Mean 标准差。

当前 `config/spc_config.yaml` 将 Python 的周期 `period_sigma_source` 配置为
`point_value`，因此标准差来源与这段 FineReport SQL 的方向一致。

数据库核验显示规格表的 `prod_code + step_id + param_name` 当前没有重复键，
但 SQL 仍用 `AVG(spec_line)` 会把未来重复规格静默平均，建议改成显式唯一性校验。

### 6.4 `long_text_8B68...txt`：月/周/日箱线图

范围：第 120～209 行。

`FANEL_DATA`（应是 `PANEL_DATA` 的拼写错误）把每条点位数据复制到三种周期：

- DAY：`date_timekey`；
- WEEK：`dwd_calendar.week_timekey`；
- MONTH：`dwd_calendar.month_timekey`。

随后按 `step_id + param_name` 对各周期倒序 `dense_rank`，使用
`daycnt/weekcnt/monthcnt` 截取最近周期，再计算每个周期的 Q1、中位数、Q3 和 Tukey 须。

数据库日历格式核验：

```text
日：YYYYMMDD
周：YYYYWww
月：YYYYMmm
```

因此 `RIGHT(DATE_VALUE, 4/3)` 会生成 `MMDD`、`Www`、`Mmm` 展示键。

风险：

- `rn1` 分区没有 `prod_code` 和 `factory`。当一次查询包含多个产品/厂别且数据覆盖不齐时，
  “最近 N 个周期”不是按每个产品/厂别独立计算；
- 点位数据被 `UNION ALL` 复制三次，查询区间大时成本较高；
- 周期缺失时不补空桶，前端轴可能跳期；
- 当前 Python 页面箱线图使用最近 2 个有数据月、3 个有数据周、7 个有数据日；
  原 SQL 使用 FineReport 参数控制数量，两者展示契约不同。

### 6.5 `long_text_BA37...txt`：主设备/腔室追溯箱线图

范围：第 120～228 行。这是五段 SQL 中业务含量最高的一段。

#### 6.5.1 规格表驱动的追溯路由

`dwd_imp_dv_param_spec` 提供：

- `main_step_id`：测量参数真正应追溯的上游主工序；
- `main_eqp_type`：应按整机 `EQP` 还是腔室 `CHAMBER` 展示。

空值回退：

```text
main_step_id 为空 → 当前 step_id
main_eqp_type 为空 → EQP
```

只读数据库核验得到：

| `main_eqp_type` | 规格记录数 | 涉及产品数 |
|---|---:|---:|
| EQP | 510 | 8 |
| CHAMBER | 251 | 4 |
| 空值 | 4 | 4 |

这说明 `EQP/CHAMBER` 不是临时判断，而是规格元数据中的正式业务维度。

#### 6.5.2 厂别 × 设备类型路由

| 厂别 | `main_eqp_type` | 履历来源 | 设备/腔室字段 |
|---|---|---|---|
| ARRAY | EQP | `DWT_INOUT_SHT` | `EQP_ID`，失败回退量测表 `equip_id` |
| ARRAY | CHAMBER | `DWT_INOUT_SUB_UNIT_SHT` / `DWT_INOUT_UNIT_SHT` | `SUB_UNIT_ID` 优先，其次 `UNIT_ID` |
| OLED | EQP | `DWT_INOUT_GLS` | `EQP_ID`，失败回退量测表 `equip_id` |
| TP | EQP | `DWT_INOUT_GLS` | `EQP_ID`，失败回退量测表 `equip_id` |
| TP | CHAMBER | `DWT_INOUT_SUB_UNIT_GLS` | `SUB_UNIT_ID` |
| OLED | CHAMBER | `DWT_INOUT_SUB_UNIT_GLS` + `DWD_MES_OLED_OPER_LAYER_V` | `SUB_UNIT_ID` |

OLED 还把 `21200-CVD1`～`21200-CVD4` 在符合腔室命名条件时归一为
`21200-CVD`，以便与 `main_step_id` 匹配。

#### 6.5.3 设备时间的真实含义

SQL 中原本准备使用设备履历的 `event_timekey`，但整段已被注释。当前实际使用：

```sql
to_char(a.start_time, 'YYYYMMDDHH24miss') as EQ_TIMEKEY
```

因此 `EQ_TIMEKEY` 实际是 SPC 量测时间，不是设备过站时间。字段名会误导后续维护者。

#### 6.5.4 履历时间窗口

设备履历查询范围为：

```text
dtStartDate - 1 个月 ～ dtEndDate
```

业务假设是：量测对应的主工序过站事件最多提前一个月发生。

#### 6.5.5 风险

- 各履历表 JOIN 没有选择“最接近量测时间的一条 OUT 记录”，同一物料和工序存在多次过站时，
  会产生多对多放大；
- `cmcbFactory` 通过 `CASE WHEN ... THEN 1=1 ELSE 1=2 END` 禁用不相关 JOIN。
  当厂别为空或是多选值时，所有精确厂别判断都可能失败，CHAMBER 路由返回空；
- 腔室名称依赖固定字符位置和 `CVD/SPU/DRE/OVE` 命名约定，设备命名格式变化会静默漏数；
- 最终仍按物料分组计算箱线图，并非“设备整体分布”；设备只是排序/着色维度；
- 当前 Python 页面仅从量测表 `unit_id` 粗略推导腔室，没有实现这套
  `main_step_id + main_eqp_type + 履历表` 的正式追溯逻辑。

## 7. 主要表及业务角色

| 表 | 角色 |
|---|---|
| `eda.spc_tzbjx_array` | ARRAY Sheet 点位量测时序表 |
| `eda.spc_tzbjx_oled` | OLED Glass 点位量测时序表 |
| `eda.spc_tzbjx_tsp` | TP Glass 点位量测时序表 |
| `eda.IMP_SPC_TZBJX` | 产品/站点/参数白名单及 SPC、CTQ、空类型分类 |
| `eda.IMP_SPC_TZBJX_MQC` | MQC 的 PPID → 子产品代码映射 |
| `DWR_MES_PRODUCTSPEC_V` | `prod_id → product_code` 产品字典 |
| `DWR_MES_PROCESSOPERATIONSPEC_V` | 工序描述、厂别、工序类型 |
| `DWD_IMP_DV_PARAM_SPEC` | 规格线、管控线、目标值、主工序、EQP/CHAMBER 路由 |
| `DWD_CALENDAR` | 日 → 周/月的制造日历映射 |
| `DWT_INOUT_SHT` | ARRAY Sheet 工序过站整机履历 |
| `DWT_INOUT_SUB_UNIT_SHT` | ARRAY Sheet 子单元/腔室履历 |
| `DWT_INOUT_UNIT_SHT` | ARRAY Sheet 单元履历 |
| `DWT_INOUT_GLS` | OLED/TP Glass 工序过站整机履历 |
| `DWT_INOUT_SUB_UNIT_GLS` | OLED/TP Glass 子单元/腔室履历 |
| `DWD_MES_OLED_OPER_LAYER_V` | OLED 工序与腔室的归一化映射 |

数据库中的白名单分类分布为：

| 类型 | 记录数 | 产品规格数 |
|---|---:|---:|
| SPC | 1333 | 31 |
| CTQ | 558 | 31 |
| 空/NULL | 342 | 25 |

这与当前 Python 将空类型归类为 AOI、SPC/CTQ 分应用入口处理的设计一致。

## 8. 与当前 `inline_domain` SPC 实现的对照

| 维度 | FineReport SQL | 当前 Python 实现 | 影响 |
|---|---|---|---|
| 三厂路由 | 三段 SQL 手写 `UNION ALL` | `factory_meta` 循环构造三段 SQL | Python 更易维护 |
| 物料字段统一 | `sheet_id/glass_id → mat_id` | 统一命名为 `sheet_id` | 语义等价 |
| 点位去重 | 不含 `site_name` 的 `dense_rank` | 含 `site_name` 的 `ROW_NUMBER`，仓储层再去重 | Python 更符合 Sheet 多点测量 |
| 产品白名单 | SQL 内 JOIN，但未约束产品 | 先按产品查白名单，再在 Repo merge | Python 避免了跨产品 JOIN 放大 |
| 白名单粒度 | `step_id + param_name` | 只按 `param_name` merge | Python 若同名参数在不同站点分类不同，仍有歧义 |
| SPC/CTQ | SQL 固定 `data_type='SPC'` | 应用服务强制 SPC 或 CTQ | Python 边界更清晰 |
| MQC 子产品 | `MQC-<PRODCODE>` | 未实现 | MQC 报表口径不等价 |
| 工序排除 | 排除 `VIEW/AOI` | 未做同类工序字典过滤 | 原始量测范围可能不同 |
| 哨兵值 | 排除 `abs(value)=299.99` | 没有通用 299.99 规则 | Python 可能保留设备错误码 |
| 参数排除 | 无全局 `LOSS` 规则 | SQL 与历史快照兜底排除 `LOSS` | Python 范围更窄 |
| 物理异常 | OLED CASE 硬编码 | 外部 Excel/CSV 规则 | Python 可配置、可迁移，但应核验规则文件是否覆盖原范围 |
| 规格绑定 | SQL JOIN 规格表 | Repo 查询规格，再由 Core merge；支持 YAML 覆盖 | Python 更可测试 |
| Sheet 特征 | SQL 窗口均值/点位极值 | Core 计算 mean/max/min | 业务目标一致 |
| 状态优先级 | 实际 OOC 先于 OOS | 明确 OOS > SOOS > OOC | 结果可能显著不同 |
| CPK σ 来源 | 原始点位 | 当前全局配置为 `point_value` | 当前配置方向一致 |
| CPK 的 PPA | 可参与 | Python 明确排除参数名含 PPA | 能力报表口径不同 |
| 月周日箱线图轴 | FineReport 参数控制数量 | 页面取最近 2 个有数据月、3 个有数据周、7 个有数据日 | 展示契约不同 |
| 箱线图 | SQL 预聚合 Q1/Q2/Q3/须 | Python 把点位交给 Plotly | Python 可保留离群点与交互 |
| 设备/腔室 | 主工序 + EQP/CHAMBER + 履历追溯 | 优先使用量测表 `unit_id` 粗略生成标签 | Python 尚未等价迁移 |
| 缓存/容灾 | 依赖 FineReport/数据库 | 3 个月 Parquet 快照、8h TTL、失败降级 | Python 更健壮 |
| 人工修饰 | SQL 无 | Sheet OOS 与 CPK decoration 文件 | Python 结果还包含额外业务修饰 |

### 8.1 当前 Python 的结束日期差异

FineReport 会取到结束日全天，并额外包含下一日零点。

当前 `SpcRepository.get_spc_measurements` 将 `req_end_dt` 解析为结束日 00:00:00，
再执行：

```python
df_final[time_col] <= req_end_dt
```

这会在仓储层排除结束日 00:00:00 之后的记录。虽然 DAO 查询本身取到
`23:59:59`，但随后又被仓储层截掉。服务层再使用 `< end + 1 day` 已无法恢复这些记录。

建议双方统一为半开区间 `[start_date, end_date + 1 day)`，并增加边界测试。

### 8.2 当前 Python 的腔室图并非原 SQL 等价实现

页面 `_resolve_chamber_column` 首选量测表的 `unit_id`，再取连字符前缀作为腔室标签。
原 SQL 则由规格表决定应追溯 EQP 还是 CHAMBER，并可能切换到上游 `main_step_id`，
然后查询过站履历表。

因此当前页面的“按腔室排序”只能视为近似展示，不能直接宣称已经复刻
FineReport 的“过货腔室”业务口径。

## 9. SQL 是否由软件自动生成

### 9.1 支持“存在软件生成/导出环节”的证据

- `${IF(LEN(...))}` 是 FineReport 模板表达式；
- 文件名为 `long_text_<UUID>.txt`，明显像解析/导出工具对 CPT 长文本字段的命名；
- 大量显式 `varchar(100/200)`、`where 1=1` 和恒真/恒假 CASE 门控具有模板化风格；
- 五个数据集共享同一大段前缀，可能由报表设计器复制数据集后修改。

### 9.2 支持“SQL 主体由人编写/维护”的证据

- 注释直接写有《V3 SPC监控for北极星-V2》和作者“王书”；
- OLED 阈值、设备命名位置、CVD 工序归一化等规则高度业务特定；
- 存在 `FANEL_DATA` 拼写错误、大小写和缩进不一致；
- 存在被注释掉的旧字段和未使用的日历 JOIN；
- 状态编号与 CASE 顺序互相矛盾；
- 五份公共 CTE 是整段复制，而不是一个参数化、结构稳定的生成器输出；
- 设备 JOIN 用 `CASE WHEN ... THEN 1=1 ELSE 1=2` 开关，是常见的报表人工技巧。

### 9.3 判断

较可信的结论是：

> FineReport 或 CPT 提取工具生成了存储载体、模板参数包装和 UUID 文件名；  
> SQL 主体则很可能由开发者/报表工程师手工编写，经过复制粘贴和长期局部修改。

它不像从领域模型自动生成的 SQL，也不像 ORM 生成 SQL。复杂度主要来自把数据访问、
数据清洗、SPC 规则、图表预聚合和设备追溯全部塞进了单个模板数据集。

## 10. 风险分级

### P0：应优先核验

1. **白名单 JOIN 跨产品放大数据**  
   已由数据库聚合核验。应增加 `productspecname` 条件或改用 `EXISTS`。

2. **告警优先级错误**  
   编号表示 OOS > SOOS > OOC，但 CASE 实际先判 OOC，可能让 OOS 不可达。

3. **物理清洗提前删除真实超规点**  
   特别是 PPA 的 ±6 清洗比部分 ±6.5 规格更窄。

4. **结束日期口径不一致**  
   FineReport 多包含下一日零点；当前 Python 仓储层少包含结束日绝大部分数据。

### P1：结果或血缘风险

1. 最新记录分区缺少 `site_name`；
2. 设备履历 JOIN 未选最近/唯一过站事件，可能多对多放大；
3. 厂别 CASE 门控只适合精确单选厂别；
4. 当前 Python 未复刻 MQC 子产品和主设备/腔室血缘；
5. 缺规格数据被默认为正常，而不是不可判定。

### P2：维护性与性能

1. 五份公共 CTE 重复，修复极易漏改；
2. 未使用的 `dwd_calendar` JOIN；
3. 图表统计在 SQL 中重复实现；
4. `string_agg` 把明细变成长文本；
5. 参数使用字符串拼接而非绑定；
6. `AVG(spec_line)` 会静默掩盖未来重复规格。

## 11. 推荐的可测试拆分方式

建议将原 SQL 逻辑拆成以下稳定层次：

```text
01 selected_product_specs
   产品筛选 + MQC 映射

02 raw_measurements
   三厂字段统一，仅做源表级过滤

03 valid_measurements
   SPC 白名单 EXISTS
   299.99/配置化物理异常过滤
   VIEW/AOI 工序排除

04 latest_site_measurements
   产品 + 厂别 + 物料 + 站点 + 参数 + 点位级去重

05 measurements_with_spec
   绑定规格线、管控线、目标、main_step_id、main_eqp_type

06 sheet_features
   Sheet Mean/Min/Max

07 sheet_status
   明确且可测试的 OOS > SOOS > OOC > OK / NO_SPEC

08 period_distribution
   月/周/日点位或 Sheet Mean 分布

09 capability
   明确 sigma_source 的 CPM/CPK

10 equipment_lineage
   每个物料选择最接近量测时间的唯一过站设备/腔室
```

实施原则：

- 公共取数只保留一个实现；
- 清洗阈值放入配置表并标注“物理有效域”或“业务规格”；
- 白名单使用 `EXISTS` 或带完整产品键的唯一 JOIN；
- 日期统一使用半开区间；
- 设备追溯使用 `LATERAL JOIN ... ORDER BY event_time DESC LIMIT 1`
  或预先建立唯一事件 CTE；
- SQL 负责获取稳定事实数据，箱线图统计和告警规则放在可单测的 Core 层；
- 对 FineReport 与 Python 选取同一小时间窗做逐层行数、唯一键、Sheet Mean、
  状态和 CPK 对账，而不是只比较最终图形。

## 12. 建议的对账检查点

正式迁移或替换原报表时，至少记录以下行数和唯一键数量：

| 检查点 | 建议唯一键 |
|---|---|
| 三厂原始量测 | 厂别 + 产品规格 + 物料 + 站点 + 参数 + 点位 + 时间 |
| 白名单后 | 厂别 + 产品 + 物料 + 站点 + 参数 + 点位 + 时间 |
| 点位最新记录 | 厂别 + 产品 + 物料 + 站点 + 参数 + 点位 |
| Sheet 特征 | 厂别 + 产品 + 物料 + 站点 + 参数 |
| Sheet 状态 | 同 Sheet 特征键 |
| 周期能力 | 厂别 + 产品 + 站点 + 参数 + 周期类型 + 周期 |
| 设备血缘 | 厂别 + 产品 + 物料 + 主工序 + EQP/CHAMBER 类型 |

推荐专项样例：

- 一个普通 ARRAY 产品；
- 一个普通 OLED 产品；
- 一个 TP 产品；
- 一个 MQC 子产品；
- 21200 PPA 参数；
- 21230 光学参数；
- 一个 `main_eqp_type = EQP` 指标；
- 一个 `main_eqp_type = CHAMBER` 指标；
- 一个缺规格、单边规格、σ=0、单点超规但均值正常的边界样例。

只有上述中间层都能对账，才能判断 FineReport 与 Python 的差异是有意修正还是迁移遗漏。

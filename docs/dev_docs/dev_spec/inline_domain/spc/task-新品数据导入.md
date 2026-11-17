# Task：SPC新品数据导入
- 身份：我是一家OLED制造厂的员工，该项目是一个用于给客户展示厂内数据的报表。
- 任务：现在需要优化`app\pages\SPC监控报表.py`，在其中导入一款新产品“Z622”的数据

## References
1. 如果有不理解的词汇，请参考：`04-Resources\domain\GLOSSARY.md`
2. 数据探查与读取：
    - 数据库链接例程：`src\shared_kernel\infrastructure\db_handler.py`
	- 数据库链接凭证：`.env`

### Data Source
针对“Z622”这款产品，我们需要更换数据源：
1. 当前数据源：
```
--eda.spc_tzbjx_array   — ARRAY SPC 测量明细表
--eda.spc_tzbjx_oled    — OLED SPC 测量明细表
--eda.spc_tzbjx_tsp     — TP SPC 测量明细表
```
我们需要在当前表中提取监控站点及参数，参考语句如下： 
```
-- 查看监控站点
--eda.spc_tzbjx_array   — ARRAY SPC 测量明细表
--eda.spc_tzbjx_oled    — OLED SPC 测量明细表
--eda.spc_tzbjx_tsp     — TP SPC 测量明细表
SELECT DISTINCT
    sta.step_id,
    sta.param_name
FROM eda.spc_tzbjx_array AS sta
WHERE sta.sheet_start_time >= TIMESTAMP '2026-09-01 00:00:00'
  AND sta.sheet_start_time <  TIMESTAMP '2026-11-17 00:00:00'
  AND (sta.step_id <> '15690' OR sta.step_id IS NULL)
  AND EXISTS (
      SELECT 1
      FROM mdw.dwr_mes_productspec AS p
      WHERE p.productspecname = sta.product_spec
        AND p.productcode = 'M626'
  )
```

2. 新数据源：
```
eda.ARRAY_PDS_RESULT_T
```
参考sql语句如下：
```
SELECT DISTINCT ON (aprt.glass_id, aprt.site_name)
    aprt.glass_id,
    aprt.step_id,
    aprt.param_name,
    aprt.site_name,
    aprt.glass_start_time,
    aprt.value
FROM eda.ARRAY_PDS_RESULT_T AS aprt
WHERE aprt.glass_start_time > TIMESTAMP '2026-10-01 00:00:00'
  AND left(aprt.glass_id, 9) = 'L3ZB6BE03%'
  AND aprt.step_id = '11140'
  AND aprt.param_name = 'SE_L2T'
ORDER BY
    aprt.glass_id,
    aprt.site_name,
    aprt.glass_start_time DESC
LIMIT 10;
```

【Hint】
- 我们只需要提取“L3ZB6BE03”这一个lot（lot_id是glass_id的前九位）的相关数据，因此无需筛选产品型号
- 可能需要根据“glass_id - site_name”去重，保留“glass_start_time”最新的一条记录。你可以探查数据库判断是否需要去重
- 该数据表巨大，任何语句都必须筛选分区字段“glass_start_time”

## Requirements
1. 仅需修改spc模块的infrastructure层：`src\inline_domain\infrastructure\spc\spc_repository.py`及相关程序，其余后端、前端逻辑同其它产品数据一致。
2. 以上sql语句仅供参考，可进行优化，复杂的数据处理逻辑可提取到内存中使用python计算。

## Workflow
1. 请先分析当前数据源，提取出需要监控的“站点及参数配置表”，输出一份xlsx文档至`resources\inline_domain\spc`
2. 根据第一步的结果，从新数据源中提取出“Z622”的数据

## Goal
不断迭代优化，直至E2E测试通过
- 除非有权限问题，否则执行到底，决策直接采用默认选项





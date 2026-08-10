# AOI_RS 数据库探查结果

- 生成时间: 2026-08-10T17:36:03
- 数据库版本: `PostgreSQL 9.4.26 (Greenplum Database 6.26.4 build commit:bcbaaead795b417d18644f36f334827f0815cf37) on x86_64-unknown-linux-gnu, compiled by gcc (GCC) 6.4.0, 64-bit compiled on Mar  8 2024 00:39:37`

## 1. RS 明细表 `eda.spc_tzbjx_rs_array`


### 1.x `eda.spc_tzbjx_rs_array`

**列清单（information_schema.columns）：**
```sql
SELECT ordinal_position, column_name, data_type FROM information_schema.columns WHERE table_schema = 'eda' AND table_name = 'spc_tzbjx_rs_array' ORDER BY ordinal_position
```
| ordinal_position | column_name | data_type |
|---|---|---|
| 1 | product_spec | character varying |
| 2 | productcode | character varying |
| 3 | subproductiontype | character varying |
| 4 | wororder | character varying |
| 5 | step_id | character varying |
| 6 | lot_id | character varying |
| 7 | sheet_id | character varying |
| 8 | sheet_start_time | timestamp without time zone |
| 9 | rs_code | character varying |
| 10 | code_qty1 | numeric |
| 11 | spec | numeric |
| 12 | eqp_id | character varying |
| 13 | sub_eqp | character varying |
| 14 | update_time | timestamp without time zone |
| 15 | code_qty | integer |

**样例（LIMIT 5）：**
```sql
SELECT * FROM eda.spc_tzbjx_rs_array LIMIT 5
```
| product_spec | productcode | subproductiontype | wororder | step_id | lot_id | sheet_id | sheet_start_time | rs_code | code_qty1 | spec | eqp_id | sub_eqp | update_time | code_qty |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| G3659FP101FA-007E |  | ESLC | 20006633A01 | 11629 | L3MR57E0HAA | L3MR57E0H02 | 2025-07-31 04:36:52 | A1PPS | 1 | None | NA | NA | 2025-10-16 16:15:35.516768 | 1 |
| G3659FP101FA-007E |  | ESLC | 20006633A01 | 11629 | L3MR57E03AA | L3MR57E0324 | 2025-07-31 19:10:53 | A1DPS | 3 | None | NA | NA | 2025-10-16 16:15:35.516768 | 3 |
| G3659FP101FA-007E |  | ESLC | 20006633A01 | 11629 | L3MR57E0HAA | L3MR57E0H09 | 2025-07-31 04:35:17 | A1PPS | 4 | None | NA | NA | 2025-10-16 16:15:35.516768 | 4 |
| G3659FP101FA-007E |  | ESLC | 20006633A01 | 11629 | L3MR57E03AA | L3MR57E0310 | 2025-07-31 19:10:22 | A1PPS | 3 | None | NA | NA | 2025-10-16 16:15:35.516768 | 3 |
| G3659FP101FA-007E |  | ESLC | 20006633A01 | 11629 | L3MR57E0BAA | L3MR57E0B19 | 2025-07-31 07:00:27 | A1CIP | 15 | None | NA | NA | 2025-10-16 16:15:35.516768 | 15 |

- 识别到 ID 字段: `sheet_id`；过货时间字段: `sheet_start_time`

code_qty 取值分布（10 万行采样）：
```sql
SELECT code_qty, COUNT(*) AS cnt FROM (SELECT code_qty FROM eda.spc_tzbjx_rs_array LIMIT 100000) s GROUP BY code_qty ORDER BY cnt DESC
```
| code_qty | cnt |
|---|---|
| 1 | 27387 |
| 2 | 21174 |
| 3 | 12112 |
| 4 | 7070 |
| 5 | 4955 |
| 6 | 3445 |
| 7 | 2330 |
| 8 | 1918 |
| 9 | 1605 |
| 10 | 1493 |
| 11 | 1362 |
| 14 | 1343 |
| 13 | 1329 |
| 12 | 1307 |
| 15 | 1117 |
| 16 | 1104 |
| 17 | 1048 |
| 18 | 930 |
| 19 | 811 |
| 20 | 741 |
| 21 | 697 |
| 22 | 577 |
| 23 | 538 |
| 24 | 436 |
| 25 | 417 |
| 27 | 338 |
| 26 | 330 |
| 28 | 278 |
| 30 | 201 |
| 29 | 201 |
| 31 | 163 |
| 32 | 152 |
| 33 | 137 |
| 34 | 103 |
| 35 | 85 |
| 37 | 71 |
| 38 | 65 |
| 36 | 62 |
| 39 | 49 |
| 41 | 42 |
| 40 | 42 |
| 42 | 36 |
| 43 | 30 |
| 46 | 30 |
| 44 | 26 |
| 45 | 22 |
| 47 | 20 |
| 50 | 19 |
| 53 | 15 |
| 51 | 14 |
| 58 | 13 |
| 48 | 12 |
| 55 | 10 |
| 57 | 10 |
| 56 | 10 |
| 49 | 10 |
| 60 | 9 |
| 52 | 9 |
| 65 | 8 |
| 64 | 7 |

**存在 lot 相关字段: ['lot_id']**
lot_id 样例：
```sql
SELECT DISTINCT lot_id FROM eda.spc_tzbjx_rs_array LIMIT 10
```
| lot_id |
|---|
| L3MR5A009AA |
| L3MR5900AAA |
| L3MR66010AA |
| L3MY6500CAA |
| L3MY5C014AA |
| L3MR5A002AB |
| L3DH6400UAA |
| L3MR66007AA |
| L3MR5B00NAA |
| L3MR5B049AA |


rs_code distinct 样例（10 万行采样取 20 个，用于判断是否五位代码）：
```sql
SELECT DISTINCT rs_code FROM (SELECT rs_code FROM eda.spc_tzbjx_rs_array LIMIT 100000) s LIMIT 20
```
| rs_code |
|---|
| A3DMR |
| A1CFB |
| A7PFB |
| A8SIP |
| A3SIP |
| A2CIP |
| A3PMR |
| A1PPS |
| A5SIP |
| A2CFB |
| A5PMR |
| A5DMR |
| A4CFB |
| A2DMR |
| A8DBE |
| A7PBH |
| A2PMR |
| A1CIP |
| A3CIP |
| A1DPS |

step_id 全部 distinct 值：
```sql
SELECT DISTINCT step_id FROM eda.spc_tzbjx_rs_array ORDER BY 1
```
| step_id |
|---|
| 11629 |
| 12629 |
| 13629 |
| 15629 |
| 18629 |

最近一个月行数（按 `sheet_start_time` 过滤）：
```sql
SELECT COUNT(*) AS cnt_last_month FROM eda.spc_tzbjx_rs_array WHERE sheet_start_time >= now() - interval '1 month'
```
| cnt_last_month |
|---|
| 70593 |

`sheet_start_time` 时间范围：
```sql
SELECT MIN(sheet_start_time) AS min_t, MAX(sheet_start_time) AS max_t FROM eda.spc_tzbjx_rs_array
```
| min_t | max_t |
|---|---|
| 2025-07-31T04:34:35.000000000 | 2026-08-10T06:30:43.000000000 |

productcode distinct 样例（10 个）：
```sql
SELECT DISTINCT productcode FROM (SELECT productcode FROM eda.spc_tzbjx_rs_array LIMIT 100000) s LIMIT 10
```
| productcode |
|---|
|  |


## 1. RS 明细表 `eda.spc_tzbjx_rs_oled`


### 1.x `eda.spc_tzbjx_rs_oled`

**列清单（information_schema.columns）：**
```sql
SELECT ordinal_position, column_name, data_type FROM information_schema.columns WHERE table_schema = 'eda' AND table_name = 'spc_tzbjx_rs_oled' ORDER BY ordinal_position
```
| ordinal_position | column_name | data_type |
|---|---|---|
| 1 | product_spec | character varying |
| 2 | productcode | character varying |
| 3 | subproductiontype | character varying |
| 4 | wororder | character varying |
| 5 | step_id | character varying |
| 6 | lot_id | character varying |
| 7 | glass_id | character varying |
| 8 | glass_start_time | timestamp without time zone |
| 9 | rs_code | character varying |
| 10 | code_qty1 | numeric |
| 11 | spec | numeric |
| 12 | eqp_id | character varying |
| 13 | sub_eqp | character varying |
| 14 | update_time | timestamp without time zone |
| 15 | code_qty | integer |

**样例（LIMIT 5）：**
```sql
SELECT * FROM eda.spc_tzbjx_rs_oled LIMIT 5
```
| product_spec | productcode | subproductiontype | wororder | step_id | lot_id | glass_id | glass_start_time | rs_code | code_qty1 | spec | eqp_id | sub_eqp | update_time | code_qty |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| G3659FP101FQ-007E |  | ESLC | 20006843F01 | 21329 | F3MR58E13AB | L3MR57E0Q222 | 2025-08-31 18:30:35 | C4BP3 | 0 | None | NA | NA | 2025-10-28 15:27:18.548578 | 0 |
| G3659FP101FQ-007E |  | ESLC | 20006843F01 | 21329 | F3MR58E0MAB | L3MR57E1K242 | 2025-08-29 15:25:55 | C4BP3 | 0 | None | NA | NA | 2025-10-17 14:54:11.962774 | 0 |
| G3659FP101FQ-007E |  | ESLC | 20006843F01 | 21329 | F3MR58E0XAB | L3MR57E04212 | 2025-08-31 07:17:03 | C4BP2 | 0 | None | NA | NA | 2025-10-31 12:23:37.325568 | 0 |
| G3659FP101FQ-007E |  | ESLC | 20006843F01 | 21329 | F3MR58E0XAB | L3MR57E04212 | 2025-08-31 07:17:03 | C4BP3 | 0 | None | NA | NA | 2025-10-28 15:27:18.548578 | 0 |
| G3659FP101FQ-007E |  | ESLC | 20006843F01 | 21329 | F3MR58E12AB | L3MR57E0Q121 | 2025-08-31 17:21:27 | C4BP2 | 0 | None | NA | NA | 2025-10-31 12:23:37.325568 | 0 |

- 识别到 ID 字段: `glass_id`；过货时间字段: `glass_start_time`

code_qty 取值分布（10 万行采样）：
```sql
SELECT code_qty, COUNT(*) AS cnt FROM (SELECT code_qty FROM eda.spc_tzbjx_rs_oled LIMIT 100000) s GROUP BY code_qty ORDER BY cnt DESC
```
| code_qty | cnt |
|---|---|
| 0.0 | 39765.0 |
| 1.0 | 6872.0 |
| 2.0 | 2004.0 |
| 3.0 | 866.0 |
| 4.0 | 445.0 |
| 5.0 | 352.0 |
| 6.0 | 189.0 |
| 7.0 | 93.0 |
| 9.0 | 86.0 |
| 8.0 | 53.0 |
| 10.0 | 3.0 |
| nan | 2.0 |

**存在 lot 相关字段: ['lot_id']**
lot_id 样例：
```sql
SELECT DISTINCT lot_id FROM eda.spc_tzbjx_rs_oled LIMIT 10
```
| lot_id |
|---|
| F3MR5C0HUAB |
| F3MR5C07WAB |
| F3MR5B0USAB |
| F3MR5B0HGAA |
| F3MR5A02PAB |
| F3MY6509TAB |
| F3MR5A0HKAB |
| F3MR610AHAB |
| F3MR5B0F4AB |
| F3MY6102TAB |


rs_code distinct 样例（10 万行采样取 20 个，用于判断是否五位代码）：
```sql
SELECT DISTINCT rs_code FROM (SELECT rs_code FROM eda.spc_tzbjx_rs_oled LIMIT 100000) s LIMIT 20
```
| rs_code |
|---|
| C4BP2 |
| C4BP3 |
| C4PL0 |
| T3DMR |
| T3WSC |
| T0GMR |
| T1PMR |
| C4BP1 |
| C4CP3 |
| T1DMR |
| T3PMR |
| C4PCP3 |

step_id 全部 distinct 值：
```sql
SELECT DISTINCT step_id FROM eda.spc_tzbjx_rs_oled ORDER BY 1
```
| step_id |
|---|
| 21329 |
| 43629 |

最近一个月行数（按 `glass_start_time` 过滤）：
```sql
SELECT COUNT(*) AS cnt_last_month FROM eda.spc_tzbjx_rs_oled WHERE glass_start_time >= now() - interval '1 month'
```
| cnt_last_month |
|---|
| 3478 |

`glass_start_time` 时间范围：
```sql
SELECT MIN(glass_start_time) AS min_t, MAX(glass_start_time) AS max_t FROM eda.spc_tzbjx_rs_oled
```
| min_t | max_t |
|---|---|
| 2025-08-29T06:59:39.000000000 | 2026-08-10T05:41:10.000000000 |

productcode distinct 样例（10 个）：
```sql
SELECT DISTINCT productcode FROM (SELECT productcode FROM eda.spc_tzbjx_rs_oled LIMIT 100000) s LIMIT 10
```
| productcode |
|---|
| M626 |
|  |
| M673 |
| Z517 |
| M678 |
| Z571 |


## 1. RS 明细表 `eda.spc_tzbjx_rs_tsp`


### 1.x `eda.spc_tzbjx_rs_tsp`

**列清单（information_schema.columns）：**
```sql
SELECT ordinal_position, column_name, data_type FROM information_schema.columns WHERE table_schema = 'eda' AND table_name = 'spc_tzbjx_rs_tsp' ORDER BY ordinal_position
```
| ordinal_position | column_name | data_type |
|---|---|---|
| 1 | product_spec | character varying |
| 2 | productcode | character varying |
| 3 | subproductiontype | character varying |
| 4 | wororder | character varying |
| 5 | step_id | character varying |
| 6 | lot_id | character varying |
| 7 | glass_id | character varying |
| 8 | glass_start_time | timestamp without time zone |
| 9 | rs_code | character varying |
| 10 | code_qty1 | numeric |
| 11 | spec | numeric |
| 12 | eqp_id | character varying |
| 13 | sub_eqp | character varying |
| 14 | update_time | timestamp without time zone |
| 15 | code_qty | integer |

**样例（LIMIT 5）：**
```sql
SELECT * FROM eda.spc_tzbjx_rs_tsp LIMIT 5
```
| product_spec | productcode | subproductiontype | wororder | step_id | lot_id | glass_id | glass_start_time | rs_code | code_qty1 | spec | eqp_id | sub_eqp | update_time | code_qty |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| G3659FP101FT-007P |  | P | 10000032T01 | 43629 | T3MR5900PAA | L3MR57E1H262 | 2025-09-16 18:38:29 | T3DMR | 1 | None | NA | NA | 2026-01-07 21:10:59.070097 | 1 |
| G3659FP101FT-007P |  | P | 10000032T01 | 43629 | T3MR59004AA | L3MR57E15191 | 2025-09-11 14:52:54 | T1DMR | 1 | None | NA | NA | 2025-10-16 16:28:44.713102 | 1 |
| G3659FP101FT-007P |  | P | 10000121T01 | 43629 | T3MR5901WAA | L3MR57E1Q032 | 2025-09-23 09:08:19 | T1DMR | 1 | None | NA | NA | 2026-01-08 19:29:15.332680 | 1 |
| G3659FP101FT-007P |  | P | 10000121T01 | 43629 | T3MR5902DAA | L3MR58E1U082 | 2025-09-24 14:39:31 | T1DMR | 1 | None | NA | NA | 2026-01-08 19:29:15.332680 | 1 |
| G3659FP101FT-007P |  | P | 10000121T01 | 43629 | T3MR59018AA | L3MR57E1B291 | 2025-09-22 01:31:57 | T3DMR | 1 | None | NA | NA | 2026-01-07 21:10:59.070097 | 1 |

- 识别到 ID 字段: `glass_id`；过货时间字段: `glass_start_time`

code_qty 取值分布（10 万行采样）：
```sql
SELECT code_qty, COUNT(*) AS cnt FROM (SELECT code_qty FROM eda.spc_tzbjx_rs_tsp LIMIT 100000) s GROUP BY code_qty ORDER BY cnt DESC
```
| code_qty | cnt |
|---|---|
| 1 | 62521 |
| 2 | 13184 |
| 3 | 4564 |
| 4 | 2110 |
| 5 | 1136 |
| 6 | 633 |
| 7 | 440 |
| 8 | 286 |
| 0 | 191 |
| 9 | 178 |
| 10 | 120 |
| 11 | 87 |
| 12 | 76 |
| 13 | 41 |
| 15 | 35 |
| 14 | 31 |
| 18 | 24 |
| 16 | 23 |
| 17 | 17 |
| 20 | 16 |
| 25 | 14 |
| 19 | 10 |
| 23 | 10 |
| 22 | 8 |
| 21 | 8 |
| 27 | 6 |
| 26 | 6 |
| 24 | 6 |
| 28 | 5 |
| 35 | 5 |
| 38 | 2 |
| 30 | 2 |
| 32 | 2 |
| 29 | 2 |
| 62 | 1 |
| 36 | 1 |
| 41 | 1 |
| 34 | 1 |
| 45 | 1 |
| 31 | 1 |
| 33 | 1 |
| 42 | 1 |
| 40 | 1 |
| 47 | 1 |

**存在 lot 相关字段: ['lot_id']**
lot_id 样例：
```sql
SELECT DISTINCT lot_id FROM eda.spc_tzbjx_rs_tsp LIMIT 10
```
| lot_id |
|---|
| T3MY61003AA |
| T3MR5903WAA |
| T3MR6501CAA |
| T3MR5B0BDAA |
| T3MR5C00SAA |
| T3MR64002AA |
| T3MY6401RAA |
| T3MR5B0A4AA |
| T3MY6601PAA |
| T3MR5B013AA |


rs_code distinct 样例（10 万行采样取 20 个，用于判断是否五位代码）：
```sql
SELECT DISTINCT rs_code FROM (SELECT rs_code FROM eda.spc_tzbjx_rs_tsp LIMIT 100000) s LIMIT 20
```
| rs_code |
|---|
| T3WPE |
| T3DMR |
| T3WSC |
| T1PMR |
| T0GMR |
| T1DMR |
| T3PMR |
| T3PPL |

step_id 全部 distinct 值：
```sql
SELECT DISTINCT step_id FROM eda.spc_tzbjx_rs_tsp ORDER BY 1
```
| step_id |
|---|
| 43629 |

最近一个月行数（按 `glass_start_time` 过滤）：
```sql
SELECT COUNT(*) AS cnt_last_month FROM eda.spc_tzbjx_rs_tsp WHERE glass_start_time >= now() - interval '1 month'
```
| cnt_last_month |
|---|
| 4226 |

`glass_start_time` 时间范围：
```sql
SELECT MIN(glass_start_time) AS min_t, MAX(glass_start_time) AS max_t FROM eda.spc_tzbjx_rs_tsp
```
| min_t | max_t |
|---|---|
| 2025-09-03T05:42:34.000000000 | 2026-08-09T22:11:10.000000000 |

productcode distinct 样例（10 个）：
```sql
SELECT DISTINCT productcode FROM (SELECT productcode FROM eda.spc_tzbjx_rs_tsp LIMIT 100000) s LIMIT 10
```
| productcode |
|---|
| M626 |
|  |
| M673 |
| M678 |
| Z571 |


## 2. 过货视图 `eda.spot_eda_array_view_sht_v`


### `eda.spot_eda_array_view_sht_v`

**列清单：**
```sql
SELECT ordinal_position, column_name, data_type FROM information_schema.columns WHERE table_schema = 'eda' AND table_name = 'spot_eda_array_view_sht_v' ORDER BY ordinal_position
```
| ordinal_position | column_name | data_type |
|---|---|---|
| 1 | sheet_id | character varying |
| 2 | sheet_start_time | timestamp without time zone |
| 3 | lot_id | character varying |
| 4 | lot_type | character varying |
| 5 | product_spec | character varying |
| 6 | sheet_end_time | timestamp without time zone |
| 7 | equipment_id | character varying |
| 8 | unit_id | character varying |
| 9 | cassette_id | character varying |
| 10 | recipe_id | character varying |
| 11 | ppid | character varying |
| 12 | slot_no | character varying |
| 13 | port_id | character varying |
| 14 | panel_count | numeric |
| 15 | sheet_judge | character varying |
| 16 | workorder | character varying |
| 17 | pre_step_id | character varying |
| 18 | pre_equip_id | character varying |
| 19 | pre_recipe_id | character varying |
| 20 | pre_sub_unit_id | character varying |
| 21 | operator_id | character varying |
| 22 | sheet_total_defect | numeric |
| 23 | sheet_map_image_name | character varying |
| 24 | sheet_map_image_judge | character varying |
| 25 | sheet_dm_image_name | character varying |
| 26 | mura_type_image_name1 | character varying |
| 27 | mura_type_image_name2 | character varying |
| 28 | mura_type_image_name3 | character varying |
| 29 | video_pixel_size | numeric |
| 30 | recipe_modify_user | character varying |
| 31 | recipe_modify_time | timestamp without time zone |
| 32 | auto_mode | character varying |
| 33 | program_ver | character varying |
| 34 | scan_count | character varying |
| 35 | step_id | character varying |
| 36 | eqp_type | character varying |

**样例（LIMIT 3）：**
```sql
SELECT * FROM eda.spot_eda_array_view_sht_v LIMIT 3
```
| sheet_id | sheet_start_time | lot_id | lot_type | product_spec | sheet_end_time | equipment_id | unit_id | cassette_id | recipe_id | ppid | slot_no | port_id | panel_count | sheet_judge | workorder | pre_step_id | pre_equip_id | pre_recipe_id | pre_sub_unit_id | operator_id | sheet_total_defect | sheet_map_image_name | sheet_map_image_judge | sheet_dm_image_name | mura_type_image_name1 | mura_type_image_name2 | mura_type_image_name3 | video_pixel_size | recipe_modify_user | recipe_modify_time | auto_mode | program_ver | scan_count | step_id | eqp_type |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| L3C50AE0305 | 2020-10-27 14:31:56 | L3C50AE03AA | C | G3657FB107FA-106E | 2020-10-27 14:53:19 | 3ATV01 |  | AN0022 | 03100 | 03100 | 15 | 2 | 4 | G |  | 13100 | 3AFC04 | D100C23001 | C23001 | V0042528 | 628 | L3C50AE0305_13120_20201027140832_MA.JPG |  | L3C50AE0305_13120_20201027140832_DM4.JPG |  |  |  | 0.173 |  | 2020-10-26 18:38:21 |  | 2020-10-16 08:43:37 | 21 | 13129 | VIEW |
| L3C50AE0310 | 2020-10-26 13:16:18 | L3C50AE03AA | C | G3657FB107FA-106E | 2020-10-26 13:25:10 | 3ATV01 |  | AN0022 | MACONLYMODE | MACONLYMODE | 20 | 1 | 2 | G |  | 11100 | 3AFC01 | D100C21001 | C21001 | V0049938 | 183 | L3C50AE0310_11120_20201026101034_MA.JPG |  | L3C50AE0310_11120_20201026101034_DM4.JPG |  |  |  | 0.173 |  | 2020-10-23 15:44:46 |  | 2020-10-16 08:43:37 | 21 | 11129 | VIEW |
| L3C50AE1006 | 2020-10-31 10:46:23 | L3C50AE10AA | C | G3657FP101FA-008E | 2020-10-31 10:59:28 | 3ATV01 |  | AN0180 | MARSE01002000201 | MARSE01002000201 | 16 | 1 | 2 | G |  | 10800 | 3AOV01 | MARSE01080000001 | E01080000001 | V0050070 | 333 | L3C50AE1006_10020_20201031103102_MA.JPG |  | L3C50AE1006_10020_20201031103102_DM4.JPG |  |  |  | 0.173 |  | 2020-10-29 11:05:16 |  | 2020-10-29 11:00:54 | 21 | 10029 | VIEW |

- 识别到 ID 字段: `sheet_id`；时间字段: `sheet_start_time`；step_id: `step_id`；product_spec: `product_spec`

step_id 全部 distinct 值：
```sql
SELECT DISTINCT step_id FROM eda.spot_eda_array_view_sht_v ORDER BY 1
```
| step_id |
|---|
| 10009 |
| 10029 |
| 1002B |
| 10109 |
| 10129 |
| 1062B |
| 10809 |
| 10829 |
| 11129 |
| 11329 |
| 11409 |
| 11429 |
| 11439 |
| 11609 |
| 11629 |
| 11729 |
| 12129 |
| 12229 |
| 12429 |
| 12439 |
| 12609 |
| 12629 |
| 12709 |
| 12729 |
| 13029 |
| 13129 |
| 13229 |
| 13429 |
| 13439 |
| 13609 |
| 13629 |
| 13829 |
| 14129 |
| 14429 |
| 14439 |
| 14629 |
| 14809 |
| 14822 |
| 14829 |
| 15229 |
| 15429 |
| 15439 |
| 15520 |
| 15609 |
| 15629 |
| 16429 |
| 16439 |
| 16629 |
| 17229 |
| 17429 |
| 17439 |
| 17629 |
| 17829 |
| 17929 |
| 18229 |
| 18429 |
| 18439 |
| 18609 |
| 18629 |
| 19409 |


## 2. 过货视图 `eda.spot_eda_oled_view_gls_v`


### `eda.spot_eda_oled_view_gls_v`

**列清单：**
```sql
SELECT ordinal_position, column_name, data_type FROM information_schema.columns WHERE table_schema = 'eda' AND table_name = 'spot_eda_oled_view_gls_v' ORDER BY ordinal_position
```
| ordinal_position | column_name | data_type |
|---|---|---|
| 1 | glass_id | character varying |
| 2 | glass_start_time | timestamp without time zone |
| 3 | lot_id | character varying |
| 4 | lot_type | character varying |
| 5 | product_spec | character varying |
| 6 | glass_end_time | timestamp without time zone |
| 7 | equipment_id | character varying |
| 8 | unit_id | character varying |
| 9 | cassette_id | character varying |
| 10 | recipe_id | character varying |
| 11 | ppid | character varying |
| 12 | slot_no | character varying |
| 13 | port_id | character varying |
| 14 | panel_count | numeric |
| 15 | glass_judge | character varying |
| 16 | workorder | character varying |
| 17 | pre_step_id | character varying |
| 18 | pre_equip_id | character varying |
| 19 | pre_recipe_id | character varying |
| 20 | pre_sub_unit_id | character varying |
| 21 | ab_flag | character varying |
| 22 | operator_id | character varying |
| 23 | mask_id | character varying |
| 24 | mask_cycle_count | numeric |
| 25 | glass_total_defect | numeric |
| 26 | glass_map_judge | character varying |
| 27 | glass_map_name | character varying |
| 28 | glass_dm_name | character varying |
| 29 | mura_type_image_name1 | character varying |
| 30 | mura_type_image_name2 | character varying |
| 31 | mura_type_image_name3 | character varying |
| 32 | video_pixel_size | numeric |
| 33 | spectrum_r_image | character varying |
| 34 | spectrum_g_image | character varying |
| 35 | spectrum_b_image | character varying |
| 36 | spectrum_w_image | character varying |
| 37 | step_id | character varying |
| 38 | eqp_type | character varying |

**样例（LIMIT 3）：**
```sql
SELECT * FROM eda.spot_eda_oled_view_gls_v LIMIT 3
```
| glass_id | glass_start_time | lot_id | lot_type | product_spec | glass_end_time | equipment_id | unit_id | cassette_id | recipe_id | ppid | slot_no | port_id | panel_count | glass_judge | workorder | pre_step_id | pre_equip_id | pre_recipe_id | pre_sub_unit_id | ab_flag | operator_id | mask_id | mask_cycle_count | glass_total_defect | glass_map_judge | glass_map_name | glass_dm_name | mura_type_image_name1 | mura_type_image_name2 | mura_type_image_name3 | video_pixel_size | spectrum_r_image | spectrum_g_image | spectrum_b_image | spectrum_w_image | step_id | eqp_type |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| L3MA0BE08041 | 2020-11-30 10:27:38 | F3MA0BE05AA | E | G3657FP101FQ-008E | 2020-11-30 10:31:24 | 3CTV01 |  |  | MARSE0211290001 | MARSE0211290001 | 23 | 1 | 95 | G |  |  |  |  |  |  | V0042536 |  | None | None | Y | C/VIEW/21129/3CTV01/L3MA/0B/E08/SOURCE/L3MA0BE08041.IMG/L3MA0BE08041_21120_20201129185114_MA.JPG | C/VIEW/21129/3CTV01/L3MA/0B/E08/SOURCE/L3MA0BE08041.IMG/L3MA0BE08041_21120_20201129185114_DM6.JPG |  |  |  | 142 | None | None | None | None | 21129 | VIEW |
| L3MA0BE08041 | 2020-11-30 10:12:15 | F3MA0BE05AA | E | G3657FP101FQ-008E | 2020-11-30 10:15:35 | 3CTV01 |  |  | MARSE0211290001 | MARSE0211290001 | 23 | 1 | 95 | G |  |  |  |  |  |  | V0042536 |  | None | None | Y | C/VIEW/21129/3CTV01/L3MA/0B/E08/SOURCE/L3MA0BE08041.IMG/L3MA0BE08041_21120_20201129185114_MA.JPG | C/VIEW/21129/3CTV01/L3MA/0B/E08/SOURCE/L3MA0BE08041.IMG/L3MA0BE08041_21120_20201129185114_DM6.JPG |  |  |  | 142 | None | None | None | None | 21129 | VIEW |
| L3MA0BE08041 | 2020-11-30 08:44:04 | F3MA0BE05AA | E | G3657FP101FQ-008E | 2020-11-30 08:52:35 | 3CTV01 |  |  | MARSE0211290001 | MARSE0211290001 | 23 | 1 | 95 | G |  |  |  |  |  |  | V0042536 |  | None | None | Y | C/VIEW/21129/3CTV01/L3MA/0B/E08/SOURCE/L3MA0BE08041.IMG/L3MA0BE08041_21120_20201129185114_MA.JPG | C/VIEW/21129/3CTV01/L3MA/0B/E08/SOURCE/L3MA0BE08041.IMG/L3MA0BE08041_21120_20201129185114_DM6.JPG |  |  |  | 142 | None | None | None | None | 21129 | VIEW |

- 识别到 ID 字段: `glass_id`；时间字段: `glass_start_time`；step_id: `step_id`；product_spec: `product_spec`

step_id 全部 distinct 值：
```sql
SELECT DISTINCT step_id FROM eda.spot_eda_oled_view_gls_v ORDER BY 1
```
| step_id |
|---|
|  |
| 21129 |
| 21209 |
| 21219 |
| 21229 |
| 21239 |
| 21249 |
| 21259 |
| 212A9 |
| 212B9 |
| 212C9 |
| 21329 |
| 21339 |
| 21409 |
| 21429 |
| 214A9 |
| 22129 |
| 22209 |
| 22229 |
| 22329 |
| 22409 |
| 22429 |
| 22509 |
| 22529 |
| 22B29 |
| 2B229 |
| 2B239 |
| 2B329 |
| 2B3A9 |
| 2B3B9 |
| 2G219 |
| 2G229 |
| 2G239 |
| 2G329 |
| 2G3A9 |
| 2G3B9 |
| 2R219 |
| 2R229 |
| 2R239 |
| 2R329 |
| 2R3A9 |
| 2R3B9 |
| 2W219 |
| 2W4A9 |
| 2W4C9 |


## 2. 过货视图 `eda.spot_eda_tsp_view_gls_v`


### `eda.spot_eda_tsp_view_gls_v`

**视图 `eda.spot_eda_tsp_view_gls_v` 不存在，模糊搜索：**
```sql
SELECT table_schema, table_name FROM information_schema.tables WHERE table_name ILIKE '%spot_eda_tsp_view_%' ORDER BY 1, 2 LIMIT 30
```
_(0 行)_


## 3. 产品字典 `mdw.dwr_mes_productspec`

**列清单：**
```sql
SELECT ordinal_position, column_name, data_type FROM information_schema.columns WHERE table_schema = 'mdw' AND table_name = 'dwr_mes_productspec' ORDER BY ordinal_position
```
| ordinal_position | column_name | data_type |
|---|---|---|
| 1 | factoryname | character varying |
| 2 | productspecname | character varying |
| 3 | productspecversion | character varying |
| 4 | description | character varying |
| 5 | checkstate | character varying |
| 6 | activestate | character varying |
| 7 | createtime | timestamp without time zone |
| 8 | createuser1 | character varying |
| 9 | checkouttime | timestamp without time zone |
| 10 | checkoutuser | character varying |
| 11 | productiontype | character varying |
| 12 | producttype | character varying |
| 13 | productquantity | numeric |
| 14 | subproducttype | character varying |
| 15 | subproductunitquantity1 | numeric |
| 16 | subproductunitquantity2 | numeric |
| 17 | processflowname | character varying |
| 18 | processflowversion | character varying |
| 19 | estimatedcycletime | numeric |
| 20 | multiproductspectype | character varying |
| 21 | productspec2name | character varying |
| 22 | productspec2version | character varying |
| 23 | productspectype | character varying |
| 24 | productcounttoxaxis | numeric |
| 25 | productcounttoyaxis | numeric |
| 26 | glasstype | character varying |
| 27 | categorytype | character varying |
| 28 | rootproductspec | character varying |
| 29 | productspecgroup | character varying |
| 30 | innerpackingquantity | numeric |
| 31 | outerpackingquantity | numeric |
| 32 | palletquantity | numeric |
| 33 | productcode | character varying |
| 34 | interface_time | timestamp without time zone |
| 36 | oldproductcode | character varying |

**样例（LIMIT 5）：**
```sql
SELECT * FROM mdw.dwr_mes_productspec LIMIT 5
```
| factoryname | productspecname | productspecversion | description | checkstate | activestate | createtime | createuser1 | checkouttime | checkoutuser | productiontype | producttype | productquantity | subproducttype | subproductunitquantity1 | subproductunitquantity2 | processflowname | processflowversion | estimatedcycletime | multiproductspectype | productspec2name | productspec2version | productspectype | productcounttoxaxis | productcounttoyaxis | glasstype | categorytype | rootproductspec | productspecgroup | innerpackingquantity | outerpackingquantity | palletquantity | productcode | interface_time | oldproductcode |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| OLED | G3199TR101FQ-T7EP | 00001 | 199TR Q-cell半成品-TFE（5T&4T） | CheckedIn | NotActive | 2025-06-19 13:01:47 | MDM | None | None | P | Glass | 30 | Panel | 720 | 0 | None | None | 0 | MultiMain | None | None | F | 24 | 30 | L | LTPS | None | None | 0 | 0 | 0 | H1-5T2C | 2026-08-10 17:03:11.832373 | H3 |
| OLED | G3669FP101FW-00EP | 00001 | 6.69 FP Q-Cell半成品（剥离后） | CheckedIn | NotActive | 2025-06-19 13:01:47 | MDM | None | None | P | Glass | 30 | Panel | 190 | 0 | None | None | 0 | MultiMain | None | None | B | 19 | 10 | L | LTPS | None | None | 0 | 0 | None | C19 | 2026-08-10 17:03:11.832373 | L8 |
| OLED | G3670FP103FQ-00CP | 00001 | 6.70FP Q-CELL半成品-TFE | CheckedIn | NotActive | 2025-06-19 13:01:47 | MDM | None | None | P | Glass | 30 | Panel | 190 | 0 | None | None | 0 | MultiMain | None | None | F | 19 | 10 | L | LTPS | None | None | 0 | 0 | 0 | M511 | 2026-08-10 17:03:11.832373 | V2 |
| POSTCELL | G3192TR101FC-007E | 00001 | 1.92 TR Cell半成品（华山C514+HV2） | CheckedIn | Active | 2025-07-15 10:28:00 | MDM | None | None | E | Panel | 30 | Panel | 1224 | 0 | PME192TR1011 | 00001 | 0 | MultiMain | None | None | P | 36 | 34 | L | LTPS | None | None | 360 | 1 | 0 | C514 | 2026-08-10 17:03:11.832373 | CS |
| OLED | G3160TR101FQ-T0JE | 00001 | 1.60 TR Q-Cell半成品（W532项目-ViP-VM9M） | CheckedIn | Active | 2025-07-23 10:28:00 | MDM | None | None | E | Glass | 30 | Panel | 912 | 0 | FME160TR1011 | 00001 | 0 | MultiMain | None | None | F | 24 | 38 | L | LTPS | None | None | 0 | 0 | 0 | W532 | 2026-08-10 17:03:11.832373 | AB |

- productspecname 候选字段: `productspecname`；productcode 候选字段: `productcode`

映射样例 `productspecname` → `productcode`：
```sql
SELECT DISTINCT productspecname, productcode FROM mdw.dwr_mes_productspec LIMIT 20
```
| productspecname | productcode |
|---|---|
| G3678FP109FW-00AP | F156 |
| G3678TS103FT-00CE | M702 |
| G3B40QH004GC-004E | O678 |
| G3791FP101FW-018P | None |
| G3689FA101FW-T0JP | None |
| LTPOZDSJFA | LTPOZDSJ |
| G3659FP103FC-007E | Z553 |
| G3B14QH001GW-T09P | None |
| G3795FP101FC-T0EE | C471 |
| G3675FP101FA-T1HP | None |
| G3689FA101FC-T3DP | None |
| G3675FP103FT-00EP | C550 |
| LTPOZBSJFA-FHS | LTPOZBSJ-FHS |
| G3B14QH001GC-T29E | 天青石中尺寸预研项目 |
| G3150TR102FC-T2EE | L311 |
| G3B16QH001GC-T49P | None |
| G3792FP004FT-00EP | D127 |
| G3758FP101FQ-00EP | None |
| G3678TS101FQ-T1CP | HBS |
| G3657FB107FT-106E | C5 |


## 4. RS 规格表 `dwd_imp_rs_code_xishu_fo_tzsbjx`

**定位规格表（rs_code / xishu 模糊搜索）：**
```sql
SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_name ILIKE '%rs_code%' OR table_name ILIKE '%xishu%' ORDER BY 1, 2
```
| table_schema | table_name | table_type |
|---|---|---|
| mdw | dwd_imp_rs_code_xishu_fo_int | BASE TABLE |
| mdw | dwd_imp_rs_code_xishu_fo_tzsbjx | BASE TABLE |
| mdw | imp_tp_rs_code_remark | BASE TABLE |
| mdw | imp_tp_rs_code_spec_new | BASE TABLE |


**选定目标表: `mdw.dwd_imp_rs_code_xishu_fo_tzsbjx`**

**列清单：**
```sql
SELECT ordinal_position, column_name, data_type FROM information_schema.columns WHERE table_schema = 'mdw' AND table_name = 'dwd_imp_rs_code_xishu_fo_tzsbjx' ORDER BY ordinal_position
```
| ordinal_position | column_name | data_type |
|---|---|---|
| 1 | prod_code | character varying |
| 2 | factory | character varying |
| 3 | type_flag | character varying |
| 4 | step_id | character varying |
| 5 | rs_code | character varying |
| 6 | code_desc | character varying |
| 7 | spec | numeric |
| 8 | owner_id | character varying |
| 9 | interface_time | timestamp without time zone |
| 10 | main_step_id | character varying |
| 11 | main_eqp_type | character varying |

**样例（LIMIT 5）：**
```sql
SELECT * FROM mdw.dwd_imp_rs_code_xishu_fo_tzsbjx LIMIT 5
```
| prod_code | factory | type_flag | step_id | rs_code | code_desc | spec | owner_id | interface_time | main_step_id | main_eqp_type |
|---|---|---|---|---|---|---|---|---|---|---|
| M673 | OLED | LOT_RATIO | 21329 | C4CP3 | 膜层彩色异物 | 0.2 | V0012306 | 2026-06-04 10:49:43.508682 | 21200 | EQP |
| M673 | OLED | GLASS_ID | 21329 | C4CP3 | 膜层彩色异物 | 4 | V0012306 | 2026-06-04 10:49:43.508682 | 21200 | EQP |
| M673 | OLED | MWD_RATIO | 21329 | C4CP3 | 膜层彩色异物 | 0.02 | V0012306 | 2026-06-04 10:49:43.508682 | 21200 | EQP |
| M678 | ARRAY | LOT_RATIO | 15629 | A4DBH | 干刻责ILD1孔异常 | 3 | V0130368 | 2025-11-06 16:20:39.512225 | 14500 | CHAMBER |
| Z571 | ARRAY | MWD_RATIO | 15629 | A5PMR | PHT责M3残留 | 3 | V0130368 | 2026-05-20 11:19:14.056045 | 15400 | EQP |

**总行数：**
```sql
SELECT COUNT(*) AS total_rows FROM mdw.dwd_imp_rs_code_xishu_fo_tzsbjx
```
| total_rows |
|---|
| 555 |

code 类字段 `prod_code` 全部 distinct 值（≤50）：
```sql
SELECT DISTINCT prod_code FROM mdw.dwd_imp_rs_code_xishu_fo_tzsbjx ORDER BY 1 LIMIT 50
```
| prod_code |
|---|
| M626 |
| M673 |
| M678 |
| Z517 |
| Z571 |

code 类字段 `rs_code` 全部 distinct 值（≤50）：
```sql
SELECT DISTINCT rs_code FROM mdw.dwd_imp_rs_code_xishu_fo_tzsbjx ORDER BY 1 LIMIT 50
```
| rs_code |
|---|
| A1CFB |
| A1CIP |
| A1DPS |
| A1PPS |
| A2CFB |
| A2CIP |
| A2DMR |
| A2PMR |
| A2SIP |
| A3CFB |
| A3CIP |
| A3DMR |
| A3PMR |
| A3SIP |
| A4CFB |
| A4DBH |
| A5DMR |
| A5PMR |
| A5SIP |
| A7PBH |
| A7PFB |
| A8DMR |
| A8PMR |
| A8SIP |
| C4BP1 |
| C4BP2 |
| C4BP3 |
| C4CP3 |
| C4PL0 |
| T0GMR |
| T1DMR |
| T1PMR |
| T3DMR |
| T3PMR |
| T3PPL |
| T3WPE |
| T3WSC |

code 类字段 `code_desc` 全部 distinct 值（≤50）：
```sql
SELECT DISTINCT code_desc FROM mdw.dwd_imp_rs_code_xishu_fo_tzsbjx ORDER BY 1 LIMIT 50
```
| code_desc |
|---|
| CVD责CI膜内异物 |
| CVD责CI膜破 |
| CVD责GI膜内异物 |
| CVD责GI膜破 |
| CVD责ILD膜破 |
| CVD责PSI膜内异物 |
| CVD责PSI膜破 |
| M1 PH金属残留 |
| M1 干刻金属残留 |
| M2 PH金属图形缺失 |
| M2 PH金属残留 |
| M2 剥离后膜层peeling |
| M2 干刻金属残留 |
| Peeling导致刻蚀残留 |
| PHT责M1残留 |
| PHT责M2残留 |
| PHT责M3残留 |
| PHT责M4残留 |
| PHT责PLN2有机胶盲孔 |
| PHT责PLN2有机胶膜破 |
| PHT责PSI残留 |
| PVD责M1膜内异物 |
| PVD责M2膜内异物 |
| PVD责M3膜内异物 |
| PVD责M4膜内异物 |
| 划伤 |
| 干刻责ILD1孔异常 |
| 干刻责M1残留 |
| 干刻责M2残留 |
| 干刻责M3残留 |
| 干刻责M4残留 |
| 干刻责PSI残留 |
| 膜层彩色异物 |
| 金属peeling |
| 黑色不透明异物 |
| 黑色半透明异物 |
| 黑色零散异物 |

数值字段 `spec` 分布（辅助判断限值 vs 系数）：
```sql
SELECT MIN(spec) AS min_v, MAX(spec) AS max_v, AVG(spec) AS avg_v, COUNT(DISTINCT spec) AS distinct_cnt FROM mdw.dwd_imp_rs_code_xishu_fo_tzsbjx
```
| min_v | max_v | avg_v | distinct_cnt |
|---|---|---|---|
| 0.02 | 80 | 10.8722522522522523 | 28 |


## 5. RS Code 名称/中文描述码表搜索

**搜索表名含 rs/code 的表：**
```sql
SELECT table_schema, table_name FROM information_schema.tables WHERE table_name ILIKE '%rs%' OR table_name ILIKE '%code%' ORDER BY 1, 2 LIMIT 100
```
| table_schema | table_name |
|---|---|
| eda | array_pds_result_t_1_prt_p_202506w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202506w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202506w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202506w4_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202507w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202507w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202507w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202507w4_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202508w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202508w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202508w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202508w4_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202509w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202509w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202509w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202509w4_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202510w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202510w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202510w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202510w4_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202511w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202511w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202511w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202511w4_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202512w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202512w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202512w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202512w4_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202601w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202601w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202601w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202601w4_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202602w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202602w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202602w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202602w4_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202603w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202603w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202603w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202603w4_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202604w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202604w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202604w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202604w4_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202605w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202605w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202605w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202605w4_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202606w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202606w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202606w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202606w4_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202607w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202607w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202607w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202607w4_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202608w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202608w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202608w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202608w4_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202609w1_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202609w2_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202609w3_2_prt_p_cdmrsmsertpm |
| eda | array_pds_result_t_1_prt_p_202609w4_2_prt_p_cdmrsmsertpm |
| eda | spc_tzbjx_rs_array |
| eda | spc_tzbjx_rs_array_1_prt_p_202507 |
| eda | spc_tzbjx_rs_array_1_prt_p_202508 |
| eda | spc_tzbjx_rs_array_1_prt_p_202509 |
| eda | spc_tzbjx_rs_array_1_prt_p_202510 |
| eda | spc_tzbjx_rs_array_1_prt_p_202511 |
| eda | spc_tzbjx_rs_array_1_prt_p_202512 |
| eda | spc_tzbjx_rs_array_1_prt_p_202601 |
| eda | spc_tzbjx_rs_array_1_prt_p_202602 |
| eda | spc_tzbjx_rs_array_1_prt_p_202603 |
| eda | spc_tzbjx_rs_array_1_prt_p_202604 |
| eda | spc_tzbjx_rs_array_1_prt_p_202605 |
| eda | spc_tzbjx_rs_array_1_prt_p_202606 |
| eda | spc_tzbjx_rs_array_1_prt_p_202607 |
| eda | spc_tzbjx_rs_array_1_prt_p_202608 |
| eda | spc_tzbjx_rs_array_1_prt_p_202609 |
| eda | spc_tzbjx_rs_array_1_prt_p_202610 |
| eda | spc_tzbjx_rs_array_1_prt_p_202611 |
| eda | spc_tzbjx_rs_array_1_prt_p_202612 |
| eda | spc_tzbjx_rs_array_old |
| eda | spc_tzbjx_rs_oled |
| eda | spc_tzbjx_rs_oled_1_prt_p_202508 |
| eda | spc_tzbjx_rs_oled_1_prt_p_202509 |
| eda | spc_tzbjx_rs_oled_1_prt_p_202510 |
| eda | spc_tzbjx_rs_oled_1_prt_p_202511 |
| eda | spc_tzbjx_rs_oled_1_prt_p_202512 |
| eda | spc_tzbjx_rs_oled_1_prt_p_202601 |
| eda | spc_tzbjx_rs_oled_1_prt_p_202602 |
| eda | spc_tzbjx_rs_oled_1_prt_p_202603 |
| eda | spc_tzbjx_rs_oled_1_prt_p_202604 |
| eda | spc_tzbjx_rs_oled_1_prt_p_202605 |
| eda | spc_tzbjx_rs_oled_1_prt_p_202606 |
| eda | spc_tzbjx_rs_oled_1_prt_p_202607 |
| eda | spc_tzbjx_rs_oled_1_prt_p_202608 |
| eda | spc_tzbjx_rs_oled_1_prt_p_202609 |
| eda | spc_tzbjx_rs_oled_1_prt_p_202610 |


**筛选其中含 name/desc 类字段的表：**

| schema | table | name/desc 字段 |
|---|---|---|
| eda | array_pds_result_t_1_prt_p_202506w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202506w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202506w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202506w4_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202507w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202507w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202507w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202507w4_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202508w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202508w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202508w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202508w4_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202509w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202509w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202509w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202509w4_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202510w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202510w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202510w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202510w4_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202511w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202511w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202511w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202511w4_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202512w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202512w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202512w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202512w4_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202601w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202601w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202601w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202601w4_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202602w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202602w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202602w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202602w4_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202603w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202603w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202603w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202603w4_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202604w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202604w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202604w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202604w4_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202605w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202605w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202605w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202605w4_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202606w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202606w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202606w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202606w4_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202607w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202607w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202607w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202607w4_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202608w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202608w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202608w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202608w4_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202609w1_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202609w2_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202609w3_2_prt_p_cdmrsmsertpm | param_name, site_name |
| eda | array_pds_result_t_1_prt_p_202609w4_2_prt_p_cdmrsmsertpm | param_name, site_name |

候选码表 `eda.array_pds_result_t_1_prt_p_202506w1_2_prt_p_cdmrsmsertpm` 样例：
```sql
SELECT * FROM eda.array_pds_result_t_1_prt_p_202506w1_2_prt_p_cdmrsmsertpm LIMIT 5
```
| step_id | glass_id | glass_start_time | sub_equip_id | param_group | param_collection | param_name | site_name | value | str_value | item1 | item2 | item3 | num_item1 | num_item2 | num_item3 | num_item4 | num_item5 | count | updateflag |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 11451 | L3LA55E0J30 | 2025-06-01 10:23:48 | NA | DV | 3AMC04-CDM | H1_OVL2_Y_MAX | G | -299.99 | None | None | None | None | None | None | None | None | None | None | None |
| 11451 | L3LA55E0J30 | 2025-06-01 10:23:48 | NA | DV | 3AMC04-CDM | H2_CD1_MAX | G | 3.84 | None | None | None | None | None | None | None | None | None | None | None |
| 1A453 | L3CB55E0G02 | 2025-06-01 10:28:44 | NA | DV | 3AMC05-CDM | OVL1_X | 88 | -299.99 | None | None | None | None | None | None | None | None | None | None | None |
| 1A453 | L3CB55E0G02 | 2025-06-01 10:28:44 | NA | DV | 3AMC05-CDM | OVL1_X | 197 | -299.99 | None | None | None | None | None | None | None | None | None | None | None |
| 1A453 | L3CB55E0G02 | 2025-06-01 10:28:44 | NA | DV | 3AMC05-CDM | OVL1_X | 74 | -299.99 | None | None | None | None | None | None | None | None | None | None | None |

候选码表 `eda.array_pds_result_t_1_prt_p_202506w2_2_prt_p_cdmrsmsertpm` 样例：
```sql
SELECT * FROM eda.array_pds_result_t_1_prt_p_202506w2_2_prt_p_cdmrsmsertpm LIMIT 5
```
| step_id | glass_id | glass_start_time | sub_equip_id | param_group | param_collection | param_name | site_name | value | str_value | item1 | item2 | item3 | num_item1 | num_item2 | num_item3 | num_item4 | num_item5 | count | updateflag |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 12652 | L3Z555E0324 | 2025-06-08 00:00:23 | NA | DV | 3AMC08-CDM | H2_OVL1_X_SIG | G | -299.99 | None | None | None | None | None | None | None | None | None | None | None |
| 11141 | L3N255E0M16 | 2025-06-08 00:06:08 | NA | DV | 3AMS04-SER | VisionoxReserved#4_DV | G | None | None | None | None | None | None | None | None | None | None | None | None |
| 10841 | L3MO56E0510 | 2025-06-08 00:07:00 | NA | DV | 3AMS05-SER | SE_L2T_MAX | G | -299.99 | None | None | None | None | None | None | None | None | None | None | None |
| 1K452 | L3CE56E0530 | 2025-06-08 00:08:51 | NA | DV | 3AMC04-CDM | H2_OVL1_Y_MIN | G | -299.99 | None | None | None | None | None | None | None | None | None | None | None |
| 1K452 | L3CE56E0530 | 2025-06-08 00:08:51 | NA | DV | 3AMC04-CDM | CD2_MAX | G | -299.99 | None | None | None | None | None | None | None | None | None | None | None |

候选码表 `eda.array_pds_result_t_1_prt_p_202506w3_2_prt_p_cdmrsmsertpm` 样例：
```sql
SELECT * FROM eda.array_pds_result_t_1_prt_p_202506w3_2_prt_p_cdmrsmsertpm LIMIT 5
```
| step_id | glass_id | glass_start_time | sub_equip_id | param_group | param_collection | param_name | site_name | value | str_value | item1 | item2 | item3 | num_item1 | num_item2 | num_item3 | num_item4 | num_item5 | count | updateflag |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 15451 | L3MK55E0113 | 2025-06-15 00:05:19 | NA | DV | 3AMC05-CDM | H2_CD2_AVE | G | -299.99 | None | None | None | None | None | None | None | None | None | None | None |
| 15451 | L3MK55E0113 | 2025-06-15 00:05:19 | NA | DV | 3AMC05-CDM | PRE_STEP_ID | G | 15400 | None | None | None | None | None | None | None | None | None | None | None |
| 19450 | L3MH55E1B30 | 2025-06-15 00:03:20 | NA | DV | 3AMC03-CDM | H1_OVL1_X_MAX | G | 0.19 | None | None | None | None | None | None | None | None | None | None | None |
| 19450 | L3MH55E1B30 | 2025-06-15 00:03:20 | NA | DV | 3AMC03-CDM | VisionoxReserved#7_DV | G | None | None | None | None | None | None | None | None | None | None | None | None |
| 19450 | L3MH55E1B30 | 2025-06-15 00:03:20 | NA | DV | 3AMC03-CDM | H2_OVL2_X_AVE | G | -299.99 | None | None | None | None | None | None | None | None | None | None | None |

候选码表 `eda.array_pds_result_t_1_prt_p_202506w4_2_prt_p_cdmrsmsertpm` 样例：
```sql
SELECT * FROM eda.array_pds_result_t_1_prt_p_202506w4_2_prt_p_cdmrsmsertpm LIMIT 5
```
| step_id | glass_id | glass_start_time | sub_equip_id | param_group | param_collection | param_name | site_name | value | str_value | item1 | item2 | item3 | num_item1 | num_item2 | num_item3 | num_item4 | num_item5 | count | updateflag |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 12650 | L3CN56E0121 | 2025-06-22 00:01:28 | NA | DV | 3AMC08-CDM | OVL1_Y_AVE | G | -299.99 | None | None | None | None | None | None | None | None | None | None | None |
| 12650 | L3CN56E0122 | 2025-06-22 00:05:42 | NA | DV | 3AMC08-CDM | H2_OVL2_Y_MAX | G | -299.99 | None | None | None | None | None | None | None | None | None | None | None |
| 12650 | L3CN56E0122 | 2025-06-22 00:05:42 | NA | DV | 3AMC08-CDM | H1_CD3_MIN | G | -299.99 | None | None | None | None | None | None | None | None | None | None | None |
| 12650 | L3CN56E0122 | 2025-06-22 00:05:42 | NA | DV | 3AMC08-CDM | OVL2_X_SIG | G | -299.99 | None | None | None | None | None | None | None | None | None | None | None |
| 12650 | L3CN56E0122 | 2025-06-22 00:05:42 | NA | DV | 3AMC08-CDM | OVL2_Y_AVE | G | -299.99 | None | None | None | None | None | None | None | None | None | None | None |

候选码表 `eda.array_pds_result_t_1_prt_p_202507w1_2_prt_p_cdmrsmsertpm` 样例：
```sql
SELECT * FROM eda.array_pds_result_t_1_prt_p_202507w1_2_prt_p_cdmrsmsertpm LIMIT 5
```
| step_id | glass_id | glass_start_time | sub_equip_id | param_group | param_collection | param_name | site_name | value | str_value | item1 | item2 | item3 | num_item1 | num_item2 | num_item3 | num_item4 | num_item5 | count | updateflag |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 13140 | L3CB56E0F30 | 2025-07-01 00:02:46 | NA | DV | 3AMS03-SER | SE_L2T_AVG | G | -299.99 | None | None | None | None | None | None | None | None | None | None | None |
| 1J652 | L3MN56E0406 | 2025-07-01 00:04:07 | NA | DV | 3AMC08-CDM | WORKORDER_ID | G | None | 20006202A01 | None | None | None | None | None | None | None | None | None | None |
| 1J652 | L3MN56E0406 | 2025-07-01 00:04:07 | NA | DV | 3AMC08-CDM | OVL2_Y | 88 | -299.99 | None | None | None | None | None | None | None | None | None | None | None |
| 1J652 | L3MN56E0406 | 2025-07-01 00:04:07 | NA | DV | 3AMC08-CDM | OVL2_Y | 197 | -299.99 | None | None | None | None | None | None | None | None | None | None | None |
| 1J652 | L3MN56E0406 | 2025-07-01 00:04:07 | NA | DV | 3AMC08-CDM | OVL2_Y | 74 | -299.99 | None | None | None | None | None | None | None | None | None | None | None |

---

# 补充探查（第二轮）


## 6. TSP 过货视图定位

**所有表名含 tsp 的表/视图：**
```sql
SELECT table_schema, table_name, table_type FROM information_schema.tables WHERE table_name ILIKE '%tsp%' ORDER BY 1, 2 LIMIT 60
```
| table_schema | table_name | table_type |
|---|---|---|
| eda | spc_tzbjx_rs_tsp | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202509 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202510 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202511 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202512 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202601 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202602 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202603 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202604 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202605 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202606 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202607 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202608 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202609 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202610 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202611 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_1_prt_p_202612 | BASE TABLE |
| eda | spc_tzbjx_rs_tsp_old | BASE TABLE |
| eda | spc_tzbjx_tsp | BASE TABLE |
| eda | spot_eda_tsp_dv_v | VIEW |
| eda | spot_eda_tsp_pds_cut_v | VIEW |
| eda | spot_eda_tsp_pds_result_v | VIEW |
| eda | spot_eda_tsp_tact_time_cut_v | VIEW |
| eda | spot_eda_tsp_tact_time_result_v | VIEW |
| eda | tsp_cut_hst_t | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_201907 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_201908 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_201909 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_201910 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_201911 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_201912 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202001 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202002 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202003 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202004 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202005 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202006 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202007 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202008 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202009 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202010 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202011 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202012 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202101 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202102 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202103 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202104 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202105 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202106 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202107 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202108 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202109 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202110 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202111 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202112 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202201 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202202 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202203 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202204 | BASE TABLE |
| eda | tsp_cut_hst_t_1_prt_p_202205 | BASE TABLE |


**选定 TSP 过货候选: `eda.spot_eda_tsp_dv_v`**

**列清单：**
```sql
SELECT ordinal_position, column_name, data_type FROM information_schema.columns WHERE table_schema = 'eda' AND table_name = 'spot_eda_tsp_dv_v' ORDER BY ordinal_position
```
| ordinal_position | column_name | data_type |
|---|---|---|
| 1 | glass_id | character varying |
| 2 | step_id | character varying |
| 3 | glass_start_time | timestamp without time zone |
| 4 | unit_id | character varying |
| 5 | sub_equip_id | character varying |
| 6 | product_id | character varying |
| 7 | lot_id | character varying |
| 8 | event_user | character varying |
| 9 | equip_id | character varying |
| 10 | pre_equip_id | character varying |
| 11 | recipe_id | character varying |
| 12 | carrier_name | character varying |
| 13 | process_flow_name | character varying |
| 14 | pre_step_id | character varying |
| 15 | pre_recipe_id | character varying |
| 16 | pre_sub_unit_id | character varying |
| 17 | pre_end_time | timestamp without time zone |
| 18 | workorder | character varying |
| 19 | update_time | timestamp without time zone |
| 20 | base_line_flag | character varying |
| 21 | param_name | character varying |
| 22 | site_name | character varying |
| 23 | value | numeric |
| 24 | str_value | character varying |

**样例（LIMIT 3）：**
```sql
SELECT * FROM eda.spot_eda_tsp_dv_v LIMIT 3
```
| glass_id | step_id | glass_start_time | unit_id | sub_equip_id | product_id | lot_id | event_user | equip_id | pre_equip_id | recipe_id | carrier_name | process_flow_name | pre_step_id | pre_recipe_id | pre_sub_unit_id | pre_end_time | workorder | update_time | base_line_flag | param_name | site_name | value | str_value |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| L3LF54E2Z062 | 41001 | 2025-06-01 00:00:23 | 3TCP01-CPD | NA | G3688FP101FT-00EE | T3LF55E3TAA | 3TCP01 | 3TCP01 |  | C57E04100111001 | TN0043 | TME688FP1011 |  |  |  | 2025-06-01 12:30:15 | 20006137T01 | 2025-06-01 12:30:15 | Y | DAK_LO_CDA_FL | G | 2458 | None |
| L3LF54E2Z062 | 41001 | 2025-06-01 00:00:23 | 3TCP01-CPD | NA | G3688FP101FT-00EE | T3LF55E3TAA | 3TCP01 | 3TCP01 |  | C57E04100111001 | TN0043 | TME688FP1011 |  |  |  | 2025-06-01 12:30:15 | 20006137T01 | 2025-06-01 12:30:15 | Y | RECIPE_VERSION | G | 20201118110233 | None |
| L3LF54E2F261 | 44800 | 2025-06-01 00:54:20 | 3TOV01-OVE | 3TOV01-OVE-CH1 | G3688FP101FT-00EE | T3LF55E2KAA | 3TOV01 | 3TOV01 | 3TPP03 | C57E04480014800 | TN0104 | TME688FP1011 | 44400 | C57E04440014400 | 3TPP03-DRY-DY1 | 2025-06-01 12:30:30 | 20006137T01 | 2025-06-01 12:30:30 | Y | OV_TEMP_PV_1ZONE | G | 85.0 | None |

step_id 全部 distinct 值：
```sql
SELECT DISTINCT step_id FROM eda.spot_eda_tsp_dv_v ORDER BY 1
```
> **SQL 报错**: `OperationalError: (psycopg2.errors.QueryCanceled) canceling statement due to user request: "WLM Rule Engine canceled the query based on rule #39"

[SQL: SELECT DISTINCT step_id FROM eda.spot_eda_tsp_dv_v ORDER BY 1]
(Background on this error at: https://sqlalche.me/e/20/e3q8)`
_(查询失败)_


## 7. 规格表 `mdw.dwd_imp_rs_code_xishu_fo_tzsbjx` 语义细化

**type_flag 全部取值：**
```sql
SELECT DISTINCT type_flag FROM mdw.dwd_imp_rs_code_xishu_fo_tzsbjx ORDER BY 1
```
| type_flag |
|---|
| GLASS_ID |
| LOT_RATIO |
| MWD_RATIO |
| SHEET_ID |

**factory 全部取值：**
```sql
SELECT DISTINCT factory FROM mdw.dwd_imp_rs_code_xishu_fo_tzsbjx ORDER BY 1
```
| factory |
|---|
| ARRAY |
| OLED |
| TP |

**step_id 全部取值：**
```sql
SELECT DISTINCT step_id FROM mdw.dwd_imp_rs_code_xishu_fo_tzsbjx ORDER BY 1
```
| step_id |
|---|
| 11629 |
| 12629 |
| 13629 |
| 15629 |
| 18629 |
| 21329 |
| 43629 |

**粒度校验：(prod_code, factory, step_id, rs_code, type_flag) 是否有重复（有重复说明粒度更细）：**
```sql
SELECT prod_code, factory, step_id, rs_code, type_flag, COUNT(*) AS cnt FROM mdw.dwd_imp_rs_code_xishu_fo_tzsbjx GROUP BY 1,2,3,4,5 HAVING COUNT(*) > 1 LIMIT 20
```
_(0 行)_

**各 type_flag 下 spec 数值范围（判断语义：比例/张数/系数）：**
```sql
SELECT type_flag, MIN(spec) AS min_v, MAX(spec) AS max_v, COUNT(DISTINCT spec) AS distinct_spec FROM mdw.dwd_imp_rs_code_xishu_fo_tzsbjx GROUP BY type_flag ORDER BY 1
```
| type_flag | min_v | max_v | distinct_spec |
|---|---|---|---|
| GLASS_ID | 4 | 30 | 4 |
| LOT_RATIO | 0.2 | 40 | 15 |
| MWD_RATIO | 0.02 | 27 | 16 |
| SHEET_ID | 10 | 80 | 6 |

**prod_code × rs_code 组合样例（判断规格是否按产品区分）：**
```sql
SELECT DISTINCT prod_code, rs_code FROM mdw.dwd_imp_rs_code_xishu_fo_tzsbjx ORDER BY 1, 2 LIMIT 60
```
| prod_code | rs_code |
|---|---|
| M626 | A1CFB |
| M626 | A1CIP |
| M626 | A1DPS |
| M626 | A1PPS |
| M626 | A2CFB |
| M626 | A2CIP |
| M626 | A2DMR |
| M626 | A2PMR |
| M626 | A2SIP |
| M626 | A3CFB |
| M626 | A3CIP |
| M626 | A3DMR |
| M626 | A3PMR |
| M626 | A3SIP |
| M626 | A4CFB |
| M626 | A4DBH |
| M626 | A5DMR |
| M626 | A5PMR |
| M626 | A5SIP |
| M626 | A7PBH |
| M626 | A7PFB |
| M626 | A8DMR |
| M626 | A8PMR |
| M626 | A8SIP |
| M626 | C4BP1 |
| M626 | C4BP2 |
| M626 | C4BP3 |
| M626 | C4CP3 |
| M626 | C4PL0 |
| M626 | T0GMR |
| M626 | T1DMR |
| M626 | T1PMR |
| M626 | T3DMR |
| M626 | T3PMR |
| M626 | T3PPL |
| M626 | T3WPE |
| M626 | T3WSC |
| M673 | A1CFB |
| M673 | A1CIP |
| M673 | A1DPS |
| M673 | A1PPS |
| M673 | A2CFB |
| M673 | A2CIP |
| M673 | A2DMR |
| M673 | A2PMR |
| M673 | A2SIP |
| M673 | A3CFB |
| M673 | A3CIP |
| M673 | A3DMR |
| M673 | A3PMR |
| M673 | A3SIP |
| M673 | A4CFB |
| M673 | A4DBH |
| M673 | A5DMR |
| M673 | A5PMR |
| M673 | A5SIP |
| M673 | A7PBH |
| M673 | A7PFB |
| M673 | A8DMR |
| M673 | A8PMR |


### 7.b 对比表 `mdw.dwd_imp_rs_code_xishu_fo_int`

**列清单：**
```sql
SELECT ordinal_position, column_name, data_type FROM information_schema.columns WHERE table_schema = 'mdw' AND table_name = 'dwd_imp_rs_code_xishu_fo_int' ORDER BY ordinal_position
```
| ordinal_position | column_name | data_type |
|---|---|---|
| 1 | factory | character varying |
| 2 | prod_code | character varying |
| 3 | step_id | character varying |
| 4 | rs_code | character varying |
| 5 | ratio | numeric |
| 6 | owner_id | character varying |
| 7 | interface_time | timestamp without time zone |

**样例（LIMIT 5）：**
```sql
SELECT * FROM mdw.dwd_imp_rs_code_xishu_fo_int LIMIT 5
```
_(0 行)_

**总行数：**
```sql
SELECT COUNT(*) AS cnt FROM mdw.dwd_imp_rs_code_xishu_fo_int
```
| cnt |
|---|
| 0 |


## 8. RS Code 名称/描述码表（mdw 候选）


### `mdw.imp_tp_rs_code_remark`

**列清单：**
```sql
SELECT ordinal_position, column_name, data_type FROM information_schema.columns WHERE table_schema = 'mdw' AND table_name = 'imp_tp_rs_code_remark' ORDER BY ordinal_position
```
| ordinal_position | column_name | data_type |
|---|---|---|
| 1 | department | character varying |
| 2 | eqp_group | character varying |
| 3 | shift_timekey | character varying |
| 4 | rs_step | character varying |
| 5 | aoi_step | character varying |
| 6 | rs_code | character varying |
| 7 | eqp_id | character varying |
| 8 | chamber_id | character varying |
| 9 | remark | character varying |
| 10 | main_step | character varying |

**样例（LIMIT 5）：**
```sql
SELECT * FROM mdw.imp_tp_rs_code_remark LIMIT 5
```
| department | eqp_group | shift_timekey | rs_step | aoi_step | rs_code | eqp_id | chamber_id | remark | main_step |
|---|---|---|---|---|---|---|---|---|---|
| 薄膜科 | CVD | 20260510（白班） | 42129 | 42120 | T2CPC | 3TFC03 | CHC | None | None |
| 薄膜科 | CVD | 20260510（白班） | 42129 | 42120 | T2CPC | 3TFC01 | CHD | None | None |
| 薄膜科 | CVD | 20260510（白班） | 41129 | 41120 | T1COP | 3TFC01 | CHD | None | None |
| 薄膜科 | CVD | 20260510（白班） | 42129 | 42120 | T2CPC | 3TFC03 | USC | None | None |
| 薄膜科 | CVD | 20260510（白班） | 43629 | 43620 | T2CFB | 3TES01 | / | None | None |

**总行数：**
```sql
SELECT COUNT(*) AS cnt FROM mdw.imp_tp_rs_code_remark
```
| cnt |
|---|
| 14260 |


### `mdw.imp_tp_rs_code_spec_new`

**列清单：**
```sql
SELECT ordinal_position, column_name, data_type FROM information_schema.columns WHERE table_schema = 'mdw' AND table_name = 'imp_tp_rs_code_spec_new' ORDER BY ordinal_position
```
| ordinal_position | column_name | data_type |
|---|---|---|
| 1 | department | character varying |
| 2 | eqp_group | character varying |
| 3 | rs_step | character varying |
| 4 | aoi_step | character varying |
| 5 | rs_code | character varying |
| 6 | spec | numeric |
| 7 | main_step | character varying |

**样例（LIMIT 5）：**
```sql
SELECT * FROM mdw.imp_tp_rs_code_spec_new LIMIT 5
```
| department | eqp_group | rs_step | aoi_step | rs_code | spec | main_step |
|---|---|---|---|---|---|---|
| 薄膜科 | CVD | 42129 | 42120 | T2CSC | 2 | 42100 |
| 薄膜科 | PVD | 41229 | 41220 | T1SOP | 0.7 | 41200 |
| 黄光科 | TRC | 41429 | 41420 | T1PSC | 0.2 | 41400 |
| 黄光科 | TRC | 41429 | 41420 | T1PSC | 0.2 | 41100 |
| 黄光科 | TRC | 41629 | 41620 | T1PSC | 0.2 | 41400 |

**总行数：**
```sql
SELECT COUNT(*) AS cnt FROM mdw.imp_tp_rs_code_spec_new
```
| cnt |
|---|
| 119 |


---

# 补充探查（第三轮：TSP 过货视图确认）

## 9. TSP 过货视图 = `eda.spot_eda_tp_view_gls_v`

**结论：`eda.spot_eda_tsp_view_gls_v` 不存在；eda schema 全部视图清单中 TSP 族只有
`spot_eda_tsp_dv_v` / `spot_eda_tsp_pds_cut_v` / `spot_eda_tsp_pds_result_v` / `spot_eda_tsp_tact_time_*`
（均为参数级，一行一个参数，不是每 glass 一行的过货视图）。
TP 族的 `eda.spot_eda_tp_view_gls_v` 结构与 OLED 过货视图完全一致，且包含 step_id=43629
（与 `eda.spc_tzbjx_rs_tsp` 的唯一 step_id 吻合），最近一月该站点行数 73001，
确认 TSP 侧过货明细应使用 `eda.spot_eda_tp_view_gls_v`。**

**`eda.spot_eda_tp_view_gls_v` 列清单（38 列，与 `spot_eda_oled_view_gls_v` 完全同构）：**

glass_id, glass_start_time, lot_id, lot_type, product_spec, glass_end_time, equipment_id,
unit_id, cassette_id, recipe_id, ppid, slot_no, port_id, panel_count, glass_judge, workorder,
pre_step_id, pre_equip_id, pre_recipe_id, pre_sub_unit_id, ab_flag, operator_id, mask_id,
mask_cycle_count, glass_total_defect, glass_map_judge, glass_map_name, glass_dm_name,
mura_type_image_name1/2/3, video_pixel_size, spectrum_r/g/b/w_image, step_id, eqp_type

**验证 SQL 与结果：**

```sql
SELECT COUNT(*) FROM eda.spot_eda_tp_view_gls_v
WHERE step_id='43629' AND glass_start_time >= now() - interval '1 month'
-- => 73001
```

step_id distinct 共 97 个值（40029/40429/40829/40A29/41129/.../D4629，含 43629），
与 RS 明细表 step_id=43629 口径一致（同为 varchar 五位编码）。

另：`eda.spc_tzbjx_tsp`（BASE TABLE）为 TSP 参数级数据（glass_id, step_id, glass_start_time,
unit_id, product_spec, lot_id, equip_id, ..., param_name, site_name, param_value, update_time），
step_id 取值 41140/41260/41450/41650/42140/42450/42650/43260/43450/43620/43650/44040/44450，
非过货明细用途。

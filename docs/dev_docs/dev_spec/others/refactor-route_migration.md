# Task：配置文件整理（路径迁移）

## Context
你好，我将原本直接放在`resources`下的各类文件，根据模块进行了整理，如下所示：

1. inline_domain - `resources\inline_domain`:

| 文件路径 | 对应子模块 | 作用 |
| --- | --- | --- |
| `resources\inline_domain\aoi_rs_sheet_oos_decoration.xlsx` | aoi_rs | sheet明细修饰 |
| `resources\inline_domain\aoi_tt_sheet_oos_decoration.xlsx` | aoi_tt | sheet明细修饰 |
| `resources\inline_domain\compliance_config.xlsx` | monitor | 面板显示数据修饰 |
| `resources\inline_domain\ctq_sheet_oos_decoration.xlsx` | ctq | sheet明细修饰 |
| `resources\inline_domain\scrap_sheets.xlsx` | monitor | 报废品ID |
| `resources\inline_domain\spc_cpk_decoration.xlsx` | spc_cpk | cpk数据修饰 |
| `resources\inline_domain\spc_outlier_filters.xlsx` | spc | spc点位数据筛选 |
| `resources\inline_domain\spc_sheet_oos_decoration.xlsx` | spc_sheet | sheet明细修饰 |

2. yield_domain - `resources\yield_domain`：

| 文件路径 | 对应子模块 | 作用 |
| --- | --- | --- |
| `resources\yield_domain\mapping_config.xlsx` | mapping | mapping分布修饰 |
| `resources\yield_domain\override_rates.xlsx` | sheet_lot | 指定ID数据覆写 |
| `resources\yield_domain\入库不良率规格.xlsx` | mwd | 各Defect Code不良率规格 |
| `resources\yield_domain\入库良率修饰表.xlsx` | mwd | 月周天良率修饰 |
| `resources\yield_domain\入库良率修饰表.xlsx` | 原mwd | 现不再需要，但暂不删除 |
| `resources\yield_domain\codebaseline.xlsx` | 原mwd | 现不再需要，但暂不删除 |

3. equipment_domain - `resources\equipment_domain`：

| 文件路径 | 对应子模块 | 作用 |
| --- | --- | --- |
| `resources\equipment_domain\critical_parts_baseline.csv` | 无 | 关键备件规格数据 |


## Workflow
1. 请你分析`src`中各模块调用文件的路径，将其修正为可配置化（路径不应该硬编码在程序里）
2. 然后在`config`中的各domain对应的配置文件（yaml）下配置：
    - `config\domain\equipment_domain.yaml`：原“equipment_config”
    - `config\domain\inline_domain.yaml`：原“inline_config”
    - `config\domain\yield_domain.yaml`：原“yield_domain”
3. 修正代码中各domain配置文件的路径，将其修正为可配置化
4. 在`config\global.yaml`配置各domain配置文件的路径：相当于让`config\global.yaml`充当路由功能
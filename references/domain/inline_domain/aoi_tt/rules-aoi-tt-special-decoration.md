# AOI_TT 专用修饰：厂别日期窗口与 Sheet 规格比例

核验日期：2026-10-21。复用 AOI_RS 的可配置 Sheet 比例修饰语义，按 AOI_TT 自身的 UCL、单片明细和整数数量契约适配。

## 1. 独立配置

`config/domain/inline_domain.yaml`：

```yaml
aoi_tt:
  special_decoration:
    enabled: true
    factories:
      - "OLED"
    start_date: "2026-09-21"
    end_date: "today"
    sheet_threshold_ratio: 0.5
    sheet_replacement_ratio: 0.25
```

配置独立于 `aoi_rs.special_decoration`。未配置 `enabled` 时关闭；启用后省略厂别默认 OLED，空厂别列表也关闭。厂别去除首尾空白并忽略大小写。日期为报表显示日期，首尾包含，结束日覆盖全天；省略时默认 2026-09-21 至服务器当日。非法配置结构、非布尔开关、非法厂别列表、无效日期及反向日期区间报错。

`sheet_threshold_ratio` 默认 0.5，`sheet_replacement_ratio` 默认 0.25。两项必须是有限非负数，拒绝布尔值、字符串、空值、负数及非有限数。修改任一配置不影响 AOI_RS。

## 2. 处理规则

1. 基础设施先完成显示日期映射、厂别日期排除与单片复测选择，不修改原始快照。
2. 应用层读取 USL 超规清单和三态决策；清单仍按 `tt_qty > USL` 判定。
3. 对命中厂别及 `start_time` 显示日期窗口的 Total 单片，以 `(step_id, tt_name)` 匹配 UCL。严格满足 `tt_qty > sheet_threshold_ratio × UCL` 时，将数量修饰为 `[0, sheet_replacement_ratio × UCL]` 内的稳定整数哈希值。等于阈值时保留原值。
4. 哈希种子为产品、厂别、站点、TT 名称、Sheet/Glass ID，不依赖行顺序、当前数量、查询窗口或缓存。SHA-256 的稳定比例乘以配置上限后向下取整，保证遵循 [ADR-0025](../../../../docs/ADR/0025-aoi-tt-stable-particle-size-generation.md) 的整数数量契约，同时不会因四舍五入越过小数上限。UCL 为 0 时，命中的正值归零；缺失、负数、无穷或无法解析的 UCL 不执行专用数值修饰，也不回退 USL。
5. `Delete` 删除单片，`False` 保留当前数量，`True` / 无决策允许自动修饰；参数名称豁免阻止数值修饰，Delete 仍优先。决策键沿用产品、站点、TT 名称、Sheet；厂别进入哈希种子，不改变人工台账键。
6. 关闭专用规则、未选厂别及窗口外记录沿用常规 UCL 三态修饰：超过 UCL 的数量稳定落在其 85%～95% 区间。
7. Total 修饰完成后才生成或组合 Particle Size；趋势、By Lot、By Sheet、周度报告及趋势预警消费同一份修饰后明细。默认生成粒径使用修饰后 Total；真实粒径模式继续使用其独立缺陷计数，不按 Total 比例缩放。检测片数继续采用 TT 测量自身的去重 Sheet 口径。

本次移植的是 Sheet 比例规则与厂别/日期作用域。TT 未引入 RS 的 `LOT_RATIO` 归零或周/日相对月密度的 1.3 倍限幅。

## 3. 调用与缓存

纯 Core 入口为 `aoi_tt_decoration.apply_aoi_tt_decoration()`，专用数值规则位于 `aoi_tt_special_decoration.py`，无数据库或配置读取。`prepare_aoi_tt_decoration()` 负责决策编排，`AoiTtReportService.get_aoi_tt_report_data()` 读取独立配置并传入缓存函数。

缓存键包含规则版本、生效厂别、解析后的日期元组和比例元组。配置变更或 `today` 跨天后无需等待 TTL 重建。缓存只存原生 DataFrame payload，ViewModel 在缓存外构建。页面继续通过现有应用服务入口加载，不在页面重复执行修饰或暴露内部配置。

## 4. 验证

- `tests/unit/test_aoi_tt_special_config.py`：独立配置、默认值、非法比例与日期解析。
- `tests/unit/inline_domain/core/aoi_tt/test_aoi_tt_special_decoration.py`：严格阈值、整数哈希范围、稳定性、厂别与日期首尾、三态与豁免、无效及零 UCL。
- `tests/unit/inline_domain/application/aoi_tt/test_aoi_tt_service.py`：配置文件变化使缓存失效，报表消费修饰后 Total。
- 既有 TT Core/Application/Page/Chart 测试继续覆盖常规 UCL 修饰、粒径、图表和空数据行为。

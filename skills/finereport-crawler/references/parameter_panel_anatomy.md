# 参数面板 HTML 结构解析参考

> 帆软报表的参数面板配置以 JSON 内嵌在入口页 HTML 中。
> 本文件以 `response/response1.md`（某实际项目入口页 HTML）为样本，解剖其结构。

---

## 1. 定位方法

在入口页 HTML 中搜索以下关键词即可定位参数面板：

| 关键词 | 说明 |
|--------|------|
| `"widgetName"` | 所有控件的唯一标识名 |
| `"widgetType"` | 控件类型（如 `tagcombocheckbox`） |
| `parameters : {` | 参数面板 JSON 的起始块 |
| `FR.SessionMgr` | Session 注册代码（通常紧邻参数面板） |

---

## 2. 参数面板 JSON 结构

以下是从实际项目中提炼的参数面板 JSON 结构：

```json
{
    "widgets": [
        // ========== 标签控件（只读，显示文本） ==========
        {
            "widgetName": "LBLEQPID",          // 标签控件名，"设备"文字的标签
            "widgetType": "label",             // 控件类型: label
            "attributes": {
                "labelName": {
                    "widgetName": "LBLEQPID"   // 自引用
                }
            }
        },

        // ========== 筛选控件（用户交互） ==========
        {
            "widgetName": "CMCBEQPID",          // 🔑 筛选参数名！
            "widgetType": "tagcombocheckbox",   // 控件类型: 组合复选框
            "attributes": {
                "labelName": {
                    "widgetName": "LBLEQPID"    // 关联的标签控件
                }
            },
            "style": {
                "fontSize": 13.0
            }
        },

        // ========== 查询按钮 ==========
        {
            "widgetName": "SearchBtn",
            "widgetType": "button",
            "attributes": {
                "labelName": {
                    "text": "查询"
                }
            }
        }
    ]
}
```

---

## 3. 控件命名约定

根据实际项目观察，命名遵循匈牙利命名法（Hungarian Notation）：

| 前缀 | 全称 | 控件类型 | 用途 |
|------|------|----------|------|
| `LBL` | Label | `label` | 纯文本显示标签 |
| `CMCB` | Combo Checkbox | `tagcombocheckbox` | 组合复选框（多选筛选） |
| `CMC` | Combo | `combo` | 下拉组合框（单选筛选） |
| `BTN` | Button | `button` | 按钮（查询、重置等） |
| `DT` | Date/Time | `date` / `time` | 日期/时间选择器 |
| `TXB` | Text Box | `text` | 文本框输入 |
| `RDL` | Radio | `radio` | 单选按钮 |
| `CHK` | Checkbox | `checkbox` | 复选框 |

### 实际映射例子

```
LBLEQPID  →  Label  →  "设备" (显示文本)
CMCBEQPID →  Combo Checkbox → 设备筛选控件（多选）
```

> **命名规则**: `{控件类型前缀}{业务字段名}`，全部大写。

---

## 4. 控件类型说明

| `widgetType` | 中文名 | 说明 |
|-------------|--------|------|
| `label` | 标签 | 只读文本，不能交互 |
| `tagcombocheckbox` | 组合复选框 | 可选多个值的下拉框 |
| `combo` | 组合框 | 单选下拉框 |
| `button` | 按钮 | 可点击触发事件 |
| `date` | 日期控件 | 日期选择 |
| `text` | 文本框 | 文本输入 |
| `radio` | 单选按钮 | 单选 |
| `checkbox` | 复选框 | 是/否选择 |

---

## 5. 查找筛选控件的搜寻路径

```
入口页 HTML (response1.md)
  │
  ├─ 搜索 "widgetName" → 获取所有控件名
  │     └─ LBLEQPID (标签)
  │     └─ CMCBEQPID (筛选控件) ← 🔑 关注点
  │     └─ SearchBtn (按钮)
  │
  ├─ 搜索 "widgetType": "tagcombocheckbox" → 定位筛选控件
  │     └─ 取该控件的 widgetName 作为参数名
  │
  ├─ 搜索 "labelName" → 找到筛选控件的关联标签
  │     └─ 标签文本通常就是筛选条件的中文名
  │
  └─ 搜索 parameters : { → 查看完整面板 JSON 结构
        └─ 确认所有控件及配置
```

---

## 6. 在 response1.md 中的实际位置

以 `response/response1.md`（1240 行）为例：

| 内容 | 行号范围 | 说明 |
|------|----------|------|
| Session 注册 | ~L46 | `FR.SessionMgr.register(...)` |
| 参数面板 JSON 起始 | ~L1086 | `parameters : {` |
| LBLEQPID 标签定义 | ~L1104 | 标签控件 |
| CMCBEQPID 筛选控件 | ~L1127 | 🔑 组合复选框 |
| 查询按钮 | ~L1174 | `SearchBtn` |

---

## 7. 注意事项

1. **大小写敏感**: `widgetName` 在 JSON 中是全大写驼峰命名（如 `CMCBEQPID`），但在 API 调用时通常需要**全小写**（如 `cmcbeqpid`）
2. **标签 vs 控件**: `LBLxxx` 是**标签**（显示文本），`CMCBxxx` 是**交互控件**（能提交筛选值），不要混淆
3. **多选值**: `tagcombocheckbox` 支持多选，提交时用逗号分隔或多个同名参数
4. **参数面板 JSON 不完整时**: 有时 HTML 中的 JSON 会被截断，此时需查看完整的 `response1.md` 或直接从浏览器复制

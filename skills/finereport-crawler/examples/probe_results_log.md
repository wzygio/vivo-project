# 探针结果记录示例

> 本文件记录了某实际项目中探针执行的过程与结果，作为方法论的具体例证。
> **注意**: 实际值已脱敏，仅保留结构。

---

## 背景

- **目标服务器**: `http://10.73.17.76:8080`
- **报表入口 UUID**: `22ce8bfb-620c-485f-a521-2fae23f53b63`
- **筛选条件**: "设备"，希望筛选值为 `3TED01`
- **已知问题**: 初次运行时筛选未生效，返回全部 31 行数据

---

## 探针 1: 参数名猜测（失败）

### 方法

将候选参数名作为 URL 查询参数添加到 `page_content` 请求中。

### 候选参数

```python
candidates = [
    "EQPID", "cmcbEQPID", "CMCBEQPID",
    "设备", "eqpid", "device",
    "LBLEQPID", "cmcbEQPID", "c_m_c_b_E_Q_P_I_D_",
    "E.Q.P.I.D", "E_Q_P_I_D", "fr_EQPID", "EQPID_"
]
```

### 结果

```text
无筛选(基线):             31 行
EQPID=3TED01:             31 行 ❌
cmcbEQPID=3TED01:         31 行 ❌
CMCBEQPID=3TED01:         31 行 ❌
设备=3TED01:               31 行 ❌
... (全部 13 个候选均失败)
```

### 结论

❌ 筛选参数**不是**通过 URL 查询参数传递的。

---

## 探针 2: 发现 parameters_d 端点

### 方法

查看入口页 HTML（`response/response1.md`），在参数面板 JSON 中找到提交按钮的回调。

### 在 HTML 中的发现

在 `response1.md` 约 L1174 处发现按钮定义：

```json
{
    "widgetName": "SearchBtn",
    "widgetType": "button",
    "attributes": {
        ...
    }
}
```

进一步分析发现，筛选提交的目标端点是：

```
POST /webroot/decision/view/report?op=fr_dialog&cmd=parameters_d&sessionID={sid}
```

### 结果

```text
POST parameters_d (form-data: cmcbEQPID=3TED01) → 7 行 ✅
```

### 结论

✅ 筛选项通过 `parameters_d` 端点提交，且必须使用 **form-data** 格式。

---

## 探针 3: 确认 form-data vs JSON 差异

### 方法

在独立 Session 中分别测试 form-data 和 JSON 两种提交方式。

### 结果

| 提交方式 | 代码 | 结果 |
|----------|------|------|
| Form-data | `session.post(url, data={"cmcbEQPID": "3TED01"})` | ✅ 7 行 |
| JSON | `session.post(url, json={"cmcbEQPID": "3TED01"})` | ❌ 31 行 |

### 结论

⚠️ `parameters_d` 端点**只接受 form-data**，不接受 JSON。
使用 `requests` 时，必须用 `data=` 参数而非 `json=`。

---

## 探针 4: 可用值发现

### 方法

通过 widget 端点获取筛选控件的可用值列表。

### 请求

```
POST /webroot/decision/view/report?op=widget&widgetname=cmcbEQPID&sessionID={sid}
```

### 响应

```json
[
    {"value": "3TED01", "text": "3TED01"},
    {"value": "3TED02", "text": "3TED02"},
    {"value": "3TED03", "text": "3TED03"},
    {"value": "3TED04", "text": "3TED04"},
    {"value": "3TED05", "text": "3TED05"},
    {"value": "3TED06", "text": "3TED06"},
    {"value": "3TED07", "text": "3TED07"},
    {"value": "3TED08", "text": "3TED08"}
]
```

### 结论

✅ 该筛选控件的可用值为 `3TED01` ~ `3TED08`。

---

## 探针 5: 会话状态污染验证

### 方法

在同一个 Session 中连续调用多次，观察状态污染。

### 流程

```
Session A: POST parameters_d (cmcbEQPID=3TED01) → GET page_content → 7 行
Session A: GET page_content (无参数)           → 7 行 (状态已被污染！)
```

### 结论

⚠️ 帆软服务器端的 Session 会记住筛选状态。
**每个探针必须使用全新的 `requests.Session()`**，否则结果不可靠。

---

## 最终流水线配置

经过所有探针后，确定的配置：

| 配置项 | 值 |
|--------|-----|
| 登录端点 | `POST /webroot/decision/login` (JSON) |
| Token 位置 | `data.accessToken` |
| 报表入口 | `/webroot/decision/v10/entry/access/{uuid}` |
| Session 提取 | `FR.SessionMgr.register('{sid}'` |
| 筛选参数名 | `cmcbEQPID` |
| 筛选提交端点 | `POST ...?op=fr_dialog&cmd=parameters_d` |
| 提交格式 | form-data (`data=` 非 `json=`) |
| 数据端点 | `POST ...?op=page_content` |
| 解析方式 | BeautifulSoup `table#r-0` |

---

## 关键教训

1. **不要假设**参数是 URL 查询参数 — 帆软有自己的筛选提交机制
2. **不要混用** form-data 和 JSON — `parameters_d` 只认 form-data
3. **每次使用新 Session** — 服务器端 Session 状态会污染后续请求
4. **先看 HTML** — 参数面板 JSON 就在入口页 HTML 中，包含所有控件信息
5. **命名约定** — LBL=标签, CMCB=组合复选框, 利用这个规律快速定位筛选控件

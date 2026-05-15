"""
探针模板: 筛选参数 & 可用值发现
================================
目的:
  1. 从入口页 HTML 参数面板 JSON 中解析控件名
  2. 通过 widget 端点获取筛选控件的可用值
  3. 通过 parameters_d 端点提交筛选条件
  4. 验证筛选是否生效（对比行数）

使用方法:
  1. 修改 HOST / ACCESS_TOKEN / ENTRY_UUID
  2. 如果已知入口页 HTML，可跳过步骤1直接填入 response_html
  3. python probe_widget_filter.py

输出:
  - filter_param: 筛选参数名（如 "cmcbEQPID"）
  - available_values: 可用值列表
  - 筛选生效验证结果
"""

import requests
import re
import json

# ==================== 配置 ====================
HOST = "http://your-server:8080"
ACCESS_TOKEN = "your_access_token_here"
ENTRY_UUID = "your-entry-uuid-here"

# 如果已有入口页 HTML（如从 response/response1.md 中复制），直接填入此处
# 留空则程序会自动请求
RESPONSE_HTML = None  # 或 """<!DOCTYPE html>..."""

# 要测试的筛选候选参数名（基于命名约定推测）
CANDIDATE_PARAMS = [
    # 常见命名模式 - 添加你认为可能的参数名
    "cmcbEQPID", "CMCBEQPID", "EQPID", "eqpid",
    "cmcbDevice", "device", "设备",
    # 添加更多...
]

# 筛选测试值
TEST_FILTER_VALUE = "3TED01"

VERIFY_SSL = False
requests.packages.urllib3.disable_warnings()


def login_and_get_session():
    """完整的登录 + 获取 Session ID 流程"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"
    })

    # 登录
    login_resp = session.post(
        f"{HOST}/webroot/decision/login",
        json={"username": "admin", "password": "your_password", "validity": -1},
        verify=VERIFY_SSL, timeout=10
    )
    token = login_resp.json().get("data", {}).get("accessToken")
    session.headers.update({"Authorization": f"Bearer {token}"})
    session.cookies.set("fine_auth_token", token)

    # 获取 Session ID
    entry_url = f"{HOST}/webroot/decision/v10/entry/access/{ENTRY_UUID}"
    entry_resp = session.get(entry_url, verify=VERIFY_SSL, timeout=15)
    match = re.search(r"FR\.SessionMgr\.register\('([a-f0-9\-]{36})'", entry_resp.text)

    if not match:
        raise RuntimeError("无法从入口页提取 Session ID")

    session_id = match.group(1)
    session.headers.update({"sessionID": session_id})
    entry_html = entry_resp.text

    return session, session_id, entry_html


def parse_widgets_from_html(html):
    """从入口页 HTML 中解析参数面板控件信息"""
    print("=" * 60)
    print("[步骤1] 从 HTML 解析参数面板控件")
    print("=" * 60)

    # 方法1: 搜索所有 widgetName
    widget_names = re.findall(r'"widgetName"\s*:\s*"(\w+)"', html)
    print(f"找到的 widgetName 列表:")
    for name in widget_names:
        print(f"  - {name}")

    # 方法2: 搜索 widgetType 和 labelName
    widget_types = re.findall(
        r'"widgetName"\s*:\s*"(\w+)".*?"widgetType"\s*:\s*"(\w+)"',
        html, re.DOTALL
    )
    if widget_types:
        print(f"\n控件类型映射:")
        for name, wtype in widget_types:
            print(f"  {name}: {wtype}")

    # 方法3: 尝试提取整个 parameters JSON
    json_match = re.search(r'parameters\s*:\s*(\{.*?\})\)', html, re.DOTALL)
    if json_match:
        try:
            params_json = json.loads(json_match.group(1))
            print(f"\n✅ 成功解析参数面板 JSON")
            print(f"  顶层键: {list(params_json.keys())}")
        except json.JSONDecodeError:
            print("\n⚠️  找到 parameters 块但 JSON 解析失败")
    else:
        print("\n⚠️  未找到 parameters JSON 块")

    # 根据命名约定推断筛选控件
    print("\n--- 控件命名约定分析 ---")
    for name in widget_names:
        if name.startswith("CMCB"):
            print(f"  ✅ {name}: 可能是组合复选框筛选控件")
        elif name.startswith("CMC"):
            print(f"  ⚠️  {name}: 可能是组合框控件")
        elif name.startswith("LBL"):
            print(f"  ℹ️  {name}: 标签控件（通常不是筛选参数）")
        elif name.startswith("BTN"):
            print(f"  ℹ️  {name}: 按钮控件（通常不是筛选参数）")

    return widget_names


def probe_widget_values(session, session_id, widget_name):
    """通过 widget 端点获取控件可用值"""
    print(f"\n[步骤2] 获取控件 '{widget_name}' 的可用值")

    widget_url = f"{HOST}/webroot/decision/view/report"
    params = {
        "op": "widget",
        "widgetname": widget_name,
        "sessionID": session_id
    }

    try:
        resp = session.post(widget_url, params=params,
                            verify=VERIFY_SSL, timeout=10)
        print(f"  状态码: {resp.status_code}")

        try:
            data = resp.json()
            print(f"  响应类型: {type(data)}")

            # 解析不同的返回格式
            if isinstance(data, list):
                values = [item.get("value", item) for item in data]
                print(f"  ✅ 可用值: {values}")
                return values
            elif isinstance(data, dict):
                print(f"  JSON 键: {list(data.keys())}")
                # 尝试常见的键名
                for key in ["data", "result", "values", "options"]:
                    if key in data:
                        print(f"  {key}: {data[key]}")
                        return data[key]
                return None
            else:
                print(f"  响应内容: {resp.text[:300]}")
                return None

        except (json.JSONDecodeError, TypeError):
            print(f"  非 JSON 响应: {resp.text[:300]}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"  请求失败: {e}")
        return None


def probe_filter_effect(session, session_id, param_name, param_value):
    """提交筛选参数并验证是否生效"""
    print(f"\n[步骤3] 提交筛选 '{param_name}={param_value}'")

    # 先获取基线（无筛选）
    baseline = get_row_count(session, session_id)
    print(f"  基线（无筛选）行数: {baseline}")

    # 提交筛选参数 - 注意: 必须用 data= 不是 json=
    params_url = f"{HOST}/webroot/decision/view/report?op=fr_dialog&cmd=parameters_d&sessionID={session_id}"
    resp = session.post(
        params_url,
        headers={"sessionID": session_id},
        data={param_name: param_value},  # ❗ 关键: form-data
        verify=VERIFY_SSL, timeout=15
    )
    print(f"  参数提交状态码: {resp.status_code}")

    # 获取筛选后行数
    filtered = get_row_count(session, session_id)
    print(f"  筛选后行数: {filtered}")

    if filtered < baseline:
        print(f"  ✅ 筛选生效！行数从 {baseline} 减少到 {filtered}")
        return True, baseline, filtered
    else:
        print(f"  ❌ 筛选未生效（行数未减少）")
        return False, baseline, filtered


def get_row_count(session, session_id):
    """获取报表数据行数"""
    # 注意: 需要替换为实际的报表文件名
    page_url = f"{HOST}/webroot/decision/view/report?viewlet=schedule%2Fyour_report.cpt&op=page_content&sessionID={session_id}"
    try:
        resp = session.post(page_url, headers={"sessionID": session_id},
                            verify=VERIFY_SSL, timeout=15)
        # 粗略估计行数
        row_count = resp.text.count("<tr>")
        return row_count
    except Exception:
        return -1


def main():
    print("=" * 60)
    print("筛选参数 & 可用值发现探针")
    print("=" * 60)
    print(f"目标服务器: {HOST}")
    print()

    # 获取入口页 HTML
    if RESPONSE_HTML:
        html = RESPONSE_HTML
        session, session_id = None, None
        print("使用预设的 HTML（跳过网络请求）")
    else:
        session, session_id, html = login_and_get_session()
        print(f"Session ID: {session_id}")

    print()

    # 步骤1: 解析控件
    widget_names = parse_widgets_from_html(html)

    # 步骤2: 对每个疑似筛选控件探测可用值
    print("\n" + "=" * 60)
    print("[步骤2] 探测控件可用值")
    print("=" * 60)

    for name in widget_names:
        if name.startswith("CMCB"):
            probe_widget_values(session, session_id, name.lower())

    # 步骤3: 验证筛选效果
    print("\n" + "=" * 60)
    print("[步骤3] 验证筛选效果")
    print("=" * 60)

    if session:
        for param in CANDIDATE_PARAMS:
            print(f"\n--- 测试参数: {param} ---")
            # 每个参数用新的 Session
            s, sid, _ = login_and_get_session()
            success, base, filtered = probe_filter_effect(s, sid, param, TEST_FILTER_VALUE)
            if success:
                print(f"\n🎯 找到有效筛选参数: {param}")
                break
    else:
        print("跳过验证（未提供 session）")

    print("\n" + "=" * 60)
    print("探针结论")
    print("=" * 60)
    print("根据输出确定:")
    print("  1. 筛选参数名是什么？")
    print("  2. 可用值有哪些？")
    print("  3. 提交方式为 form-data 还是 JSON？")


if __name__ == "__main__":
    main()

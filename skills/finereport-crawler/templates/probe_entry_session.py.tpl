"""
探针模板: 报表入口 URL 发现 & Session ID 提取
===============================================
目的:
  1. 找到正确的报表入口 URL（entry/access 类型）
  2. 从入口页 HTML 中提取 Session ID

使用方法:
  1. 修改 HOST / TOKEN / ENTRY_UUID 配置
  2. python probe_entry_session.py

输出:
  - entry_url: 正确的入口 URL（可访问）
  - session_id: 动态 Session ID（后续请求用）
"""

import requests
import re

# ==================== 配置 ====================
HOST = "http://your-server:8080"
ACCESS_TOKEN = "your_access_token_here"

# 报表的 Entry UUID（从浏览器 DevTools 或 HTML 源码中获取）
# 尝试多个常见的 UUID 格式入口
ENTRY_CANDIDATES = [
    "22ce8bfb-620c-485f-a521-2fae23f53b63",  # 示例 UUID
    # 添加更多候选项...
]

# Viewlet URL 候选（如果 entry/access 不可用）
VIEWLET_CANDIDATES = [
    # "/webroot/decision/view/report?viewlet=schedule%2Fyour_report.cpt",
]

VERIFY_SSL = False
requests.packages.urllib3.disable_warnings()


def test_entry_access(session, entry_uuid):
    """测试 entry/access URL"""
    url = f"{HOST}/webroot/decision/v10/entry/access/{entry_uuid}"
    try:
        resp = session.get(url, verify=VERIFY_SSL, timeout=15)
        print(f"  URL: {url}")
        print(f"  状态码: {resp.status_code}")
        print(f"  响应长度: {len(resp.text)} 字符")

        # 检查是否包含 Session 注册代码
        session_pattern = r"FR\.SessionMgr\.register\('([a-f0-9\-]{36})'"
        match = re.search(session_pattern, resp.text)

        if match:
            session_id = match.group(1)
            print(f"  ✅ 找到 Session ID: {session_id}")
            return True, session_id, resp.text
        else:
            # 检查是否有其他标识
            if "Cannot find template file" in resp.text:
                print("  ❌ 错误: Cannot find template file (viewlet 类型 URL 不适合)")
            elif "FR.SessionMgr" in resp.text:
                print("  ⚠️  找到 FR.SessionMgr 但 register 格式不同")
                # 打印附近内容
                idx = resp.text.find("FR.SessionMgr")
                print(f"     附近内容: {resp.text[idx:idx+200]}")
            else:
                print("  ⚠️  未找到 FR.SessionMgr.register 标识")
                # 打印页面标题和开头
                title_match = re.search(r'<title>(.*?)</title>', resp.text)
                if title_match:
                    print(f"     页面标题: {title_match.group(1)}")
            return False, None, resp.text

    except requests.exceptions.RequestException as e:
        print(f"  ❌ 请求失败: {e}")
        return False, None, None


def test_viewlet(session, viewlet_url):
    """测试 viewlet URL"""
    url = f"{HOST}{viewlet_url}"
    try:
        resp = session.get(url, verify=VERIFY_SSL, timeout=15)
        print(f"  URL: {url}")
        print(f"  状态码: {resp.status_code}")

        # viewlet 失败时会在响应中包含错误信息
        if "Cannot find template file" in resp.text:
            print("  ❌ 错误: Cannot find template file")
            print(f"     响应前 200 字符: {resp.text[:200]}")
            return False, None
        else:
            print(f"  ✅ 响应长度: {len(resp.text)} 字符")
            session_pattern = r"FR\.SessionMgr\.register\('([a-f0-9\-]{36})'"
            match = re.search(session_pattern, resp.text)
            if match:
                print(f"  ✅ 找到 Session ID: {match.group(1)}")
                return True, match.group(1)
            return False, None

    except requests.exceptions.RequestException as e:
        print(f"  ❌ 请求失败: {e}")
        return False, None


def main():
    print("=" * 60)
    print("报表入口 URL 发现 & Session ID 提取")
    print("=" * 60)
    print(f"目标服务器: {HOST}")
    print()

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"
    })
    # 设置 Cookie 以保兼容
    session.cookies.set("fine_auth_token", ACCESS_TOKEN)

    print("--- 测试 entry/access URL ---")
    found_entry = False
    session_id = None

    for uuid in ENTRY_CANDIDATES:
        success, sid, html = test_entry_access(session, uuid)
        if success:
            found_entry = True
            session_id = sid
            print(f"\n✅ 找到有效入口: entry_uuid = {uuid}")
            print(f"✅ Session ID: {session_id}")
            break
        print()

    if not found_entry and VIEWLET_CANDIDATES:
        print("--- 测试 viewlet URL ---")
        for v_url in VIEWLET_CANDIDATES:
            success, sid = test_viewlet(session, v_url)
            if success:
                session_id = sid
                break
            print()

    if session_id:
        print("\n" + "=" * 60)
        print("🎯 探针结论:")
        print("=" * 60)
        print(f"  session_id = \"{session_id}\"")
        print(f"  请在后续请求 Header 中添加:")
        print(f"    \"sessionID\": \"{session_id}\"")
    else:
        print("\n" + "=" * 60)
        print("❌ 探针结论: 未找到有效的报表入口")
        print("=" * 60)
        print("建议:")
        print("  1. 使用浏览器 DevTools 抓包确认入口 URL")
        print("  2. 检查 Token 是否有效")
        print("  3. 检查服务器地址和端口是否正确")


if __name__ == "__main__":
    main()

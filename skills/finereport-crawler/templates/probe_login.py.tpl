"""
探针模板: 登录 API 逆向
========================
目的: 确定登录端点、请求格式、Token 返回位置

使用方法:
  1. 修改 HOST / USERNAME / PASSWORD 配置
  2. python probe_login.py

输出:
  - access_token: 登录成功后获取的 Token
  - 如果失败，打印响应内容供分析
"""

import requests
import json

# ==================== 配置 ====================
HOST = "http://your-server:8080"
USERNAME = "admin"
PASSWORD = "your_password"
VERIFY_SSL = False  # 是否验证 SSL 证书

# ==================== 探针代码 ====================
requests.packages.urllib3.disable_warnings()


def probe_json_login():
    """探针1: JSON 格式登录"""
    print("=" * 60)
    print("[探针1] JSON 格式登录")
    print("=" * 60)

    session = requests.Session()
    login_url = f"{HOST}/webroot/decision/login"
    payload = {
        "username": USERNAME,
        "password": PASSWORD,
        "validity": -1  # -1 表示不过期
    }

    try:
        resp = session.post(login_url, json=payload,
                            verify=VERIFY_SSL, timeout=10)
        print(f"状态码: {resp.status_code}")
        print(f"响应头 Set-Cookie: {resp.headers.get('Set-Cookie', '无')}")
        print(f"响应 Body (前500字符): {resp.text[:500]}")

        # 尝试解析 JSON
        try:
            data = resp.json()
            print(f"\nJSON 结构顶层键: {list(data.keys())}")

            # 检查 Token 在哪个位置
            token_sources = {
                "data.accessToken": data.get("data", {}).get("accessToken"),
                "data.token": data.get("data", {}).get("token"),
                "accessToken": data.get("accessToken"),
                "token": data.get("token"),
            }

            print("\nToken 位置探测:")
            for source, token in token_sources.items():
                status = "✅ 找到" if token else "❌ 未找到"
                print(f"  {source}: {status}")

            # 检查 Cookie
            print(f"\nCookie 中的 fine_auth_token: {session.cookies.get('fine_auth_token', '未找到')}")

        except json.JSONDecodeError:
            print("响应不是 JSON 格式")

        return session

    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return None


def probe_form_login():
    """探针2: Form 格式登录"""
    print("\n" + "=" * 60)
    print("[探针2] Form 格式登录")
    print("=" * 60)

    session = requests.Session()
    login_url = f"{HOST}/webroot/decision/login"
    payload = {
        "username": USERNAME,
        "password": PASSWORD,
        "validity": -1
    }

    try:
        resp = session.post(login_url, data=payload,
                            verify=VERIFY_SSL, timeout=10)
        print(f"状态码: {resp.status_code}")
        print(f"响应头 Set-Cookie: {resp.headers.get('Set-Cookie', '无')}")

        try:
            data = resp.json()
            token = data.get("data", {}).get("accessToken")
            if token:
                print(f"✅ Form 登录成功，Token: {token[:20]}...")
            else:
                print(f"❌ Form 登录失败，响应: {resp.text[:300]}")
        except json.JSONDecodeError:
            print(f"响应不是 JSON: {resp.text[:300]}")

        return session

    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return None


def main():
    print(f"目标服务器: {HOST}")
    print(f"用户名: {USERNAME}")
    print()

    session1 = probe_json_login()
    session2 = probe_form_login()

    print("\n" + "=" * 60)
    print("探针结论:")
    print("=" * 60)
    print("根据以上输出判断:")
    print("  1. 登录端点是否为 /webroot/decision/login")
    print("  2. Token 在 JSON body 中 (data.accessToken) 还是 Cookie 中")
    print("  3. 请求格式为 JSON 还是 Form")
    print("  4. 是否需要其他参数 (如验证码)")


if __name__ == "__main__":
    main()

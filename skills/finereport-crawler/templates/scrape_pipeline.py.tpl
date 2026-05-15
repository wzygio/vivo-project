"""
爬虫模板: 帆软报表完整数据爬取流水线
=====================================
将探针阶段确定的配置填入下方 CONFIG 区，即可执行完整爬取。

流程:
  登录 → 获取 Session ID → 设置筛选 → 获取数据 → 解析 → 导出 CSV

使用方法:
  1. 修改 CONFIG 区的配置（根据探针阶段的结果）
  2. python scrape_pipeline.py
  3. 输出 report_data.csv
"""

import requests
import re
import csv
import os
from bs4 import BeautifulSoup

# ============================================================
# CONFIG — 根据探针阶段的结果修改此处
# ============================================================

class Config:
    # --- 服务器 ---
    HOST = "http://your-server:8080"
    VERIFY_SSL = False

    # --- 登录凭证 ---
    USERNAME = "admin"
    PASSWORD = "your_password"

    # --- 报表入口 UUID ---
    ENTRY_UUID = "your-entry-uuid-here"

    # --- 筛选参数 (探针阶段确定的值) ---
    FILTER_PARAM = "cmcbEQPID"     # 筛选参数名
    FILTER_VALUE = "3TED01"        # 筛选值

    # --- 输出 ---
    OUTPUT_FILE = "report_data.csv"
    OUTPUT_ENCODING = "utf-8-sig"  # UTF-8 BOM 兼容 Excel

    # --- 报表文件名 (从 page_content URL 中提取) ---
    REPORT_CPT = "your_report.cpt"


# ============================================================
# 核心爬虫类
# ============================================================

class FineReportCrawler:
    """帆软报表爬虫"""

    def __init__(self, config):
        self.cfg = config
        self.session = requests.Session()
        self.access_token = None
        self.dynamic_session_id = None

        # 禁用 SSL 警告
        requests.packages.urllib3.disable_warnings()

        # 设置 User-Agent
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0.0.0 Safari/537.36"
        })

    def login(self) -> bool:
        """第1步: 登录获取 Access Token"""
        print("[1/4] 登录中...", end=" ")

        login_url = f"{self.cfg.HOST}/webroot/decision/login"
        payload = {
            "username": self.cfg.USERNAME,
            "password": self.cfg.PASSWORD,
            "validity": -1
        }

        try:
            resp = self.session.post(
                login_url, json=payload,
                verify=self.cfg.VERIFY_SSL, timeout=10
            )

            if resp.status_code != 200:
                print(f"❌ HTTP {resp.status_code}")
                return False

            data = resp.json()

            # 尝试从 JSON body 提取 Token
            self.access_token = data.get("data", {}).get("accessToken")

            # 如果 JSON body 中没有，检查 Cookie
            if not self.access_token:
                self.access_token = self.session.cookies.get("fine_auth_token")

            if not self.access_token:
                print("❌ 未找到 Token")
                print(f"   响应: {resp.text[:200]}")
                return False

            # 设置 Authorization Header 和 Cookie
            self.session.headers.update({
                "Authorization": f"Bearer {self.access_token}"
            })
            self.session.cookies.set("fine_auth_token", self.access_token)

            print(f"✅ (Token: {self.access_token[:20]}...)")
            return True

        except requests.exceptions.RequestException as e:
            print(f"❌ 网络错误: {e}")
            return False
        except (KeyError, ValueError, TypeError) as e:
            print(f"❌ 解析错误: {e}")
            print(f"   响应: {resp.text[:200]}")
            return False

    def get_session_id(self) -> bool:
        """第2步: 访问报表入口页，提取 Session ID"""
        print("[2/4] 获取 Session ID...", end=" ")

        entry_url = (
            f"{self.cfg.HOST}"
            f"/webroot/decision/v10/entry/access/"
            f"{self.cfg.ENTRY_UUID}"
        )

        try:
            resp = self.session.get(
                entry_url, verify=self.cfg.VERIFY_SSL, timeout=15
            )

            pattern = r"FR\.SessionMgr\.register\('([a-f0-9\-]{36})'"
            match = re.search(pattern, resp.text)

            if not match:
                print("❌ 未找到 FR.SessionMgr.register")
                return False

            self.dynamic_session_id = match.group(1)

            # 在 Header 中设置 sessionID
            self.session.headers.update({
                "sessionID": self.dynamic_session_id
            })

            print(f"✅ ({self.dynamic_session_id[:8]}...)")
            return True

        except requests.exceptions.RequestException as e:
            print(f"❌ 网络错误: {e}")
            return False

    def set_filter(self) -> bool:
        """第3步: 通过 parameters_d 端点设置筛选条件"""
        print("[3/4] 设置筛选条件...", end=" ")

        if not self.cfg.FILTER_PARAM:
            print("⏭️  跳过（未配置筛选参数）")
            return True

        params_url = (
            f"{self.cfg.HOST}/webroot/decision/view/report"
            f"?op=fr_dialog&cmd=parameters_d"
            f"&sessionID={self.dynamic_session_id}"
        )

        try:
            # ❗ 关键: 使用 data= 而非 json=
            resp = self.session.post(
                params_url,
                data={self.cfg.FILTER_PARAM: self.cfg.FILTER_VALUE},
                verify=self.cfg.VERIFY_SSL, timeout=15
            )

            if resp.status_code != 200:
                print(f"❌ HTTP {resp.status_code}")
                return False

            # 部分版本返回 JSON
            try:
                result = resp.json()
                if result.get("error"):
                    print(f"❌ 服务器错误: {result['error']}")
                    return False
            except (ValueError, TypeError):
                pass  # 非 JSON 响应也属正常

            print(f"✅ ({self.cfg.FILTER_PARAM}={self.cfg.FILTER_VALUE})")
            return True

        except requests.exceptions.RequestException as e:
            print(f"❌ 网络错误: {e}")
            return False

    def fetch_data(self) -> str | None:
        """第4步: 请求数据页，返回 HTML"""
        print("[4/4] 获取报表数据...", end=" ")

        page_url = (
            f"{self.cfg.HOST}/webroot/decision/view/report"
            f"?viewlet=schedule%2F{self.cfg.REPORT_CPT}"
            f"&op=page_content"
            f"&sessionID={self.dynamic_session_id}"
        )

        try:
            resp = self.session.post(
                page_url,
                verify=self.cfg.VERIFY_SSL, timeout=30
            )

            if resp.status_code != 200:
                print(f"❌ HTTP {resp.status_code}")
                return None

            print(f"✅ ({len(resp.text)} 字符)")
            return resp.text

        except requests.exceptions.RequestException as e:
            print(f"❌ 网络错误: {e}")
            return None

    @staticmethod
    def parse_html_table(html: str) -> tuple[list, list]:
        """解析 HTML 表格为表头 + 数据行"""
        soup = BeautifulSoup(html, "html.parser")

        # 尝试多种表格选择器
        table = (
            soup.find("table", id="r-0")
            or soup.find("table", class_="x-table")
            or soup.find("table")
        )

        if not table:
            print("  ⚠️  未找到表格")
            return [], []

        rows = table.find_all("tr")
        if not rows:
            return [], []

        # 表头
        headers = [
            th.get_text(strip=True)
            for th in rows[0].find_all(["th", "td"])
        ]

        # 数据行
        data = []
        for tr in rows[1:]:
            row = [
                td.get_text(strip=True)
                for td in tr.find_all("td")
            ]
            if row:  # 跳过空行
                data.append(row)

        print(f"  📊 表头: {headers}")
        print(f"  📊 数据: {len(data)} 行")
        return headers, data

    @staticmethod
    def export_csv(headers: list, data: list,
                   filepath: str, encoding: str = "utf-8-sig") -> bool:
        """导出为 CSV 文件"""
        try:
            with open(filepath, "w", newline="", encoding=encoding) as f:
                writer = csv.writer(f)
                writer.writerow(headers)
                writer.writerows(data)
            print(f"  💾 已导出: {os.path.abspath(filepath)}")
            return True
        except IOError as e:
            print(f"  ❌ 导出失败: {e}")
            return False

    def run(self) -> bool:
        """执行完整爬取流水线"""
        print("=" * 60)
        print("帆软报表爬虫流水线")
        print("=" * 60)
        print(f"目标: {self.cfg.HOST}")
        print()

        # 第1步: 登录
        if not self.login():
            return False
        print()

        # 第2步: 获取 Session ID
        if not self.get_session_id():
            return False
        print()

        # 第3步: 设置筛选
        if not self.set_filter():
            return False
        print()

        # 第4步: 获取并解析数据
        html = self.fetch_data()
        if not html:
            return False
        print()

        # 解析表格
        print("-" * 40)
        print("解析数据...")
        headers, data = self.parse_html_table(html)

        if not headers:
            print("❌ 未解析到有效数据")
            return False

        # 导出 CSV
        self.export_csv(
            headers, data,
            self.cfg.OUTPUT_FILE,
            self.cfg.OUTPUT_ENCODING
        )

        print()
        print("=" * 60)
        print(f"✅ 爬取完成！共 {len(data)} 条记录")
        print(f"   输出文件: {os.path.abspath(self.cfg.OUTPUT_FILE)}")
        print("=" * 60)
        return True


# ============================================================
# 主入口
# ============================================================

def main():
    config = Config()
    crawler = FineReportCrawler(config)
    success = crawler.run()

    if not success:
        print("\n❌ 爬取失败，请检查:")
        print("  1. 服务器地址是否正确")
        print("  2. 登录凭证是否正确")
        print("  3. Entry UUID 是否正确")
        print("  4. 筛选参数名是否正确")
        print("  5. 报表文件名是否正确")
        print("\n使用 skills/finereport-crawler/templates/ 下的探针脚本排查问题")


if __name__ == "__main__":
    main()

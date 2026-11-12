"""最简调用示例：修改文件末尾的 params，然后直接运行。"""

import json
import os
from getpass import getpass
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def main(params=None, api_key=None):
    # ① API 地址
    api_url = "http://10.72.26.31:8890/api/v1/sheet-anomalies"
    
    # ② Params：筛选条件拼在网址后，例如：http://10.72.26.31:8890/api/v1/sheet-anomalies?sub_domain=SPC
    if params:
        api_url += "?" + urlencode(params)

    # ③ Headers：密钥放在请求头，不放在 params 中
    api_key = "TIANZHUBEIJIXINGZHUANXIANG@20261102"
    request = Request(api_url, method="GET", headers={"Accept": "application/json"})
    request.add_unredirected_header("X-API-Key", api_key)  # 密钥不随重定向转发

    # ④ Body：本接口不需要请求体，所以不设置 data
    with urlopen(request, timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))

if __name__ == "__main__":
    # ===== 在这里设置查询条件：需要哪项，就删除该行开头的 # =====
    # 全部注释时，查询上周的全部授权范围；值必须加引号。
    params = {
        # "product": "M626",           # 产品
        # "sub_domain": "AOI_TT",      # 分类：SPC / CTQ / AOI_TT / AOI_RS / Yield / Q-Time
        # "event_type": "OOS",         # 事件：OOS / OOC / CPK / CPM；不填为全部适用事件
        # "start_date": "2026-11-02",  # 开始日期，包含当天
        # "end_date": "2026-11-08",    # 截止日期，包含当天
        # "factory": "ARRAY",         # 厂别
        # "step_id": "00100",          # 站点
        # "layer": "PI2",              # 层别
        # "parameter_name": "参数名",  # 监控参数
        # "entity_type": "sheet",      # 对象类型：sheet / lot / capability
        # "entity_id": "00001",        # 单个 Sheet ID / Lot ID
    }

    result = main(params)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    # result["data"] 是明细；result["meta"]["complete"] 表示来源是否全部可用。

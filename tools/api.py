from getpass import getpass
from tools.call_anomaly_api import main

result = main(api_key='TIANZHUBEIJIXINGZHUANXIANG@20261102', params={'product': 'M626', 'sub_domain': 'SPC'})

records = result["data"]
print(f"获取 {len(records)} 条明细")
print("来源是否完整：", result["meta"]["complete"])
print(records)
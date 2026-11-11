import json
import os
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


def main(std_result: dict | str | None = None, api_key: str | None = None,
         product: str | None = None, sub_domain: str | None = None,
         event_type: str | None = None, start_date: str | None = None,
         end_date: str | None = None, factory: str | None = None,
         step_id: str | None = None, layer: str | None = None,
         parameter_name: str | None = None, entity_type: str | None = None,
         entity_id: str | None = None, **kwargs: object) -> dict:
    # ===== ① 取「参数标准化」节点的 result（字典或 JSON 字符串）=====
    if isinstance(std_result, str):
        try:
            std_result = json.loads(std_result)
        except ValueError:
            return {"success": False, "error": "std_result 不是有效 JSON", "data": [], "meta": {}}
    if std_result is not None and not isinstance(std_result, dict):
        return {"success": False, "error": "std_result 须为字典或 JSON 对象", "data": [], "meta": {}}
    std = std_result or {}

    # ===== ② 组装查询参数；标准化结果优先，空值使用形参，均为空则省略 =====
    fields = {
        "product": product, "sub_domain": sub_domain, "event_type": event_type,
        "start_date": start_date, "end_date": end_date, "factory": factory,
        "step_id": step_id, "layer": layer, "parameter_name": parameter_name,
        "entity_type": entity_type, "entity_id": entity_id,
    }
    params = {}
    for name, default in fields.items():
        value = std.get(name) or default
        if value is not None and value != "":
            if not isinstance(value, str):
                return {"success": False, "error": f"{name} 须为字符串", "data": [], "meta": {}}
            params[name] = value

    api_key = api_key or os.environ.get("API_KEY") or os.environ.get("VIVO_API_KEY")
    if (not isinstance(api_key, str) or not api_key or len(api_key) > 1024
            or any(not 33 <= ord(char) <= 126 for char in api_key)):
        return {"success": False, "error": "请提供有效原始 API Key", "data": [], "meta": {}}

    # ===== ③ GET 请求：参数放 URL，不发送 JSON 请求体 =====
    api_url = "http://10.72.26.31:8890/api/v1/sheet-anomalies"
    if params:
        api_url += "?" + urlencode(params)
    req = Request(url=api_url, method="GET", headers={"Accept": "application/json"})
    # 正常请求携带密钥；若地址重定向，不将密钥转发给新地址。
    req.add_unredirected_header("X-API-Key", api_key)
    try:
        with urlopen(req, timeout=120) as resp:
            res = json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        status = exc.code
        exc.close()
        return {"success": False, "error": f"HTTP {status}：请检查密钥、查询条件或服务状态",
                "data": [], "meta": {}}
    except (URLError, OSError, TimeoutError):
        return {"success": False, "error": "连接失败或超时，请检查网络与 API 地址", "data": [], "meta": {}}
    except (ValueError, UnicodeError):
        return {"success": False, "error": "服务未返回有效 JSON", "data": [], "meta": {}}

    # ===== ④ 返回明细和完整性说明；来源缺失与没有异常是两回事 =====
    if (not isinstance(res, dict) or not isinstance(res.get("data"), list)
            or not isinstance(res.get("meta"), dict)):
        return {"success": False, "error": "响应缺少 data 或 meta", "data": [], "meta": {}}
    meta = res["meta"]
    if (type(meta.get("complete")) is not bool
            or not isinstance(meta.get("failed_sources"), list)
            or meta["complete"] != (not meta["failed_sources"])
            or type(meta.get("has_more")) is not bool):
        return {"success": False, "error": "响应完整性说明无效", "data": [], "meta": {}}
    if meta["has_more"]:
        return {"success": False, "error": "意外收到分页结果，未将部分页视为完整查询", "data": [], "meta": {}}
    return {"success": True, "data": res["data"], "meta": meta,
            "msg": "查询成功" if meta["complete"] else "查询成功，但部分来源不可用"}


if __name__ == "__main__":
    from getpass import getpass

    key = "TIANZHUBEIJIXINGZHUANXIANG@20261102"
    result = main(api_key=key)
    del key
    print(json.dumps(result, ensure_ascii=False, indent=2))

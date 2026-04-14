from operators.active_store import run_active_store_operator
from operators.retained_intention import run_retained_intention_operator


def _is_active_store_plan(plan: dict, user_query: str) -> bool:
    metric = (plan or {}).get("metric", {}) or {}
    text = " ".join(
        [
            str(user_query or ""),
            str(metric.get("alias") or ""),
            str(metric.get("business_name") or ""),
            str(metric.get("field") or ""),
        ]
    )
    return "在营门店" in text


def _is_retained_intention_plan(plan: dict, user_query: str) -> bool:
    metric = (plan or {}).get("metric", {}) or {}
    text = " ".join(
        [
            str(user_query or ""),
            str(metric.get("alias") or ""),
            str(metric.get("business_name") or ""),
            str(metric.get("field") or ""),
        ]
    )
    return "留存小订" in text


def run_registered_operator(plan: dict, user_query: str, query_tool) -> dict | None:
    if _is_retained_intention_plan(plan, user_query):
        query_tool._load_datasets()
        df = query_tool.datasets.get("order_data")
        if df is None:
            return {"type": "retained_intention", "error": "dataset_not_found", "message": "缺少 order_data 数据集"}
        time = (plan or {}).get("time", {}) or {}
        start = time.get("start")
        end = time.get("end")
        if not start or not end:
            return {"type": "retained_intention", "error": "missing_time_window", "message": "留存小订算子需要明确 start/end"}
        series = None
        filters = plan.get("filters", [])
        for f in filters:
            if f.get("field") in ("series", "series_group_logic") and f.get("op") == "==":
                series = f.get("value")
                break
        
        # 确保数据集应用了 series_group_logic
        if "series_group_logic" not in df.columns:
            import sys
            import json
            sys.path.append('/Users/zihao_/Documents/coding/dataset/scripts')
            from analyze_order import apply_series_group_logic
            try:
                with open('/Users/zihao_/Documents/github/26W06_Tool_calls/schema/business_definition.json', 'r') as bdf:
                    bdef = json.load(bdf)
                df = apply_series_group_logic(df.copy(), bdef)
            except Exception as e:
                pass
                
        return run_retained_intention_operator(df=df, series=series, start=start, end=end)

    if not _is_active_store_plan(plan, user_query):
        return None
    query_tool._load_datasets()
    df = query_tool.datasets.get("order_data")
    if df is None:
        return {"type": "active_store", "error": "dataset_not_found", "message": "缺少 order_data 数据集"}
    time = (plan or {}).get("time", {}) or {}
    start = time.get("start")
    end = time.get("end")
    if not start or not end:
        return {"type": "active_store", "error": "missing_time_window", "message": "在营门店算子需要明确 start/end"}
    return run_active_store_operator(df=df, start=start, end=end)

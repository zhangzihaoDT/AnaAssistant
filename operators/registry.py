from operators.active_store import run_active_store_operator


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


def run_registered_operator(plan: dict, user_query: str, query_tool) -> dict | None:
    if not _is_active_store_plan(plan, user_query):
        return None
    query_tool._load_datasets()
    df = query_tool.datasets.get("order_full_data")
    if df is None:
        return {"type": "active_store", "error": "dataset_not_found", "message": "缺少 order_full_data 数据集"}
    time = (plan or {}).get("time", {}) or {}
    start = time.get("start")
    end = time.get("end")
    if not start or not end:
        return {"type": "active_store", "error": "missing_time_window", "message": "在营门店算子需要明确 start/end"}
    return run_active_store_operator(df=df, start=start, end=end)

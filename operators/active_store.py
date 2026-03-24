import pandas as pd


def _to_day_series(df: pd.DataFrame) -> pd.Series:
    date_series = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    if "order_create_date" in df.columns:
        s = pd.to_datetime(df["order_create_date"], errors="coerce")
        date_series = s
    if "order_create_time" in df.columns:
        s2 = pd.to_datetime(df["order_create_time"], errors="coerce").dt.floor("D")
        date_series = date_series.fillna(s2)
    return pd.to_datetime(date_series, errors="coerce").dt.floor("D")


def _calc_active_store_count(df: pd.DataFrame, target_date: pd.Timestamp) -> int:
    work = df.copy()
    work["date"] = _to_day_series(work)
    work["store_create_date"] = pd.to_datetime(work.get("store_create_date"), errors="coerce")
    work = work.dropna(subset=["store_name", "date"])
    if work.empty:
        return 0
    open_map = work.groupby("store_name")["store_create_date"].min()
    d = pd.Timestamp(target_date).normalize()
    window_start = d - pd.Timedelta(days=29)
    activity = work[(work["date"] >= window_start) & (work["date"] <= d)]
    if activity.empty:
        return 0
    stores = pd.Index(activity["store_name"].dropna().unique())
    store_open_dates = open_map.reindex(stores)
    is_open = store_open_dates <= d
    open_stores = stores[is_open.fillna(False).to_numpy()]
    return int(open_stores.size)


def run_active_store_operator(df: pd.DataFrame, start: str, end: str) -> dict:
    start_ts = pd.to_datetime(start, errors="coerce")
    end_ts = pd.to_datetime(end, errors="coerce")
    if pd.isna(start_ts) or pd.isna(end_ts):
        return {"type": "active_store", "error": "invalid_time_range", "message": "start/end 时间解析失败"}
    start_day = pd.Timestamp(start_ts).normalize()
    end_day = pd.Timestamp(end_ts).normalize()
    if end_day <= start_day:
        return {"type": "active_store", "error": "invalid_time_range", "message": "end 必须大于 start"}
    days = pd.date_range(start_day, end_day - pd.Timedelta(days=1), freq="D")
    rows: list[dict] = []
    for d in days:
        rows.append({"date": d.strftime("%Y-%m-%d"), "active_store_count": _calc_active_store_count(df, d)})
    if not rows:
        return {"type": "active_store", "start": start_day.strftime("%Y-%m-%d"), "end": end_day.strftime("%Y-%m-%d"), "daily_rows": []}
    max_row = max(rows, key=lambda x: x["active_store_count"])
    min_row = min(rows, key=lambda x: x["active_store_count"])
    return {
        "type": "active_store",
        "start": start_day.strftime("%Y-%m-%d"),
        "end": end_day.strftime("%Y-%m-%d"),
        "window_days": len(rows),
        "max_active_store_count": int(max_row["active_store_count"]),
        "max_date": max_row["date"],
        "min_active_store_count": int(min_row["active_store_count"]),
        "min_date": min_row["date"],
        "daily_rows": rows,
    }

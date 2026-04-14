import pandas as pd

def run_retained_intention_operator(df: pd.DataFrame, series: str, start: str, end: str) -> dict:
    if df is None or df.empty:
        return {"error": "dataset_empty", "message": "数据集为空"}

    start_day = pd.to_datetime(start)
    end_day = pd.to_datetime(end)
    
    # 由于 planner 解析自然语言时，如果是“2025-08-15到2025-09-10”，
    # end_day 会被处理成开区间 2025-09-11 00:00:00，
    # 而如果 end_day 是 00:00:00 的开区间，实际上代表的业务截止日是它减去 1 天。
    if end_day.hour == 0 and end_day.minute == 0 and end_day.second == 0:
        actual_end_day = end_day - pd.Timedelta(days=1)
    else:
        actual_end_day = end_day

    n_days = int((actual_end_day.normalize() - start_day.normalize()).days + 1)
    n_days = max(1, n_days)
    
    presale_end_excl = actual_end_day + pd.Timedelta(days=1)
    window_end_excl = start_day + pd.Timedelta(days=n_days)
    window_end_excl = min(window_end_excl, presale_end_excl)

    df_model = df
    if series:
        if "series_group_logic" in df.columns:
            df_model = df[df['series_group_logic'] == series]
        elif "series" in df.columns:
            df_model = df[df['series'] == series]

    mask_time = (df_model['intention_payment_time'].notna()) & \
                (df_model['intention_payment_time'] >= start_day) & \
                (df_model['intention_payment_time'] < window_end_excl)

    mask_retained = df_model['intention_refund_time'].isna() | \
                    (df_model['intention_refund_time'] > window_end_excl)

    retained_orders = df_model.loc[mask_time & mask_retained, 'order_number'].dropna().drop_duplicates()
    retained_count = int(retained_orders.nunique())

    return {
        "type": "retained_intention",
        "series": series,
        "start": start_day.strftime("%Y-%m-%d"),
        "end": actual_end_day.strftime("%Y-%m-%d"),
        "retained_count": retained_count
    }

import pandas as pd


class StatisticsTool:
    def perform_statistics(self, request: dict, input_df: pd.DataFrame) -> dict | str:
        stat_type = (request or {}).get("type")
        if stat_type == "weekly_decline_ratio":
            return self._weekly_decline_ratio(request, input_df)
        if stat_type == "daily_threshold_count":
            return self._daily_threshold_count(request, input_df)
        if stat_type == "daily_mean":
            return self._daily_mean(request, input_df)
        if stat_type == "daily_percentile_rank":
            return self._daily_percentile_rank(request, input_df)
        if stat_type == "weekend_percentile_rank":
            return self._weekend_percentile_rank(request, input_df)
        if stat_type == "weekday_percentile_rank":
            return self._weekday_percentile_rank(request, input_df)
        return f"不支持的统计类型: {stat_type}"

    @staticmethod
    def _weekly_decline_ratio(request: dict, input_df: pd.DataFrame) -> dict | str:
        if input_df is None or input_df.empty:
            return "统计分析无可用数据。"

        weekdays = StatisticsTool._normalize_weekdays(request.get("weekdays"))
        window_weeks = request.get("window_weeks")
        if isinstance(window_weeks, str) and window_weeks.isdigit():
            window_weeks = int(window_weeks)
        if not isinstance(window_weeks, int) or window_weeks <= 0:
            window_weeks = 10

        series_cols = {"week_start", "numerator", "denominator", "lock_rate", "delta", "is_decline"}
        if bool(request.get("series_input")):
            if not series_cols.issubset(set(input_df.columns)):
                return f"统计分析缺少必要列: {', '.join(sorted(series_cols))}"
            grouped = input_df.copy()
        else:
            time_field = request.get("time_field") or "Assign Time 年/月/日"
            numerator_alias = request.get("numerator_alias") or "门店当日锁单数"
            denominator_alias = request.get("denominator_alias") or "门店线索数"
            grouped = StatisticsTool.build_weekly_wow_series(
                input_df=input_df,
                time_field=time_field,
                numerator_alias=numerator_alias,
                denominator_alias=denominator_alias,
                weekdays=weekdays,
                window_weeks=window_weeks,
            )
            if isinstance(grouped, str):
                return grouped

        grouped = grouped.tail(window_weeks).reset_index(drop=True)
        grouped["is_decline"] = grouped["is_decline"].astype(bool)
        decline_count = int(grouped["is_decline"].sum())
        total_weeks = int(len(grouped))
        ratio = 0.0 if total_weeks == 0 else (decline_count / total_weeks)
        weekly_rows: list[dict] = []
        for _, row in grouped.iterrows():
            lock_rate = row.get("lock_rate")
            delta = row.get("delta")
            weekly_rows.append(
                {
                    "week_start": row["week_start"].strftime("%Y-%m-%d"),
                    "numerator": float(row["numerator"]),
                    "denominator": float(row["denominator"]),
                    "lock_rate": None if pd.isna(lock_rate) else float(lock_rate),
                    "delta": None if pd.isna(delta) else float(delta),
                    "is_decline": bool(row["is_decline"]),
                }
            )

        return {
            "type": "weekly_decline_ratio",
            "window_weeks": int(window_weeks),
            "weekdays": weekdays,
            "decline_weeks": decline_count,
            "total_weeks": total_weeks,
            "decline_ratio": ratio,
            "weekly_rows": weekly_rows,
        }

    @staticmethod
    def _normalize_weekdays(weekdays: list[int] | None) -> list[int]:
        if not isinstance(weekdays, list) or not weekdays:
            weekdays = [4, 5]
        normalized = [int(w) for w in weekdays if isinstance(w, (int, float, str)) and str(w).isdigit()]
        normalized = [w for w in normalized if 1 <= int(w) <= 7]
        normalized = sorted(list(dict.fromkeys(int(w) for w in normalized)))
        if not normalized:
            return [4, 5]
        return normalized

    @staticmethod
    def build_weekly_wow_series(
        input_df: pd.DataFrame,
        time_field: str,
        numerator_alias: str,
        denominator_alias: str,
        weekdays: list[int],
        window_weeks: int,
    ) -> pd.DataFrame | str:
        if time_field not in input_df.columns:
            return f"统计分析缺少时间列: {time_field}"
        if numerator_alias not in input_df.columns or denominator_alias not in input_df.columns:
            return f"统计分析缺少必要列: {numerator_alias} / {denominator_alias}"

        df = input_df.copy()
        raw_time = df[time_field].astype(str).str.strip()
        parsed_cn = pd.to_datetime(raw_time, errors="coerce", format="%Y年%m月%d日")
        if float(parsed_cn.notna().mean()) >= 0.8:
            df[time_field] = parsed_cn
        else:
            df[time_field] = pd.to_datetime(raw_time, errors="coerce")
        df = df[df[time_field].notna()]
        if df.empty:
            return "统计分析时间列无法解析为日期。"

        normalized_weekdays = StatisticsTool._normalize_weekdays(weekdays)
        df["_weekday"] = df[time_field].dt.dayofweek + 1
        df = df[df["_weekday"].isin(normalized_weekdays)]
        if df.empty:
            return "筛选周内日后无数据。"

        df["week_start"] = (df[time_field] - pd.to_timedelta(df[time_field].dt.dayofweek, unit="D")).dt.normalize()
        grouped = (
            df.groupby("week_start", as_index=False)
            .agg({numerator_alias: "sum", denominator_alias: "sum"})
            .sort_values("week_start")
            .tail(int(window_weeks))
            .reset_index(drop=True)
        )
        grouped = grouped.rename(columns={numerator_alias: "numerator", denominator_alias: "denominator"})
        grouped["lock_rate"] = grouped.apply(
            lambda r: None if float(r["denominator"]) == 0.0 else float(r["numerator"]) / float(r["denominator"]),
            axis=1,
        )
        grouped["delta"] = grouped["lock_rate"] - grouped["lock_rate"].shift(1)
        grouped["is_decline"] = grouped["delta"].apply(lambda x: bool(pd.notna(x) and x < 0))
        return grouped[["week_start", "numerator", "denominator", "lock_rate", "delta", "is_decline"]]

    @staticmethod
    def _daily_threshold_count(request: dict, input_df: pd.DataFrame) -> dict | str:
        if input_df is None or input_df.empty:
            return "统计分析无可用数据。"

        time_field = request.get("time_field")
        metric_alias = request.get("metric_alias")
        if not isinstance(time_field, str) or not time_field:
            return "统计分析缺少必要参数: time_field"
        if not isinstance(metric_alias, str) or not metric_alias:
            return "统计分析缺少必要参数: metric_alias"
        if time_field not in input_df.columns:
            return f"统计分析缺少时间列: {time_field}"
        if metric_alias not in input_df.columns:
            return f"统计分析缺少指标列: {metric_alias}"

        op = request.get("op")
        if op not in {">", ">=", "<", "<=", "==", "!="}:
            op = ">"

        threshold = request.get("threshold")
        try:
            threshold = float(threshold)
        except Exception:
            threshold = 0.0

        window_days = request.get("window_days")
        if isinstance(window_days, str) and window_days.isdigit():
            window_days = int(window_days)
        if not isinstance(window_days, int) or window_days <= 0:
            window_days = 30

        df = input_df.copy()
        raw_time = df[time_field].astype(str).str.strip()
        parsed_cn = pd.to_datetime(raw_time, errors="coerce", format="%Y年%m月%d日")
        if float(parsed_cn.notna().mean()) >= 0.8:
            df[time_field] = parsed_cn
        else:
            df[time_field] = pd.to_datetime(raw_time, errors="coerce")
        df = df[df[time_field].notna()]
        if df.empty:
            return "统计分析时间列无法解析为日期。"

        df["date"] = df[time_field].dt.normalize()
        grouped = (
            df.groupby("date", as_index=False)
            .agg({metric_alias: "sum"})
            .sort_values("date")
            .tail(window_days)
            .reset_index(drop=True)
        )
        if grouped.empty:
            return "统计分析在窗口内无可用日期数据。"

        grouped["value"] = grouped[metric_alias].astype(float)
        date_start_raw = request.get("date_start")
        date_end_raw = request.get("date_end")
        date_start = pd.to_datetime(date_start_raw, errors="coerce") if isinstance(date_start_raw, str) else pd.NaT
        date_end = pd.to_datetime(date_end_raw, errors="coerce") if isinstance(date_end_raw, str) else pd.NaT
        if pd.notna(date_start) and pd.notna(date_end) and pd.Timestamp(date_end) > pd.Timestamp(date_start):
            start = pd.Timestamp(date_start).normalize()
            end = pd.Timestamp(date_end).normalize()
        else:
            end = pd.Timestamp(grouped["date"].max()).normalize() + pd.Timedelta(days=1)
            start = end - pd.Timedelta(days=int(window_days))
        date_index = pd.date_range(start=start, end=end - pd.Timedelta(days=1), freq="D")
        series = grouped.set_index("date")["value"].reindex(date_index, fill_value=0.0)

        def _match(v: float) -> bool:
            if op == ">":
                return v > threshold
            if op == ">=":
                return v >= threshold
            if op == "<":
                return v < threshold
            if op == "<=":
                return v <= threshold
            if op == "==":
                return v == threshold
            return v != threshold

        matched = series.apply(_match)
        matched_days = int(matched.sum())
        total_days = int(len(series))
        matched_ratio = 0.0 if total_days == 0 else (matched_days / total_days)

        daily_rows: list[dict] = []
        for date, value in series.items():
            daily_rows.append(
                {
                    "date": pd.Timestamp(date).strftime("%Y-%m-%d"),
                    "value": float(value),
                    "matched": bool(_match(float(value))),
                }
            )

        return {
            "type": "daily_threshold_count",
            "window_days": int(window_days),
            "op": op,
            "threshold": float(threshold),
            "metric_alias": metric_alias,
            "matched_days": matched_days,
            "total_days": total_days,
            "matched_ratio": matched_ratio,
            "daily_rows": daily_rows,
        }

    @staticmethod
    def _daily_mean(request: dict, input_df: pd.DataFrame) -> dict | str:
        if input_df is None or input_df.empty:
            return "统计分析无可用数据。"

        time_field = request.get("time_field")
        metric_alias = request.get("metric_alias")
        if not isinstance(time_field, str) or not time_field:
            return "统计分析缺少必要参数: time_field"
        if not isinstance(metric_alias, str) or not metric_alias:
            return "统计分析缺少必要参数: metric_alias"
        if time_field not in input_df.columns:
            return f"统计分析缺少时间列: {time_field}"
        if metric_alias not in input_df.columns:
            return f"统计分析缺少指标列: {metric_alias}"

        window_days = request.get("window_days")
        if isinstance(window_days, str) and window_days.isdigit():
            window_days = int(window_days)
        if not isinstance(window_days, int) or window_days <= 0:
            window_days = 30

        df = input_df.copy()
        raw_time = df[time_field].astype(str).str.strip()
        parsed_cn = pd.to_datetime(raw_time, errors="coerce", format="%Y年%m月%d日")
        if float(parsed_cn.notna().mean()) >= 0.8:
            df[time_field] = parsed_cn
        else:
            df[time_field] = pd.to_datetime(raw_time, errors="coerce")
        df = df[df[time_field].notna()]
        if df.empty:
            return "统计分析时间列无法解析为日期。"

        df["date"] = df[time_field].dt.normalize()
        grouped = (
            df.groupby("date", as_index=False)
            .agg({metric_alias: "sum"})
            .sort_values("date")
            .tail(window_days)
            .reset_index(drop=True)
        )
        if grouped.empty:
            return "统计分析在窗口内无可用日期数据。"

        grouped["value"] = grouped[metric_alias].astype(float)
        date_start_raw = request.get("date_start")
        date_end_raw = request.get("date_end")
        date_start = pd.to_datetime(date_start_raw, errors="coerce") if isinstance(date_start_raw, str) else pd.NaT
        date_end = pd.to_datetime(date_end_raw, errors="coerce") if isinstance(date_end_raw, str) else pd.NaT
        if pd.notna(date_start) and pd.notna(date_end) and pd.Timestamp(date_end) > pd.Timestamp(date_start):
            start = pd.Timestamp(date_start).normalize()
            end = pd.Timestamp(date_end).normalize()
        else:
            end = pd.Timestamp(grouped["date"].max()).normalize() + pd.Timedelta(days=1)
            start = end - pd.Timedelta(days=int(window_days))
        date_index = pd.date_range(start=start, end=end - pd.Timedelta(days=1), freq="D")
        series = grouped.set_index("date")["value"].reindex(date_index, fill_value=0.0)
        total_days = int(len(series))
        daily_mean = float(series.mean()) if total_days else 0.0
        daily_rows: list[dict] = []
        for date, value in series.items():
            daily_rows.append(
                {
                    "date": pd.Timestamp(date).strftime("%Y-%m-%d"),
                    "value": float(value),
                }
            )

        return {
            "type": "daily_mean",
            "window_days": int(window_days),
            "metric_alias": metric_alias,
            "daily_mean": daily_mean,
            "total_days": total_days,
            "daily_rows": daily_rows,
        }

    @staticmethod
    def _daily_percentile_rank(request: dict, input_df: pd.DataFrame) -> dict | str:
        if input_df is None or input_df.empty:
            return "统计分析无可用数据。"

        time_field = request.get("time_field")
        metric_alias = request.get("metric_alias")
        if not isinstance(time_field, str) or not time_field:
            return "统计分析缺少必要参数: time_field"
        if not isinstance(metric_alias, str) or not metric_alias:
            return "统计分析缺少必要参数: metric_alias"
        if time_field not in input_df.columns:
            return f"统计分析缺少时间列: {time_field}"
        if metric_alias not in input_df.columns:
            return f"统计分析缺少指标列: {metric_alias}"

        window_days = request.get("window_days")
        if isinstance(window_days, str) and window_days.isdigit():
            window_days = int(window_days)
        if not isinstance(window_days, int) or window_days <= 0:
            window_days = 30

        df = input_df.copy()
        raw_time = df[time_field].astype(str).str.strip()
        parsed_cn = pd.to_datetime(raw_time, errors="coerce", format="%Y年%m月%d日")
        if float(parsed_cn.notna().mean()) >= 0.8:
            df[time_field] = parsed_cn
        else:
            df[time_field] = pd.to_datetime(raw_time, errors="coerce")
        df = df[df[time_field].notna()]
        if df.empty:
            return "统计分析时间列无法解析为日期。"

        df["date"] = df[time_field].dt.normalize()
        grouped = (
            df.groupby("date", as_index=False)
            .agg({metric_alias: "sum"})
            .sort_values("date")
            .tail(window_days)
            .reset_index(drop=True)
        )
        if grouped.empty:
            return "统计分析在窗口内无可用日期数据。"

        grouped["value"] = grouped[metric_alias].astype(float)
        date_start_raw = request.get("date_start")
        date_end_raw = request.get("date_end")
        date_start = pd.to_datetime(date_start_raw, errors="coerce") if isinstance(date_start_raw, str) else pd.NaT
        date_end = pd.to_datetime(date_end_raw, errors="coerce") if isinstance(date_end_raw, str) else pd.NaT
        if pd.notna(date_start) and pd.notna(date_end) and pd.Timestamp(date_end) > pd.Timestamp(date_start):
            start = pd.Timestamp(date_start).normalize()
            end = pd.Timestamp(date_end).normalize()
        else:
            end = pd.Timestamp(grouped["date"].max()).normalize() + pd.Timedelta(days=1)
            start = end - pd.Timedelta(days=int(window_days))
        date_index = pd.date_range(start=start, end=end - pd.Timedelta(days=1), freq="D")
        series = grouped.set_index("date")["value"].reindex(date_index, fill_value=0.0)
        total_days = int(len(series))
        ref_raw = request.get("reference_date")
        reference_date = pd.to_datetime(ref_raw, errors="coerce") if isinstance(ref_raw, str) else pd.NaT
        if pd.isna(reference_date):
            reference_date = date_index.max()
        reference_date = pd.Timestamp(reference_date).normalize()
        if reference_date not in set(pd.Timestamp(d).normalize() for d in date_index):
            reference_date = date_index.max()
        reference_value = float(series.get(reference_date, 0.0))

        less_count = int((series < reference_value).sum())
        le_count = int((series <= reference_value).sum())
        percentile_rank = 0.0 if total_days == 0 else (le_count / total_days)

        daily_rows: list[dict] = []
        for date, value in series.items():
            daily_rows.append(
                {
                    "date": pd.Timestamp(date).strftime("%Y-%m-%d"),
                    "value": float(value),
                }
            )

        return {
            "type": "daily_percentile_rank",
            "window_days": int(window_days),
            "metric_alias": metric_alias,
            "reference_date": reference_date.strftime("%Y-%m-%d"),
            "reference_value": reference_value,
            "less_count": less_count,
            "le_count": le_count,
            "total_days": total_days,
            "percentile_rank": percentile_rank,
            "percentile_pct": percentile_rank * 100.0,
            "daily_rows": daily_rows,
        }

    @staticmethod
    def _weekend_percentile_rank(request: dict, input_df: pd.DataFrame) -> dict | str:
        if input_df is None or input_df.empty:
            return "统计分析无可用数据。"

        time_field = request.get("time_field")
        metric_alias = request.get("metric_alias")
        if not isinstance(time_field, str) or not time_field:
            return "统计分析缺少必要参数: time_field"
        if not isinstance(metric_alias, str) or not metric_alias:
            return "统计分析缺少必要参数: metric_alias"
        if time_field not in input_df.columns:
            return f"统计分析缺少时间列: {time_field}"
        if metric_alias not in input_df.columns:
            return f"统计分析缺少指标列: {metric_alias}"

        window_weekends = request.get("window_weekends")
        if isinstance(window_weekends, str) and window_weekends.isdigit():
            window_weekends = int(window_weekends)
        if not isinstance(window_weekends, int) or window_weekends <= 0:
            window_weekends = 10

        df = input_df.copy()
        raw_time = df[time_field].astype(str).str.strip()
        parsed_cn = pd.to_datetime(raw_time, errors="coerce", format="%Y年%m月%d日")
        if float(parsed_cn.notna().mean()) >= 0.8:
            df[time_field] = parsed_cn
        else:
            df[time_field] = pd.to_datetime(raw_time, errors="coerce")
        df = df[df[time_field].notna()]
        if df.empty:
            return "统计分析时间列无法解析为日期。"

        df["date"] = df[time_field].dt.normalize()
        df["weekday"] = df["date"].dt.dayofweek
        df = df[df["weekday"].isin([5, 6, 0])]
        if df.empty:
            return "统计分析窗口内无周末日数据。"

        adjusted_weekday = df["weekday"].replace({0: 7})
        df["weekend_start"] = df["date"] - pd.to_timedelta(adjusted_weekday - 5, unit="D")
        grouped = (
            df.groupby("weekend_start", as_index=False)
            .agg({metric_alias: "sum"})
            .sort_values("weekend_start")
            .tail(window_weekends)
            .reset_index(drop=True)
        )
        if grouped.empty:
            return "统计分析在窗口内无可用周末数据。"

        grouped["value"] = grouped[metric_alias].astype(float)
        total_weekends = int(len(grouped))
        ref_raw = request.get("reference_date")
        reference_date = pd.to_datetime(ref_raw, errors="coerce") if isinstance(ref_raw, str) else pd.NaT
        if pd.isna(reference_date):
            reference_date = grouped["weekend_start"].max()
        reference_date = pd.Timestamp(reference_date).normalize()
        ref_weekend_start = reference_date - pd.Timedelta(days=((reference_date.dayofweek - 5) % 7))
        ref_rows = grouped[grouped["weekend_start"] == ref_weekend_start]
        if ref_rows.empty:
            ref_row = grouped.tail(1).iloc[0]
            ref_weekend_start = pd.Timestamp(ref_row["weekend_start"]).normalize()
            reference_value = float(ref_row["value"])
        else:
            reference_value = float(ref_rows.iloc[0]["value"])

        less_count = int((grouped["value"] < reference_value).sum())
        le_count = int((grouped["value"] <= reference_value).sum())
        percentile_rank = 0.0 if total_weekends == 0 else (le_count / total_weekends)

        weekend_rows: list[dict] = []
        for _, row in grouped.iterrows():
            weekend_start = pd.Timestamp(row["weekend_start"]).normalize()
            weekend_end = weekend_start + pd.Timedelta(days=2)
            weekend_rows.append(
                {
                    "weekend_start": weekend_start.strftime("%Y-%m-%d"),
                    "weekend_end": weekend_end.strftime("%Y-%m-%d"),
                    "value": float(row["value"]),
                }
            )

        return {
            "type": "weekend_percentile_rank",
            "window_weekends": int(window_weekends),
            "metric_alias": metric_alias,
            "reference_weekend_start": ref_weekend_start.strftime("%Y-%m-%d"),
            "reference_value": reference_value,
            "less_count": less_count,
            "le_count": le_count,
            "total_weekends": total_weekends,
            "percentile_rank": percentile_rank,
            "percentile_pct": percentile_rank * 100.0,
            "weekend_rows": weekend_rows,
        }

    @staticmethod
    def _weekday_percentile_rank(request: dict, input_df: pd.DataFrame) -> dict | str:
        if input_df is None or input_df.empty:
            return "统计分析无可用数据。"

        time_field = request.get("time_field")
        metric_alias = request.get("metric_alias")
        if not isinstance(time_field, str) or not time_field:
            return "统计分析缺少必要参数: time_field"
        if not isinstance(metric_alias, str) or not metric_alias:
            return "统计分析缺少必要参数: metric_alias"
        if time_field not in input_df.columns:
            return f"统计分析缺少时间列: {time_field}"
        if metric_alias not in input_df.columns:
            return f"统计分析缺少指标列: {metric_alias}"

        window_weeks = request.get("window_weeks")
        if isinstance(window_weeks, str) and str(window_weeks).isdigit():
            window_weeks = int(window_weeks)
        if not isinstance(window_weeks, int) or window_weeks <= 0:
            window_weeks = 10

        weekdays = request.get("weekdays")
        if not isinstance(weekdays, list) or not weekdays:
            weekdays = [7]
        weekdays = [int(w) for w in weekdays if isinstance(w, (int, float, str)) and str(w).isdigit()]
        weekdays = [w for w in weekdays if 1 <= int(w) <= 7]
        weekdays = sorted(list(dict.fromkeys(weekdays)))
        if not weekdays:
            weekdays = [7]

        df = input_df.copy()
        raw_time = df[time_field].astype(str).str.strip()
        parsed_cn = pd.to_datetime(raw_time, errors="coerce", format="%Y年%m月%d日")
        if float(parsed_cn.notna().mean()) >= 0.8:
            df[time_field] = parsed_cn
        else:
            df[time_field] = pd.to_datetime(raw_time, errors="coerce")
        df = df[df[time_field].notna()]
        if df.empty:
            return "统计分析时间列无法解析为日期。"

        df["date"] = df[time_field].dt.normalize()
        df["_weekday"] = df["date"].dt.dayofweek + 1
        df = df[df["_weekday"].isin(weekdays)]

        grouped = (
            df.groupby("date", as_index=False)
            .agg({metric_alias: "sum"})
            .sort_values("date")
            .reset_index(drop=True)
        )

        date_start_raw = request.get("date_start")
        date_end_raw = request.get("date_end")
        date_start = pd.to_datetime(date_start_raw, errors="coerce") if isinstance(date_start_raw, str) else pd.NaT
        date_end = pd.to_datetime(date_end_raw, errors="coerce") if isinstance(date_end_raw, str) else pd.NaT
        if pd.notna(date_start) and pd.notna(date_end) and pd.Timestamp(date_end) > pd.Timestamp(date_start):
            start = pd.Timestamp(date_start).normalize()
            end = pd.Timestamp(date_end).normalize()
        else:
            end = (
                pd.Timestamp(grouped["date"].max()).normalize() + pd.Timedelta(days=1)
                if not grouped.empty
                else pd.Timestamp.today().normalize() + pd.Timedelta(days=1)
            )
            start = end - pd.Timedelta(days=int(window_weeks) * 7)

        date_index = pd.date_range(start=start, end=end - pd.Timedelta(days=1), freq="D")
        weekday_index = pd.DatetimeIndex([d for d in date_index if (pd.Timestamp(d).dayofweek + 1) in set(weekdays)])
        if weekday_index.empty:
            return "统计分析窗口内无指定周内日数据。"

        series = (
            grouped.set_index("date")[metric_alias]
            .astype(float)
            .reindex(weekday_index, fill_value=0.0)
            .tail(int(window_weeks))
        )
        total = int(len(series))

        ref_raw = request.get("reference_date")
        reference_date = pd.to_datetime(ref_raw, errors="coerce") if isinstance(ref_raw, str) else pd.NaT
        if pd.isna(reference_date):
            reference_date = series.index.max()
        reference_date = pd.Timestamp(reference_date).normalize()
        if reference_date not in set(pd.Timestamp(d).normalize() for d in series.index):
            reference_date = series.index.max()
        reference_value = float(series.get(reference_date, 0.0))

        less_count = int((series < reference_value).sum())
        le_count = int((series <= reference_value).sum())
        percentile_rank = 0.0 if total == 0 else (le_count / total)

        daily_rows: list[dict] = []
        for date, value in series.items():
            daily_rows.append({"date": pd.Timestamp(date).strftime("%Y-%m-%d"), "value": float(value)})

        return {
            "type": "weekday_percentile_rank",
            "weekdays": weekdays,
            "window_weeks": int(window_weeks),
            "metric_alias": metric_alias,
            "reference_date": reference_date.strftime("%Y-%m-%d"),
            "reference_value": reference_value,
            "less_count": less_count,
            "le_count": le_count,
            "total_days": total,
            "percentile_rank": percentile_rank,
            "percentile_pct": percentile_rank * 100.0,
            "daily_rows": daily_rows,
        }


STATISTICS_TOOL_SCHEMA = {
    "type": "function",
    "function": {
        "name": "perform_statistics",
        "description": "执行单窗口统计后处理（周环比序列统计、下降占比统计、日阈值计数）。",
        "parameters": {
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "enum": [
                        "weekly_decline_ratio",
                        "daily_threshold_count",
                        "daily_mean",
                        "daily_percentile_rank",
                        "weekend_percentile_rank",
                        "weekday_percentile_rank",
                    ],
                },
                "time_field": {"type": "string"},
                "window_weeks": {"type": "integer"},
                "window_days": {"type": "integer"},
                "window_weekends": {"type": "integer"},
                "date_start": {"type": "string"},
                "date_end": {"type": "string"},
                "reference_date": {"type": "string"},
                "weekdays": {"type": "array", "items": {"type": "integer"}},
                "op": {"type": "string", "enum": [">", ">=", "<", "<=", "==", "!="]},
                "threshold": {"type": "number"},
                "metric_alias": {"type": "string"},
                "numerator_alias": {"type": "string"},
                "numerator_alias": {"type": "string"},
                "denominator_alias": {"type": "string"},
            },
            "required": ["type"],
        },
    },
}

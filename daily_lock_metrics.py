import argparse
from pathlib import Path

import pandas as pd

from verify_data import (
    add_time_to_lock_days,
    build_main_code_base,
    coerce_datetime_columns,
    pick_column,
    resolve_dataset_path,
    resolve_journey_columns,
)


PLOT_DEFAULT_START = pd.Timestamp("2025-01-01")
PLOT_DEFAULT_END_EXCLUSIVE = pd.Timestamp("2026-03-11")


def parse_ymd(value: str) -> pd.Timestamp:
    return pd.Timestamp(value).normalize()


def build_daily_metrics(
    base: pd.DataFrame,
    user_col: str,
    main_code_col: str,
    channel_col: str,
    create_time_col: str,
    lock_time_col: str,
    start: pd.Timestamp | None,
    end: pd.Timestamp | None,
) -> pd.DataFrame:
    df = base[[user_col, main_code_col, channel_col, create_time_col, lock_time_col, "time_to_lock_days"]].copy() if "time_to_lock_days" in base.columns else base[[user_col, main_code_col, channel_col, create_time_col, lock_time_col]].copy()
    df = df.sort_values([user_col, create_time_col, main_code_col], kind="mergesort")
    df["touch_index"] = df.groupby(user_col, dropna=False).cumcount() + 1

    locked = df[df[lock_time_col].notna()].copy()
    locked["lock_day"] = locked[lock_time_col].dt.normalize()
    if start is not None:
        locked = locked[locked["lock_day"].ge(start)]
    if end is not None:
        locked = locked[locked["lock_day"].lt(end)]

    if locked.empty:
        return pd.DataFrame(
            columns=[
                "lock_day",
                "locked_users",
                "touch_mean",
                "ttl_days_mean",
            ]
        )

    locked = locked.sort_values([user_col, "lock_day", lock_time_col, main_code_col], kind="mergesort")
    per_user_day = locked.groupby(["lock_day", user_col], dropna=False, as_index=False).first()

    out = (
        per_user_day.groupby("lock_day")
        .agg(
            locked_users=(user_col, "nunique"),
            touch_mean=("touch_index", "mean"),
            ttl_days_mean=("time_to_lock_days", "mean"),
        )
        .reset_index()
        .sort_values("lock_day")
    )

    if start is None:
        start = out["lock_day"].min()
    if end is None:
        end = out["lock_day"].max() + pd.Timedelta(days=1)

    full_days = pd.DataFrame({"lock_day": pd.date_range(start=start, end=end - pd.Timedelta(days=1), freq="D")})
    out = full_days.merge(out, on="lock_day", how="left")
    out["locked_users"] = out["locked_users"].fillna(0).astype(int)
    return out


def build_order_daily_locked_users(order_df: pd.DataFrame, start: pd.Timestamp | None, end: pd.Timestamp | None) -> pd.DataFrame:
    lock_time_col = pick_column(order_df, ["lock_time"]) or "lock_time"
    person_col = pick_column(order_df, ["owner_cell_phone", "owner_phone", "owner_phone_md5", "phone_md5"]) or "owner_cell_phone"
    order_type_col = pick_column(order_df, ["order_type"]) or "order_type"
    if lock_time_col not in order_df.columns or person_col not in order_df.columns:
        return pd.DataFrame({"lock_day": [], "daily_locked_users": []})

    cols = [lock_time_col, person_col]
    if order_type_col in order_df.columns:
        cols.append(order_type_col)
    df = order_df[cols].copy()
    df[lock_time_col] = pd.to_datetime(df[lock_time_col], errors="coerce")
    df = df[df[lock_time_col].notna()].copy()
    if order_type_col in df.columns:
        df = df[df[order_type_col] != "试驾车"].copy()
    df["lock_day"] = df[lock_time_col].dt.normalize()
    if start is not None:
        df = df[df["lock_day"].ge(start)]
    if end is not None:
        df = df[df["lock_day"].lt(end)]

    out = df.groupby("lock_day", as_index=False)[person_col].nunique(dropna=True)
    out = out.rename(columns={person_col: "daily_locked_users"})
    return out


def build_yearly_lock_channel_topn(
    base: pd.DataFrame,
    user_col: str,
    main_code_col: str,
    channel_col: str,
    create_time_col: str,
    lock_time_col: str,
    year: int,
    top_n: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    required = [user_col, main_code_col, channel_col, create_time_col, lock_time_col]
    if any(c not in base.columns for c in required):
        return (
            pd.DataFrame(columns=["channel", "locked_users", "pct"]),
            pd.DataFrame(
                columns=[
                    "user",
                    "first_lock_time",
                    "lock_channel",
                    "touches_to_lock",
                    "distinct_channels_to_lock",
                    "ttl_days",
                    "primary_type",
                    "tags",
                ]
            ),
            pd.DataFrame(columns=["assist_channel", "assist_touches", "pct"]),
        )

    cols = [user_col, main_code_col, channel_col, create_time_col, lock_time_col]
    if "time_to_lock_days" in base.columns:
        cols.append("time_to_lock_days")
    df = base[cols].copy()
    df[create_time_col] = pd.to_datetime(df[create_time_col], errors="coerce")
    df[lock_time_col] = pd.to_datetime(df[lock_time_col], errors="coerce")

    year_start = pd.Timestamp(f"{year}-01-01")
    year_end = pd.Timestamp(f"{year + 1}-01-01")

    locked_in_year = df[df[lock_time_col].notna() & df[lock_time_col].ge(year_start) & df[lock_time_col].lt(year_end)].copy()
    if locked_in_year.empty:
        return (
            pd.DataFrame(columns=["channel", "locked_users", "pct"]),
            pd.DataFrame(
                columns=[
                    "user",
                    "first_lock_time",
                    "lock_channel",
                    "touches_to_lock",
                    "distinct_channels_to_lock",
                    "ttl_days",
                    "primary_type",
                    "tags",
                ]
            ),
            pd.DataFrame(columns=["assist_channel", "assist_touches", "pct"]),
        )

    locked_in_year = locked_in_year.sort_values([user_col, lock_time_col, create_time_col, main_code_col], kind="mergesort")
    first_lock_idx = locked_in_year.groupby(user_col, dropna=False)[lock_time_col].idxmin()
    first_lock = locked_in_year.loc[first_lock_idx].copy()
    first_lock = first_lock.rename(columns={lock_time_col: "first_lock_time", channel_col: "lock_channel"})
    first_lock["user"] = first_lock[user_col]
    first_lock["lock_main_code"] = first_lock[main_code_col]

    if "time_to_lock_days" in first_lock.columns:
        first_lock["ttl_days"] = first_lock["time_to_lock_days"].astype("Float64")
    else:
        delta = first_lock["first_lock_time"] - first_lock[create_time_col]
        first_lock["ttl_days"] = (delta.dt.total_seconds() / 86400).astype("Float64")

    lock_time_by_user = first_lock[["user", "first_lock_time", "lock_channel", "ttl_days", "lock_main_code"]].copy()

    touches = df[[user_col, main_code_col, channel_col, create_time_col]].copy()
    touches = touches[touches[create_time_col].notna()].copy()
    touches["user"] = touches[user_col]
    touches = touches.merge(lock_time_by_user[["user", "first_lock_time"]], on="user", how="inner")
    touches = touches[touches[create_time_col].le(touches["first_lock_time"])].copy()

    touch_agg = touches.groupby("user", dropna=False).agg(
        touches_to_lock=(create_time_col, "size"),
        distinct_channels_to_lock=(channel_col, lambda s: int(pd.Series(s.dropna()).nunique())),
    )
    touch_agg = touch_agg.reset_index()

    user_summary = lock_time_by_user.merge(touch_agg, on="user", how="left")
    user_summary["touches_to_lock"] = user_summary["touches_to_lock"].fillna(0).astype(int)
    user_summary["distinct_channels_to_lock"] = user_summary["distinct_channels_to_lock"].fillna(0).astype(int)

    cond_one_touch = user_summary["touches_to_lock"].eq(1)
    cond_cross = user_summary["distinct_channels_to_lock"].gt(1)
    cond_same_channel_multi = user_summary["touches_to_lock"].gt(1) & user_summary["distinct_channels_to_lock"].eq(1)

    primary_type = pd.Series([pd.NA] * len(user_summary), dtype="string")
    primary_type = primary_type.mask(cond_one_touch, "Profile 1: One-Touch (Decisive)")
    primary_type = primary_type.mask(primary_type.isna() & cond_cross, "Profile 3: Cross-Channel (Comparison Shopper)")
    primary_type = primary_type.mask(primary_type.isna() & cond_same_channel_multi, "Profile 2: Hesitant (Same Channel, Multiple Touches)")
    primary_type = primary_type.fillna("Unclassified")
    user_summary["primary_type"] = primary_type

    tags = pd.Series([""] * len(user_summary), dtype="string")
    tags = tags.mask(user_summary["ttl_days"].gt(14), "Profile 4: Long Consideration (>14 Days)")
    user_summary["tags"] = tags

    channel_series = user_summary["lock_channel"].astype("string").fillna("(missing)")
    vc = channel_series.value_counts(dropna=False)
    total = int(vc.sum())
    top = vc.head(max(int(top_n), 1))
    out = pd.DataFrame({"channel": top.index.astype("string"), "locked_users": top.values})
    other_cnt = int(vc.iloc[len(top) :].sum())
    if other_cnt:
        out = pd.concat([out, pd.DataFrame([{"channel": "其他", "locked_users": other_cnt}])], ignore_index=True)
    out["pct"] = out["locked_users"] / max(total, 1)

    cross_users = user_summary.loc[user_summary["distinct_channels_to_lock"].gt(1), "user"].copy()
    if cross_users.empty:
        assist_out = pd.DataFrame(columns=["assist_channel", "assist_touches", "pct"])
    else:
        cross_users = cross_users.astype("string")
        assist = touches[touches["user"].astype("string").isin(cross_users)].copy()
        assist["user"] = assist["user"].astype("string")
        assist = assist.merge(lock_time_by_user[["user", "lock_channel", "lock_main_code"]], on="user", how="left")
        assist["assist_channel"] = assist[channel_col].astype("string")
        assist = assist[assist["assist_channel"].notna() & assist["lock_channel"].notna() & assist["lock_main_code"].notna()].copy()
        assist = assist[assist["assist_channel"] != assist["lock_channel"]].copy()
        assist = assist[assist[main_code_col] != assist["lock_main_code"]].copy()

        vc_assist = assist["assist_channel"].value_counts(dropna=False)
        assist_total = int(len(assist))
        top_assist = vc_assist.head(max(int(top_n), 1))
        assist_out = pd.DataFrame({"assist_channel": top_assist.index.astype("string"), "assist_touches": top_assist.values})
        assist_out["pct"] = assist_out["assist_touches"] / max(assist_total, 1)

    user_summary = user_summary[
        [
            "user",
            "first_lock_time",
            "lock_channel",
            "touches_to_lock",
            "distinct_channels_to_lock",
            "ttl_days",
            "primary_type",
            "tags",
        ]
    ].copy()
    return out, user_summary, assist_out


def render_plotly_lines(daily: pd.DataFrame, out_html: str, title: str) -> str:
    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
    except Exception as e:
        raise RuntimeError(f"Plotly 未安装或不可用: {e}") from e

    df = daily.copy()
    df["lock_day"] = pd.to_datetime(df["lock_day"], errors="coerce")

    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        specs=[[{"secondary_y": True}], [{}], [{}]],
        subplot_titles=(
            "锁单用户数（按用户在该日的首次锁单计）",
            "到首次锁单的触达次数（均值）",
            "转化时长（create→lock，天）（均值）",
        ),
    )

    fig.add_trace(
        go.Scatter(x=df["lock_day"], y=df["locked_users"], mode="lines+markers", name="locked_users"),
        row=1,
        col=1,
        secondary_y=False,
    )
    if "seed_ratio" in df.columns:
        fig.add_trace(
            go.Scatter(x=df["lock_day"], y=df["seed_ratio"], mode="lines", name="seed_ratio"),
            row=1,
            col=1,
            secondary_y=True,
        )

    fig.add_trace(
        go.Scatter(x=df["lock_day"], y=df["touch_mean"], mode="lines+markers", name="touch_mean"),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(x=df["lock_day"], y=df["ttl_days_mean"], mode="lines+markers", name="ttl_days_mean"),
        row=3,
        col=1,
    )

    fig.update_layout(
        title=title,
        height=900,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
        margin=dict(l=50, r=30, t=80, b=40),
    )
    fig.update_xaxes(title_text="lock_day", row=3, col=1)
    fig.update_yaxes(title_text="users", row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text="ratio", row=1, col=1, secondary_y=True, tickformat=",.1%")
    fig.update_yaxes(title_text="touches", row=2, col=1)
    fig.update_yaxes(title_text="days", row=3, col=1)

    out_path = Path(out_html).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(str(out_path), include_plotlyjs="cdn", full_html=True)
    return str(out_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", default="锁单归因")
    parser.add_argument("--start", type=parse_ymd, default=None)
    parser.add_argument("--end", type=parse_ymd, default=None)
    parser.add_argument("--out", default=None)
    parser.add_argument("--tail", type=int, default=120)
    parser.add_argument("--plot", action="store_true")
    parser.add_argument("--plot-out", default="daily_lock_metrics.html")
    parser.add_argument("--plot-title", default="Daily Lock Metrics")
    parser.add_argument("--analysis-year", type=int, default=2025)
    parser.add_argument("--analysis-top-n", type=int, default=5)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent
    md_path = repo_root / "schema" / "data_path.md"
    dataset_path = resolve_dataset_path(label=args.label, md_path=md_path)

    start = args.start
    end = args.end
    if args.plot:
        if start is None:
            start = PLOT_DEFAULT_START
        if end is None:
            end = PLOT_DEFAULT_END_EXCLUSIVE

    df = pd.read_parquet(dataset_path)
    df = coerce_datetime_columns(df)

    cols = resolve_journey_columns(df)
    main_code_col = cols["main_code"] or "lc_main_code"
    user_col = cols["user_phone_md5"] or "lc_user_phone_md5"
    channel_col = cols["channel"] or "lc_small_channel_name"
    create_time_col = cols["create_time"] or "lc_create_time"
    lock_time_col = cols["lock_time"] or "lc_order_lock_time_min"

    base = build_main_code_base(df, main_code_col=main_code_col)
    base = add_time_to_lock_days(base, create_time_col=create_time_col, lock_time_col=lock_time_col)

    daily = build_daily_metrics(
        base=base,
        user_col=user_col,
        main_code_col=main_code_col,
        channel_col=channel_col,
        create_time_col=create_time_col,
        lock_time_col=lock_time_col,
        start=start,
        end=end,
    )

    order_path = resolve_dataset_path(label="订单表", md_path=md_path)
    try:
        order_df = pd.read_parquet(order_path, columns=["lock_time", "owner_cell_phone", "order_type"])
    except Exception:
        order_df = pd.read_parquet(order_path)
    order_daily = build_order_daily_locked_users(order_df, start=start, end=end)
    daily = daily.merge(order_daily, on="lock_day", how="left")
    daily["daily_locked_users"] = daily["daily_locked_users"].fillna(0).astype(int)
    daily["seed_ratio"] = daily["locked_users"] / daily["daily_locked_users"].replace(0, pd.NA)

    print(f"数据标签: {args.label}")
    print(f"数据路径: {dataset_path}")
    if start is not None or end is not None:
        print(f"锁单日期范围: [{start.date() if start is not None else '-inf'}, {end.date() if end is not None else '+inf'})")

    with pd.option_context("display.width", 200, "display.max_rows", 400):
        print("\n[daily 锁单用户与触达/转化时长]")
        print(daily.tail(args.tail).to_string(index=False))

    channel_top, user_profiles, assist_top = build_yearly_lock_channel_topn(
        base=base,
        user_col=user_col,
        main_code_col=main_code_col,
        channel_col=channel_col,
        create_time_col=create_time_col,
        lock_time_col=lock_time_col,
        year=args.analysis_year,
        top_n=args.analysis_top_n,
    )
    if not user_profiles.empty:
        with pd.option_context("display.width", 160):
            print(f"\n[{args.analysis_year} 锁单用户主要渠道 Top{args.analysis_top_n}]")
            print(channel_top.to_string(index=False, formatters={"pct": "{:.2%}".format}))

            total = int(user_profiles.shape[0])
            one_touch_users = int(user_profiles["touches_to_lock"].astype(int).eq(1).sum())
            same_channel_multi_users = int(
                (user_profiles["touches_to_lock"].astype(int).gt(1) & user_profiles["distinct_channels_to_lock"].astype(int).eq(1)).sum()
            )
            cross_channel_users = int(user_profiles["distinct_channels_to_lock"].astype(int).gt(1).sum())
            long_users = int(user_profiles["ttl_days"].astype("Float64").gt(14).fillna(False).sum())
            long_14_60_users = int(
                (user_profiles["ttl_days"].astype("Float64").gt(14) & user_profiles["ttl_days"].astype("Float64").lt(60)).fillna(False).sum()
            )
            year_start = pd.Timestamp(f"{args.analysis_year}-01-01")
            prior_lock_users = base.loc[
                base[lock_time_col].notna() & base[lock_time_col].lt(year_start),
                user_col,
            ].astype("string")
            prior_lock_users = set(prior_lock_users.dropna().tolist())
            repeat_lock_users = int(user_profiles["user"].astype("string").isin(prior_lock_users).sum())

            lens_out = pd.DataFrame(
                [
                    {"category": "One-Touch (Decisive)", "users": one_touch_users},
                    {"category": "Hesitant (Same Channel, Multiple Touches)", "users": same_channel_multi_users},
                    {"category": "Cross-Channel (Comparison Shopper)", "users": cross_channel_users},
                    {"category": "Long Consideration (>14 Days)", "users": long_users},
                    {"category": "Long Consideration (>14 Days & <60 Days)", "users": long_14_60_users},
                    {"category": "Repeat Lockers (Had Prior Locks)", "users": repeat_lock_users},
                ]
            )
            lens_out["pct"] = lens_out["users"] / max(total, 1)
            print(f"\n[{args.analysis_year} 锁单用户分类占比（观察口径）]")
            print(lens_out.to_string(index=False, formatters={"pct": "{:.2%}".format}))

            if int(cross_channel_users) > 0 and not assist_top.empty:
                print(f"\n[{args.analysis_year} 跨渠道锁单用户主要助攻渠道 Top{args.analysis_top_n}]")
                assist_print = assist_top.copy()
                assist_print["pct"] = assist_print["pct"].map(lambda x: "{:.2%}".format(float(x)) if pd.notna(x) else "")
                print(assist_print.to_string(index=False))
    else:
        print(f"\n[{args.analysis_year} 锁单用户分析] 没有找到该年份的锁单记录")

    if args.plot:
        plot_title = args.plot_title
        if start is not None or end is not None:
            plot_title = f"{plot_title} ({start.date() if start is not None else '-inf'} ~ {end.date() if end is not None else '+inf'})"
        out_html = render_plotly_lines(daily, out_html=args.plot_out, title=plot_title)
        print(f"\nPlotly 图表已写入: {out_html}")

    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        daily.to_csv(args.out, index=False, encoding="utf-8-sig")
        print(f"\n已写入: {args.out}")


if __name__ == "__main__":
    main()

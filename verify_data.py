import pandas as pd
import sys
from pathlib import Path
import glob

def _extract_value_after_colon(line: str) -> str:
    if "：" in line:
        _, value = line.split("：", 1)
    elif ":" in line:
        _, value = line.split(":", 1)
    else:
        raise ValueError(f"无法解析路径行（未找到冒号分隔符）：{line!r}")
    return value.strip().replace("\\_", "_").replace("\\*", "*")


def resolve_dataset_path(label: str, md_path: Path) -> Path:
    md_text = md_path.read_text(encoding="utf-8")
    candidates = []
    for raw_line in md_text.splitlines():
        line = raw_line.strip()
        if not line or "：" not in line and ":" not in line:
            continue
        if line.startswith(f"{label}：") or line.startswith(f"{label}:"):
            candidates.append(line)

    if not candidates:
        raise FileNotFoundError(f"未在 {md_path} 找到标签 {label!r} 的路径配置")
    if len(candidates) > 1:
        raise ValueError(f"{md_path} 中标签 {label!r} 出现多次：{candidates}")

    raw_path = _extract_value_after_colon(candidates[0])
    expanded = glob.glob(raw_path)
    if expanded:
        expanded = sorted(expanded, key=lambda p: (len(p), p))
        return Path(expanded[0])

    p = Path(raw_path).expanduser()
    if p.exists():
        return p
    raise FileNotFoundError(f"路径不存在且未匹配到通配符：{raw_path}")


def print_overview(df: pd.DataFrame) -> None:
    print("\n[概览]")
    print(f"- 行数: {len(df):,}")
    print(f"- 列数: {df.shape[1]:,}")
    dup_cnt = int(df.duplicated().sum())
    print(f"- 重复行: {dup_cnt:,} ({dup_cnt / max(len(df), 1):.2%})")

    mem = df.memory_usage(deep=True).sum()
    print(f"- 内存占用(估算): {mem / (1024**2):.2f} MiB")


def coerce_datetime_columns(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    datetime_like_names = ("time", "date", "dt", "timestamp", "created", "updated", "lock")
    for col in df2.columns:
        name = str(col).lower()
        if any(k in name for k in datetime_like_names) and not pd.api.types.is_datetime64_any_dtype(df2[col]):
            if pd.api.types.is_object_dtype(df2[col]) or pd.api.types.is_string_dtype(df2[col]):
                df2[col] = pd.to_datetime(df2[col], errors="coerce")
    return df2


def print_column_profile(df: pd.DataFrame) -> None:
    print("\n[字段画像]")
    rows = len(df)
    profile_rows = []
    for col in df.columns:
        s = df[col]
        missing = int(s.isna().sum())
        nunique = int(s.nunique(dropna=True))
        profile_rows.append(
            {
                "column": col,
                "dtype": str(s.dtype),
                "missing": missing,
                "missing_rate": missing / max(rows, 1),
                "nunique": nunique,
            }
        )

    prof = pd.DataFrame(profile_rows).sort_values(["missing_rate", "nunique"], ascending=[False, False])
    with pd.option_context("display.max_rows", 200, "display.max_colwidth", 80, "display.width", 140):
        print(prof.to_string(index=False))


def print_numeric_stats(df: pd.DataFrame) -> None:
    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    if not numeric_cols:
        return
    print("\n[数值字段统计]")
    desc = df[numeric_cols].describe(percentiles=[0.01, 0.05, 0.5, 0.95, 0.99]).T
    with pd.option_context("display.max_rows", 200, "display.width", 160):
        print(desc.to_string())


def print_datetime_ranges(df: pd.DataFrame) -> None:
    datetime_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
    if not datetime_cols:
        return
    print("\n[时间字段范围]")
    rows = []
    for col in datetime_cols:
        s = df[col]
        rows.append(
            {
                "column": col,
                "min": s.min(),
                "max": s.max(),
                "missing": int(s.isna().sum()),
            }
        )
    out = pd.DataFrame(rows).sort_values("column")
    with pd.option_context("display.max_rows", 200, "display.width", 200):
        print(out.to_string(index=False))


def print_top_categories(df: pd.DataFrame, top_n: int = 20) -> None:
    print("\n[类别字段 Top 分布]")
    rows = len(df)
    for col in df.columns:
        s = df[col]
        if pd.api.types.is_bool_dtype(s) or pd.api.types.is_numeric_dtype(s) or pd.api.types.is_datetime64_any_dtype(s):
            continue

        nunique = int(s.nunique(dropna=True))
        if nunique == 0:
            continue

        unique_rate = nunique / max(rows, 1)
        if unique_rate > 0.2 and nunique > 5000:
            continue

        vc_all = s.value_counts(dropna=False)
        top1_cnt = int(vc_all.iloc[0]) if len(vc_all) else 0
        if nunique > 5000 and top1_cnt / max(rows, 1) < 0.01:
            continue

        vc = vc_all.head(top_n)
        print(f"\n- {col} (unique={nunique:,}, missing={int(s.isna().sum()):,})")
        print(vc.to_string())


def print_datetime_histograms(df: pd.DataFrame) -> None:
    datetime_cols = [c for c in df.columns if pd.api.types.is_datetime64_any_dtype(df[c])]
    if not datetime_cols:
        return
    print("\n[时间字段月度分布]")
    for col in datetime_cols:
        s = df[col]
        period = s.dropna().dt.to_period("M")
        if period.empty:
            continue
        vc = period.value_counts().sort_index()
        print(f"\n- {col} (missing={int(s.isna().sum()):,})")
        with pd.option_context("display.max_rows", 240, "display.width", 160):
            print(vc.to_string())


def print_simple_crosstabs(df: pd.DataFrame) -> None:
    if "metric_name" in df.columns and "metric_value" in df.columns:
        print("\n[关键字段交叉表]")
        ct = pd.crosstab(df["metric_name"], df["metric_value"], dropna=False)
        with pd.option_context("display.width", 160):
            print("\n- metric_name x metric_value")
            print(ct.to_string())


def pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols_lower = {str(c).lower(): c for c in df.columns}
    for cand in candidates:
        key = str(cand).lower()
        if key in cols_lower:
            return str(cols_lower[key])
    return None


def resolve_journey_columns(df: pd.DataFrame) -> dict[str, str | None]:
    return {
        "user_phone_md5": pick_column(df, ["lc_user_phone_md5", "ic_user_phone_md5", "user_phone_md5", "phone_md5"]),
        "main_code": pick_column(df, ["lc_main_code", "ic_main_code", "main_code", "clue_code"]),
        "channel": pick_column(
            df,
            [
                "lc_small_channel_name",
                "ic_small_channel_name",
                "lc_small_channel",
                "small_channel_name",
                "channel_name",
            ],
        ),
        "create_time": pick_column(df, ["lc_create_time", "ic_create_time", "create_time", "created_time"]),
        "lock_time": pick_column(
            df,
            ["lc_order_lock_time_min", "ic_order_lock_time_min", "order_lock_time_min", "order_lock_time", "lock_time"],
        ),
        "metric_name": pick_column(df, ["metric_name"]),
        "metric_value": pick_column(df, ["metric_value"]),
    }


def build_main_code_base(df: pd.DataFrame, main_code_col: str) -> pd.DataFrame:
    if main_code_col not in df.columns:
        raise KeyError(f"缺少 main_code 字段: {main_code_col}")
    return df.drop_duplicates(subset=[main_code_col], keep="first").copy()


def print_main_code_duplication_checks(
    df: pd.DataFrame,
    main_code_col: str,
    cols_to_check: list[str],
) -> None:
    print("\n[数据一致性检查]")
    size_by_code = df.groupby(main_code_col, dropna=False).size()
    print(f"- 线索码(main_code) 总数: {int(size_by_code.shape[0]):,}")
    print(f"- 每个线索码的记录数: min={int(size_by_code.min())}, p50={int(size_by_code.median())}, max={int(size_by_code.max())}")
    multi = int((size_by_code > 1).sum())
    print(f"- 线索码出现多行的数量: {multi:,} ({multi / max(int(size_by_code.shape[0]), 1):.2%})")

    for col in cols_to_check:
        if col not in df.columns:
            continue
        nunique = df.groupby(main_code_col)[col].nunique(dropna=False)
        inconsistent = int((nunique > 1).sum())
        print(f"- main_code 对 {col} 存在不一致: {inconsistent:,} ({inconsistent / max(int(nunique.shape[0]), 1):.2%})")


def build_metric_wide(
    df: pd.DataFrame,
    main_code_col: str,
    metric_name_col: str,
    metric_value_col: str,
) -> pd.DataFrame:
    if metric_name_col not in df.columns or metric_value_col not in df.columns:
        return pd.DataFrame({main_code_col: []})
    wide = (
        df.pivot_table(
            index=main_code_col,
            columns=metric_name_col,
            values=metric_value_col,
            aggfunc="max",
            dropna=False,
        )
        .reset_index()
        .copy()
    )
    wide.columns = [str(c) for c in wide.columns]
    return wide


def print_journey_overview(
    base: pd.DataFrame,
    user_col: str,
    main_code_col: str,
    channel_col: str,
    create_time_col: str,
    lock_time_col: str,
) -> None:
    print("\n[旅程视角概览]")
    rows = len(base)
    unique_users = int(base[user_col].nunique(dropna=True)) if user_col in base.columns else 0
    unique_codes = int(base[main_code_col].nunique(dropna=True))
    unique_channels = int(base[channel_col].nunique(dropna=True)) if channel_col in base.columns else 0
    lock_cnt = int(base[lock_time_col].notna().sum()) if lock_time_col in base.columns else 0
    print(f"- 线索数(main_code): {unique_codes:,} (base 行数: {rows:,})")
    if user_col in base.columns:
        print(f"- 用户数(user_phone_md5): {unique_users:,}")
    if channel_col in base.columns:
        print(f"- 渠道数(channel): {unique_channels:,}")
    if lock_time_col in base.columns:
        print(f"- 锁单线索数(lock_time 非空): {lock_cnt:,} ({lock_cnt / max(rows, 1):.2%})")

    if create_time_col in base.columns and pd.api.types.is_datetime64_any_dtype(base[create_time_col]):
        s = base[create_time_col]
        print(f"- create_time 范围: {s.min().date()} ～ {s.max().date()} (missing={int(s.isna().sum()):,})")
    if lock_time_col in base.columns and pd.api.types.is_datetime64_any_dtype(base[lock_time_col]):
        s = base[lock_time_col]
        if s.notna().any():
            print(f"- lock_time 范围(非空): {s.min().date()} ～ {s.max().date()} (missing={int(s.isna().sum()):,})")
        else:
            print(f"- lock_time 全为空 (missing={int(s.isna().sum()):,})")


def add_time_to_lock_days(base: pd.DataFrame, create_time_col: str, lock_time_col: str) -> pd.DataFrame:
    base2 = base.copy()
    if create_time_col not in base2.columns or lock_time_col not in base2.columns:
        base2["time_to_lock_days"] = pd.Series([pd.NA] * len(base2), dtype="Float64")
        return base2
    if not pd.api.types.is_datetime64_any_dtype(base2[create_time_col]) or not pd.api.types.is_datetime64_any_dtype(base2[lock_time_col]):
        base2["time_to_lock_days"] = pd.Series([pd.NA] * len(base2), dtype="Float64")
        return base2
    delta = base2[lock_time_col] - base2[create_time_col]
    base2["time_to_lock_days"] = (delta.dt.total_seconds() / 86400).astype("Float64")
    return base2


def print_user_level_stats(base: pd.DataFrame, user_col: str, lock_time_col: str) -> None:
    if user_col not in base.columns:
        return
    print("\n[用户层面]")
    g = base.groupby(user_col, dropna=False)
    n_codes = g.size().rename("n_main_code")
    n_locked = g[lock_time_col].apply(lambda s: int(s.notna().sum())).rename("n_locked") if lock_time_col in base.columns else None

    users = pd.DataFrame({"n_main_code": n_codes})
    if n_locked is not None:
        users["n_locked"] = n_locked
        users["has_lock"] = users["n_locked"] > 0
        users["user_lock_rate"] = users["n_locked"] / users["n_main_code"].clip(lower=1)

    with pd.option_context("display.width", 160):
        print("\n- 每用户线索数分布")
        print(users["n_main_code"].describe(percentiles=[0.5, 0.9, 0.95, 0.99]).to_string())

    if "has_lock" in users.columns:
        has_lock_rate = float(users["has_lock"].mean())
        print(f"\n- 有至少 1 次锁单的用户占比: {has_lock_rate:.2%}")
        if has_lock_rate >= 0.999:
            print("- 该数据集看起来只覆盖“至少锁过 1 单”的用户（种子用户口径），不包含从未锁单的用户")
        with pd.option_context("display.width", 160):
            print("\n- 每用户锁单线索数分布")
            print(users["n_locked"].describe(percentiles=[0.5, 0.9, 0.95, 0.99]).to_string())
            print("\n- 每用户线索锁单率分布")
            print(users["user_lock_rate"].describe(percentiles=[0.5, 0.9, 0.95, 0.99]).to_string())


def print_time_to_lock_distribution(base: pd.DataFrame, ttl_col: str) -> None:
    if ttl_col not in base.columns:
        return
    s = base[ttl_col].dropna()
    if s.empty:
        return
    print("\n[转化时长(从 create 到 lock)]")
    with pd.option_context("display.width", 160):
        print(s.describe(percentiles=[0.5, 0.75, 0.9, 0.95, 0.99]).to_string())

    bins = [-float("inf"), 0, 1, 3, 7, 14, 30, 60, 90, float("inf")]
    labels = ["<=0d", "0-1d", "1-3d", "3-7d", "7-14d", "14-30d", "30-60d", "60-90d", "90d+"]
    b = pd.cut(s, bins=bins, labels=labels, right=True, include_lowest=True)
    vc = b.value_counts(dropna=False).sort_index()
    out = pd.DataFrame({"count": vc, "rate": vc / max(int(vc.sum()), 1)})
    with pd.option_context("display.width", 120):
        print("\n- 分桶分布(仅 lock 非空)")
        print(out.to_string())


def print_channel_funnel(
    base: pd.DataFrame,
    channel_col: str,
    lock_time_col: str,
    ttl_col: str,
    min_leads_for_rate: int = 1000,
) -> None:
    if channel_col not in base.columns:
        return
    print("\n[渠道漏斗]")
    df = base.copy()
    df["is_locked"] = df[lock_time_col].notna() if lock_time_col in df.columns else False
    g = df.groupby(channel_col, dropna=False)
    leads = g.size().rename("leads")
    locked = g["is_locked"].sum().astype(int).rename("locked")
    out = pd.concat([leads, locked], axis=1)
    out["lock_rate"] = out["locked"] / out["leads"].clip(lower=1)

    if ttl_col in df.columns:
        locked_only = df[df["is_locked"] & df[ttl_col].notna()]
        if not locked_only.empty:
            g2 = locked_only.groupby(channel_col, dropna=False)[ttl_col]
            out["ttl_p50"] = g2.median()
            out["ttl_p90"] = g2.quantile(0.9)

    top_by_leads = out.sort_values(["leads", "lock_rate"], ascending=[False, False]).head(30)
    with pd.option_context("display.width", 200):
        print("\n- leads Top 30")
        print(top_by_leads.to_string())

    stable = out[out["leads"] >= min_leads_for_rate].copy()
    if not stable.empty:
        top_by_rate = stable.sort_values(["lock_rate", "leads"], ascending=[False, False]).head(30)
        with pd.option_context("display.width", 200):
            print(f"\n- lock_rate Top 30 (leads>={min_leads_for_rate})")
            print(top_by_rate.to_string())


def print_create_month_cohort(base: pd.DataFrame, create_time_col: str, lock_time_col: str) -> None:
    if create_time_col not in base.columns or lock_time_col not in base.columns:
        return
    if not pd.api.types.is_datetime64_any_dtype(base[create_time_col]):
        return
    print("\n[按创建月份 Cohort]")
    df = base.copy()
    df["create_month"] = df[create_time_col].dt.to_period("M")
    df["is_locked"] = df[lock_time_col].notna()
    g = df.groupby("create_month", dropna=False)
    out = pd.DataFrame({"leads": g.size(), "locked": g["is_locked"].sum().astype(int)})
    out["lock_rate"] = out["locked"] / out["leads"].clip(lower=1)
    out = out.sort_index()
    if len(out) > 36:
        out = out.tail(36)
    with pd.option_context("display.width", 160):
        print(out.to_string())


def print_quality_flags(base: pd.DataFrame, user_col: str, main_code_col: str, create_time_col: str, lock_time_col: str) -> None:
    print("\n[质量信号]")
    if create_time_col in base.columns:
        print(f"- create_time 缺失: {int(base[create_time_col].isna().sum()):,}")
    if lock_time_col in base.columns:
        print(f"- lock_time 缺失: {int(base[lock_time_col].isna().sum()):,}")
    if create_time_col in base.columns and lock_time_col in base.columns:
        if pd.api.types.is_datetime64_any_dtype(base[create_time_col]) and pd.api.types.is_datetime64_any_dtype(base[lock_time_col]):
            neg = base[lock_time_col].notna() & base[create_time_col].notna() & (base[lock_time_col] < base[create_time_col])
            print(f"- lock_time < create_time 的线索数: {int(neg.sum()):,}")
            if bool(neg.any()):
                cols = [c for c in [main_code_col, user_col, create_time_col, lock_time_col] if c in base.columns]
                sample = base.loc[neg, cols].head(10)
                with pd.option_context("display.width", 200):
                    print("\n- lock_time < create_time 示例(最多10条)")
                    print(sample.to_string(index=False))

    if user_col in base.columns:
        empty_user = int(base[user_col].isna().sum())
        print(f"- user_phone_md5 缺失: {empty_user:,}")

    if main_code_col in base.columns and user_col in base.columns:
        nunique_user = base.groupby(main_code_col)[user_col].nunique(dropna=False)
        multi_user = int((nunique_user > 1).sum())
        print(f"- 同一 main_code 对应多个 user_phone_md5: {multi_user:,}")


def print_metric_association_with_lock(base: pd.DataFrame, metric_wide: pd.DataFrame, main_code_col: str, lock_time_col: str) -> None:
    if metric_wide.empty or main_code_col not in metric_wide.columns:
        return
    if lock_time_col not in base.columns:
        return

    df = base[[main_code_col, lock_time_col]].merge(metric_wide, on=main_code_col, how="left")
    df["is_locked"] = df[lock_time_col].notna()

    flag_col = "来源辅助标记"
    if flag_col in df.columns:
        print("\n[来源辅助标记 与 锁单关系]")
        g = df.groupby(flag_col, dropna=False)["is_locked"]
        out = pd.DataFrame({"leads": g.size(), "locked": g.sum().astype(int)})
        out["lock_rate"] = out["locked"] / out["leads"].clip(lower=1)
        with pd.option_context("display.width", 120):
            print(out.to_string())


def print_user_sequence_analysis(
    base: pd.DataFrame,
    user_col: str,
    main_code_col: str,
    channel_col: str,
    create_time_col: str,
    lock_time_col: str,
    top_n: int = 30,
) -> None:
    required = [user_col, main_code_col, channel_col, create_time_col]
    if any(c not in base.columns for c in required):
        return
    if not pd.api.types.is_datetime64_any_dtype(base[create_time_col]):
        return

    print("\n[用户序列/路径分析]")
    df = base[[user_col, main_code_col, channel_col, create_time_col, lock_time_col]].copy() if lock_time_col in base.columns else base[[user_col, main_code_col, channel_col, create_time_col]].copy()
    df = df.sort_values([user_col, create_time_col, main_code_col], kind="mergesort")
    df["lead_index"] = df.groupby(user_col, dropna=False).cumcount() + 1
    df["is_locked"] = df[lock_time_col].notna() if lock_time_col in df.columns else False

    df["prev_channel"] = df.groupby(user_col, dropna=False)[channel_col].shift(1)
    df["transition"] = df["prev_channel"].astype("string") + " -> " + df[channel_col].astype("string")
    trans = df[df["prev_channel"].notna() & (df["prev_channel"] != df[channel_col])]["transition"].value_counts().head(top_n)
    if len(trans):
        print(f"\n- Top {top_n} 渠道切换(相邻触达)")
        print(trans.to_string())

    df["prev_create_time"] = df.groupby(user_col, dropna=False)[create_time_col].shift(1)
    gap_days = (df[create_time_col] - df["prev_create_time"]).dt.total_seconds() / 86400
    gap_days = gap_days.dropna()
    if not gap_days.empty:
        with pd.option_context("display.width", 160):
            print("\n- 相邻触达间隔(天)分布")
            print(gap_days.describe(percentiles=[0.5, 0.75, 0.9, 0.95, 0.99]).to_string())

    first = df.groupby(user_col, dropna=False).first(numeric_only=False)
    user_summary = pd.DataFrame(
        {
            "first_channel": first[channel_col],
            "first_create_time": first[create_time_col],
            "n_leads": df.groupby(user_col, dropna=False).size(),
            "n_distinct_channels": df.groupby(user_col, dropna=False)[channel_col].nunique(dropna=True),
        }
    )

    locked_rows = df[df["is_locked"]].copy()
    if not locked_rows.empty:
        first_lock_idx = locked_rows.groupby(user_col, dropna=False)["lead_index"].idxmin()
        first_lock_rows = locked_rows.loc[first_lock_idx, [user_col, "lead_index", channel_col, create_time_col, lock_time_col]].set_index(user_col)
        user_summary = user_summary.join(
            first_lock_rows.rename(
                columns={
                    "lead_index": "first_lock_lead_index",
                    channel_col: "first_lock_channel",
                    create_time_col: "first_lock_create_time",
                    lock_time_col: "first_lock_time",
                }
            ),
            how="left",
        )

        user_summary["locked_on_first_touch"] = user_summary["first_lock_lead_index"] == 1
        lock_on_first_rate = float(user_summary["locked_on_first_touch"].mean())
        print(f"\n- 首次触达即锁单(lead_index=1) 用户占比: {lock_on_first_rate:.2%}")
        with pd.option_context("display.width", 160):
            print("\n- 首次锁单发生在第 N 次触达：分布")
            print(user_summary["first_lock_lead_index"].describe(percentiles=[0.5, 0.75, 0.9, 0.95, 0.99]).to_string())

    with pd.option_context("display.width", 160):
        print("\n- 每用户触达渠道数分布")
        print(user_summary["n_distinct_channels"].describe(percentiles=[0.5, 0.9, 0.95, 0.99]).to_string())

    first_touch = user_summary["first_channel"].value_counts(dropna=False).head(top_n)
    print(f"\n- Top {top_n} 首触渠道(按用户)")
    print(first_touch.to_string())

    if "first_lock_channel" in user_summary.columns:
        first_lock_touch = user_summary["first_lock_channel"].value_counts(dropna=False).head(top_n)
        print(f"\n- Top {top_n} 首次锁单触达渠道(按用户)")
        print(first_lock_touch.to_string())


def parse_lock_date_arg(args: list[str]) -> pd.Timestamp | None:
    for a in args:
        s = str(a).strip()
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            try:
                return pd.Timestamp(s)
            except Exception:
                continue
    return None


def bucket_touch_count(n: int) -> str:
    if n <= 1:
        return "1"
    if n == 2:
        return "2"
    if n == 3:
        return "3"
    if n == 4:
        return "4"
    if n == 5:
        return "5"
    if 6 <= n <= 7:
        return "6-7"
    if 8 <= n <= 10:
        return "8-10"
    if 11 <= n <= 15:
        return "11-15"
    if 16 <= n <= 20:
        return "16-20"
    return "21+"


def print_lock_day_metrics_and_paths(
    base: pd.DataFrame,
    lock_day: pd.Timestamp,
    user_col: str,
    main_code_col: str,
    channel_col: str,
    create_time_col: str,
    lock_time_col: str,
    top_n: int = 30,
) -> None:
    required = [user_col, main_code_col, channel_col, create_time_col, lock_time_col]
    if any(c not in base.columns for c in required):
        return
    if not pd.api.types.is_datetime64_any_dtype(base[create_time_col]) or not pd.api.types.is_datetime64_any_dtype(base[lock_time_col]):
        return

    day_start = lock_day.normalize()
    day_end = day_start + pd.Timedelta(days=1)

    df = base[[user_col, main_code_col, channel_col, create_time_col, lock_time_col, "time_to_lock_days"]].copy() if "time_to_lock_days" in base.columns else base[[user_col, main_code_col, channel_col, create_time_col, lock_time_col]].copy()
    df = df.sort_values([user_col, create_time_col, main_code_col], kind="mergesort")
    df["lead_index"] = df.groupby(user_col, dropna=False).cumcount() + 1

    locked_on_day = df[df[lock_time_col].ge(day_start) & df[lock_time_col].lt(day_end)].copy()
    if locked_on_day.empty:
        print(f"\n[指定日期分析] {day_start.date()} 没有锁单记录")
        return

    first_lock_idx = locked_on_day.groupby(user_col, dropna=False)[lock_time_col].idxmin()
    user_first_lock = locked_on_day.loc[first_lock_idx, [user_col, "lead_index", channel_col, lock_time_col]].copy()
    if "time_to_lock_days" in locked_on_day.columns:
        user_first_lock["time_to_lock_days"] = locked_on_day.loc[first_lock_idx, "time_to_lock_days"].astype("Float64").values
    user_first_lock = user_first_lock.rename(columns={"lead_index": "touches_to_lock", channel_col: "lock_channel"})

    touches = user_first_lock["touches_to_lock"].astype(int)
    ttl = user_first_lock["time_to_lock_days"].dropna() if "time_to_lock_days" in user_first_lock.columns else pd.Series([], dtype="Float64")
    print(f"\n[指定日期分析] {day_start.date()}")
    print(f"- 锁单用户数: {int(user_first_lock[user_col].nunique(dropna=True)):,}")
    print(f"- 平均触达次数(到首次锁单): {touches.mean():.3f}; 中位数: {touches.median():.0f}")
    if not ttl.empty:
        print(f"- 平均转化时长(天): {ttl.mean():.3f}; 中位数(天): {ttl.median():.0f}")

    user_scope = df[df[create_time_col].lt(day_end)].copy()
    user_lock_on_day = user_first_lock[[user_col]].copy()
    user_lock_on_day["lock_on_day"] = True

    user_touches = user_scope.groupby(user_col, dropna=False).size().rename("touches_to_day").to_frame()
    user_touches = user_touches.join(user_lock_on_day.set_index(user_col), how="left")
    user_touches["lock_on_day"] = user_touches["lock_on_day"].fillna(False)
    user_touches["touch_bucket"] = user_touches["touches_to_day"].astype(int).map(bucket_touch_count)

    print("\n[触达次数分桶后的锁单率(按用户，到当日为止)]")
    g = user_touches.groupby("touch_bucket", dropna=False)["lock_on_day"]
    out = pd.DataFrame({"users": g.size(), "locked_users": g.sum().astype(int)})
    out["lock_rate"] = out["locked_users"] / out["users"].clip(lower=1)
    order = ["1", "2", "3", "4", "5", "6-7", "8-10", "11-15", "16-20", "21+"]
    out = out.reindex([b for b in order if b in out.index])
    with pd.option_context("display.width", 140):
        print(out.to_string())

    print("\n[Top 路径的锁单率差异(按用户，到当日为止)]")
    u = user_scope[[user_col, channel_col, create_time_col, main_code_col]].copy()
    u = u.sort_values([user_col, create_time_col, main_code_col], kind="mergesort")
    u["lead_index"] = u.groupby(user_col, dropna=False).cumcount() + 1

    def first_k_path(k: int) -> pd.Series:
        pivot = u[u["lead_index"].le(k)].pivot_table(
            index=user_col,
            columns="lead_index",
            values=channel_col,
            aggfunc="first",
            dropna=False,
        )
        cols = [i for i in range(1, k + 1) if i in pivot.columns]
        pivot = pivot[cols].copy()
        for i in cols:
            pivot[i] = pivot[i].astype("string")
        path = pivot[cols[0]]
        for i in cols[1:]:
            path = path + " -> " + pivot[i]
        return path.rename(f"path_{k}")

    for k in (2, 3):
        pathk = first_k_path(k)
        tmp = user_touches.join(pathk, how="left")
        tmp = tmp[tmp["touches_to_day"] >= k].copy()
        key = f"path_{k}"
        if tmp.empty:
            continue
        g2 = tmp.groupby(key, dropna=False)["lock_on_day"]
        out2 = pd.DataFrame({"users": g2.size(), "locked_users": g2.sum().astype(int)})
        out2["lock_rate"] = out2["locked_users"] / out2["users"].clip(lower=1)

        top_by_users = out2.sort_values(["users", "lock_rate"], ascending=[False, False]).head(top_n)
        with pd.option_context("display.width", 220):
            print(f"\n- path{k} Top {top_n} (按用户数)")
            print(top_by_users.to_string())

        stable = out2[out2["users"] >= 200].copy()
        if not stable.empty:
            top_by_rate = stable.sort_values(["lock_rate", "users"], ascending=[False, False]).head(top_n)
            with pd.option_context("display.width", 220):
                print(f"\n- path{k} Top {top_n} (按锁单率，users>=200)")
                print(top_by_rate.to_string())


def main() -> None:
    label = "锁单归因"
    if len(sys.argv) >= 2 and str(sys.argv[1]).strip():
        label = str(sys.argv[1]).strip()

    repo_root = Path(__file__).resolve().parent
    md_path = repo_root / "schema" / "data_path.md"
    dataset_path = resolve_dataset_path(label=label, md_path=md_path)

    print(f"数据标签: {label}")
    print(f"路径来源: {md_path}")
    print(f"读取数据: {dataset_path}")

    df = pd.read_parquet(dataset_path)
    df = coerce_datetime_columns(df)

    cols = resolve_journey_columns(df)
    main_code_col = cols["main_code"] or "lc_main_code"
    user_col = cols["user_phone_md5"] or "lc_user_phone_md5"
    channel_col = cols["channel"] or "lc_small_channel_name"
    create_time_col = cols["create_time"] or "lc_create_time"
    lock_time_col = cols["lock_time"] or "lc_order_lock_time_min"
    metric_name_col = cols["metric_name"] or "metric_name"
    metric_value_col = cols["metric_value"] or "metric_value"

    print_overview(df)
    print_column_profile(df)
    print_datetime_ranges(df)
    print_datetime_histograms(df)
    print_numeric_stats(df)
    print_simple_crosstabs(df)

    print_main_code_duplication_checks(
        df,
        main_code_col=main_code_col,
        cols_to_check=[c for c in [user_col, channel_col, create_time_col, lock_time_col] if c in df.columns],
    )

    base = build_main_code_base(df, main_code_col=main_code_col)
    base = add_time_to_lock_days(base, create_time_col=create_time_col, lock_time_col=lock_time_col)
    metric_wide = build_metric_wide(
        df,
        main_code_col=main_code_col,
        metric_name_col=metric_name_col,
        metric_value_col=metric_value_col,
    )

    print_journey_overview(
        base,
        user_col=user_col,
        main_code_col=main_code_col,
        channel_col=channel_col,
        create_time_col=create_time_col,
        lock_time_col=lock_time_col,
    )
    print_quality_flags(
        base,
        user_col=user_col,
        main_code_col=main_code_col,
        create_time_col=create_time_col,
        lock_time_col=lock_time_col,
    )
    print_user_level_stats(base, user_col=user_col, lock_time_col=lock_time_col)
    print_create_month_cohort(base, create_time_col=create_time_col, lock_time_col=lock_time_col)
    print_time_to_lock_distribution(base, ttl_col="time_to_lock_days")
    print_channel_funnel(base, channel_col=channel_col, lock_time_col=lock_time_col, ttl_col="time_to_lock_days")
    print_metric_association_with_lock(base, metric_wide=metric_wide, main_code_col=main_code_col, lock_time_col=lock_time_col)
    print_user_sequence_analysis(
        base,
        user_col=user_col,
        main_code_col=main_code_col,
        channel_col=channel_col,
        create_time_col=create_time_col,
        lock_time_col=lock_time_col,
        top_n=30,
    )
    lock_day = parse_lock_date_arg(sys.argv[2:]) or pd.Timestamp("2026-03-05")
    print_lock_day_metrics_and_paths(
        base,
        lock_day=lock_day,
        user_col=user_col,
        main_code_col=main_code_col,
        channel_col=channel_col,
        create_time_col=create_time_col,
        lock_time_col=lock_time_col,
        top_n=30,
    )

    print_top_categories(base, top_n=20)


if __name__ == "__main__":
    main()

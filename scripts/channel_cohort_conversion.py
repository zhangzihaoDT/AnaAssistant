"""
---
name: channel_cohort_conversion
type: script
path: scripts/channel_cohort_conversion.py
updated_at: "2026-04-29 00:00"
summary: 读取“渠道归因”CSV（支持 Long→Wide），按 Cohort 时间窗回溯用户全局触达，并对锁单/小订做转化归因；输出 HTML 报告。
inputs:
  - 渠道归因_YYYYMMDD.csv（utf-16/utf-8; tab/comma；支持 Long/Wide）
outputs:
  - out/<stem>_report.html（默认）
cli:
  - python3 scripts/channel_cohort_conversion.py <csv> -s 2026-04-16 -e 2026-04-17 -o out/<name>.html
  - python3 scripts/channel_cohort_conversion.py <csv> -s 2026-04-16 -e 2026-04-17 -c "APP"
---
"""

import argparse
import difflib
import os
from pathlib import Path
import re

import numpy as np
import pandas as pd
import plotly.graph_objects as go

_ROOT = Path(__file__).resolve().parents[1]


def normalize_channel_name(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).replace("\u3000", " ").strip()
    return s if s else None


def normalize_channel_key(value: object) -> str | None:
    s = normalize_channel_name(value)
    if s is None:
        return None
    return s.casefold()


def parse_cn_date(value: object) -> pd.Timestamp:
    if value is None:
        return pd.NaT
    if isinstance(value, float) and np.isnan(value):
        return pd.NaT
    s = str(value).strip()
    if not s:
        return pd.NaT
    clean = s.replace("年", "-").replace("月", "-").replace("日", "")
    return pd.to_datetime(clean, errors="coerce")


def parse_date_bound(value: str | None, is_end: bool) -> pd.Timestamp | None:
    if value is None:
        return None
    ts = pd.Timestamp(value)
    if ts.hour == 0 and ts.minute == 0 and ts.second == 0 and ts.microsecond == 0 and ts.nanosecond == 0:
        if is_end:
            return ts + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        return ts
    return ts


def read_csv_flexible(file_path: Path) -> pd.DataFrame:
    encodings = ["utf-16", "utf-8-sig", "utf-8", "utf-16le", "utf-16be", "gbk"]
    seps = ["\t", ","]
    last_error: Exception | None = None

    for encoding in encodings:
        for sep in seps:
            try:
                df = pd.read_csv(file_path, encoding=encoding, sep=sep, low_memory=False)
                if len(df.columns) <= 1:
                    continue
                return df
            except Exception as e:
                last_error = e

    try:
        df = pd.read_csv(file_path, encoding="utf-8-sig", sep=None, engine="python")
        if len(df.columns) > 1:
            return df
    except Exception as e:
        last_error = e

    raise RuntimeError(f"读取失败: {file_path}\n原始错误: {last_error}")


def pivot_long_to_wide(df: pd.DataFrame) -> pd.DataFrame:
    required = {"度量名称", "度量值"}
    if not required.issubset(set(df.columns)):
        return df

    index_cols = [c for c in df.columns if c not in ["度量名称", "度量值"]]
    df_temp = df.copy()
    for col in index_cols:
        df_temp[col] = df_temp[col].fillna("__NAN_PLACEHOLDER__")

    df_wide = (
        df_temp.pivot_table(index=index_cols, columns="度量名称", values="度量值", aggfunc="first")
        .reset_index()
        .copy()
    )

    for col in index_cols:
        df_wide[col] = df_wide[col].replace("__NAN_PLACEHOLDER__", np.nan)

    df_wide.columns = [str(c).strip() for c in df_wide.columns]
    return df_wide


def detect_channel_from_filename(file_path: Path) -> str | None:
    stem = file_path.stem
    if "渠道归因_" in stem:
        return stem.split("渠道归因_", 1)[1]

    rules = [
        ("IM智己|未来智舱", "IM智己|未来智舱"),
        ("智己生活家", "智己生活家"),
        ("智己汽车官方直播", "智己汽车官方直播"),
        ("快慢闪", "Popup（门店）"),
        ("CM2", "Popup（门店）"),
    ]
    for token, channel in rules:
        if token in stem:
            return channel
    return None


def resolve_target_channel(
    file_path: Path, available_channels: list[object], target_channel: str | None
) -> str:
    available_list = [normalize_channel_name(c) for c in available_channels]
    available_list = [c for c in available_list if c is not None]
    normalized_available = {normalize_channel_key(c): c for c in available_list}

    alias_map = {
        "快慢闪": "Popup（门店）",
        "popup（门店）": "Popup（门店）",
        "popup": "Popup（门店）",
        "popup(门店)": "Popup（门店）",
        "popup（門店）": "Popup（门店）",
        "im智己": "IM智己汽车",
        "im": "IM智己汽车",
        "app小程序": "APP",
        "app": "APP",
    }

    if target_channel is None:
        guess = detect_channel_from_filename(file_path)
        target_channel = guess if guess else "IM智己汽车"

    normalized_target = normalize_channel_key(target_channel)
    if normalized_target in alias_map:
        normalized_target = normalize_channel_key(alias_map[normalized_target])

    if normalized_target in normalized_available:
        return normalized_available[normalized_target]

    suggestions = difflib.get_close_matches(
        normalized_target or "",
        [k for k in normalized_available.keys() if k is not None],
        n=5,
        cutoff=0.6,
    )
    if suggestions:
        suggestion_text = ", ".join([normalized_available[s] for s in suggestions if s in normalized_available])
        raise ValueError(f"渠道 '{target_channel}' 不在数据中，可选接近项: {suggestion_text}")
    raise ValueError(f"渠道 '{target_channel}' 不在数据中")


def list_channels_to_analyze(file_path: Path, available_channels: list[object], target_channel: str | None) -> list[str]:
    if target_channel is not None:
        return [resolve_target_channel(file_path, available_channels, target_channel)]

    available_list = [normalize_channel_name(c) for c in available_channels]
    available_list = [c for c in available_list if c is not None]
    return sorted(set(available_list))


def resolve_middle_channel_col(columns: list[object]) -> str | None:
    candidates = [
        "lc_middle_channel_name",
        "lc_mid_channel_name",
        "middle_channel_name",
        "Middle Channel Name",
        "middle channel name",
        "中渠道",
        "中间渠道",
    ]
    col_set = {str(c): c for c in columns}
    for c in candidates:
        if c in col_set:
            return str(col_set[c])
    for c in columns:
        s = str(c)
        k = s.casefold()
        if "middle" in k and "channel" in k:
            return s
        if "mid" in k and "channel" in k:
            return s
        if "中渠道" in s or "中间渠道" in s:
            return s
    return None


def categorize_channel(small_channel_name: str, middle_channel_name: str | None) -> str:
    small = small_channel_name or ""
    middle = middle_channel_name or ""

    if ("APP" in small) or ("小程序" in small):
        return "APP小程序"

    if small == "Popup（门店）":
        return "快慢闪"

    if ("直播" in small) or small.startswith("IM") or small.startswith("智己"):
        return "直播"

    if small == "自然客流":
        return "门店"

    if (small in {"懂车帝", "汽车之家", "易车"}) and (middle == "网销平台"):
        return "门店"

    platform_tokens_1 = {"抖音", "快手", "巨懂车", "视频号", "B站", "小红书", "头条", "腾讯ADQ"}
    platform_tokens_2 = {"汽车之家", "懂车帝", "易车"}

    part1 = any(t in small for t in platform_tokens_1) and (middle != "自有渠道")
    part2 = any(t in small for t in platform_tokens_2) and (middle != "网销平台")
    part3 = small.startswith("品牌投放-")

    excluded = ("直播" in small) or small.startswith("IM")

    if (part1 or part2 or part3) and (not excluded):
        return "平台"

    return "其他"


def perform_attribution_analysis(
    df_cohort_global: pd.DataFrame,
    conversion_col_name: str,
    target_channel: str,
    conversion_type: str,
    conversion_end_date: pd.Timestamp | None = None,
) -> dict | None:
    return perform_attribution_analysis_set(
        df_cohort_global=df_cohort_global,
        conversion_col_name=conversion_col_name,
        target_channels={target_channel},
        conversion_type=conversion_type,
        conversion_end_date=conversion_end_date,
    )


def perform_attribution_analysis_set(
    df_cohort_global: pd.DataFrame,
    conversion_col_name: str,
    target_channels: set[str],
    conversion_type: str,
    conversion_end_date: pd.Timestamp | None = None,
) -> dict | None:
    if conversion_end_date is None:
        converted_md5s = df_cohort_global[df_cohort_global[conversion_col_name].notna()]["lc_user_phone_md5"].unique()
    else:
        conv_dt = df_cohort_global[conversion_col_name].apply(parse_cn_date)
        eligible = df_cohort_global[
            df_cohort_global[conversion_col_name].notna() & conv_dt.notna() & (conv_dt <= conversion_end_date)
        ]
        converted_md5s = eligible["lc_user_phone_md5"].unique()

    if len(converted_md5s) == 0:
        return None

    df_converted = df_cohort_global[df_cohort_global["lc_user_phone_md5"].isin(converted_md5s)].copy()
    if conversion_end_date is not None:
        df_converted["_conversion_dt"] = df_converted[conversion_col_name].apply(parse_cn_date)

    df_converted = df_converted.sort_values(["lc_user_phone_md5", "parsed_create_time"])

    stats: list[dict] = []

    for md5, group in df_converted.groupby("lc_user_phone_md5"):
        group = group.sort_values("parsed_create_time")
        touches = group[group["lc_small_channel_name"].isin(target_channels)]
        if touches.empty:
            continue

        total_interactions = len(group)
        target_touch_count = len(touches)

        is_first_touch = bool(group.iloc[0]["lc_small_channel_name"] in target_channels)

        user_conversion_dates = group[conversion_col_name].dropna().unique()
        is_last_touch = False
        has_pre_conversion_target = False

        if conversion_end_date is not None:
            conv_dates = group["_conversion_dt"].dropna()
            conv_dates = conv_dates[conv_dates <= conversion_end_date]
            if not conv_dates.empty:
                conversion_date = conv_dates.min()
                cutoff_time = conversion_date + pd.Timedelta(days=1)
                pre_conversion_interactions = group[group["parsed_create_time"] < cutoff_time]
                if not pre_conversion_interactions.empty:
                    pre_target = pre_conversion_interactions[
                        pre_conversion_interactions["lc_small_channel_name"].isin(target_channels)
                    ]
                    has_pre_conversion_target = not pre_target.empty
                    last_interaction = pre_conversion_interactions.iloc[-1]
                    if last_interaction["lc_small_channel_name"] in target_channels:
                        is_last_touch = True
            else:
                has_pre_conversion_target = True
                last_interaction = group.iloc[-1]
                if last_interaction["lc_small_channel_name"] in target_channels:
                    is_last_touch = True
        elif len(user_conversion_dates) > 0:
            conversion_date = parse_cn_date(user_conversion_dates[0])
            if pd.notna(conversion_date):
                cutoff_time = conversion_date + pd.Timedelta(days=1)
                pre_conversion_interactions = group[group["parsed_create_time"] < cutoff_time]
                if not pre_conversion_interactions.empty:
                    pre_target = pre_conversion_interactions[
                        pre_conversion_interactions["lc_small_channel_name"].isin(target_channels)
                    ]
                    has_pre_conversion_target = not pre_target.empty
                    last_interaction = pre_conversion_interactions.iloc[-1]
                    if last_interaction["lc_small_channel_name"] in target_channels:
                        is_last_touch = True
            else:
                has_pre_conversion_target = True
                last_interaction = group.iloc[-1]
                if last_interaction["lc_small_channel_name"] in target_channels:
                    is_last_touch = True
        else:
            has_pre_conversion_target = True
            last_interaction = group.iloc[-1]
            if last_interaction["lc_small_channel_name"] in target_channels:
                is_last_touch = True

        if conversion_end_date is not None:
            rows_with_conversion = group[group["_conversion_dt"].notna() & (group["_conversion_dt"] <= conversion_end_date)]
        else:
            rows_with_conversion = group[group[conversion_col_name].notna()]
        conversion_channels = rows_with_conversion["lc_small_channel_name"].unique().tolist()
        is_im_conversion = any(ch in target_channels for ch in conversion_channels)

        stats.append(
            {
                "md5": md5,
                "total_interactions": total_interactions,
                "target_touch_count": target_touch_count,
                "is_first_touch": is_first_touch,
                "is_last_touch": is_last_touch,
                "has_pre_conversion_target": has_pre_conversion_target,
                "conversion_channels": conversion_channels,
                "is_im_conversion": is_im_conversion,
            }
        )

    stats_df = pd.DataFrame(stats)
    if stats_df.empty:
        return None

    im_conversion_df = stats_df[stats_df["is_im_conversion"]]
    direct_count = im_conversion_df[im_conversion_df["is_last_touch"]].shape[0]
    attributed_count = im_conversion_df[~im_conversion_df["is_last_touch"]].shape[0]

    assisted_df = stats_df[~stats_df["is_im_conversion"]]
    first_touch_assist_count = assisted_df[assisted_df["is_first_touch"]].shape[0]

    middle_assist_df = assisted_df[(~assisted_df["is_first_touch"]) & (assisted_df["has_pre_conversion_target"])]
    middle_assist_count = middle_assist_df.shape[0]

    post_conversion_df = assisted_df[(~assisted_df["is_first_touch"]) & (~assisted_df["has_pre_conversion_target"])]
    post_conversion_count = post_conversion_df.shape[0]

    def classify(row: pd.Series) -> str:
        if row["is_im_conversion"]:
            return "Direct" if row["is_last_touch"] else "Attributed"
        if row["is_first_touch"]:
            return "First Touch Assist"
        if row["has_pre_conversion_target"]:
            return "Middle Assist"
        return f"Post-{conversion_type} Interaction"

    stats_df["Category"] = stats_df.apply(classify, axis=1)

    return {
        "conversion_type": conversion_type,
        "converted_users": len(converted_md5s),
        "im_conversion_count": len(im_conversion_df),
        "direct_count": direct_count,
        "attributed_count": attributed_count,
        "assisted_count": len(assisted_df),
        "first_touch_assist_count": first_touch_assist_count,
        "middle_assist_count": middle_assist_count,
        "post_conversion_count": post_conversion_count,
        "stats_df": stats_df,
    }


def generate_mermaid_graph(result: dict | None, conversion_type: str) -> str:
    if not result:
        return ""

    converted_users = result["converted_users"]
    im_count = result["im_conversion_count"]
    direct = result["direct_count"]
    attributed = result["attributed_count"]
    assisted = result["assisted_count"]
    first_touch = result["first_touch_assist_count"]
    middle = result["middle_assist_count"]
    post = result["post_conversion_count"]

    conversion_label = "Lock" if conversion_type == "锁单" else "Intention"

    return f"""
graph TD
    Start[Global {conversion_type}<br/>{converted_users} Users]

    Start --> A{{{conversion_type} Channel?}}

    A -->|Target {conversion_label}: {im_count}| B{{Last Touch?}}
    B -->|Target Last Touch: {direct}| C[Direct {conversion_label}<br/>{direct} Users]
    B -->|Other Last Touch: {attributed}| D[Attributed {conversion_label}<br/>{attributed} Users]

    A -->|Other {conversion_label}: {assisted}| E{{First Touch?}}
    E -->|Target First Touch: {first_touch}| F[First Touch Assist<br/>{first_touch} Users]
    E -->|Other First Touch| G{{Pre-{conversion_label} Interaction?}}

    G -->|Has Target Pre-{conversion_label}: {middle}| H[Middle Assist<br/>{middle} Users]
    G -->|No Target Pre-{conversion_label}: {post}| I[Post-{conversion_label} Interaction<br/>{post} Users]

    style Start fill:#f9f,stroke:#333,stroke-width:2px
    style C fill:#bfb,stroke:#333,stroke-width:2px
    style D fill:#dfd,stroke:#333,stroke-width:2px
    style F fill:#ff9,stroke:#333,stroke-width:2px
    style H fill:#ff9,stroke:#333,stroke-width:2px
    style I fill:#eee,stroke:#999,stroke-width:1px,stroke-dasharray: 5 5
    """


def generate_plotly_table(result: dict | None) -> str:
    if not result or "stats_df" not in result:
        return "<p>No data available</p>"

    stats_df = result["stats_df"]
    preferred_cols = [
        "md5",
        "total_interactions",
        "target_touch_count",
        "is_first_touch",
        "is_last_touch",
        "is_im_conversion",
        "Category",
    ]
    cols = [c for c in preferred_cols if c in stats_df.columns]
    table_df = stats_df[cols].head(50)

    bool_cols = ["is_first_touch", "is_last_touch", "is_im_conversion"]
    for col in bool_cols:
        if col in table_df.columns:
            table_df[col] = table_df[col].map({True: "✅", False: "❌"})

    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=[f"<b>{c}</b>" for c in table_df.columns],
                    fill_color="#373f4a",
                    align=["left", "right", "right", "center", "center", "center", "left"],
                    font=dict(color="white", size=12),
                    height=40,
                ),
                cells=dict(
                    values=[table_df[k].tolist() for k in table_df.columns],
                    fill=dict(color="white"),
                    line=dict(color="#e1e4e8"),
                    align=["left", "right", "right", "center", "center", "center", "left"],
                    font=dict(color="#373f4a", size=12),
                    height=32,
                ),
            )
        ]
    )

    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), height=min(400 + len(table_df) * 32, 800))
    return fig.to_html(full_html=False, include_plotlyjs="cdn")


def resolve_output_path(file_path: Path, output_file: str | None) -> Path:
    stem = file_path.stem
    out_dir = _ROOT / "out"

    if output_file is None:
        return out_dir / f"{stem}_report.html"

    p = Path(output_file)
    if p.is_absolute():
        return p

    s = str(p)
    if s.startswith(f"out{os.sep}") or s == "out":
        return _ROOT / p
    if s.startswith(f"reports{os.sep}") or s == "reports":
        return Path(__file__).resolve().parent / p

    return out_dir / p


def channel_id(value: str) -> str:
    s = value.strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^0-9A-Za-z\u4e00-\u9fff_\\-]+", "_", s)
    if not s:
        s = "channel"
    return f"ch_{s}"


def build_summary_table(rows: list[dict]) -> str:
    if not rows:
        return "<p>无可分析渠道</p>"
    df = pd.DataFrame(rows)
    if "category" in df.columns:
        sort_cols: list[str] = ["category"]
        ascending: list[bool] = [True]
        if "cohort_users" in df.columns:
            sort_cols.append("cohort_users")
            ascending.append(False)
        if "channel" in df.columns:
            sort_cols.append("channel")
            ascending.append(True)
        df = df.sort_values(sort_cols, ascending=ascending)
    total_row: dict[str, object] = {}
    if "category" in df.columns:
        total_row["category"] = "<strong>总计</strong>"
    if "channel" in df.columns:
        total_row["channel"] = "<strong>总计</strong>"
    for col in df.columns:
        if col in {"category", "channel"}:
            continue
        s = pd.to_numeric(df[col], errors="coerce").fillna(0)
        total_row[col] = int(s.sum())
        df[col] = s.astype(int)
    df = pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)
    preferred = [
        "category",
        "channel",
        "cohort_users",
        "global_records",
        "locked_users",
        "channel_lock_users",
        "category_lock_users",
        "direct_lock",
        "attributed_lock",
        "intention_users",
        "channel_intention_users",
        "category_intention_users",
        "direct_intention",
        "attributed_intention",
    ]
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    df = df[cols]
    df = df.fillna(0)
    return df.to_html(index=False, classes="table", escape=False)


def build_channel_section(
    target_channel: str,
    cohort_users: int,
    global_records: int,
    lock_result: dict | None,
    intention_result: dict | None,
    included_channels: list[str] | None = None,
) -> str:
    lock_mermaid = generate_mermaid_graph(lock_result, "锁单") if lock_result else ""
    intention_mermaid = generate_mermaid_graph(intention_result, "小订") if intention_result else ""
    lock_table_html = generate_plotly_table(lock_result) if lock_result else "<p>无锁单数据</p>"
    intention_table_html = generate_plotly_table(intention_result) if intention_result else "<p>无小订数据</p>"

    lock_stats_html = "<p>无锁单数据</p>"
    if lock_result:
        lock_stats_html = f"""
<ul>
    <li><strong>锁单用户:</strong> {lock_result["converted_users"]}</li>
    <li><strong>本渠道锁单:</strong> {lock_result["im_conversion_count"]} (直接 {lock_result["direct_count"]} / 归因 {lock_result["attributed_count"]})</li>
    <li><strong>辅助锁单:</strong> {lock_result["assisted_count"]} (首触 {lock_result["first_touch_assist_count"]} / 过程 {lock_result["middle_assist_count"]} / 锁后 {lock_result["post_conversion_count"]})</li>
</ul>
"""

    intention_stats_html = "<p>无小订数据</p>"
    if intention_result:
        intention_stats_html = f"""
<ul>
    <li><strong>小订用户:</strong> {intention_result["converted_users"]}</li>
    <li><strong>本渠道小订:</strong> {intention_result["im_conversion_count"]} (直接 {intention_result["direct_count"]} / 归因 {intention_result["attributed_count"]})</li>
    <li><strong>辅助小订:</strong> {intention_result["assisted_count"]} (首触 {intention_result["first_touch_assist_count"]} / 过程 {intention_result["middle_assist_count"]} / 小订后 {intention_result["post_conversion_count"]})</li>
</ul>
"""

    lock_detail_html = ""
    if lock_result:
        lock_detail_html = f"""
<details>
    <summary>锁单归因明细（流程图 + Top50）</summary>
    <div class="mermaid">
{lock_mermaid}
    </div>
    {lock_table_html}
</details>
"""

    intention_detail_html = ""
    if intention_result:
        intention_detail_html = f"""
<details>
    <summary>小订归因明细（流程图 + Top50）</summary>
    <div class="mermaid">
{intention_mermaid}
    </div>
    {intention_table_html}
</details>
"""

    ch_id = channel_id(target_channel)
    included_html = ""
    if included_channels:
        items = "".join([f"<li>{c}</li>" for c in included_channels])
        included_html = f"""
<details>
    <summary>包含的小渠道 ({len(included_channels)})</summary>
    <ul>
        {items}
    </ul>
</details>
"""
    return f"""
<section id="{ch_id}">
    <h2>{target_channel}</h2>
    <p><strong>Cohort 用户数:</strong> {cohort_users}；<strong>全局记录数:</strong> {global_records}</p>
    {included_html}
    <h3>锁单归因摘要</h3>
    {lock_stats_html}
    {lock_detail_html}
    <h3>小订归因摘要</h3>
    {intention_stats_html}
    {intention_detail_html}
</section>
"""


def analyze_conversion(file_path: str, output_file: str | None, start_date: str | None, end_date: str | None, channel: str | None) -> None:
    input_path = Path(file_path)
    df = read_csv_flexible(input_path)
    df.columns = [str(c).strip().replace("\ufeff", "") for c in df.columns]
    df = pivot_long_to_wide(df)

    lock_cols = [c for c in df.columns if "lc_order_lock_time_min" in str(c)]
    if not lock_cols:
        raise ValueError("未找到锁单时间列 (lc_order_lock_time_min)")
    lock_col_name = lock_cols[0]

    intention_cols = [c for c in df.columns if "lc_order_intention_pay_time_min" in str(c)]
    intention_col_name = intention_cols[0] if intention_cols else None

    df["lc_small_channel_name"] = df["lc_small_channel_name"].apply(normalize_channel_name)
    df["parsed_create_time"] = pd.to_datetime(df["lc_create_time"], errors="coerce")

    start_ts = parse_date_bound(start_date, is_end=False) or pd.Timestamp("2026-02-01")
    end_ts = parse_date_bound(end_date, is_end=True) or (pd.Timestamp("2026-02-27") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1))

    available_channels = df["lc_small_channel_name"].dropna().unique().tolist()
    channels_to_analyze = list_channels_to_analyze(input_path, available_channels, channel)
    middle_col = resolve_middle_channel_col(list(df.columns))

    mask_time = (df["parsed_create_time"] >= start_ts) & (df["parsed_create_time"] <= end_ts)

    output_path = resolve_output_path(input_path, output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    is_multi_channel = channel is None
    title = "全渠道 转化归因分析报告" if is_multi_channel else f"{channels_to_analyze[0]} 转化归因分析报告"

    summary_rows: list[dict] = []
    sections: list[str] = []

    if is_multi_channel:
        channel_meta: dict[str, dict] = {}
        for ch in channels_to_analyze:
            middle_value = None
            if middle_col is not None and middle_col in df.columns:
                vc = df[df["lc_small_channel_name"] == ch][middle_col].dropna().value_counts()
                if not vc.empty:
                    middle_value = str(vc.index[0])
            channel_meta[ch] = {
                "middle": middle_value,
                "category": categorize_channel(ch, middle_value),
            }

        category_order = ["APP小程序", "平台", "快慢闪", "直播", "门店", "其他"]
        category_to_channels: dict[str, list[str]] = {k: [] for k in category_order}
        for ch, meta in channel_meta.items():
            cat = meta["category"]
            if cat not in category_to_channels:
                category_to_channels[cat] = []
            category_to_channels[cat].append(ch)
        for cat in list(category_to_channels.keys()):
            category_to_channels[cat] = sorted(category_to_channels[cat])

        for cat in category_order + [c for c in category_to_channels.keys() if c not in category_order]:
            target_channels = set(category_to_channels.get(cat, []))
            if not target_channels:
                continue
            cohort_md5s = df[mask_time & (df["lc_small_channel_name"].isin(target_channels))]["lc_user_phone_md5"].dropna().unique()
            if len(cohort_md5s) == 0:
                continue
            df_cohort_global = df[df["lc_user_phone_md5"].isin(cohort_md5s)].copy()
            global_records = len(df_cohort_global)

            lock_result = perform_attribution_analysis_set(df_cohort_global, lock_col_name, target_channels, "锁单")
            intention_result = (
                perform_attribution_analysis_set(df_cohort_global, intention_col_name, target_channels, "小订", conversion_end_date=end_ts)
                if intention_col_name
                else None
            )

            summary_rows.append(
                {
                    "category": f'<a href="#{channel_id(cat)}">{cat}</a>',
                    "cohort_users": len(cohort_md5s),
                    "global_records": global_records,
                    "locked_users": (lock_result or {}).get("converted_users", 0),
                    "category_lock_users": (lock_result or {}).get("im_conversion_count", 0),
                    "direct_lock": (lock_result or {}).get("direct_count", 0),
                    "attributed_lock": (lock_result or {}).get("attributed_count", 0),
                    "intention_users": (intention_result or {}).get("converted_users", 0),
                    "category_intention_users": (intention_result or {}).get("im_conversion_count", 0),
                    "direct_intention": (intention_result or {}).get("direct_count", 0),
                    "attributed_intention": (intention_result or {}).get("attributed_count", 0),
                }
            )

            sections.append(
                build_channel_section(
                    cat,
                    len(cohort_md5s),
                    global_records,
                    lock_result,
                    intention_result,
                    included_channels=category_to_channels.get(cat, []),
                )
            )
    else:
        target_channel = channels_to_analyze[0]
        cohort_md5s = df[mask_time & (df["lc_small_channel_name"] == target_channel)]["lc_user_phone_md5"].dropna().unique()
        df_cohort_global = df[df["lc_user_phone_md5"].isin(cohort_md5s)].copy()
        global_records = len(df_cohort_global)

        lock_result = perform_attribution_analysis(df_cohort_global, lock_col_name, target_channel, "锁单")
        intention_result = (
            perform_attribution_analysis(df_cohort_global, intention_col_name, target_channel, "小订", conversion_end_date=end_ts)
            if intention_col_name
            else None
        )

        summary_rows.append(
            {
                "category": categorize_channel(target_channel, None),
                "channel": f'<a href="#{channel_id(target_channel)}">{target_channel}</a>',
                "cohort_users": len(cohort_md5s),
                "global_records": global_records,
                "locked_users": (lock_result or {}).get("converted_users", 0),
                "channel_lock_users": (lock_result or {}).get("im_conversion_count", 0),
                "direct_lock": (lock_result or {}).get("direct_count", 0),
                "attributed_lock": (lock_result or {}).get("attributed_count", 0),
                "intention_users": (intention_result or {}).get("converted_users", 0),
                "channel_intention_users": (intention_result or {}).get("im_conversion_count", 0),
                "direct_intention": (intention_result or {}).get("direct_count", 0),
                "attributed_intention": (intention_result or {}).get("attributed_count", 0),
            }
        )

        sections.append(build_channel_section(target_channel, len(cohort_md5s), global_records, lock_result, intention_result))

    summary_html = build_summary_table(summary_rows)

    html_content = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <script type="module">
        import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.esm.min.mjs';
        mermaid.initialize({{
            startOnLoad: true,
            theme: 'default',
            securityLevel: 'loose'
        }});
    </script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            line-height: 1.6;
            max-width: 1000px;
            margin: 0 auto;
            padding: 40px 20px;
            color: #24292e;
            background-color: #ffffff;
        }}
        h1 {{ border-bottom: 1px solid #eaecef; padding-bottom: 0.3em; }}
        h2 {{ color: #0366d6; margin-top: 30px; }}
        .stats-container {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            background-color: #f6f8fa;
            padding: 20px;
            border-radius: 6px;
            border: 1px solid #e1e4e8;
        }}
        .stat-box {{
            padding: 10px;
        }}
        .mermaid {{ margin: 30px 0; text-align: center; }}
        .plotly-graph-div {{ margin: 20px 0; border: 1px solid #e1e4e8; border-radius: 6px; }}
        table.table {{ border-collapse: collapse; width: 100%; }}
        table.table th, table.table td {{ border: 1px solid #e1e4e8; padding: 8px 10px; }}
        table.table th {{ background: #f6f8fa; text-align: left; }}
        details {{ margin: 10px 0 20px; }}
    </style>
</head>
<body>
    <h1>{title}</h1>
    <p>数据源: {input_path.name}</p>
    <p>Cohort 时间窗: {start_ts} ~ {end_ts}</p>
    <h2>汇总</h2>
    {summary_html}

    {"".join(sections)}

</body>
</html>
"""

    output_path.write_text(html_content, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("file_path")
    parser.add_argument("--output", "-o")
    parser.add_argument("--start-date", "-s")
    parser.add_argument("--end-date", "-e")
    parser.add_argument("--channel", "-c")
    args = parser.parse_args()

    analyze_conversion(args.file_path, args.output, args.start_date, args.end_date, args.channel)


if __name__ == "__main__":
    main()

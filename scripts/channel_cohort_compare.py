"""
---
name: channel_cohort_compare
type: script
path: scripts/channel_cohort_compare.py
updated_at: "2026-04-29 00:00"
summary: 对比两份“渠道归因_YYYYMMDD.csv”在各渠道大类（APP小程序/平台/快慢闪/直播/门店/其他）的 Cohort 规模与锁单/小订归因差异（B-A），并输出 HTML。
inputs:
  - 渠道归因_YYYYMMDD.csv（A）
  - 渠道归因_YYYYMMDD.csv（B）
outputs:
  - out/<stem>_report.html（A/B 明细，内部调用 channel_cohort_conversion）
  - out/channel_cohort_compare_<A>_vs_<B>.html（默认）
cli:
  - python3 scripts/channel_cohort_compare.py <file_a.csv> <file_b.csv> -o out/channel_cohort_compare_<A>_vs_<B>.html
---
"""

import argparse
import os
import re
from pathlib import Path

import pandas as pd

from channel_cohort_conversion import (
    analyze_conversion,
    categorize_channel,
    list_channels_to_analyze,
    normalize_channel_name,
    parse_date_bound,
    perform_attribution_analysis_set,
    pivot_long_to_wide,
    read_csv_flexible,
    resolve_middle_channel_col,
 )


def extract_yyyymmdd(path: Path) -> str | None:
    m = re.search(r"(20\d{6})", path.stem)
    if not m:
        return None
    return m.group(1)


def yyyymmdd_to_range(yyyymmdd: str) -> tuple[str, str]:
    start = pd.Timestamp(f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}")
    end = start + pd.Timedelta(days=1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def compute_category_summary(file_path: Path, start_date: str, end_date: str) -> pd.DataFrame:
    df = read_csv_flexible(file_path)
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
    end_ts = parse_date_bound(end_date, is_end=True) or (
        pd.Timestamp("2026-02-27") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    )

    available_channels = df["lc_small_channel_name"].dropna().unique().tolist()
    channels_to_analyze = list_channels_to_analyze(file_path, available_channels, None)
    middle_col = resolve_middle_channel_col(list(df.columns))

    mask_time = (df["parsed_create_time"] >= start_ts) & (df["parsed_create_time"] <= end_ts)

    channel_meta: dict[str, dict] = {}
    for ch in channels_to_analyze:
        middle_value = None
        if middle_col is not None and middle_col in df.columns:
            vc = df[df["lc_small_channel_name"] == ch][middle_col].dropna().value_counts()
            if not vc.empty:
                middle_value = str(vc.index[0])
        channel_meta[ch] = {"category": categorize_channel(ch, middle_value)}

    category_order = ["APP小程序", "平台", "快慢闪", "直播", "门店", "其他"]
    category_to_channels: dict[str, list[str]] = {k: [] for k in category_order}
    for ch, meta in channel_meta.items():
        cat = meta["category"]
        if cat not in category_to_channels:
            category_to_channels[cat] = []
        category_to_channels[cat].append(ch)
    for cat in list(category_to_channels.keys()):
        category_to_channels[cat] = sorted(category_to_channels[cat])

    rows: list[dict] = []
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

        rows.append(
            {
                "category": cat,
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

    df_out = pd.DataFrame(rows)
    if df_out.empty:
        return df_out
    for c in df_out.columns:
        if c == "category":
            continue
        df_out[c] = pd.to_numeric(df_out[c], errors="coerce").fillna(0).astype(int)

    total = {"category": "总计"}
    for c in df_out.columns:
        if c == "category":
            continue
        total[c] = int(df_out[c].sum())
    df_out = pd.concat([df_out, pd.DataFrame([total])], ignore_index=True)
    return df_out


def ensure_numeric(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if c == "category":
            continue
        out[c] = pd.to_numeric(out[c], errors="coerce").fillna(0).astype(int)
    return out


def build_compare(df_left: pd.DataFrame, df_right: pd.DataFrame, left_label: str, right_label: str) -> pd.DataFrame:
    left = ensure_numeric(df_left[df_left["category"] != "总计"].copy()).set_index("category")
    right = ensure_numeric(df_right[df_right["category"] != "总计"].copy()).set_index("category")

    all_categories = sorted(set(left.index.tolist()) | set(right.index.tolist()))
    left = left.reindex(all_categories).fillna(0).astype(int)
    right = right.reindex(all_categories).fillna(0).astype(int)

    merged = pd.concat(
        {
            left_label: left,
            right_label: right,
            "delta": (right - left),
        },
        axis=1,
    )
    merged.columns = [f"{a}__{b}" for a, b in merged.columns]
    merged = merged.reset_index()
    return merged


def to_html_table(df: pd.DataFrame) -> str:
    return df.to_html(index=False, classes="table", escape=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("file_a")
    parser.add_argument("file_b")
    parser.add_argument("--output", "-o")
    parser.add_argument("--start-a")
    parser.add_argument("--end-a")
    parser.add_argument("--start-b")
    parser.add_argument("--end-b")
    args = parser.parse_args()

    file_a = Path(args.file_a)
    file_b = Path(args.file_b)

    a_token = extract_yyyymmdd(file_a) or "A"
    b_token = extract_yyyymmdd(file_b) or "B"

    if args.start_a and args.end_a:
        start_a, end_a = args.start_a, args.end_a
    else:
        if a_token == "A":
            raise RuntimeError("缺少 --start-a/--end-a")
        start_a, end_a = yyyymmdd_to_range(a_token)

    if args.start_b and args.end_b:
        start_b, end_b = args.start_b, args.end_b
    else:
        if b_token == "B":
            raise RuntimeError("缺少 --start-b/--end-b")
        start_b, end_b = yyyymmdd_to_range(b_token)

    report_a = f"out/{file_a.stem}_report.html"
    report_b = f"out/{file_b.stem}_report.html"

    analyze_conversion(str(file_a), report_a, start_a, end_a, None)
    analyze_conversion(str(file_b), report_b, start_b, end_b, None)

    scripts_dir = Path(__file__).resolve().parent
    root_dir = scripts_dir.parent
    report_a_path = root_dir / report_a
    report_b_path = root_dir / report_b

    df_a = compute_category_summary(file_a, start_a, end_a)
    df_b = compute_category_summary(file_b, start_b, end_b)

    left_label = f"{a_token}({start_a}~{end_a})"
    right_label = f"{b_token}({start_b}~{end_b})"
    df_delta = build_compare(df_a, df_b, left_label, right_label)

    out_dir = root_dir / "out"
    out_path = Path(args.output) if args.output else out_dir / f"channel_cohort_compare_{a_token}_vs_{b_token}.html"
    if not out_path.is_absolute():
        s = str(out_path)
        if s.startswith(f"out{os.sep}") or s == "out":
            out_path = root_dir / out_path
        elif s.startswith(f"reports{os.sep}") or s == "reports":
            out_path = scripts_dir / out_path
        else:
            out_path = out_dir / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    html = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>渠道归因对比报告</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      line-height: 1.6;
      max-width: 1100px;
      margin: 0 auto;
      padding: 40px 20px;
      color: #24292e;
      background-color: #ffffff;
    }}
    h1 {{ border-bottom: 1px solid #eaecef; padding-bottom: 0.3em; }}
    h2 {{ color: #0366d6; margin-top: 30px; }}
    table.table {{ border-collapse: collapse; width: 100%; }}
    table.table th, table.table td {{ border: 1px solid #e1e4e8; padding: 8px 10px; }}
    table.table th {{ background: #f6f8fa; text-align: left; }}
    .grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
    }}
    .box {{
      border: 1px solid #e1e4e8;
      border-radius: 6px;
      padding: 12px;
      background: #ffffff;
    }}
    .muted {{ color: #57606a; }}
  </style>
</head>
<body>
  <h1>渠道归因对比报告</h1>
  <p class="muted">A: {file_a.name}（{start_a}~{end_a}）｜B: {file_b.name}（{start_b}~{end_b}）</p>

  <h2>单期汇总</h2>
  <div class="grid">
    <div class="box">
      <h3>{left_label}</h3>
      <p class="muted">明细报告：<a href="{report_a_path.name}">{report_a_path.name}</a></p>
      {to_html_table(df_a)}
    </div>
    <div class="box">
      <h3>{right_label}</h3>
      <p class="muted">明细报告：<a href="{report_b_path.name}">{report_b_path.name}</a></p>
      {to_html_table(df_b)}
    </div>
  </div>

  <h2>对比（B - A）</h2>
  {to_html_table(df_delta)}
</body>
</html>
"""
    out_path.write_text(html, encoding="utf-8")


if __name__ == "__main__":
    main()

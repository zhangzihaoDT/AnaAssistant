"""
---
name: series_group_logic_trend_by_date
type: workflow_script
path: scripts/series_group_logic_trend_by_date.py
updated_at: "2026-04-29 00:00"
summary: 参考 index_summary.py 的锁单口径，按 series_group_logic 分组计算每日 locks 与 first_assign_lock_time 的中位数(mid)，并用 Plotly 输出两张对比折线图。
inputs:
  - schema/data_path.md (optional, via --data-path-md)
  - schema/business_definition.json (optional, via --business-definition)
  - order_data.parquet (from data_path.md: 订单分析)
outputs:
  - HTML report (via --html-out)
  - optional: daily metrics csv (via --csv-out)
cli:
  - python3 scripts/series_group_logic_trend_by_date.py --models CM2 LS8
  - python3 scripts/series_group_logic_trend_by_date.py --start 2025-03-01 --end 2026-04-28 --models LS8 LS9 CM2
---
"""

import argparse
import glob
import json
import sys
from pathlib import Path
import re

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from operators.series_group_logic import apply_series_group_logic


COLOR_MAIN = "#3498DB"
COLOR_CONTRAST = "#E67E22"
COLOR_DARK = "#373f4a"
COLOR_GRID = "#ebedf0"
COLOR_AXIS = "#7B848F"
COLOR_BG = "#FFFFFF"


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _load_json(path: Path) -> dict:
    return json.loads(_read_text(path))


def _read_data_paths(md_path: Path) -> dict[str, Path]:
    raw = _read_text(md_path).splitlines()
    out: dict[str, Path] = {}
    for line in raw:
        line = line.strip()
        if not line:
            continue
        if "：" in line:
            name, path = line.split("：", 1)
        elif ":" in line:
            name, path = line.split(":", 1)
        else:
            continue
        name = name.strip()
        path = path.strip().replace("\\_", "_").replace("\\*", "*")
        expanded = glob.glob(path)
        if expanded:
            expanded = sorted(expanded, key=lambda p: (len(p), p))
            out[name] = Path(expanded[0])
        else:
            out[name] = Path(path)
    return out


def _parse_target_date(value: str) -> pd.Timestamp:
    value = str(value).strip()
    lower = value.lower()
    if lower in {"yesterday", "昨日"}:
        return (pd.Timestamp.today().normalize() - pd.Timedelta(days=1)).normalize()
    if lower in {"today", "今日"}:
        return pd.Timestamp.today().normalize()

    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", value)
    if m:
        y, mo, d = map(int, m.groups())
        return pd.Timestamp(year=y, month=mo, day=d).normalize()

    m = re.fullmatch(r"(\d{4})年(\d{1,2})月(\d{1,2})日", value)
    if m:
        y, mo, d = map(int, m.groups())
        return pd.Timestamp(year=y, month=mo, day=d).normalize()

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"无法解析日期: {value}")
    return pd.Timestamp(parsed).normalize()


def _to_interval_days(series: pd.Series) -> pd.Series:
    if pd.api.types.is_timedelta64_dtype(series):
        return (series.dt.total_seconds() / 86400.0).astype("Float64")
    as_num = pd.to_numeric(series, errors="coerce")
    if as_num.notna().any():
        return as_num.astype("Float64")
    td = pd.to_timedelta(series, errors="coerce")
    if td.notna().any():
        return (td.dt.total_seconds() / 86400.0).astype("Float64")
    return pd.Series(pd.NA, index=series.index, dtype="Float64")


def _build_color_map(names: list[str]) -> dict[str, str]:
    ordered = sorted([str(x) for x in names if str(x).strip()])
    palette = [COLOR_MAIN, COLOR_CONTRAST, COLOR_DARK]
    return {name: palette[i % len(palette)] for i, name in enumerate(ordered)}


def _resolve_launch_dates(business_definition: dict) -> dict[str, pd.Timestamp]:
    out: dict[str, pd.Timestamp] = {}
    periods = (business_definition or {}).get("time_periods") or {}
    if not isinstance(periods, dict):
        return out
    for k, v in periods.items():
        if not isinstance(v, dict):
            continue
        end = v.get("end")
        if not end:
            continue
        dt = pd.to_datetime(end, errors="coerce")
        if pd.isna(dt):
            continue
        out[str(k)] = pd.Timestamp(dt).normalize()
    return out


def _apply_launch_mask(
    pivot_locks: pd.DataFrame,
    pivot_mid: pd.DataFrame,
    all_dates: pd.DatetimeIndex,
    launch_dates: dict[str, pd.Timestamp],
    global_start: pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    pivot_locks = pivot_locks.reindex(all_dates)
    pivot_mid = pivot_mid.reindex(all_dates)

    for col in list(pivot_locks.columns):
        ld = launch_dates.get(str(col))
        line_start = global_start.normalize() if ld is None else max(global_start.normalize(), ld)
        mask_pre = pivot_locks.index < line_start
        pivot_locks.loc[mask_pre, col] = pd.NA
        pivot_locks.loc[~mask_pre, col] = pivot_locks.loc[~mask_pre, col].fillna(0)

    for col in list(pivot_mid.columns):
        ld = launch_dates.get(str(col))
        line_start = global_start.normalize() if ld is None else max(global_start.normalize(), ld)
        pivot_mid.loc[pivot_mid.index < line_start, col] = pd.NA

    return pivot_locks, pivot_mid



def _load_orders(order_path: Path) -> pd.DataFrame:
    cols = [
        "order_number",
        "lock_time",
        "order_type",
        "product_name",
        "first_assign_time",
        "first_assign_lock_time",
    ]
    try:
        df = pd.read_parquet(order_path, columns=cols)
    except Exception:
        df = pd.read_parquet(order_path)

    if "order_number" in df.columns:
        df["order_number"] = df["order_number"].astype("string")
    if "product_name" in df.columns:
        df["product_name"] = df["product_name"].astype("string")
    if "order_type" in df.columns:
        df["order_type"] = df["order_type"].astype("string")
    if "lock_time" in df.columns:
        df["lock_time"] = pd.to_datetime(df["lock_time"], errors="coerce")

    if "first_assign_lock_time" not in df.columns and "first_assign_time" in df.columns:
        df["first_assign_time"] = pd.to_datetime(df["first_assign_time"], errors="coerce")
        delta = df["lock_time"] - df["first_assign_time"]
        days = (delta.dt.total_seconds() / 86400.0).astype("Float64")
        df["first_assign_lock_time"] = days.where(days.ge(0))

    if "first_assign_lock_time" in df.columns:
        df["first_assign_lock_time"] = _to_interval_days(df["first_assign_lock_time"]).where(
            lambda s: s.ge(0)
        )

    return df


def _compute_daily_metrics(
    df: pd.DataFrame,
    business_definition: dict,
    start: pd.Timestamp,
    end: pd.Timestamp,
    target_models: list[str] | None,
) -> pd.DataFrame:
    df = df.copy()
    df = apply_series_group_logic(df, business_definition)

    if "lock_time" not in df.columns:
        raise ValueError("订单数据缺少 lock_time 列")
    if "order_number" not in df.columns:
        raise ValueError("订单数据缺少 order_number 列")
    if "order_type" not in df.columns:
        df["order_type"] = pd.NA

    df = df[df["lock_time"].notna()].copy()
    df = df[df["order_type"] != "试驾车"].copy()
    df = df.sort_values(["order_number", "lock_time"], kind="mergesort")
    df = df.drop_duplicates(subset=["order_number"], keep="first").copy()

    df["lock_date"] = df["lock_time"].dt.floor("D")

    start = start.normalize()
    end = end.normalize()
    if end < start:
        raise ValueError("--end 不能早于 --start")

    df = df[(df["lock_date"] >= start) & (df["lock_date"] <= end)].copy()
    if df.empty:
        return pd.DataFrame(columns=["lock_date", "series_group_logic", "locks", "mid"])

    if target_models is not None:
        allowed = {str(x).strip() for x in target_models if str(x).strip()}
        df = df[df["series_group_logic"].astype("string").isin(sorted(allowed))].copy()

    has_interval = "first_assign_lock_time" in df.columns
    if not has_interval:
        df["first_assign_lock_time"] = pd.NA

    def _median_1dp(s: pd.Series) -> float | None:
        v = _to_interval_days(s).dropna()
        if v.empty:
            return None
        return round(float(v.median()), 1)

    agg = (
        df.groupby(["lock_date", "series_group_logic"], dropna=False)
        .agg(
            locks=("order_number", lambda s: int(pd.Series(s).nunique())),
            mid=("first_assign_lock_time", _median_1dp),
        )
        .reset_index()
    )

    agg["series_group_logic"] = agg["series_group_logic"].astype("string").fillna("其他")
    agg = agg.sort_values(["lock_date", "series_group_logic"], kind="mergesort")
    return agg


def _build_line_figure(
    pivot: pd.DataFrame,
    title: str,
    y_title: str,
    color_map: dict[str, str],
) -> go.Figure:
    fig = go.Figure()
    x = pivot.index
    for name in pivot.columns:
        fig.add_trace(
            go.Scatter(
                x=x,
                y=pivot[name],
                mode="lines+markers",
                name=str(name),
                line=dict(color=color_map.get(str(name))),
            )
        )
    fig.update_layout(
        title=dict(text=title, x=0, xanchor="left"),
        hovermode="x unified",
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.02,
            bgcolor="rgba(255,255,255,0.7)",
            bordercolor=COLOR_GRID,
            borderwidth=1,
        ),
        margin=dict(l=40, r=170, t=60, b=40),
        plot_bgcolor=COLOR_BG,
        paper_bgcolor=COLOR_BG,
    )
    fig.update_xaxes(
        showgrid=True,
        gridcolor=COLOR_GRID,
        gridwidth=1,
        ticks="outside",
        ticklen=4,
        tickcolor=COLOR_GRID,
        tickformat="%Y-%m-%d",
        nticks=12,
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor=COLOR_GRID,
        gridwidth=1,
        ticks="outside",
        ticklen=4,
        tickcolor=COLOR_GRID,
        title=y_title,
        color=COLOR_AXIS,
        nticks=6,
    )
    return fig


def _build_locks_summary_table(pivot_locks: pd.DataFrame) -> pd.DataFrame:
    if pivot_locks is None or pivot_locks.empty:
        return pd.DataFrame(columns=["series_group_logic", "累计锁单数", "日均锁单数", "残差CV(按星期,%)"])

    rows: list[dict[str, object]] = []
    for col in pivot_locks.columns:
        s = pivot_locks[col].dropna()
        if s.empty:
            total = 0.0
            mean = None
            cv = None
        else:
            s_num = pd.to_numeric(s, errors="coerce").dropna()
            if s_num.empty:
                total = 0.0
                mean = None
                cv = None
            else:
                total = float(s_num.sum())
                mean = float(s_num.mean())
                weekday = s_num.index.dayofweek
                weekday_mean = s_num.groupby(weekday).transform("mean")
                resid = s_num - weekday_mean
                resid_std = float(resid.std(ddof=0))
                cv = (None if mean == 0.0 else (resid_std / mean * 100.0))

        rows.append(
            {
                "series_group_logic": str(col),
                "累计锁单数": int(round(total)),
                "日均锁单数": (None if mean is None else round(mean, 2)),
                "残差CV(按星期,%)": (None if cv is None else round(cv, 1)),
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["累计锁单数", "series_group_logic"], ascending=[False, True], kind="mergesort")
        df = df.reset_index(drop=True)
    return df


def _build_mid_summary_table(pivot_mid: pd.DataFrame) -> pd.DataFrame:
    if pivot_mid is None or pivot_mid.empty:
        return pd.DataFrame(columns=["series_group_logic", "线索转化中位数均值(天)", "残差CV(按星期,%)"])

    rows: list[dict[str, object]] = []
    for col in pivot_mid.columns:
        s = pivot_mid[col].dropna()
        if s.empty:
            mean = None
            cv = None
        else:
            s_num = pd.to_numeric(s, errors="coerce").dropna()
            if s_num.empty:
                mean = None
                cv = None
            else:
                mean = float(s_num.mean())
                weekday = s_num.index.dayofweek
                weekday_mean = s_num.groupby(weekday).transform("mean")
                resid = s_num - weekday_mean
                resid_std = float(resid.std(ddof=0))
                cv = (None if mean == 0.0 else (resid_std / mean * 100.0))

        rows.append(
            {
                "series_group_logic": str(col),
                "线索转化中位数均值(天)": (None if mean is None else round(mean, 2)),
                "残差CV(按星期,%)": (None if cv is None else round(cv, 1)),
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values(["series_group_logic"], ascending=[True], kind="mergesort")
        df = df.reset_index(drop=True)
    return df


def _render_html_table(df: pd.DataFrame) -> str:
    if df is None or df.empty:
        return "<div class='table-title'>统计</div><div class='table-empty'>无数据</div>"
    return "\n".join(
        [
            "<div class='table-title'>统计</div>",
            df.to_html(index=False, na_rep="", border=0, classes="summary-table"),
        ]
    )


def _write_html_report(
    out_path: Path,
    fig_locks: go.Figure,
    fig_mid: go.Figure,
    locks_table_html: str,
    mid_table_html: str,
    title: str,
    subtitle: str,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    locks_html = pio.to_html(fig_locks, include_plotlyjs="cdn", full_html=False)
    mid_html = pio.to_html(fig_mid, include_plotlyjs=False, full_html=False)

    html = "\n".join(
        [
            "<!doctype html>",
            "<html>",
            "<head>",
            '<meta charset="utf-8"/>',
            f"<title>{title}</title>",
            '<meta name="viewport" content="width=device-width, initial-scale=1"/>',
            "<style>",
            "body{font-family:-apple-system,BlinkMacSystemFont,Segoe UI,Roboto,Helvetica,Arial,'PingFang SC','Hiragino Sans GB','Microsoft YaHei',sans-serif;margin:24px;background:#fff;color:#111;}",
            "h1{margin:0 0 6px 0;font-size:20px;}",
            "p{margin:0 0 18px 0;color:#555;font-size:13px;}",
            ".card{border:1px solid #eee;border-radius:10px;padding:12px 12px 4px 12px;margin:14px 0;}",
            ".table-title{margin:8px 0 8px 0;font-size:13px;color:#111;font-weight:600;}",
            ".summary-table{width:100%;border-collapse:collapse;margin:0 0 10px 0;font-size:12px;}",
            ".summary-table th,.summary-table td{padding:8px 10px;border-bottom:1px solid #eee;}",
            ".summary-table th{text-align:left;color:#555;font-weight:600;background:#fafafa;}",
            ".summary-table td{text-align:left;color:#111;}",
            ".table-empty{padding:10px 0;color:#777;font-size:12px;}",
            "</style>",
            "</head>",
            "<body>",
            f"<h1>{title}</h1>",
            f"<p>{subtitle}</p>",
            '<div class="card">',
            locks_html,
            locks_table_html,
            "</div>",
            '<div class="card">',
            mid_html,
            mid_table_html,
            "</div>",
            "</body>",
            "</html>",
        ]
    )
    out_path.write_text(html, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default=None)
    parser.add_argument("--end", default=None)
    parser.add_argument("--models", nargs="*", default=None)
    parser.add_argument(
        "--data-path-md",
        default=str(_ROOT / "schema" / "data_path.md"),
    )
    parser.add_argument(
        "--business-definition",
        default=str(_ROOT / "schema" / "business_definition.json"),
    )
    parser.add_argument(
        "--html-out",
        default=str(_ROOT / "scripts" / "reports" / "series_group_logic_trend_by_date.html"),
    )
    parser.add_argument("--csv-out", default=None)
    args = parser.parse_args()

    yesterday = pd.Timestamp.today().normalize() - pd.Timedelta(days=1)
    if args.start is None and args.end is None:
        start = yesterday - pd.Timedelta(days=29)
        end = yesterday
    else:
        end = yesterday if args.end is None else _parse_target_date(args.end)
        start = (end - pd.Timedelta(days=29)) if args.start is None else _parse_target_date(args.start)

    data_paths = _read_data_paths(Path(args.data_path_md))
    if "订单分析" not in data_paths:
        raise KeyError(f"未在 {args.data_path_md} 中找到 '订单分析' 的数据路径")
    order_path = data_paths["订单分析"]

    business_definition = _load_json(Path(args.business_definition))
    launch_dates = _resolve_launch_dates(business_definition)
    models = None if args.models is None else [str(x).strip() for x in args.models if str(x).strip()]
    if models is not None:
        known = set((business_definition.get("series_group_logic") or {}).keys())
        unknown = [m for m in models if m not in known]
        if unknown:
            raise ValueError(
                "未知车型(需为 business_definition.json 的 series_group_logic key): "
                + ", ".join(sorted(unknown))
            )

    orders = _load_orders(order_path)
    metrics = _compute_daily_metrics(
        df=orders,
        business_definition=business_definition,
        start=start,
        end=end,
        target_models=models,
    )

    all_dates = pd.date_range(start.normalize(), end.normalize(), freq="D")
    if metrics.empty:
        pivot_locks = pd.DataFrame(index=all_dates)
        pivot_mid = pd.DataFrame(index=all_dates)
        if models is not None:
            for m in models:
                pivot_locks[str(m)] = pd.NA
                pivot_mid[str(m)] = pd.NA
            pivot_locks, pivot_mid = _apply_launch_mask(
                pivot_locks=pivot_locks,
                pivot_mid=pivot_mid,
                all_dates=all_dates,
                launch_dates=launch_dates,
                global_start=start,
            )
        series_names: list[str] = [str(c) for c in pivot_locks.columns]
    else:
        pivot_locks = metrics.pivot(index="lock_date", columns="series_group_logic", values="locks")
        pivot_mid = metrics.pivot(index="lock_date", columns="series_group_logic", values="mid")
        if models is not None:
            cols = [str(x) for x in models]
            pivot_locks = pivot_locks.reindex(columns=cols)
            pivot_mid = pivot_mid.reindex(columns=cols)
        pivot_locks, pivot_mid = _apply_launch_mask(
            pivot_locks=pivot_locks,
            pivot_mid=pivot_mid,
            all_dates=all_dates,
            launch_dates=launch_dates,
            global_start=start,
        )
        series_names = [str(c) for c in pivot_locks.columns]

    color_map = _build_color_map(series_names)
    fig_locks = _build_line_figure(
        pivot=pivot_locks,
        title="series_group_logic vs 日期：locks",
        y_title="locks",
        color_map=color_map,
    )
    fig_mid = _build_line_figure(
        pivot=pivot_mid,
        title="series_group_logic vs 日期：mid(天)",
        y_title="mid(天)",
        color_map=color_map,
    )

    locks_table = _build_locks_summary_table(pivot_locks)
    locks_table_html = _render_html_table(locks_table)
    mid_table = _build_mid_summary_table(pivot_mid)
    mid_table_html = _render_html_table(mid_table)

    subtitle = f"范围：{start.date()} ~ {end.date()}；models：{('全量' if not models else ' '.join(models))}；数据：{order_path}"
    _write_html_report(
        Path(args.html_out),
        fig_locks,
        fig_mid,
        locks_table_html,
        mid_table_html,
        "series_group_logic 指标趋势",
        subtitle,
    )

    if args.csv_out:
        out_csv = Path(args.csv_out).expanduser().resolve()
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        metrics.to_csv(out_csv, index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()

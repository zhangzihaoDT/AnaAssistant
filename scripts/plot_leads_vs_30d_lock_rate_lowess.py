import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio


MATRIX_DEFAULT = (
    Path(__file__).resolve().parents[1]
    / "schema"
    / "index_summary_daily_matrix_2024-01-01_to_yesterday.csv"
)
BUSINESS_DEFINITION_DEFAULT = (
    Path(__file__).resolve().parents[1] / "schema" / "business_definition.json"
)

LEADS_METRIC = "下发线索转化率.下发线索数"
RATE30_METRIC = "下发线索转化率.下发线索当30日锁单率"
LOCK_ORDERS_METRIC = "订单表.锁单数"


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("%"):
        try:
            return float(s[:-1]) / 100.0
        except Exception:
            return None
    try:
        return float(s)
    except Exception:
        return None


def _load_series(matrix_csv: Path) -> pd.DataFrame:
    raw = pd.read_csv(matrix_csv, encoding="utf-8-sig")
    if "metric" not in raw.columns:
        raise ValueError("矩阵 CSV 缺少 metric 列")
    metric_df = raw.set_index("metric")
    if hasattr(metric_df, "map"):
        metric_df = metric_df.map(_to_float)
    else:
        metric_df = metric_df.applymap(_to_float)
    ts_df = metric_df.T.copy()
    ts_df.index = pd.to_datetime(ts_df.index, errors="coerce")
    ts_df = ts_df[~ts_df.index.isna()].sort_index()
    if LEADS_METRIC not in ts_df.columns:
        raise ValueError(f"缺少指标: {LEADS_METRIC}")
    if RATE30_METRIC not in ts_df.columns:
        raise ValueError(f"缺少指标: {RATE30_METRIC}")
    if LOCK_ORDERS_METRIC not in ts_df.columns:
        raise ValueError(f"缺少指标: {LOCK_ORDERS_METRIC}")
    out = pd.DataFrame(index=ts_df.index)
    out["leads"] = ts_df[LEADS_METRIC].astype(float)
    out["lock_rate_30d"] = ts_df[RATE30_METRIC].astype(float)
    out["lock_orders"] = ts_df[LOCK_ORDERS_METRIC].astype(float)
    out = out.replace([np.inf, -np.inf], np.nan)
    out = out.dropna(subset=["leads", "lock_rate_30d"])
    out = out[(out["leads"] >= 0) & (out["lock_rate_30d"] >= 0)]
    return out


def _load_activity_ranges(business_definition_json: Path) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    raw = json.loads(business_definition_json.read_text(encoding="utf-8"))
    periods = raw.get("time_periods", {})
    out: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    for _, p in periods.items():
        start = pd.to_datetime(p.get("start"), errors="coerce")
        finish = pd.to_datetime(p.get("finish") or p.get("end"), errors="coerce")
        if pd.isna(start) or pd.isna(finish):
            continue
        out.append((pd.Timestamp(start).normalize(), pd.Timestamp(finish).normalize()))
    return out


def _load_activity_windows(
    business_definition_json: Path,
) -> list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
    raw = json.loads(business_definition_json.read_text(encoding="utf-8"))
    periods = raw.get("time_periods", {})
    out: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]] = []
    for model_code, p in periods.items():
        if not isinstance(p, dict):
            continue
        start = pd.to_datetime(p.get("start"), errors="coerce")
        end = pd.to_datetime(p.get("end"), errors="coerce")
        finish = pd.to_datetime(p.get("finish") or p.get("end"), errors="coerce")
        if pd.isna(start) or pd.isna(end) or pd.isna(finish):
            continue
        out.append(
            (
                str(model_code),
                pd.Timestamp(start).normalize(),
                pd.Timestamp(end).normalize(),
                pd.Timestamp(finish).normalize(),
            )
        )
    return out


def _load_model_window(
    business_definition_json: Path,
    model_code: str,
) -> tuple[pd.Timestamp, pd.Timestamp, pd.Timestamp]:
    raw = json.loads(business_definition_json.read_text(encoding="utf-8"))
    periods = raw.get("time_periods", {})
    win = periods.get(model_code)
    if not isinstance(win, dict):
        raise ValueError(f"time_periods 中找不到车型周期: {model_code}")
    start = pd.to_datetime(win.get("start"), errors="coerce")
    end = pd.to_datetime(win.get("end"), errors="coerce")
    finish = pd.to_datetime(win.get("finish") or win.get("end"), errors="coerce")
    if pd.isna(start) or pd.isna(end) or pd.isna(finish):
        raise ValueError(f"{model_code} 周期日期无效: start/end/finish 需可解析")
    return pd.Timestamp(start).normalize(), pd.Timestamp(end).normalize(), pd.Timestamp(finish).normalize()


def _activity_stage_for_date(
    d: pd.Timestamp,
    activity_windows: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]],
) -> str | None:
    dn = pd.Timestamp(d).normalize()
    has_presale = False
    has_launch = False
    for _, s, e, f in activity_windows:
        if not (s <= dn <= f):
            continue
        if dn < e:
            has_presale = True
        else:
            has_launch = True
    if has_presale:
        return "预售期"
    if has_launch:
        return "上市期"
    return None


def _is_activity_day(d: pd.Timestamp, activity_ranges: list[tuple[pd.Timestamp, pd.Timestamp]]) -> bool:
    dn = pd.Timestamp(d).normalize()
    for s, e in activity_ranges:
        if s <= dn <= e:
            return True
    return False


def _tricube(u: np.ndarray) -> np.ndarray:
    a = np.clip(1.0 - np.abs(u) ** 3, 0.0, 1.0)
    return a**3


def _weighted_linear_fit(x: np.ndarray, y: np.ndarray, w: np.ndarray) -> tuple[float, float]:
    w_sum = float(np.sum(w))
    if w_sum <= 0:
        return 0.0, float(np.mean(y))
    x_bar = float(np.sum(w * x) / w_sum)
    y_bar = float(np.sum(w * y) / w_sum)
    x_center = x - x_bar
    sxx = float(np.sum(w * x_center * x_center))
    if sxx <= 0:
        return 0.0, y_bar
    sxy = float(np.sum(w * x_center * (y - y_bar)))
    beta = sxy / sxx
    alpha = y_bar - beta * x_bar
    return beta, alpha


def lowess(x: np.ndarray, y: np.ndarray, frac: float = 0.3, it: int = 2) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    if len(x) != len(y):
        raise ValueError("x/y 长度不一致")
    n = len(x)
    if n == 0:
        return np.array([], dtype=float)

    order = np.argsort(x)
    x_sorted = x[order]
    y_sorted = y[order]

    r = max(int(np.ceil(frac * n)), 2)
    y_hat = np.zeros(n, dtype=float)
    robust = np.ones(n, dtype=float)

    for _ in range(max(int(it), 1)):
        for i in range(n):
            left = max(i - r + 1, 0)
            right = min(i + r, n)
            x_win = x_sorted[left:right]
            y_win = y_sorted[left:right]

            x0 = x_sorted[i]
            dist = np.abs(x_win - x0)
            dmax = float(np.max(dist)) if len(dist) else 0.0
            if dmax <= 0:
                y_hat[i] = float(y_sorted[i])
                continue
            w = _tricube(dist / dmax) * robust[left:right]
            beta, alpha = _weighted_linear_fit(x_win, y_win, w)
            y_hat[i] = alpha + beta * x0

        residual = y_sorted - y_hat
        s = float(np.median(np.abs(residual)))
        if s <= 1e-12:
            break
        u = residual / (6.0 * s)
        robust = (1.0 - np.clip(u, -1.0, 1.0) ** 2) ** 2

    out = np.empty(n, dtype=float)
    out[order] = y_hat
    return out


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--matrix-csv", default=str(MATRIX_DEFAULT))
    parser.add_argument("--business-definition", default=str(BUSINESS_DEFINITION_DEFAULT))
    parser.add_argument("--model-code", default=None)
    parser.add_argument("--model-codes", default="DM0,CM1,CM2,DM1,LS9")
    parser.add_argument("--frac", type=float, default=0.3)
    parser.add_argument("--it", type=int, default=2)
    parser.add_argument("--max-points", type=int, default=0)
    parser.add_argument("--output", default=None)
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    matrix_csv = Path(str(args.matrix_csv)).expanduser().resolve()
    if not matrix_csv.exists():
        raise FileNotFoundError(f"找不到矩阵文件: {matrix_csv}")
    business_definition = Path(str(args.business_definition)).expanduser().resolve()
    if not business_definition.exists():
        raise FileNotFoundError(f"找不到 business_definition.json: {business_definition}")

    df_full = _load_series(matrix_csv)
    activity_windows = _load_activity_windows(business_definition)
    df_full["activity_stage"] = [_activity_stage_for_date(d, activity_windows) for d in df_full.index]
    df_full["is_activity"] = df_full["activity_stage"].notna()
    df_full["regime"] = df_full["is_activity"].map({True: "上市期", False: "非活动期"})

    df = df_full
    if args.max_points and args.max_points > 0 and len(df_full) > int(args.max_points):
        df = df_full.sample(n=int(args.max_points), random_state=20260318).sort_index()

    df_activity = df[df["is_activity"]].copy()
    df_non_activity = df[~df["is_activity"]].copy()
    df_presale = df_activity[df_activity["activity_stage"] == "预售期"].copy()
    df_launch = df_activity[df_activity["activity_stage"] == "上市期"].copy()

    def lowess_line(frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        x = frame["leads"].to_numpy(dtype=float)
        y = frame["lock_rate_30d"].to_numpy(dtype=float)
        y_smooth = lowess(x, y, frac=float(args.frac), it=int(args.it))
        order = np.argsort(x)
        return x[order], y_smooth[order]

    x_presale_line, y_presale_line = (
        lowess_line(df_presale) if len(df_presale) >= 5 else (np.array([]), np.array([]))
    )
    x_launch_line, y_launch_line = lowess_line(df_launch) if len(df_launch) >= 5 else (np.array([]), np.array([]))
    x_non_line, y_non_line = (
        lowess_line(df_non_activity) if len(df_non_activity) >= 5 else (np.array([]), np.array([]))
    )

    fig_scatter = go.Figure()
    fig_scatter.add_trace(
        go.Scatter(
            x=df_presale["leads"],
            y=df_presale["lock_rate_30d"],
            mode="markers",
            name="预售期",
            marker={"size": 6, "opacity": 0.55, "color": "#00CC96"},
            customdata=df_presale.index.astype(str),
            hovertemplate="date=%{customdata}<br>leads=%{x:.0f}<br>rate=%{y:.4f}<extra></extra>",
        )
    )
    if len(x_presale_line) > 0:
        fig_scatter.add_trace(
            go.Scatter(
                x=x_presale_line,
                y=y_presale_line,
                mode="lines",
                name=f"预售期 LOWESS (frac={float(args.frac):.2f})",
                line={"width": 3, "color": "#00CC96"},
                hovertemplate="leads=%{x:.0f}<br>lowess=%{y:.4f}<extra></extra>",
            )
        )
    fig_scatter.add_trace(
        go.Scatter(
            x=df_launch["leads"],
            y=df_launch["lock_rate_30d"],
            mode="markers",
            name="上市期",
            marker={"size": 6, "opacity": 0.55, "color": "#EF553B"},
            customdata=df_launch.index.astype(str),
            hovertemplate="date=%{customdata}<br>leads=%{x:.0f}<br>rate=%{y:.4f}<extra></extra>",
        )
    )
    if len(x_launch_line) > 0:
        fig_scatter.add_trace(
            go.Scatter(
                x=x_launch_line,
                y=y_launch_line,
                mode="lines",
                name=f"上市期 LOWESS (frac={float(args.frac):.2f})",
                line={"width": 3, "color": "#EF553B"},
                hovertemplate="leads=%{x:.0f}<br>lowess=%{y:.4f}<extra></extra>",
            )
        )
    fig_scatter.add_trace(
        go.Scatter(
            x=df_non_activity["leads"],
            y=df_non_activity["lock_rate_30d"],
            mode="markers",
            name="非活动期",
            marker={"size": 6, "opacity": 0.45, "color": "#636EFA"},
            customdata=df_non_activity.index.astype(str),
            hovertemplate="date=%{customdata}<br>leads=%{x:.0f}<br>rate=%{y:.4f}<extra></extra>",
        )
    )
    if len(x_non_line) > 0:
        fig_scatter.add_trace(
            go.Scatter(
                x=x_non_line,
                y=y_non_line,
                mode="lines",
                name=f"非活动期 LOWESS (frac={float(args.frac):.2f})",
                line={"width": 3, "color": "#636EFA"},
                hovertemplate="leads=%{x:.0f}<br>lowess=%{y:.4f}<extra></extra>",
            )
        )
    fig_scatter.update_layout(
        title="下发线索数 vs 30日锁单率（散点 + LOWESS）",
        xaxis_title=LEADS_METRIC,
        yaxis_title=RATE30_METRIC,
        template="plotly_white",
    )

    if args.model_code and str(args.model_code).strip():
        model_codes = [str(args.model_code).strip()]
    else:
        raw_codes = str(args.model_codes or "").strip()
        model_codes = [c.strip() for c in raw_codes.split(",") if c.strip()]
        if not model_codes:
            model_codes = ["DM0", "CM1", "CM2", "DM1", "LS9"]

    window_html_blocks: list[str] = []
    all_activity_dates: set[pd.Timestamp] = set()
    high_eff_dates: set[pd.Timestamp] = set()
    for model_code in model_codes:
        model_start, model_end, model_finish = _load_model_window(business_definition, model_code=model_code)
        model_dates = pd.date_range(model_start, model_finish, freq="D")
        all_activity_dates.update(pd.Timestamp(d).normalize() for d in model_dates)

        start_days = pd.date_range(model_start, model_start + pd.Timedelta(days=2), freq="D")
        end_days = pd.date_range(model_end - pd.Timedelta(days=2), model_end, freq="D")
        finish_days = pd.date_range(model_finish - pd.Timedelta(days=2), model_finish, freq="D")
        start_set = {pd.Timestamp(d).normalize() for d in start_days}
        end_set = {pd.Timestamp(d).normalize() for d in end_days}
        finish_set = {pd.Timestamp(d).normalize() for d in finish_days}
        special = start_set | end_set | finish_set
        weekend_in_cycle = {pd.Timestamp(d).normalize() for d in model_dates if int(pd.Timestamp(d).weekday()) >= 5}
        high_eff_dates.update(special | weekend_in_cycle)

        day_index = (model_dates - model_start).days.astype(int)
        date_str = model_dates.strftime("%Y-%m-%d").tolist()
        weekday_num = model_dates.weekday.to_numpy(dtype=int)
        is_weekend = weekday_num >= 5
        cal_label = ["weekend" if bool(w) else "weekday" for w in is_weekend.tolist()]
        customdata = np.column_stack([date_str, cal_label])

        window_df = df_full.reindex(model_dates)
        rate_vals = window_df["lock_rate_30d"].replace([np.inf, -np.inf], np.nan).to_numpy(dtype=float)
        leads_vals = window_df["leads"].replace([np.inf, -np.inf], np.nan).to_numpy(dtype=float)
        rate_plot = [None if np.isnan(v) else float(v) for v in rate_vals]
        leads_plot = [None if np.isnan(v) else float(v) for v in leads_vals]

        weekday_x: list[int] = []
        weekday_y: list[float] = []
        weekday_cd: list[list[str]] = []
        weekend_x: list[int] = []
        weekend_y: list[float] = []
        weekend_cd: list[list[str]] = []
        for i in range(len(day_index)):
            yv = rate_plot[i]
            if yv is None:
                continue
            xv = int(day_index[i])
            cd = [str(customdata[i, 0]), str(customdata[i, 1])]
            if bool(is_weekend[i]):
                weekend_x.append(xv)
                weekend_y.append(float(yv))
                weekend_cd.append(cd)
            else:
                weekday_x.append(xv)
                weekday_y.append(float(yv))
                weekday_cd.append(cd)

        fig_window = go.Figure()
        fig_window.add_trace(
            go.Bar(
                x=weekday_x,
                y=weekday_y,
                name="30日锁单率（weekday）",
                marker={"color": "#B6EBD3"},
                customdata=weekday_cd,
                hovertemplate="day=%{x}<br>date=%{customdata[0]}<br>%{customdata[1]}<br>rate=%{y:.2%}<extra></extra>",
            )
        )
        fig_window.add_trace(
            go.Bar(
                x=weekend_x,
                y=weekend_y,
                name="30日锁单率（weekend）",
                marker={"color": "#FFA15A"},
                customdata=weekend_cd,
                hovertemplate="day=%{x}<br>date=%{customdata[0]}<br>%{customdata[1]}<br>rate=%{y:.2%}<extra></extra>",
            )
        )
        fig_window.add_trace(
            go.Scatter(
                x=day_index,
                y=leads_plot,
                mode="lines",
                name="下发线索数",
                line={"width": 2, "color": "#2E91E5"},
                customdata=customdata,
                hovertemplate="day=%{x}<br>date=%{customdata[0]}<br>%{customdata[1]}<br>leads=%{y:.0f}<extra></extra>",
                yaxis="y2",
            )
        )
        tick_step = 7
        tick_vals = day_index[::tick_step].tolist()
        tick_text = model_dates.strftime("%m-%d")[::tick_step].tolist()
        fig_window.update_layout(
            title=f"{model_code} 周期（start={model_start.date()}）",
            template="plotly_white",
            xaxis={"title": f"day（0..{int((model_finish - model_start).days)}）"},
            yaxis={"title": "30日锁单率", "tickformat": ",.1%"},
            yaxis2={
                "title": "下发线索数",
                "overlaying": "y",
                "side": "right",
            },
            xaxis2={
                "overlaying": "x",
                "side": "top",
                "tickmode": "array",
                "tickvals": tick_vals,
                "ticktext": tick_text,
                "showgrid": False,
                "title": "date",
            },
        )
        window_html_blocks.append(pio.to_html(fig_window, include_plotlyjs=False, full_html=False))

    low_eff_dates = all_activity_dates - high_eff_dates

    def _quantile_values(frame: pd.DataFrame) -> dict[str, float | None]:
        if frame.empty:
            return {"p10": None, "p50": None, "p90": None}
        s = frame.dropna()
        if s.empty:
            return {"p10": None, "p50": None, "p90": None}
        q = s.quantile([0.1, 0.5, 0.9])
        return {
            "p10": None if pd.isna(q.loc[0.1]) else float(q.loc[0.1]),
            "p50": None if pd.isna(q.loc[0.5]) else float(q.loc[0.5]),
            "p90": None if pd.isna(q.loc[0.9]) else float(q.loc[0.9]),
        }

    high_df = df_full.reindex(sorted(high_eff_dates))[["leads", "lock_rate_30d", "lock_orders"]].copy()
    low_df = df_full.reindex(sorted(low_eff_dates))[["leads", "lock_rate_30d", "lock_orders"]].copy()

    rows: list[str] = []
    metrics: list[str] = []
    p10: list[str] = []
    p50: list[str] = []
    p90: list[str] = []

    for label, sub in [("高效活动区间", high_df), ("低效活动区间", low_df)]:
        leads_q = _quantile_values(sub["leads"])
        rate_q = _quantile_values(sub["lock_rate_30d"])
        lock_q = _quantile_values(sub["lock_orders"])

        rows.append(label)
        metrics.append("下发线索数")
        p10.append("" if leads_q["p10"] is None else f"{leads_q['p10']:.0f}")
        p50.append("" if leads_q["p50"] is None else f"{leads_q['p50']:.0f}")
        p90.append("" if leads_q["p90"] is None else f"{leads_q['p90']:.0f}")

        rows.append(label)
        metrics.append("30日锁单率")
        p10.append("" if rate_q["p10"] is None else f"{rate_q['p10']:.2%}")
        p50.append("" if rate_q["p50"] is None else f"{rate_q['p50']:.2%}")
        p90.append("" if rate_q["p90"] is None else f"{rate_q['p90']:.2%}")

        rows.append(label)
        metrics.append("锁单数")
        p10.append("" if lock_q["p10"] is None else f"{lock_q['p10']:.0f}")
        p50.append("" if lock_q["p50"] is None else f"{lock_q['p50']:.0f}")
        p90.append("" if lock_q["p90"] is None else f"{lock_q['p90']:.0f}")

    fig_table = go.Figure(
        data=[
            go.Table(
                header={"values": ["区间", "指标", "P10", "P50", "P90"], "align": "left"},
                cells={"values": [rows, metrics, p10, p50, p90], "align": "left"},
            )
        ]
    )
    fig_table.update_layout(
        title=f"高效/低效活动区间分位数（基于周期：{', '.join(model_codes)}）",
        template="plotly_white",
    )

    high_df_plot = df_full.reindex(sorted(high_eff_dates))[["leads", "lock_rate_30d"]].copy()
    high_df_plot = high_df_plot.dropna(subset=["leads", "lock_rate_30d"])
    low_df_plot = df_full.reindex(sorted(low_eff_dates))[["leads", "lock_rate_30d"]].copy()
    low_df_plot = low_df_plot.dropna(subset=["leads", "lock_rate_30d"])

    def lowess_line_xy(frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        x = frame["leads"].to_numpy(dtype=float)
        y = frame["lock_rate_30d"].to_numpy(dtype=float)
        y_smooth = lowess(x, y, frac=float(args.frac), it=int(args.it))
        order = np.argsort(x)
        return x[order], y_smooth[order]

    x_high_line, y_high_line = lowess_line_xy(high_df_plot) if len(high_df_plot) >= 5 else (np.array([]), np.array([]))
    x_low_line, y_low_line = lowess_line_xy(low_df_plot) if len(low_df_plot) >= 5 else (np.array([]), np.array([]))

    fig_eff_scatter = go.Figure()
    fig_eff_scatter.add_trace(
        go.Scatter(
            x=high_df_plot["leads"],
            y=high_df_plot["lock_rate_30d"],
            mode="markers",
            name=f"高效活动区间 (n={int(len(high_df_plot))})",
            marker={"size": 6, "opacity": 0.55, "color": "#00CC96"},
            customdata=high_df_plot.index.astype(str),
            hovertemplate="date=%{customdata}<br>leads=%{x:.0f}<br>rate=%{y:.4f}<extra></extra>",
        )
    )
    if len(x_high_line) > 0:
        fig_eff_scatter.add_trace(
            go.Scatter(
                x=x_high_line,
                y=y_high_line,
                mode="lines",
                name=f"高效活动区间 LOWESS (frac={float(args.frac):.2f})",
                line={"width": 3, "color": "#00CC96"},
                hovertemplate="leads=%{x:.0f}<br>lowess=%{y:.4f}<extra></extra>",
            )
        )
    fig_eff_scatter.add_trace(
        go.Scatter(
            x=low_df_plot["leads"],
            y=low_df_plot["lock_rate_30d"],
            mode="markers",
            name=f"低效活动区间 (n={int(len(low_df_plot))})",
            marker={"size": 6, "opacity": 0.45, "color": "#AB63FA"},
            customdata=low_df_plot.index.astype(str),
            hovertemplate="date=%{customdata}<br>leads=%{x:.0f}<br>rate=%{y:.4f}<extra></extra>",
        )
    )
    if len(x_low_line) > 0:
        fig_eff_scatter.add_trace(
            go.Scatter(
                x=x_low_line,
                y=y_low_line,
                mode="lines",
                name=f"低效活动区间 LOWESS (frac={float(args.frac):.2f})",
                line={"width": 3, "color": "#AB63FA"},
                hovertemplate="leads=%{x:.0f}<br>lowess=%{y:.4f}<extra></extra>",
            )
        )
    fig_eff_scatter.update_layout(
        title=f"高效 vs 低效活动区间（散点 + LOWESS，基于周期：{', '.join(model_codes)}）",
        xaxis_title=LEADS_METRIC,
        yaxis_title=RATE30_METRIC,
        template="plotly_white",
    )

    high_lock_df_plot = df_full.reindex(sorted(high_eff_dates))[["lock_orders", "lock_rate_30d"]].copy()
    high_lock_df_plot = high_lock_df_plot.dropna(subset=["lock_orders", "lock_rate_30d"])
    low_lock_df_plot = df_full.reindex(sorted(low_eff_dates))[["lock_orders", "lock_rate_30d"]].copy()
    low_lock_df_plot = low_lock_df_plot.dropna(subset=["lock_orders", "lock_rate_30d"])

    def lowess_line_xy_lock_orders(frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        x = frame["lock_orders"].to_numpy(dtype=float)
        y = frame["lock_rate_30d"].to_numpy(dtype=float)
        y_smooth = lowess(x, y, frac=float(args.frac), it=int(args.it))
        order = np.argsort(x)
        return x[order], y_smooth[order]

    x_high_lock_line, y_high_lock_line = (
        lowess_line_xy_lock_orders(high_lock_df_plot) if len(high_lock_df_plot) >= 5 else (np.array([]), np.array([]))
    )
    x_low_lock_line, y_low_lock_line = (
        lowess_line_xy_lock_orders(low_lock_df_plot) if len(low_lock_df_plot) >= 5 else (np.array([]), np.array([]))
    )

    fig_eff_lock_scatter = go.Figure()
    fig_eff_lock_scatter.add_trace(
        go.Scatter(
            x=high_lock_df_plot["lock_orders"],
            y=high_lock_df_plot["lock_rate_30d"],
            mode="markers",
            name=f"高效活动区间 (n={int(len(high_lock_df_plot))})",
            marker={"size": 6, "opacity": 0.55, "color": "#00CC96"},
            customdata=high_lock_df_plot.index.astype(str),
            hovertemplate="date=%{customdata}<br>lock_orders=%{x:.0f}<br>rate=%{y:.4f}<extra></extra>",
        )
    )
    if len(x_high_lock_line) > 0:
        fig_eff_lock_scatter.add_trace(
            go.Scatter(
                x=x_high_lock_line,
                y=y_high_lock_line,
                mode="lines",
                name=f"高效活动区间 LOWESS (frac={float(args.frac):.2f})",
                line={"width": 3, "color": "#00CC96"},
                hovertemplate="lock_orders=%{x:.0f}<br>lowess=%{y:.4f}<extra></extra>",
            )
        )
    fig_eff_lock_scatter.add_trace(
        go.Scatter(
            x=low_lock_df_plot["lock_orders"],
            y=low_lock_df_plot["lock_rate_30d"],
            mode="markers",
            name=f"低效活动区间 (n={int(len(low_lock_df_plot))})",
            marker={"size": 6, "opacity": 0.45, "color": "#AB63FA"},
            customdata=low_lock_df_plot.index.astype(str),
            hovertemplate="date=%{customdata}<br>lock_orders=%{x:.0f}<br>rate=%{y:.4f}<extra></extra>",
        )
    )
    if len(x_low_lock_line) > 0:
        fig_eff_lock_scatter.add_trace(
            go.Scatter(
                x=x_low_lock_line,
                y=y_low_lock_line,
                mode="lines",
                name=f"低效活动区间 LOWESS (frac={float(args.frac):.2f})",
                line={"width": 3, "color": "#AB63FA"},
                hovertemplate="lock_orders=%{x:.0f}<br>lowess=%{y:.4f}<extra></extra>",
            )
        )
    fig_eff_lock_scatter.update_layout(
        title=f"高效 vs 低效活动区间（锁单数 vs 30日锁单率，散点 + LOWESS，基于周期：{', '.join(model_codes)}）",
        xaxis_title=LOCK_ORDERS_METRIC,
        yaxis_title=RATE30_METRIC,
        template="plotly_white",
    )

    if args.output:
        output_path = Path(str(args.output)).expanduser().resolve()
    else:
        output_path = (
            Path(__file__).resolve().parents[1]
            / "out"
            / "leads_vs_30d_lock_rate_lowess.html"
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    html_scatter = pio.to_html(fig_scatter, include_plotlyjs="cdn", full_html=False)
    html_windows = "<div style='height:24px'></div>".join(window_html_blocks)
    html_table = pio.to_html(fig_table, include_plotlyjs=False, full_html=False)
    html_eff_scatter = pio.to_html(fig_eff_scatter, include_plotlyjs=False, full_html=False)
    html_eff_lock_scatter = pio.to_html(fig_eff_lock_scatter, include_plotlyjs=False, full_html=False)
    html = (
        "<html><head><meta charset='utf-8'></head><body>"
        f"{html_scatter}<div style='height:24px'></div>{html_windows}<div style='height:24px'></div>{html_table}<div style='height:24px'></div>{html_eff_scatter}<div style='height:24px'></div>{html_eff_lock_scatter}"
        "</body></html>"
    )
    output_path.write_text(html, encoding="utf-8")
    print(str(output_path))


if __name__ == "__main__":
    main()

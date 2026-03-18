import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go


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
    out = pd.DataFrame(index=ts_df.index)
    out["leads"] = ts_df[LEADS_METRIC].astype(float)
    out["lock_rate_30d"] = ts_df[RATE30_METRIC].astype(float)
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

    df = _load_series(matrix_csv)
    activity_ranges = _load_activity_ranges(business_definition)
    df["is_activity"] = [bool(_is_activity_day(d, activity_ranges)) for d in df.index]
    df["regime"] = df["is_activity"].map({True: "活动期", False: "非活动期"})

    if args.max_points and args.max_points > 0 and len(df) > int(args.max_points):
        df = df.sample(n=int(args.max_points), random_state=20260318).sort_index()

    df_activity = df[df["is_activity"]].copy()
    df_non_activity = df[~df["is_activity"]].copy()

    def lowess_line(frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        x = frame["leads"].to_numpy(dtype=float)
        y = frame["lock_rate_30d"].to_numpy(dtype=float)
        y_smooth = lowess(x, y, frac=float(args.frac), it=int(args.it))
        order = np.argsort(x)
        return x[order], y_smooth[order]

    x_act_line, y_act_line = lowess_line(df_activity) if len(df_activity) >= 5 else (np.array([]), np.array([]))
    x_non_line, y_non_line = (
        lowess_line(df_non_activity) if len(df_non_activity) >= 5 else (np.array([]), np.array([]))
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df_activity["leads"],
            y=df_activity["lock_rate_30d"],
            mode="markers",
            name="活动期",
            marker={"size": 6, "opacity": 0.55, "color": "#EF553B"},
            customdata=df_activity.index.astype(str),
            hovertemplate="date=%{customdata}<br>leads=%{x:.0f}<br>rate=%{y:.4f}<extra></extra>",
        )
    )
    if len(x_act_line) > 0:
        fig.add_trace(
            go.Scatter(
                x=x_act_line,
                y=y_act_line,
                mode="lines",
                name=f"活动期 LOWESS (frac={float(args.frac):.2f})",
                line={"width": 3, "color": "#EF553B"},
                hovertemplate="leads=%{x:.0f}<br>lowess=%{y:.4f}<extra></extra>",
            )
        )
    fig.add_trace(
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
        fig.add_trace(
            go.Scatter(
                x=x_non_line,
                y=y_non_line,
                mode="lines",
                name=f"非活动期 LOWESS (frac={float(args.frac):.2f})",
                line={"width": 3, "color": "#636EFA"},
                hovertemplate="leads=%{x:.0f}<br>lowess=%{y:.4f}<extra></extra>",
            )
        )
    fig.update_layout(
        title="下发线索数 vs 30日锁单率（散点 + LOWESS）",
        xaxis_title=LEADS_METRIC,
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
    fig.write_html(str(output_path), include_plotlyjs="cdn")
    print(str(output_path))


if __name__ == "__main__":
    main()

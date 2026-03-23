import argparse
from datetime import datetime
from pathlib import Path
import json
import re

import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
import statsmodels.api as sm


DATA_FILE = Path("/Users/zihao_/Documents/github/26W06_Tool_calls/schema/index_summary_daily_matrix_2024-01-01_to_yesterday.csv")
BUSINESS_DEFINITION_FILE = Path("/Users/zihao_/Documents/github/26W06_Tool_calls/schema/business_definition.json")
SCRIPT_DIR = Path(__file__).parent
DEFAULT_OUTPUT = SCRIPT_DIR / "reports/analyze_xx.html"
TARGET_LEADS_METRIC = "下发线索转化率.下发线索数"
TARGET_STORE_DAILY_LEADS_METRIC = "订单表.店日均下发线索数"
TARGET_30D_CONV_METRIC = "下发线索转化率.下发线索当30日锁单率"
TARGET_LOCK_ORDERS_METRIC = "订单表.锁单数"
STORE_SHARE_CANDIDATES = [
    "下发线索转化率.门店线索占比",
]
LEADS_STORE_CANDIDATES = [
    "下发线索转化率.下发线索数 (门店)",
]
LEADS_LIVE_CANDIDATES = [
    "下发线索转化率.下发线索数（直播）",
    "下发线索转化率.下发线索数（直播)",
]
LEADS_PLATFORM_CANDIDATES = [
    "下发线索转化率.下发线索数（平台)",
    "下发线索转化率.下发线索数（平台）",
]
STORE_SAME_DAY_RATE_CANDIDATES = [
    "下发线索转化率.下发 (门店)线索当日锁单率",
]
STORE_30D_RATE_CANDIDATES = [
    "下发线索转化率.下发线索数（门店)30日锁单率",
    "下发线索转化率.下发线索数（门店）30日锁单率",
]
START_DATE = pd.Timestamp("2025-01-01")

COLORS = {
    "primary": "#3498DB",
    "secondary": "#E67E22",
    "grid": "#ebedf0",
    "axis": "#7B848F",
    "bg": "#FFFFFF",
}
ACTIVITY_SERIES = {"CM2", "DM1", "LS9", "LS8"}
SERIES_PALETTE = ["#8E44AD", "#16A085", "#E74C3C", "#2E86DE", "#F39C12", "#7DCEA0", "#5D6D7E", "#AF7AC5"]


def _build_series_color_map(series_names: list[str]) -> dict[str, str]:
    ordered = sorted([s for s in series_names if s])
    return {name: SERIES_PALETTE[i % len(SERIES_PALETTE)] for i, name in enumerate(ordered)}


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)
    s = str(value).strip().replace(",", "").replace("，", "")
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _to_rate(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        v = float(value)
        return v / 100.0 if v > 1.0 else v
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    if s.endswith("%"):
        try:
            return float(s[:-1].replace(",", "").replace("，", "")) / 100.0
        except Exception:
            return None
    raw = _to_float(s)
    if raw is None:
        return None
    return raw / 100.0 if raw > 1.0 else raw


def load_metric_frame(csv_path: Path) -> pd.DataFrame:
    raw = pd.read_csv(csv_path, encoding="utf-8-sig")
    if "metric" not in raw.columns:
        raise ValueError("矩阵 CSV 缺少 metric 列")
    return raw.set_index("metric")


def load_metric_series(metric_df: pd.DataFrame, metric_name: str, value_parser, fillna_value: float) -> pd.DataFrame:
    if metric_name not in metric_df.index:
        raise ValueError(f"矩阵缺少指标: {metric_name}")
    row = metric_df.loc[metric_name]
    out = pd.DataFrame({"date": row.index.astype(str), "value": row.values})
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.normalize()
    out["value"] = out["value"].map(value_parser).fillna(fillna_value).astype(float)
    out = out.dropna(subset=["date"]).sort_values("date")
    out = out[out["date"] >= START_DATE].copy()
    return out


def _pick_existing_metric(metric_df: pd.DataFrame, candidates: list[str]) -> str:
    for name in candidates:
        if name in metric_df.index:
            return name
    raise ValueError(f"矩阵缺少指标，候选: {candidates}")


def load_leads_series(metric_df: pd.DataFrame) -> pd.DataFrame:
    return load_metric_series(
        metric_df=metric_df,
        metric_name=TARGET_LEADS_METRIC,
        value_parser=_to_float,
        fillna_value=0.0,
    )


def load_30d_conversion_series(metric_df: pd.DataFrame) -> pd.DataFrame:
    return load_metric_series(
        metric_df=metric_df,
        metric_name=TARGET_30D_CONV_METRIC,
        value_parser=_to_rate,
        fillna_value=0.0,
    )


def load_store_daily_leads_series(metric_df: pd.DataFrame) -> pd.DataFrame:
    return load_metric_series(
        metric_df=metric_df,
        metric_name=TARGET_STORE_DAILY_LEADS_METRIC,
        value_parser=_to_float,
        fillna_value=0.0,
    )


def load_store_rate_correlation_frame(metric_df: pd.DataFrame) -> pd.DataFrame:
    same_day_metric = _pick_existing_metric(metric_df, STORE_SAME_DAY_RATE_CANDIDATES)
    day30_metric = _pick_existing_metric(metric_df, STORE_30D_RATE_CANDIDATES)
    same_day = load_metric_series(metric_df, same_day_metric, _to_rate, 0.0).rename(columns={"value": "same_day_rate"})
    day30 = load_metric_series(metric_df, day30_metric, _to_rate, 0.0).rename(columns={"value": "day30_rate"})
    merged = same_day.merge(day30, on="date", how="inner")
    merged = merged[(merged["date"] >= START_DATE)].copy()
    merged = merged.dropna(subset=["same_day_rate", "day30_rate"])
    return merged


def load_store_daily_vs_30d_corr_frame(metric_df: pd.DataFrame) -> pd.DataFrame:
    store_daily = load_store_daily_leads_series(metric_df).rename(columns={"value": "store_daily_leads"})
    lock_orders = load_metric_series(metric_df, TARGET_LOCK_ORDERS_METRIC, _to_float, 0.0).rename(columns={"value": "lock_orders"})
    merged = store_daily.merge(lock_orders, on="date", how="inner")
    merged = merged[(merged["date"] >= START_DATE)].copy()
    merged = merged.dropna(subset=["store_daily_leads", "lock_orders"])
    return merged


def load_multivariate_backtest_frame(metric_df: pd.DataFrame) -> pd.DataFrame:
    target_30d = load_metric_series(metric_df, TARGET_30D_CONV_METRIC, _to_rate, 0.0).rename(columns={"value": "target_30d_rate"})
    lock_orders = load_metric_series(metric_df, TARGET_LOCK_ORDERS_METRIC, _to_float, 0.0).rename(columns={"value": "lock_orders"})
    same_day_metric = _pick_existing_metric(metric_df, STORE_SAME_DAY_RATE_CANDIDATES)
    same_day = load_metric_series(metric_df, same_day_metric, _to_rate, 0.0).rename(columns={"value": "same_day_rate"})
    leads_total = load_metric_series(metric_df, TARGET_LEADS_METRIC, _to_float, 0.0).rename(columns={"value": "leads_total"})
    live_metric = _pick_existing_metric(metric_df, LEADS_LIVE_CANDIDATES)
    platform_metric = _pick_existing_metric(metric_df, LEADS_PLATFORM_CANDIDATES)
    leads_live = load_metric_series(metric_df, live_metric, _to_float, 0.0).rename(columns={"value": "leads_live"})
    leads_platform = load_metric_series(metric_df, platform_metric, _to_float, 0.0).rename(columns={"value": "leads_platform"})
    if any(name in metric_df.index for name in STORE_SHARE_CANDIDATES):
        store_share_metric = _pick_existing_metric(metric_df, STORE_SHARE_CANDIDATES)
        store_share = load_metric_series(metric_df, store_share_metric, _to_rate, 0.0).rename(columns={"value": "store_share"})
    else:
        leads_store_metric = _pick_existing_metric(metric_df, LEADS_STORE_CANDIDATES)
        leads_store = load_metric_series(metric_df, leads_store_metric, _to_float, 0.0).rename(columns={"value": "leads_store"})
        store_share = leads_store.merge(leads_total, on="date", how="inner")
        store_share["store_share"] = store_share.apply(
            lambda r: 0.0 if float(r["leads_total"]) <= 0 else float(r["leads_store"]) / float(r["leads_total"]),
            axis=1,
        )
        store_share = store_share[["date", "store_share"]]
    merged = target_30d.merge(same_day, on="date", how="inner")
    merged = merged.merge(store_share, on="date", how="inner")
    merged = merged.merge(leads_total, on="date", how="inner")
    merged = merged.merge(lock_orders, on="date", how="inner")
    merged = merged.merge(leads_live, on="date", how="inner")
    merged = merged.merge(leads_platform, on="date", how="inner")
    merged["live_share"] = merged.apply(
        lambda r: 0.0 if float(r["leads_total"]) <= 0 else float(r["leads_live"]) / float(r["leads_total"]),
        axis=1,
    )
    merged["platform_share"] = merged.apply(
        lambda r: 0.0 if float(r["leads_total"]) <= 0 else float(r["leads_platform"]) / float(r["leads_total"]),
        axis=1,
    )
    merged = merged.dropna(
        subset=[
            "target_30d_rate",
            "lock_orders",
            "same_day_rate",
            "store_share",
            "live_share",
            "platform_share",
        ]
    )
    merged = merged[merged["date"] >= START_DATE].copy()
    return merged[
        [
            "date",
            "target_30d_rate",
            "lock_orders",
            "same_day_rate",
            "store_share",
            "live_share",
            "platform_share",
        ]
    ]


def load_lock_orders_layered_frame(metric_df: pd.DataFrame) -> pd.DataFrame:
    lock_orders = load_metric_series(metric_df, TARGET_LOCK_ORDERS_METRIC, _to_float, 0.0).rename(columns={"value": "lock_orders"})
    target_30d = load_metric_series(metric_df, TARGET_30D_CONV_METRIC, _to_rate, 0.0).rename(columns={"value": "target_30d_rate"})
    store_daily_leads = load_metric_series(metric_df, TARGET_STORE_DAILY_LEADS_METRIC, _to_float, 0.0).rename(
        columns={"value": "store_daily_leads"}
    )
    merged = lock_orders.merge(target_30d, on="date", how="inner")
    merged = merged.merge(store_daily_leads, on="date", how="inner")
    merged = merged.dropna(subset=["lock_orders", "target_30d_rate", "store_daily_leads"])
    merged = merged[merged["date"] >= START_DATE].copy()
    return merged[["date", "lock_orders", "target_30d_rate", "store_daily_leads"]]


def load_all_series(
    csv_path: Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    metric_df = load_metric_frame(csv_path)
    leads = load_leads_series(metric_df)
    store_daily_leads = load_store_daily_leads_series(metric_df)
    conv30 = load_30d_conversion_series(metric_df)
    store_daily_vs_30d_corr = load_store_daily_vs_30d_corr_frame(metric_df)
    store_corr = load_store_rate_correlation_frame(metric_df)
    multivar = load_multivariate_backtest_frame(metric_df)
    lock_orders_layered = load_lock_orders_layered_frame(metric_df)
    return leads, store_daily_leads, conv30, store_daily_vs_30d_corr, store_corr, multivar, lock_orders_layered


def load_business_time_points(json_path: Path) -> pd.DataFrame:
    if not json_path.exists():
        return pd.DataFrame(columns=["series", "phase", "date"])
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    periods = payload.get("time_periods") or {}
    rows = []
    for series, cfg in periods.items():
        if not isinstance(cfg, dict):
            continue
        if series not in ACTIVITY_SERIES:
            continue
        start = pd.to_datetime(cfg.get("start"), errors="coerce")
        finish = pd.to_datetime(cfg.get("finish"), errors="coerce")
        if pd.notna(start) and pd.notna(finish):
            s = min(start, finish).normalize()
            e = max(start, finish).normalize()
            for d in pd.date_range(s, e, freq="D"):
                rows.append({"series": series, "phase": "window", "date": d.normalize()})
    if not rows:
        return pd.DataFrame(columns=["series", "phase", "date"])
    return pd.DataFrame(rows)


def common_layout(title: str, xaxis_title: str, yaxis_title: str) -> dict:
    return {
        "title": title,
        "template": "plotly_white",
        "plot_bgcolor": COLORS["bg"],
        "hovermode": "x unified",
        "xaxis": {
            "title": xaxis_title,
            "gridcolor": COLORS["grid"],
            "zerolinecolor": COLORS["grid"],
            "tickfont": {"color": COLORS["axis"]},
            "title_font": {"color": COLORS["axis"]},
            "showgrid": True,
        },
        "yaxis": {
            "title": yaxis_title,
            "gridcolor": COLORS["grid"],
            "zerolinecolor": COLORS["grid"],
            "tickfont": {"color": COLORS["axis"]},
            "title_font": {"color": COLORS["axis"]},
            "showgrid": True,
        },
        "legend": {
            "bordercolor": COLORS["axis"],
            "font": {"color": COLORS["axis"]},
        },
    }


def module_leads_trend(data: pd.DataFrame) -> str:
    x = data["date"].map(pd.Timestamp.toordinal).astype(float).to_numpy()
    y = data["value"].astype(float).to_numpy()
    mean_value = float(data["value"].mean())
    lowess_fit = sm.nonparametric.lowess(y, x, frac=0.08, return_sorted=True)
    lowess_x = [datetime.fromordinal(int(v)).date() for v in lowess_fit[:, 0]]
    lowess_y = lowess_fit[:, 1]
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=data["date"],
            y=data["value"],
            mode="markers",
            name="下发线索数（散点）",
            marker={"color": "rgba(52, 152, 219, 0.35)", "size": 6},
            hovertemplate="%{x|%Y-%m-%d}<br>下发线索数: %{y:,.0f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=lowess_x,
            y=lowess_y,
            mode="lines",
            name="LOWESS",
            line={"color": COLORS["secondary"], "width": 3},
            hovertemplate="%{x|%Y-%m-%d}<br>LOWESS: %{y:,.1f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=[data["date"].min(), data["date"].max()],
            y=[mean_value, mean_value],
            mode="lines",
            name="平均线",
            line={"color": COLORS["axis"], "width": 2, "dash": "dash"},
            hovertemplate="平均线: %{y:,.1f}<extra></extra>",
        )
    )
    fig.update_layout(
        common_layout(
            title="下发线索数趋势（2025-01-01 ~ Max Date）",
            xaxis_title="日期",
            yaxis_title="下发线索数",
        )
    )
    return pio.to_html(fig, full_html=False, include_plotlyjs="cdn")


def module_30d_conversion_trend(data: pd.DataFrame) -> str:
    x = data["date"].map(pd.Timestamp.toordinal).astype(float).to_numpy()
    y = data["value"].astype(float).to_numpy()
    mean_value = float(data["value"].mean())
    lowess_fit = sm.nonparametric.lowess(y, x, frac=0.08, return_sorted=True)
    lowess_x = [datetime.fromordinal(int(v)).date() for v in lowess_fit[:, 0]]
    lowess_y = lowess_fit[:, 1]
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=data["date"],
            y=data["value"],
            mode="markers",
            name="30日转化率（散点）",
            marker={"color": "rgba(52, 152, 219, 0.35)", "size": 6},
            hovertemplate="%{x|%Y-%m-%d}<br>30日转化率: %{y:.2%}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=lowess_x,
            y=lowess_y,
            mode="lines",
            name="LOWESS",
            line={"color": COLORS["secondary"], "width": 3},
            hovertemplate="%{x|%Y-%m-%d}<br>LOWESS: %{y:.2%}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=[data["date"].min(), data["date"].max()],
            y=[mean_value, mean_value],
            mode="lines",
            name="平均线",
            line={"color": COLORS["axis"], "width": 2, "dash": "dash"},
            hovertemplate="平均线: %{y:.2%}<extra></extra>",
        )
    )
    fig.update_layout(
        common_layout(
            title="下发线索30日转化率趋势（2025-01-01 ~ Max Date）",
            xaxis_title="日期",
            yaxis_title="30日转化率",
        )
    )
    fig.update_yaxes(tickformat=".1%")
    return pio.to_html(fig, full_html=False, include_plotlyjs="cdn")


def module_store_daily_leads_trend(data: pd.DataFrame) -> str:
    x = data["date"].map(pd.Timestamp.toordinal).astype(float).to_numpy()
    y = data["value"].astype(float).to_numpy()
    mean_value = float(data["value"].mean())
    lowess_fit = sm.nonparametric.lowess(y, x, frac=0.08, return_sorted=True)
    lowess_x = [datetime.fromordinal(int(v)).date() for v in lowess_fit[:, 0]]
    lowess_y = lowess_fit[:, 1]
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=data["date"],
            y=data["value"],
            mode="markers",
            name="店日均下发线索数（散点）",
            marker={"color": "rgba(52, 152, 219, 0.35)", "size": 6},
            hovertemplate="%{x|%Y-%m-%d}<br>店日均下发线索数: %{y:,.2f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=lowess_x,
            y=lowess_y,
            mode="lines",
            name="LOWESS",
            line={"color": COLORS["secondary"], "width": 3},
            hovertemplate="%{x|%Y-%m-%d}<br>LOWESS: %{y:,.2f}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=[data["date"].min(), data["date"].max()],
            y=[mean_value, mean_value],
            mode="lines",
            name="平均线",
            line={"color": COLORS["axis"], "width": 2, "dash": "dash"},
            hovertemplate="平均线: %{y:,.2f}<extra></extra>",
        )
    )
    fig.update_layout(
        common_layout(
            title="订单表.店日均下发线索数趋势（2025-01-01 ~ Max Date）",
            xaxis_title="日期",
            yaxis_title="店日均下发线索数",
        )
    )
    return pio.to_html(fig, full_html=False, include_plotlyjs="cdn")


def module_store_rate_correlation(data: pd.DataFrame) -> str:
    x = data["same_day_rate"].astype(float).to_numpy()
    y = data["day30_rate"].astype(float).to_numpy()
    corr = float(pd.Series(x).corr(pd.Series(y), method="pearson"))
    lowess_fit = sm.nonparametric.lowess(y, x, frac=0.2, return_sorted=True)
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=data["same_day_rate"],
            y=data["day30_rate"],
            mode="markers",
            name="日样本",
            marker={"color": "rgba(52, 152, 219, 0.35)", "size": 7},
            hovertemplate="门店当日锁单率: %{x:.2%}<br>门店30日锁单率: %{y:.2%}<br>日期: %{customdata|%Y-%m-%d}<extra></extra>",
            customdata=data["date"],
        )
    )
    fig.add_trace(
        go.Scatter(
            x=lowess_fit[:, 0],
            y=lowess_fit[:, 1],
            mode="lines",
            name="LOWESS",
            line={"color": COLORS["secondary"], "width": 3},
            hovertemplate="LOWESS: %{y:.2%}<extra></extra>",
        )
    )
    fig.update_layout(
        common_layout(
            title=f"门店当日锁单率 vs 门店30日锁单率（Pearson r={corr:.3f}）",
            xaxis_title="门店当日锁单率",
            yaxis_title="门店30日锁单率",
        )
    )
    fig.update_xaxes(tickformat=".1%")
    fig.update_yaxes(tickformat=".1%")
    return pio.to_html(fig, full_html=False, include_plotlyjs="cdn")


def module_store_daily_vs_30d_conversion_correlation(data: pd.DataFrame) -> str:
    x = data["store_daily_leads"].astype(float).to_numpy()
    y = data["lock_orders"].astype(float).to_numpy()
    corr = float(pd.Series(x).corr(pd.Series(y), method="pearson"))
    lowess_fit = sm.nonparametric.lowess(y, x, frac=0.2, return_sorted=True)
    fig = go.Figure()
    points = load_business_time_points(BUSINESS_DEFINITION_FILE)[["date", "series"]].drop_duplicates()
    dots = data.merge(points, on="date", how="left")
    dots["series"] = dots["series"].fillna("非活动窗口")
    non_activity = dots[dots["series"] == "非活动窗口"].copy()
    non_activity_corr = None
    if len(non_activity) >= 2:
        candidate = pd.Series(non_activity["store_daily_leads"].astype(float)).corr(
            pd.Series(non_activity["lock_orders"].astype(float)), method="pearson"
        )
        if pd.notna(candidate):
            non_activity_corr = float(candidate)
    series_color_map = _build_series_color_map(dots[dots["series"] != "非活动窗口"]["series"].astype(str).unique().tolist())
    series_color_map["非活动窗口"] = "rgba(52, 152, 219, 0.35)"
    for series_name in sorted(dots["series"].astype(str).unique().tolist()):
        sub = dots[dots["series"].astype(str) == series_name].copy()
        if sub.empty:
            continue
        fig.add_trace(
            go.Scatter(
                x=sub["store_daily_leads"],
                y=sub["lock_orders"],
                mode="markers",
                name=f"日样本({series_name})",
                marker={"color": series_color_map.get(series_name, "rgba(52, 152, 219, 0.35)"), "size": 7},
                customdata=sub[["date", "series"]],
                hovertemplate="系列: %{customdata[1]}<br>日期: %{customdata[0]|%Y-%m-%d}<br>店日均下发线索数: %{x:,.2f}<br>锁单数: %{y:,.0f}<extra></extra>",
            )
        )
    fig.add_trace(
        go.Scatter(
            x=lowess_fit[:, 0],
            y=lowess_fit[:, 1],
            mode="lines",
            name="LOWESS",
            line={"color": COLORS["secondary"], "width": 3},
            hovertemplate="LOWESS: %{y:,.1f}<extra></extra>",
        )
    )
    fig.update_layout(
        common_layout(
            title=(
                f"店日均下发线索数 vs 锁单数（Pearson r={corr:.3f}；"
                f"非活动窗口 r={('N/A' if non_activity_corr is None else f'{non_activity_corr:.3f}')}）"
            ),
            xaxis_title="店日均下发线索数",
            yaxis_title="锁单数",
        )
    )
    return pio.to_html(fig, full_html=False, include_plotlyjs="cdn")


def _state_from_percentile(p: float) -> str:
    if p >= 0.7:
        return "High"
    if p <= 0.3:
        return "Low"
    return "Mid"


def module_multivariate_backtest(data: pd.DataFrame) -> str:
    features = ["same_day_rate", "store_share", "live_share", "platform_share"]
    feature_name = {
        "same_day_rate": "下发(门店)线索当日锁单率",
        "store_share": "下发线索数(门店)占比",
        "live_share": "下发线索数(直播)占比",
        "platform_share": "下发线索数(平台)占比",
    }
    df = data.copy()
    X = sm.add_constant(df[features], has_constant="add")
    model = sm.OLS(df["target_30d_rate"], X).fit()
    df["target_pct"] = df["target_30d_rate"].rank(method="average", pct=True)
    bins = [i / 10.0 for i in range(11)]
    labels = [f"p{i * 10}~p{(i + 1) * 10}" for i in range(10)]
    df["band"] = pd.cut(df["target_pct"], bins=bins, labels=labels, include_lowest=True)
    rows = []
    for band in labels:
        sub = df[df["band"] == band].copy()
        if sub.empty:
            continue
        medians = {f: float(sub[f].median()) for f in features}
        feature_states = {}
        for f in features:
            p = float((df[f] < medians[f]).mean())
            feature_states[f] = _state_from_percentile(p)
        pred = float(model.predict([1.0, medians["same_day_rate"], medians["store_share"], medians["live_share"], medians["platform_share"]])[0])
        rows.append(
            {
                "分层(按30日锁单率分位)": band,
                "样本量": int(len(sub)),
                "30日锁单率中位数(分层内)": float(sub["target_30d_rate"].median()),
                "锁单数总量(分层内)": float(sub["lock_orders"].sum()),
                "线性回归预测值(全样本OLS,分层中位输入)": pred,
                "门店当日锁单率状态(全样本分位,分层中位值)": f"{feature_states['same_day_rate']} ({medians['same_day_rate']:.2%})",
                "门店占比状态(全样本分位,分层中位值)": f"{feature_states['store_share']} ({medians['store_share']:.2%})",
                "直播占比状态(全样本分位,分层中位值)": f"{feature_states['live_share']} ({medians['live_share']:.2%})",
                "平台占比状态(全样本分位,分层中位值)": f"{feature_states['platform_share']} ({medians['platform_share']:.2%})",
            }
        )
    summary_df = pd.DataFrame(rows)
    median_fig = go.Figure()
    median_fig.add_trace(
        go.Bar(
            x=summary_df["分层(按30日锁单率分位)"],
            y=summary_df["30日锁单率中位数(分层内)"],
            marker={"color": COLORS["primary"]},
            name="30日锁单率中位数",
            hovertemplate="分层: %{x}<br>中位数: %{y:.2%}<extra></extra>",
        )
    )
    total_lock_orders = float(summary_df["锁单数总量(分层内)"].sum())
    if total_lock_orders > 0:
        summary_df["锁单数帕累托累计值"] = summary_df["锁单数总量(分层内)"].cumsum() / total_lock_orders
    else:
        summary_df["锁单数帕累托累计值"] = 0.0
    median_fig.add_trace(
        go.Scatter(
            x=summary_df["分层(按30日锁单率分位)"],
            y=summary_df["锁单数帕累托累计值"],
            mode="lines+markers",
            name="锁单数帕累托累计值",
            line={"color": COLORS["secondary"], "width": 3},
            marker={"size": 7},
            yaxis="y2",
            hovertemplate="分层: %{x}<br>帕累托累计值: %{y:.2%}<extra></extra>",
        )
    )
    median_fig.update_layout(
        common_layout(
            title="30日锁单率分层中位数（P0~P100，10层）",
            xaxis_title="分位分层",
            yaxis_title="30日锁单率中位数",
        )
    )
    median_fig.update_yaxes(tickformat=".1%")
    median_fig.update_layout(
        yaxis2={
            "title": "锁单数帕累托累计值",
            "overlaying": "y",
            "side": "right",
            "showgrid": False,
            "tickformat": ".0%",
            "range": [0, 1],
            "tickfont": {"color": COLORS["axis"]},
            "title_font": {"color": COLORS["axis"]},
        }
    )
    median_html = pio.to_html(median_fig, full_html=False, include_plotlyjs="cdn")

    zX = (df[features] - df[features].mean()) / df[features].std(ddof=0).replace(0, pd.NA)
    zY = (df["target_30d_rate"] - df["target_30d_rate"].mean()) / (df["target_30d_rate"].std(ddof=0) or 1.0)
    z = zX.join(zY.rename("target")).dropna()
    std_model = sm.OLS(z["target"], sm.add_constant(z[features], has_constant="add")).fit()
    coef_df = pd.DataFrame(
        {
            "指标": [feature_name[f] for f in features],
            "标准化系数": [float(std_model.params.get(f, 0.0)) for f in features],
        }
    )
    coef_fig = go.Figure()
    coef_fig.add_trace(
        go.Bar(
            x=coef_df["指标"],
            y=coef_df["标准化系数"],
            marker={"color": [COLORS["primary"], COLORS["secondary"], "#8E44AD", "#16A085"]},
            name="标准化系数",
        )
    )
    coef_fig.update_layout(
        common_layout(
            title="线性回归最优解（最小二乘）- 指标影响强度",
            xaxis_title="指标",
            yaxis_title="标准化系数",
        )
    )
    coef_html = pio.to_html(coef_fig, full_html=False, include_plotlyjs="cdn")
    r2 = float(model.rsquared)
    meta_html = (
        f"<p>回测样本: {len(df)} 天；线性回归 R²={r2:.3f}。"
        "分层按 30日锁单率分位区间（P0~P100，10层）。</p>"
    )
    table_html = summary_df.to_html(index=False, classes="table", escape=False)
    table_html = re.sub(
        r"<tr>(\s*<td>p70~p80</td>)",
        r'<tr style="background-color:#fff3cd;font-weight:700;">\1',
        table_html,
        count=1,
    )
    return meta_html + table_html + median_html + coef_html


def module_lock_orders_layered_dual_axis(data: pd.DataFrame) -> str:
    df = data.copy()
    df["lock_orders_pct"] = df["lock_orders"].rank(method="average", pct=True) * 100.0
    bins = [i for i in range(0, 101, 10)]
    lower_labels = [i for i in range(0, 100, 10)]
    df["band_lower"] = pd.cut(df["lock_orders_pct"], bins=bins, labels=lower_labels, include_lowest=True)
    grouped = (
        df.groupby("band_lower", observed=False)
        .agg(
            target_30d_median=("target_30d_rate", "median"),
            store_daily_leads_avg=("store_daily_leads", "mean"),
        )
        .reset_index()
    )
    grouped = grouped[grouped["band_lower"].notna()].copy()
    grouped["band_lower"] = grouped["band_lower"].astype(float)
    grouped["x_center"] = grouped["band_lower"] + 5.0
    grouped["band_label"] = grouped["band_lower"].map(lambda v: f"P{int(v)}~P{int(v + 10)}")
    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=grouped["x_center"],
            y=grouped["target_30d_median"],
            name="30日线索锁单率中位数",
            marker={"color": COLORS["primary"]},
            customdata=grouped[["band_label"]],
            hovertemplate="分层: %{customdata[0]}<br>30日线索锁单率中位数: %{y:.2%}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=grouped["x_center"],
            y=grouped["store_daily_leads_avg"],
            mode="lines+markers",
            name="店日均下发线索数",
            line={"color": COLORS["secondary"], "width": 3},
            marker={"size": 7},
            yaxis="y2",
            customdata=grouped[["band_label"]],
            hovertemplate="分层: %{customdata[0]}<br>店日均下发线索数: %{y:,.2f}<extra></extra>",
        )
    )
    points = load_business_time_points(BUSINESS_DEFINITION_FILE)
    if not points.empty:
        day_map = df[["date", "lock_orders_pct", "target_30d_rate", "store_daily_leads"]].copy()
        day_map = day_map.dropna(subset=["lock_orders_pct"])
        mapped = points.merge(day_map, on="date", how="inner")
        if not mapped.empty:
            series_names = sorted(mapped["series"].dropna().astype(str).unique().tolist())
            series_color_map = _build_series_color_map(series_names)
            for series_name in series_names:
                sub = mapped[mapped["series"].astype(str) == series_name].copy()
                if sub.empty:
                    continue
                fig.add_trace(
                    go.Scatter(
                        x=sub["lock_orders_pct"],
                        y=sub["store_daily_leads"],
                        mode="markers",
                        name=f"活动窗口店日均下发线索数({series_name})",
                        marker={
                            "size": 8,
                            "color": series_color_map[series_name],
                            "symbol": "diamond",
                            "line": {"width": 0.5, "color": "#2c3e50"},
                        },
                        yaxis="y2",
                        customdata=sub[["series", "date", "target_30d_rate"]],
                        hovertemplate="系列: %{customdata[0]}<br>日期: %{customdata[1]|%Y-%m-%d}<br>锁单数分位: P%{x:.1f}<br>店日均下发线索数: %{y:,.2f}<br>30日线索锁单率: %{customdata[2]:.2%}<extra></extra>",
                    )
                )
    fig.update_layout(
        common_layout(
            title="锁单数分层（P0~P100，10层）：30日线索锁单率 vs 店日均下发线索数",
            xaxis_title="锁单数分位（P0~P100）",
            yaxis_title="30日线索锁单率",
        )
    )
    fig.update_yaxes(tickformat=".1%")
    fig.update_xaxes(
        range=[0, 100],
        tickvals=[i for i in range(0, 101, 10)],
        ticktext=[f"P{i}" for i in range(0, 101, 10)],
    )
    fig.update_layout(
        yaxis2={
            "title": "店日均下发线索数",
            "overlaying": "y",
            "side": "right",
            "showgrid": False,
            "tickfont": {"color": COLORS["axis"]},
            "title_font": {"color": COLORS["axis"]},
        }
    )
    return pio.to_html(fig, full_html=False, include_plotlyjs="cdn")


def generate_report(
    leads_data: pd.DataFrame,
    store_daily_leads_data: pd.DataFrame,
    conv30_data: pd.DataFrame,
    store_daily_vs_30d_corr_data: pd.DataFrame,
    store_corr_data: pd.DataFrame,
    multivar_data: pd.DataFrame,
    lock_orders_layered_data: pd.DataFrame,
    output_file: Path,
) -> None:
    css = """
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 20px; color: #333; }
        h1 { color: #2c3e50; border-bottom: 2px solid #eee; padding-bottom: 10px; }
        h2 { color: #34495e; margin-top: 30px; border-left: 5px solid #3498db; padding-left: 10px; }
        h3 { color: #2980b9; margin-top: 25px; }
        table { border-collapse: collapse; width: 100%; margin-bottom: 20px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
        th, td { text-align: left; padding: 12px; border-bottom: 1px solid #ddd; }
        th { background-color: #f8f9fa; font-weight: 600; color: #555; }
        tr:hover { background-color: #f5f5f5; }
        .timestamp { color: #888; font-size: 0.9em; margin-bottom: 20px; }
        .summary-box { background: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px; }
    </style>
    """
    modules = [
        ("1. 下发线索数趋势", module_leads_trend, leads_data),
        ("2. 订单表.店日均下发线索数趋势", module_store_daily_leads_trend, store_daily_leads_data),
        ("3. 下发线索30日转化率趋势", module_30d_conversion_trend, conv30_data),
        ("4. 店日均下发线索数与锁单数相关性", module_store_daily_vs_30d_conversion_correlation, store_daily_vs_30d_corr_data),
        ("5. 门店当日锁单率与门店30日锁单率相关性", module_store_rate_correlation, store_corr_data),
        ("6. 多元数据回测与分位状态识别", module_multivariate_backtest, multivar_data),
        ("7. 锁单数分层双轴分析", module_lock_orders_layered_dual_axis, lock_orders_layered_data),
    ]
    html = [
        "<!DOCTYPE html>",
        "<html>",
        "<head>",
        "<meta charset='utf-8'>",
        "<title>Analyze_XX 可视化报告</title>",
        css,
        "</head>",
        "<body>",
        "<h1>Analyze_XX 可视化报告</h1>",
        f"<div class='timestamp'>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>",
        f"<div class='timestamp'>数据范围: {leads_data['date'].min().date()} ~ {leads_data['date'].max().date()}</div>",
    ]
    for title, renderer, module_data in modules:
        html.append(f"<h2>{title}</h2>")
        html.append(renderer(module_data))
    html.extend(["</body>", "</html>"])
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text("\n".join(html), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-csv", default=str(DATA_FILE))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()
    csv_path = Path(str(args.input_csv)).expanduser().resolve()
    output_path = Path(str(args.output)).expanduser().resolve()
    if not csv_path.exists():
        raise FileNotFoundError(f"文件不存在: {csv_path}")
    leads_data, store_daily_leads_data, conv30_data, store_daily_vs_30d_corr_data, store_corr_data, multivar_data, lock_orders_layered_data = load_all_series(csv_path)
    if leads_data.empty or store_daily_leads_data.empty or conv30_data.empty or store_daily_vs_30d_corr_data.empty or store_corr_data.empty or multivar_data.empty or lock_orders_layered_data.empty:
        raise ValueError("可视化数据为空")
    generate_report(
        leads_data,
        store_daily_leads_data,
        conv30_data,
        store_daily_vs_30d_corr_data,
        store_corr_data,
        multivar_data,
        lock_orders_layered_data,
        output_path,
    )
    print(str(output_path))


if __name__ == "__main__":
    main()

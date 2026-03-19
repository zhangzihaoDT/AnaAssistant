"""
脚本作用：
1) 基于日度矩阵（index_summary_daily_matrix）做结构化业务预测，核心恒等式为 lock_orders = leads × lock_rate。
2) 使用三类入参窗口（activity / weekday / weekend）代替保守/基准/乐观常量情景，并输出 p10/p50/p90 三档预测（对应未来 N 天）。
3) 支持月度问题口径（已发生 + 剩余预测）：指定 target_month 后，会把当月已发生锁单数（从月初到 as_of）与剩余天数预测合并，得到整月锁单数预测。
4) 月度模式内置分窗贝叶斯校准：活动周期/工作日/双休日分别给出 P10/P50/P90 分布，并映射到剩余天数。

使用方式：
1) 默认运行（as_of=样本最大日期，预测 28 天，打印 JSON）
   python scripts/structured_business_forecast.py

2) 指定预测锚点日期（例如 2026-03-15），预测未来 N 天
   python scripts/structured_business_forecast.py --as-of 2026-03-15 --forecast-days 28

3) 月度预测（例如：估计 2026-03 整月锁单数；已纳入 3/1~3/16 真实值，仅预测 3/17~3/31）
   python scripts/structured_business_forecast.py --as-of 2026-03-16 --target-month 2026-03

4) 指定输出文件（未来 N 天口径；会额外打印一行 decision_summary）
   python scripts/structured_business_forecast.py --as-of 2026-03-15 --forecast-days 28 --output out/structured_forecast_2026-03-15.json

关键输出字段：
- scenario_forecast.regime_quantile_based
- scenario_forecast.regime_quantile_bias_corrected
- scenario_forecast.bias_correction.regime_quantile_based
- decision_summary（顶层；未来 N 天或整月，取决于是否指定 --target-month）
- scenario_forecast.month_forecast（仅当指定 --target-month）
- scenario_forecast.month_forecast.bayesian_calibration（仅当指定 --target-month）
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


DEFAULT_MATRIX_CSV = (
    Path(__file__).resolve().parents[1]
    / "schema"
    / "index_summary_daily_matrix_2024-01-01_to_yesterday.csv"
)

LOCK_ORDERS_METRIC = "订单表.锁单数"
LEADS_METRIC = "下发线索转化率.下发线索数"
LOCK_RATE_CANDIDATES = [
    "下发线索转化率.下发线索当30日锁单率",
    "下发线索转化率.下发线索当7日锁单率",
]

QUANTILE_KEYS = ["p10", "p50", "p90"]
REGIME_KEYS = ["activity_high_eff", "activity_low_eff", "weekday", "weekend"]
BOOTSTRAP_KEYS = [*QUANTILE_KEYS, "mode"]


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


def _safe_float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    return float(value)


def _safe_ratio(numerator: float, denominator: float) -> float | None:
    if denominator == 0:
        return None
    return numerator / denominator


def _std_safe(series: pd.Series) -> float:
    v = series.std(ddof=0)
    if pd.isna(v):
        return 0.0
    return float(v)


def _z_score(current: float, mean: float, std: float) -> float:
    if std == 0:
        return 0.0
    return (current - mean) / std


def _pick_existing_metric(metric_index: pd.Index, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in metric_index:
            return c
    return None


def _load_matrix(matrix_csv: Path) -> pd.DataFrame:
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
    return ts_df


def _build_series(ts_df: pd.DataFrame) -> pd.DataFrame:
    if LOCK_ORDERS_METRIC not in ts_df.columns:
        raise ValueError(f"缺少关键指标: {LOCK_ORDERS_METRIC}")
    if LEADS_METRIC not in ts_df.columns:
        raise ValueError(f"缺少关键指标: {LEADS_METRIC}")

    rate_metric = _pick_existing_metric(ts_df.columns, LOCK_RATE_CANDIDATES)
    out = pd.DataFrame(index=ts_df.index)
    out["lock_orders"] = ts_df[LOCK_ORDERS_METRIC].astype(float)
    out["leads"] = ts_df[LEADS_METRIC].astype(float)
    if rate_metric is not None:
        out["lock_rate"] = ts_df[rate_metric].astype(float)
    else:
        out["lock_rate"] = out.apply(
            lambda row: _safe_ratio(_safe_float(row["lock_orders"]), _safe_float(row["leads"])),
            axis=1,
        )
    out = out.dropna(subset=["lock_orders", "leads", "lock_rate"])
    out = out[(out["leads"] >= 0) & (out["lock_orders"] >= 0) & (out["lock_rate"] >= 0)]
    return out


def _window(df: pd.DataFrame, end_date: pd.Timestamp, lookback_days: int) -> pd.DataFrame:
    start = end_date - pd.Timedelta(days=lookback_days - 1)
    return df[(df.index >= start) & (df.index <= end_date)].copy()


def _summary_stats(series: pd.Series) -> dict[str, float]:
    q = series.quantile([0.1, 0.3, 0.5, 0.7, 0.9])
    return {
        "mean": round(float(series.mean()), 6),
        "std": round(_std_safe(series), 6),
        "mode": round(_estimate_mode(series), 6),
        "p10": round(float(q.loc[0.1]), 6),
        "p30": round(float(q.loc[0.3]), 6),
        "p50": round(float(q.loc[0.5]), 6),
        "p70": round(float(q.loc[0.7]), 6),
        "p90": round(float(q.loc[0.9]), 6),
    }


def _estimate_mode(series: pd.Series) -> float:
    s = pd.Series(series).dropna().astype(float)
    if len(s) == 0:
        return 0.0

    values = s.values
    if np.all(np.isclose(values, values[0], atol=0.0)):
        return float(values[0])

    is_int_like = np.all(np.isclose(values, np.round(values), atol=1e-9))
    if is_int_like:
        vc = pd.Series(np.round(values).astype(int)).value_counts()
        if len(vc) == 0:
            return float(np.median(values))
        top = vc.max()
        candidates = vc[vc == top].index.to_numpy(dtype=float)
        if candidates.size == 1:
            return float(candidates[0])
        med = float(np.median(values))
        return float(candidates[np.argmin(np.abs(candidates - med))])

    x = np.sort(values.astype(float))
    return float(_half_sample_mode_sorted(x))


def _half_sample_mode_sorted(sorted_values: np.ndarray) -> float:
    n = int(sorted_values.size)
    if n == 0:
        return 0.0
    if n == 1:
        return float(sorted_values[0])
    if n == 2:
        return float((sorted_values[0] + sorted_values[1]) / 2.0)

    k = (n + 1) // 2
    widths = sorted_values[k - 1 :] - sorted_values[: n - k + 1]
    start = int(np.argmin(widths))
    window = sorted_values[start : start + k]
    if window.size == n:
        return float(np.median(sorted_values))
    return float(_half_sample_mode_sorted(window))


def _positive_daily_trend_delta(series: pd.Series) -> float:
    s = pd.Series(series).dropna().astype(float)
    if len(s) < 7:
        return 0.0
    x = np.arange(len(s), dtype=float)
    y = s.values
    slope, _ = np.polyfit(x, y, 1)
    if np.isnan(slope):
        return 0.0
    return float(max(slope, 0.0))


def _compute_bias_correction(
    full_series_df: pd.DataFrame,
    horizon_days: int,
    lookback_recent: int,
    lookback_history: int,
    eval_days: int,
    activity_ranges: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]],
) -> dict[str, float | int]:
    if eval_days <= 0:
        return {"enabled": 0, "samples": 0, "bias": 0.0, "mean_true": 0.0, "bias_rate": 0.0, "factor": 1.0}
    if horizon_days <= 0:
        return {"enabled": 0, "samples": 0, "bias": 0.0, "mean_true": 0.0, "bias_rate": 0.0, "factor": 1.0}

    max_date = pd.Timestamp(full_series_df.index.max()).normalize()
    last_anchor = max_date - pd.Timedelta(days=horizon_days)
    if pd.isna(last_anchor):
        return {"enabled": 0, "samples": 0, "bias": 0.0, "mean_true": 0.0, "bias_rate": 0.0, "factor": 1.0}

    anchors = pd.DatetimeIndex(full_series_df.index).sort_values()
    anchors = anchors[anchors <= last_anchor]
    if len(anchors) == 0:
        return {"enabled": 0, "samples": 0, "bias": 0.0, "mean_true": 0.0, "bias_rate": 0.0, "factor": 1.0}
    anchors = anchors[-eval_days:]

    y_true: list[float] = []
    y_pred: list[float] = []

    for as_of in anchors:
        past_df = full_series_df[full_series_df.index <= as_of].copy()
        history_df = _window(past_df, as_of, lookback_history)
        recent_df = _window(past_df, as_of, lookback_recent)
        if history_df.empty or recent_df.empty:
            continue

        future_start = as_of + pd.Timedelta(days=1)
        future_end = as_of + pd.Timedelta(days=horizon_days)
        future_df = full_series_df[(full_series_df.index >= future_start) & (full_series_df.index <= future_end)]
        if len(future_df) != horizon_days:
            continue

        lead_daily_delta = _positive_daily_trend_delta(recent_df["leads"])
        rate_daily_delta = _positive_daily_trend_delta(recent_df["lock_rate"])
        future_dates = pd.date_range(future_start, future_end, freq="D")
        _, q_forecast, _ = _regime_quantile_forecast(
            history_df=history_df.copy(),
            future_dates=future_dates,
            activity_ranges=activity_ranges,
            lead_daily_delta=lead_daily_delta,
            rate_daily_delta=rate_daily_delta,
        )
        pred = float(q_forecast["p50"]["period_lock_orders"])

        true = float(future_df["lock_orders"].sum())
        y_true.append(true)
        y_pred.append(pred)

    if len(y_true) < 30:
        return {"enabled": 0, "samples": int(len(y_true)), "bias": 0.0, "mean_true": 0.0, "bias_rate": 0.0, "factor": 1.0}

    y_true_arr = np.array(y_true, dtype=float)
    y_pred_arr = np.array(y_pred, dtype=float)
    bias = float(np.mean(y_pred_arr - y_true_arr))
    mean_true = float(np.mean(y_true_arr))
    bias_rate = float(bias / mean_true) if mean_true != 0 else 0.0
    factor = float(1.0 - bias_rate)
    return {
        "enabled": 1,
        "samples": int(len(y_true)),
        "bias": bias,
        "mean_true": mean_true,
        "bias_rate": bias_rate,
        "factor": factor,
    }


def _load_activity_ranges(
    business_definition_json: Path,
) -> list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
    raw = json.loads(business_definition_json.read_text(encoding="utf-8"))
    periods = raw.get("time_periods", {})
    out: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]] = []
    for name, p in periods.items():
        start = pd.to_datetime(p.get("start"), errors="coerce")
        end = pd.to_datetime(p.get("end"), errors="coerce")
        finish = pd.to_datetime(p.get("finish") or p.get("end"), errors="coerce")
        if pd.isna(start) or pd.isna(end) or pd.isna(finish):
            continue
        out.append(
            (
                str(name),
                pd.Timestamp(start).normalize(),
                pd.Timestamp(end).normalize(),
                pd.Timestamp(finish).normalize(),
            )
        )
    return out


def _segment_quantiles(series: pd.Series) -> dict[str, float | int]:
    s = pd.Series(series).dropna().astype(float)
    if len(s) == 0:
        return {"n": 0, "mode": 0.0, "p10": 0.0, "p50": 0.0, "p90": 0.0}
    q = s.quantile([0.1, 0.5, 0.9])
    return {
        "n": int(len(s)),
        "mode": float(_estimate_mode(s)),
        "p10": float(q.loc[0.1]),
        "p50": float(q.loc[0.5]),
        "p90": float(q.loc[0.9]),
    }


def _regime_label_for_date(
    d: pd.Timestamp,
    activity_ranges: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]],
) -> str:
    dn = pd.Timestamp(d).normalize()
    in_activity = False
    for _, s, e, f in activity_ranges:
        if not (s <= dn <= f):
            continue
        in_activity = True
        special_start = s
        special_start_end = s + pd.Timedelta(days=2)
        special_end_start = e - pd.Timedelta(days=2)
        special_end_end = e
        special_finish_start = f - pd.Timedelta(days=2)
        special_finish_end = f
        is_special = (
            (special_start <= dn <= special_start_end)
            or (special_end_start <= dn <= special_end_end)
            or (special_finish_start <= dn <= special_finish_end)
        )
        if dn.weekday() >= 5 or is_special:
            return "activity_high_eff"
    if in_activity:
        return "activity_low_eff"
    if dn.weekday() >= 5:
        return "weekend"
    return "weekday"


def _build_regime_inputs(
    history_df: pd.DataFrame, activity_ranges: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]]
) -> dict[str, dict[str, dict[str, float | int]]]:
    hist = history_df.copy()
    labels = [_regime_label_for_date(pd.Timestamp(d), activity_ranges) for d in pd.DatetimeIndex(hist.index)]
    hist["regime"] = labels
    hist = hist.dropna(subset=["leads", "lock_rate"])
    hist = hist[(hist["leads"] >= 0) & (hist["lock_rate"] >= 0)]
    if hist.empty:
        return {
            "activity_high_eff": {
                "leads": {"n": 0, "mode": 0.0, "p10": 0.0, "p50": 0.0, "p90": 0.0},
                "lock_rate": {"n": 0, "mode": 0.0, "p10": 0.0, "p50": 0.0, "p90": 0.0},
            },
            "activity_low_eff": {
                "leads": {"n": 0, "mode": 0.0, "p10": 0.0, "p50": 0.0, "p90": 0.0},
                "lock_rate": {"n": 0, "mode": 0.0, "p10": 0.0, "p50": 0.0, "p90": 0.0},
            },
            "weekday": {
                "leads": {"n": 0, "mode": 0.0, "p10": 0.0, "p50": 0.0, "p90": 0.0},
                "lock_rate": {"n": 0, "mode": 0.0, "p10": 0.0, "p50": 0.0, "p90": 0.0},
            },
            "weekend": {
                "leads": {"n": 0, "mode": 0.0, "p10": 0.0, "p50": 0.0, "p90": 0.0},
                "lock_rate": {"n": 0, "mode": 0.0, "p10": 0.0, "p50": 0.0, "p90": 0.0},
            },
        }
    all_part = hist[["leads", "lock_rate"]].copy()
    out: dict[str, dict[str, dict[str, float | int]]] = {}
    for regime in REGIME_KEYS:
        part = hist[hist["regime"] == regime][["leads", "lock_rate"]].copy()
        if part.empty:
            part = all_part.copy()
        out[regime] = {
            "leads": _segment_quantiles(part["leads"]),
            "lock_rate": _segment_quantiles(part["lock_rate"]),
        }
    return out


def _regime_quantile_forecast(
    history_df: pd.DataFrame,
    future_dates: pd.DatetimeIndex,
    activity_ranges: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]],
    lead_daily_delta: float,
    rate_daily_delta: float,
) -> tuple[dict[str, object], dict[str, dict[str, float]], dict[str, dict[str, float]]]:
    regime_inputs = _build_regime_inputs(history_df=history_df, activity_ranges=activity_ranges)
    future_regimes = [_regime_label_for_date(pd.Timestamp(d), activity_ranges) for d in pd.DatetimeIndex(future_dates)]
    regime_counts: dict[str, int] = {k: 0 for k in REGIME_KEYS}
    for r in future_regimes:
        regime_counts[r] = regime_counts.get(r, 0) + 1

    quantile_forecast: dict[str, dict[str, float]] = {}
    for qk in QUANTILE_KEYS:
        daily_rows: list[dict[str, float]] = []
        for i, d in enumerate(pd.DatetimeIndex(future_dates)):
            regime = future_regimes[i]
            leads = max(float(regime_inputs[regime]["leads"][qk]) + lead_daily_delta * i, 0.0)
            rate = max(float(regime_inputs[regime]["lock_rate"][qk]) + rate_daily_delta * i, 0.0)
            daily_rows.append(
                {
                    "date": str(pd.Timestamp(d).date()),
                    "regime": regime,
                    "pred_leads": float(leads),
                    "pred_lock_rate": float(rate),
                    "pred_lock_orders": float(leads * rate),
                }
            )
        frame = pd.DataFrame(daily_rows)
        if frame.empty:
            quantile_forecast[qk] = {
                "daily_leads": 0.0,
                "daily_lock_rate": 0.0,
                "daily_lock_orders": 0.0,
                "period_lock_orders": 0.0,
            }
        else:
            quantile_forecast[qk] = {
                "daily_leads": round(float(frame["pred_leads"].mean()), 2),
                "daily_lock_rate": round(float(frame["pred_lock_rate"].mean()), 6),
                "daily_lock_orders": round(float(frame["pred_lock_orders"].mean()), 2),
                "period_lock_orders": round(float(frame["pred_lock_orders"].sum()), 2),
            }

    regime_reference: dict[str, dict[str, float]] = {}
    for regime in REGIME_KEYS:
        regime_reference[regime] = {}
        for qk in QUANTILE_KEYS:
            leads_q = float(regime_inputs[regime]["leads"][qk])
            rate_q = float(regime_inputs[regime]["lock_rate"][qk])
            regime_reference[regime][qk] = round(leads_q * rate_q, 2)
        leads_m = float(regime_inputs[regime]["leads"]["mode"])
        rate_m = float(regime_inputs[regime]["lock_rate"]["mode"])
        regime_reference[regime]["mode"] = round(leads_m * rate_m, 2)

    calibration_meta = {
        "enabled": 1,
        "method": "regime_quantile",
        "future_days": int(len(future_dates)),
        "future_regime_days": regime_counts,
        "regime_inputs": regime_inputs,
        "regime_daily_lock_orders_reference": regime_reference,
    }
    return calibration_meta, quantile_forecast, regime_reference


def _regime_bootstrap_forecast(
    history_df: pd.DataFrame,
    future_dates: pd.DatetimeIndex,
    activity_ranges: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]],
    lead_daily_delta: float,
    rate_daily_delta: float,
    sim_n: int = 8000,
    seed: int = 202403,
) -> dict[str, dict[str, float]]:
    future_dates = pd.DatetimeIndex(future_dates)
    days = int(len(future_dates))
    if days <= 0:
        return {
            k: {"daily_leads": 0.0, "daily_lock_rate": 0.0, "daily_lock_orders": 0.0, "period_lock_orders": 0.0}
            for k in BOOTSTRAP_KEYS
        }

    hist = history_df.copy()
    labels = [_regime_label_for_date(pd.Timestamp(d), activity_ranges) for d in pd.DatetimeIndex(hist.index)]
    hist["regime"] = labels
    hist = hist.dropna(subset=["leads", "lock_rate"])
    hist = hist[(hist["leads"] >= 0) & (hist["lock_rate"] >= 0)]
    if hist.empty:
        return {
            k: {"daily_leads": 0.0, "daily_lock_rate": 0.0, "daily_lock_orders": 0.0, "period_lock_orders": 0.0}
            for k in BOOTSTRAP_KEYS
        }

    hist = hist.astype({"leads": float, "lock_rate": float})
    all_pairs = hist[["leads", "lock_rate"]].to_numpy(dtype=float)
    pairs_by_regime: dict[str, np.ndarray] = {}
    for regime in REGIME_KEYS:
        part = hist[hist["regime"] == regime][["leads", "lock_rate"]].to_numpy(dtype=float)
        pairs_by_regime[regime] = part if part.size else all_pairs

    future_regimes = [_regime_label_for_date(pd.Timestamp(d), activity_ranges) for d in future_dates]
    sim_n = int(max(1000, sim_n))
    rng = np.random.default_rng(int(seed))

    period_lock = np.zeros(sim_n, dtype=float)
    leads_sum = np.zeros(sim_n, dtype=float)
    rate_sum = np.zeros(sim_n, dtype=float)

    for i in range(days):
        regime = future_regimes[i]
        pairs = pairs_by_regime.get(regime, all_pairs)
        if pairs.size == 0:
            continue
        idx = rng.integers(0, pairs.shape[0], size=sim_n)
        leads = pairs[idx, 0] + lead_daily_delta * i
        rate = pairs[idx, 1] + rate_daily_delta * i
        leads = np.maximum(leads, 0.0)
        rate = np.maximum(rate, 0.0)
        lock = leads * rate
        period_lock += lock
        leads_sum += leads
        rate_sum += rate

    daily_leads_arr = leads_sum / float(days)
    daily_rate_arr = rate_sum / float(days)
    daily_lock_arr = period_lock / float(days)

    period_q10, period_q50, period_q90 = np.quantile(period_lock, [0.1, 0.5, 0.9])
    leads_q10, leads_q50, leads_q90 = np.quantile(daily_leads_arr, [0.1, 0.5, 0.9])
    rate_q10, rate_q50, rate_q90 = np.quantile(daily_rate_arr, [0.1, 0.5, 0.9])
    lock_q10, lock_q50, lock_q90 = np.quantile(daily_lock_arr, [0.1, 0.5, 0.9])

    period_mode = float(_estimate_mode(pd.Series(period_lock)))
    leads_mode = float(_estimate_mode(pd.Series(daily_leads_arr)))
    rate_mode = float(_estimate_mode(pd.Series(daily_rate_arr)))

    return {
        "p10": {
            "daily_leads": round(float(leads_q10), 2),
            "daily_lock_rate": round(float(rate_q10), 6),
            "daily_lock_orders": round(float(lock_q10), 2),
            "period_lock_orders": round(float(period_q10), 2),
        },
        "p50": {
            "daily_leads": round(float(leads_q50), 2),
            "daily_lock_rate": round(float(rate_q50), 6),
            "daily_lock_orders": round(float(lock_q50), 2),
            "period_lock_orders": round(float(period_q50), 2),
        },
        "p90": {
            "daily_leads": round(float(leads_q90), 2),
            "daily_lock_rate": round(float(rate_q90), 6),
            "daily_lock_orders": round(float(lock_q90), 2),
            "period_lock_orders": round(float(period_q90), 2),
        },
        "mode": {
            "daily_leads": round(float(leads_mode), 2),
            "daily_lock_rate": round(float(rate_mode), 6),
            "daily_lock_orders": round(float(period_mode / float(days)), 2),
            "period_lock_orders": round(float(period_mode), 2),
        },
    }


def _parse_target_month(value: str) -> tuple[pd.Timestamp, pd.Timestamp]:
    month_start = pd.to_datetime(f"{value}-01", errors="coerce")
    if pd.isna(month_start):
        raise ValueError(f"target_month 解析失败: {value}")
    month_start = pd.Timestamp(month_start).normalize()
    month_end = (month_start + pd.offsets.MonthEnd(0)).normalize()
    return month_start, month_end


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--matrix-csv", default=str(DEFAULT_MATRIX_CSV))
    parser.add_argument("--business-definition", default=str(Path(__file__).resolve().parents[1] / "schema" / "business_definition.json"))
    parser.add_argument("--as-of", default=None)
    parser.add_argument("--target-month", default=None)
    parser.add_argument("--lookback-recent", type=int, default=30)
    parser.add_argument("--lookback-history", type=int, default=365)
    parser.add_argument("--forecast-days", type=int, default=28)
    parser.add_argument("--bias-eval-days", type=int, default=365)
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
    if args.lookback_recent <= 0 or args.lookback_history <= 0 or args.forecast_days <= 0:
        raise ValueError("lookback_recent/lookback_history/forecast_days 必须大于 0")

    ts_df = _load_matrix(matrix_csv)
    full_series_df = _build_series(ts_df)
    if full_series_df.empty:
        raise ValueError("可用样本为空，无法预测")

    max_date = pd.Timestamp(full_series_df.index.max()).normalize()
    if args.as_of is None:
        as_of = max_date
    else:
        as_of = pd.to_datetime(args.as_of).normalize()
    if as_of > max_date:
        raise ValueError(f"as_of={as_of.date()} 超过样本最大日期 {max_date.date()}")

    month_mode = args.target_month is not None
    activity_ranges = _load_activity_ranges(business_definition)

    forecast_as_of = as_of
    forecast_days = int(args.forecast_days)
    future_dates = pd.date_range(
        forecast_as_of + pd.Timedelta(days=1),
        forecast_as_of + pd.Timedelta(days=forecast_days),
        freq="D",
    )

    series_df = full_series_df[full_series_df.index <= forecast_as_of].copy()
    recent_df = _window(series_df, forecast_as_of, args.lookback_recent)
    history_df = _window(series_df, forecast_as_of, args.lookback_history)
    if recent_df.empty or history_df.empty:
        raise ValueError("窗口样本不足，请调整 as_of 或 lookback 参数")

    recent_leads = float(recent_df["leads"].mean())
    recent_rate = float(recent_df["lock_rate"].mean())
    recent_locks = float(recent_df["lock_orders"].mean())
    lead_daily_delta = _positive_daily_trend_delta(recent_df["leads"])
    rate_daily_delta = _positive_daily_trend_delta(recent_df["lock_rate"])
    leads_stats = _summary_stats(history_df["leads"])
    rate_stats = _summary_stats(history_df["lock_rate"])
    lock_stats = _summary_stats(history_df["lock_orders"])

    if forecast_days > 0:
        bayes_calibration, regime_quantile_forecast, regime_reference = _regime_quantile_forecast(
            history_df=history_df.copy(),
            future_dates=future_dates,
            activity_ranges=activity_ranges,
            lead_daily_delta=lead_daily_delta,
            rate_daily_delta=rate_daily_delta,
        )
        regime_bootstrap_forecast = _regime_bootstrap_forecast(
            history_df=history_df.copy(),
            future_dates=future_dates,
            activity_ranges=activity_ranges,
            lead_daily_delta=lead_daily_delta,
            rate_daily_delta=rate_daily_delta,
        )
        bias_regime = _compute_bias_correction(
            full_series_df=full_series_df[full_series_df.index <= forecast_as_of].copy(),
            horizon_days=int(forecast_days),
            lookback_recent=int(args.lookback_recent),
            lookback_history=int(args.lookback_history),
            eval_days=int(args.bias_eval_days),
            activity_ranges=activity_ranges,
        )
        bias_factor = float(bias_regime["factor"])
        regime_quantile_bias_corrected = {
            qk: {
                "daily_lock_orders_bias_corrected": round(float(v["daily_lock_orders"]) * bias_factor, 2),
                "period_lock_orders_bias_corrected": round(float(v["period_lock_orders"]) * bias_factor, 2),
            }
            for qk, v in regime_quantile_forecast.items()
        }
        regime_bootstrap_bias_corrected = {
            k: {
                "daily_leads": float(v["daily_leads"]),
                "daily_lock_rate": float(v["daily_lock_rate"]),
                "daily_lock_orders_bias_corrected": round(float(v["daily_lock_orders"]) * bias_factor, 2),
                "period_lock_orders_bias_corrected": round(float(v["period_lock_orders"]) * bias_factor, 2),
            }
            for k, v in regime_bootstrap_forecast.items()
        }
    else:
        regime_quantile_forecast = {k: {"daily_leads": 0.0, "daily_lock_rate": 0.0, "daily_lock_orders": 0.0, "period_lock_orders": 0.0} for k in QUANTILE_KEYS}
        regime_reference = {k: {q: 0.0 for q in QUANTILE_KEYS} for k in REGIME_KEYS}
        bayes_calibration = {
            "enabled": 0,
            "method": "regime_quantile",
            "future_days": 0,
            "future_regime_days": {k: 0 for k in REGIME_KEYS},
            "regime_inputs": {},
            "regime_daily_lock_orders_reference": regime_reference,
        }
        bias_regime = {
            "enabled": 0,
            "samples": 0,
            "bias": 0.0,
            "mean_true": 0.0,
            "bias_rate": 0.0,
            "factor": 1.0,
        }
        regime_quantile_bias_corrected = {
            qk: {
                "daily_lock_orders_bias_corrected": 0.0,
                "period_lock_orders_bias_corrected": 0.0,
            }
            for qk in QUANTILE_KEYS
        }
        regime_bootstrap_forecast = {
            k: {"daily_leads": 0.0, "daily_lock_rate": 0.0, "daily_lock_orders": 0.0, "period_lock_orders": 0.0}
            for k in BOOTSTRAP_KEYS
        }
        regime_bootstrap_bias_corrected = {
            k: {
                "daily_leads": 0.0,
                "daily_lock_rate": 0.0,
                "daily_lock_orders_bias_corrected": 0.0,
                "period_lock_orders_bias_corrected": 0.0,
            }
            for k in BOOTSTRAP_KEYS
        }

    result = {
        "framework": "Metrics Map + 统计分布 + 情景假设 = 销量预测",
        "dataset": str(matrix_csv),
        "as_of_date": str(as_of.date()),
        "samples": {
            "recent_days": int(len(recent_df)),
            "history_days": int(len(history_df)),
        },
        "metrics_map": {
            "target_metric": LOCK_ORDERS_METRIC,
            "traffic_metric": LEADS_METRIC,
            "conversion_metric": _pick_existing_metric(ts_df.columns, LOCK_RATE_CANDIDATES)
            or "lock_orders / leads",
            "identity": "lock_orders = leads × lock_rate",
        },
        "recent_performance": {
            "daily_leads_mean": round(recent_leads, 2),
            "daily_lock_rate_mean": round(recent_rate, 6),
            "daily_lock_orders_mean": round(recent_locks, 2),
            "lead_daily_delta": round(float(lead_daily_delta), 6),
            "rate_daily_delta": round(float(rate_daily_delta), 8),
        },
        "distribution": {
            "leads": {
                **leads_stats,
                "zscore_recent_mean": round(
                    _z_score(recent_leads, leads_stats["mean"], leads_stats["std"]), 6
                ),
            },
            "lock_rate": {
                **rate_stats,
                "zscore_recent_mean": round(
                    _z_score(recent_rate, rate_stats["mean"], rate_stats["std"]), 6
                ),
            },
            "lock_orders": {
                **lock_stats,
                "zscore_recent_mean": round(
                    _z_score(recent_locks, lock_stats["mean"], lock_stats["std"]), 6
                ),
            },
        },
        "scenario_forecast": {
            "forecast_days": int(forecast_days),
            "regime_quantile_based": regime_quantile_forecast,
            "regime_quantile_bias_corrected": regime_quantile_bias_corrected,
            "regime_bootstrap": regime_bootstrap_forecast,
            "regime_bootstrap_bias_corrected": regime_bootstrap_bias_corrected,
            "bayesian_calibration": bayes_calibration,
            "bias_correction": {
                "regime_quantile_based": bias_regime,
            },
        },
    }

    if month_mode:
        target_month_for_summary = str(args.target_month)
        month_start, month_end = _parse_target_month(target_month_for_summary)
        if as_of < month_start:
            raise ValueError(
                f"as_of={as_of.date()} 早于 target_month={args.target_month} 月初 {month_start.date()}"
            )

        actual_end = min(month_end, as_of)
        expected_days = int((actual_end - month_start).days + 1) if actual_end >= month_start else 0
        month_actual_df = full_series_df[
            (full_series_df.index >= month_start) & (full_series_df.index <= actual_end)
        ].copy()
        actual_days = int(len(month_actual_df))
        actual_sum = float(month_actual_df["lock_orders"].sum()) if not month_actual_df.empty else 0.0
        if expected_days > 0:
            expected_dates = pd.date_range(month_start, actual_end, freq="D")
            actual_dates = pd.DatetimeIndex(month_actual_df.index).normalize()
            missing_days = int(len(expected_dates.difference(actual_dates)))
        else:
            missing_days = 0

        remaining_start = actual_end + pd.Timedelta(days=1)
        if remaining_start > month_end:
            remaining_days = 0
            future_dates_month = pd.DatetimeIndex([])
        else:
            remaining_days = int((month_end - remaining_start).days + 1)
            future_dates_month = pd.date_range(remaining_start, month_end, freq="D")

        if remaining_days > 0:
            month_calibration, month_forecast_quantiles, month_regime_reference = _regime_quantile_forecast(
                history_df=history_df.copy(),
                future_dates=future_dates_month,
                activity_ranges=activity_ranges,
                lead_daily_delta=lead_daily_delta,
                rate_daily_delta=rate_daily_delta,
            )
            month_bootstrap_forecast = _regime_bootstrap_forecast(
                history_df=history_df.copy(),
                future_dates=future_dates_month,
                activity_ranges=activity_ranges,
                lead_daily_delta=lead_daily_delta,
                rate_daily_delta=rate_daily_delta,
            )
            month_bias = _compute_bias_correction(
                full_series_df=full_series_df[full_series_df.index <= as_of].copy(),
                horizon_days=int(remaining_days),
                lookback_recent=int(args.lookback_recent),
                lookback_history=int(args.lookback_history),
                eval_days=int(args.bias_eval_days),
                activity_ranges=activity_ranges,
            )
            month_bias_factor = float(month_bias["factor"])
            remaining_p10 = float(month_bootstrap_forecast["p10"]["period_lock_orders"])
            remaining_p50 = float(month_bootstrap_forecast["p50"]["period_lock_orders"])
            remaining_p90 = float(month_bootstrap_forecast["p90"]["period_lock_orders"])
            remaining_mode = float(month_bootstrap_forecast["mode"]["period_lock_orders"])
            remaining_bc_p10 = remaining_p10 * month_bias_factor
            remaining_bc_p50 = remaining_p50 * month_bias_factor
            remaining_bc_p90 = remaining_p90 * month_bias_factor
            remaining_bc_mode = remaining_mode * month_bias_factor
        else:
            month_calibration = {
                "enabled": 0,
                "method": "regime_quantile",
                "future_days": 0,
                "future_regime_days": {k: 0 for k in REGIME_KEYS},
                "regime_inputs": {},
                "regime_daily_lock_orders_reference": {k: {} for k in REGIME_KEYS},
            }
            month_regime_reference = {k: {} for k in REGIME_KEYS}
            month_bias = {
                "enabled": 0,
                "samples": 0,
                "bias": 0.0,
                "mean_true": 0.0,
                "bias_rate": 0.0,
                "factor": 1.0,
            }
            remaining_p10 = remaining_p50 = remaining_p90 = remaining_mode = 0.0
            remaining_bc_p10 = remaining_bc_p50 = remaining_bc_p90 = remaining_bc_mode = 0.0

        month_totals = {
            "actual_lock_orders_to_date": round(float(actual_sum), 2),
            "remaining_lock_orders_mode": round(float(remaining_mode), 2),
            "remaining_lock_orders_p10": round(float(remaining_p10), 2),
            "remaining_lock_orders_p50": round(float(remaining_p50), 2),
            "remaining_lock_orders_p90": round(float(remaining_p90), 2),
            "remaining_lock_orders_bias_corrected_mode": round(float(remaining_bc_mode), 2),
            "remaining_lock_orders_bias_corrected_p10": round(float(remaining_bc_p10), 2),
            "remaining_lock_orders_bias_corrected_p50": round(float(remaining_bc_p50), 2),
            "remaining_lock_orders_bias_corrected_p90": round(float(remaining_bc_p90), 2),
            "month_lock_orders_mode": round(float(actual_sum + remaining_mode), 2),
            "month_lock_orders_p10": round(float(actual_sum + remaining_p10), 2),
            "month_lock_orders_p50": round(float(actual_sum + remaining_p50), 2),
            "month_lock_orders_p90": round(float(actual_sum + remaining_p90), 2),
            "month_lock_orders_bias_corrected_mode": round(float(actual_sum + remaining_bc_mode), 2),
            "month_lock_orders_bias_corrected_p10": round(float(actual_sum + remaining_bc_p10), 2),
            "month_lock_orders_bias_corrected_p50": round(float(actual_sum + remaining_bc_p50), 2),
            "month_lock_orders_bias_corrected_p90": round(float(actual_sum + remaining_bc_p90), 2),
            "regime_daily_lock_orders_reference": month_regime_reference,
        }

        month_label = target_month_for_summary.split("-")[1].lstrip("0")
        decision_summary = (
            f"截至 {pd.Timestamp(actual_end).month}/{pd.Timestamp(actual_end).day} 已锁单 {float(actual_sum):g}；"
            f"预计 {month_label} 月整月（bias 校正后）最可能值约 {float(month_totals['month_lock_orders_bias_corrected_mode']):.2f}，"
            f"P50 约 {float(month_totals['month_lock_orders_bias_corrected_p50']):.2f}，"
            f"区间 [{float(month_totals['month_lock_orders_bias_corrected_p10']):.2f}, {float(month_totals['month_lock_orders_bias_corrected_p90']):.2f}]。"
        )

        result["scenario_forecast"]["month_forecast"] = {
            "target_month": target_month_for_summary,
            "month_start": str(month_start.date()),
            "month_end": str(month_end.date()),
            "as_of_date": str(as_of.date()),
            "actual_end_date": str(actual_end.date()),
            "actual_days": int(actual_days),
            "expected_days": int(expected_days),
            "missing_days": int(missing_days),
            "actual_lock_orders_sum": round(float(actual_sum), 2),
            "remaining_days": int(remaining_days),
            "remaining_start_date": str(remaining_start.date()) if remaining_days > 0 else None,
            "month_end_date": str(month_end.date()),
            "bayesian_calibration": month_calibration,
            "bias_correction": {"regime_quantile_based": month_bias},
            "month_totals": month_totals,
        }
        result["decision_summary"] = decision_summary
    else:
        mode = float(regime_bootstrap_bias_corrected["mode"]["period_lock_orders_bias_corrected"])
        p10 = float(regime_bootstrap_bias_corrected["p10"]["period_lock_orders_bias_corrected"])
        p50 = float(regime_bootstrap_bias_corrected["p50"]["period_lock_orders_bias_corrected"])
        p90 = float(regime_bootstrap_bias_corrected["p90"]["period_lock_orders_bias_corrected"])
        result["decision_summary"] = (
            f"截至 {pd.Timestamp(forecast_as_of).month}/{pd.Timestamp(forecast_as_of).day}；"
            f"预计未来 {int(forecast_days)} 天（bias 校正后）最可能值约 {mode:.2f}，"
            f"P50 约 {p50:.2f}，区间 [{p10:.2f}, {p90:.2f}]。"
        )

    content = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output:
        output_path = Path(str(args.output)).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(content, encoding="utf-8")
        print(str(output_path))
        if "decision_summary" in result:
            print(str(result["decision_summary"]))
        return
    print(content)


if __name__ == "__main__":
    main()

import argparse
import json
import math
import random
import statistics
import csv
import datetime
from pathlib import Path

LOCK_ORDERS_METRIC = "订单分析.锁单数"
LEADS_METRIC = "下发线索转化率.下发线索数"
LOCK_RATE_CANDIDATES = [
    "下发线索转化率.下发线索当30日锁单率",
    "下发线索转化率.下发线索当7日锁单率",
]


def _metrics(y_true: list[float], y_pred: list[float]) -> dict[str, float]:
    if not y_true:
        return {"mae": float("nan"), "rmse": float("nan"), "mape": float("nan"), "bias": float("nan")}
    ae = [abs(a - b) for a, b in zip(y_true, y_pred)]
    se = [(a - b) ** 2 for a, b in zip(y_true, y_pred)]
    nz = [(a, e) for a, e in zip(y_true, ae) if a != 0]
    if nz:
        mape = float(sum(e / abs(a) for a, e in nz) / len(nz))
    else:
        mape = float("nan")
    return {
        "mae": float(sum(ae) / len(ae)),
        "rmse": float(math.sqrt(sum(se) / len(se))),
        "mape": mape,
        "bias": float(sum((b - a) for a, b in zip(y_true, y_pred)) / len(y_true)),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    repo_root = Path(__file__).resolve().parents[1]
    parser.add_argument(
        "--matrix-csv",
        default=str(repo_root / "schema" / "index_summary_daily_matrix_2024-01-01_to_yesterday.csv"),
    )
    parser.add_argument(
        "--business-definition",
        default=str(repo_root / "schema" / "business_definition.json"),
    )
    parser.add_argument("--lookback-recent", type=int, default=30)
    parser.add_argument("--lookback-history", type=int, default=180)
    parser.add_argument("--min-horizon", type=int, default=7)
    parser.add_argument("--max-horizon", type=int, default=56)
    parser.add_argument("--eval-days", type=int, default=365)
    parser.add_argument("--sim-n", type=int, default=4000)
    parser.add_argument("--seed", type=int, default=20260319)
    parser.add_argument(
        "--output",
        default=str(repo_root / "out" / "structured_forecast_horizon_backtest.json"),
    )
    return parser.parse_args()


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None
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


def _parse_ymd(s: str) -> datetime.date | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _positive_daily_trend_delta(values: list[float]) -> float:
    if len(values) < 7:
        return 0.0
    x = list(range(len(values)))
    x_bar = sum(x) / len(x)
    y_bar = sum(values) / len(values)
    sxx = sum((xi - x_bar) ** 2 for xi in x)
    if sxx <= 0:
        return 0.0
    sxy = sum((xi - x_bar) * (yi - y_bar) for xi, yi in zip(x, values))
    slope = sxy / sxx
    if not (slope == slope):
        return 0.0
    return float(max(slope, 0.0))


def _load_activity_windows(business_definition: Path) -> list[tuple[str, datetime.date, datetime.date, datetime.date]]:
    raw = json.loads(business_definition.read_text(encoding="utf-8"))
    periods = raw.get("time_periods") or {}
    out: list[tuple[str, datetime.date, datetime.date, datetime.date]] = []
    for name, p in periods.items():
        if not isinstance(p, dict):
            continue
        start = _parse_ymd(str(p.get("start") or ""))
        end = _parse_ymd(str(p.get("end") or ""))
        finish = _parse_ymd(str(p.get("finish") or p.get("end") or ""))
        if start is None or end is None or finish is None:
            continue
        out.append((str(name), start, end, finish))
    return out


def _label_v1(d: datetime.date, activity_windows: list[tuple[str, datetime.date, datetime.date, datetime.date]]) -> str:
    for _, s, _, f in activity_windows:
        if s <= d <= f:
            return "activity"
    return "weekend" if d.weekday() >= 5 else "weekday"


def _label_v2(d: datetime.date, activity_windows: list[tuple[str, datetime.date, datetime.date, datetime.date]]) -> str:
    in_act = False
    for _, s, e, f in activity_windows:
        if not (s <= d <= f):
            continue
        in_act = True
        is_special = (s <= d <= (s + datetime.timedelta(days=2))) or ((e - datetime.timedelta(days=2)) <= d <= e) or (
            (f - datetime.timedelta(days=2)) <= d <= f
        )
        if d.weekday() >= 5 or is_special:
            return "activity_high_eff"
    if in_act:
        return "activity_low_eff"
    return "weekend" if d.weekday() >= 5 else "weekday"


def _bootstrap_p50(
    history_pairs_by_regime: dict[str, list[tuple[float, float]]],
    all_pairs: list[tuple[float, float]],
    future_dates: list[datetime.date],
    label_fn,
    activity_windows: list[tuple[str, datetime.date, datetime.date, datetime.date]],
    lead_daily_delta: float,
    rate_daily_delta: float,
    sim_n: int,
    seed: int,
) -> float:
    if not all_pairs:
        return 0.0
    n = int(max(1000, sim_n))
    rng = random.Random(int(seed))
    period_lock = [0.0] * n
    for day_i, d in enumerate(future_dates):
        reg = label_fn(d, activity_windows)
        pairs = history_pairs_by_regime.get(reg) or all_pairs
        m = len(pairs)
        if m <= 0:
            continue
        for s_i in range(n):
            ld, rt = pairs[rng.randrange(m)]
            ld = max(ld + lead_daily_delta * day_i, 0.0)
            rt = max(rt + rate_daily_delta * day_i, 0.0)
            period_lock[s_i] += ld * rt
    period_lock.sort()
    mid = len(period_lock) // 2
    if len(period_lock) % 2 == 1:
        return float(period_lock[mid])
    return float((period_lock[mid - 1] + period_lock[mid]) / 2.0)


def main() -> None:
    args = _parse_args()
    matrix_csv = Path(str(args.matrix_csv)).expanduser().resolve()
    business_definition = Path(str(args.business_definition)).expanduser().resolve()
    output_path = Path(str(args.output)).expanduser().resolve()

    activity_windows = _load_activity_windows(business_definition)

    with matrix_csv.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.reader(f)
        header = next(r)
        date_cols = [h.strip() for h in header[1:]]
        rows: dict[str, list[str]] = {}
        need = {LOCK_ORDERS_METRIC, LEADS_METRIC, *LOCK_RATE_CANDIDATES}
        for row in r:
            if not row:
                continue
            m = (row[0] or "").strip()
            if m in need:
                rows[m] = row

    rate_metric = None
    for c in LOCK_RATE_CANDIDATES:
        if c in rows:
            rate_metric = c
            break

    series: list[tuple[datetime.date, float, float, float]] = []
    for i, ds in enumerate(date_cols):
        d = _parse_ymd(ds)
        if d is None:
            continue
        lo = _to_float(rows.get(LOCK_ORDERS_METRIC, [""])[i + 1] if LOCK_ORDERS_METRIC in rows else None)
        ld = _to_float(rows.get(LEADS_METRIC, [""])[i + 1] if LEADS_METRIC in rows else None)
        if lo is None or ld is None:
            continue
        lr = _to_float(rows.get(rate_metric, [""])[i + 1]) if rate_metric else None
        if lr is None:
            lr = (float(lo) / float(ld)) if float(ld) != 0 else None
        if lr is None:
            continue
        if float(lo) < 0 or float(ld) < 0 or float(lr) < 0:
            continue
        series.append((d, float(lo), float(ld), float(lr)))

    series.sort(key=lambda t: t[0])
    all_dates = [d for d, _, _, _ in series]
    if int(args.eval_days) > 0 and len(all_dates) > int(args.eval_days):
        all_dates = all_dates[-int(args.eval_days) :]
    date_to_idx = {d: i for i, d in enumerate([x[0] for x in series])}

    summaries: list[dict[str, object]] = []
    for horizon in range(args.min_horizon, args.max_horizon + 1):
        y_true: list[float] = []
        y_pred_v1: list[float] = []
        y_pred_v2: list[float] = []
        for as_of in all_dates:
            as_of_idx = date_to_idx.get(as_of)
            if as_of_idx is None:
                continue
            future_end = as_of + datetime.timedelta(days=int(horizon))
            future_end_idx = date_to_idx.get(future_end)
            if future_end_idx is None:
                continue
            if future_end != as_of + datetime.timedelta(days=int(horizon)):
                continue
            if future_end_idx - as_of_idx != int(horizon):
                continue

            hist_start_idx = max(0, as_of_idx - (int(args.lookback_history) - 1))
            hist_idx = list(range(hist_start_idx, as_of_idx + 1))
            if len(hist_idx) < 60:
                continue
            recent_start_idx = max(hist_start_idx, as_of_idx - (int(args.lookback_recent) - 1))
            recent_idx = list(range(recent_start_idx, as_of_idx + 1))

            hist_pairs: list[tuple[datetime.date, float, float]] = []
            for j in hist_idx:
                d, lo, ld, lr = series[j]
                hist_pairs.append((d, ld, lr))

            recent_leads = [series[j][2] for j in recent_idx]
            recent_rates = [series[j][3] for j in recent_idx]
            lead_daily_delta = _positive_daily_trend_delta(recent_leads)
            rate_daily_delta = _positive_daily_trend_delta(recent_rates)

            pairs_by_v1: dict[str, list[tuple[float, float]]] = {}
            pairs_by_v2: dict[str, list[tuple[float, float]]] = {}
            all_pairs: list[tuple[float, float]] = []
            for d, ld, lr in hist_pairs:
                all_pairs.append((float(ld), float(lr)))
                pairs_by_v1.setdefault(_label_v1(d, activity_windows), []).append((float(ld), float(lr)))
                pairs_by_v2.setdefault(_label_v2(d, activity_windows), []).append((float(ld), float(lr)))

            future_dates = [as_of + datetime.timedelta(days=i) for i in range(1, int(horizon) + 1)]
            pred_v1 = _bootstrap_p50(
                history_pairs_by_regime=pairs_by_v1,
                all_pairs=all_pairs,
                future_dates=future_dates,
                label_fn=_label_v1,
                activity_windows=activity_windows,
                lead_daily_delta=lead_daily_delta,
                rate_daily_delta=rate_daily_delta,
                sim_n=int(args.sim_n),
                seed=int(args.seed),
            )
            pred_v2 = _bootstrap_p50(
                history_pairs_by_regime=pairs_by_v2,
                all_pairs=all_pairs,
                future_dates=future_dates,
                label_fn=_label_v2,
                activity_windows=activity_windows,
                lead_daily_delta=lead_daily_delta,
                rate_daily_delta=rate_daily_delta,
                sim_n=int(args.sim_n),
                seed=int(args.seed),
            )

            actual = float(sum(series[j][1] for j in range(as_of_idx + 1, future_end_idx + 1)))

            y_true.append(actual)
            y_pred_v1.append(float(pred_v1))
            y_pred_v2.append(float(pred_v2))

        if len(y_true) < 30:
            continue

        summaries.append(
            {
                "horizon_days": int(horizon),
                "samples": int(len(y_true)),
                "regime_bootstrap_p50_v1": _metrics(y_true, y_pred_v1),
                "regime_bootstrap_p50_v2": _metrics(y_true, y_pred_v2),
            }
        )

    if not summaries:
        raise RuntimeError("未生成可用回测结果，请检查参数")

    best_v1 = min(summaries, key=lambda x: x["regime_bootstrap_p50_v1"]["mape"])
    best_v2 = min(summaries, key=lambda x: x["regime_bootstrap_p50_v2"]["mape"])

    result = {
        "dataset": str(matrix_csv),
        "business_definition": str(business_definition),
        "lookback_recent": int(args.lookback_recent),
        "lookback_history": int(args.lookback_history),
        "sim_n": int(args.sim_n),
        "seed": int(args.seed),
        "horizon_range": [int(args.min_horizon), int(args.max_horizon)],
        "best_horizon_by_model": {
            "regime_bootstrap_p50_v1": {
                "horizon_days": int(best_v1["horizon_days"]),
                "samples": int(best_v1["samples"]),
                **best_v1["regime_bootstrap_p50_v1"],
            },
            "regime_bootstrap_p50_v2": {
                "horizon_days": int(best_v2["horizon_days"]),
                "samples": int(best_v2["samples"]),
                **best_v2["regime_bootstrap_p50_v2"],
            },
        },
        "all_horizon_metrics": summaries,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(output_path))


if __name__ == "__main__":
    main()

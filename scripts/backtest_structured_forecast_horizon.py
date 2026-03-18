import argparse
import importlib.util
import json
from pathlib import Path

import numpy as np
import pandas as pd


def _load_forecast_module(script_path: Path):
    spec = importlib.util.spec_from_file_location("structured_business_forecast", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载脚本: {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _metrics(y_true: list[float], y_pred: list[float]) -> dict[str, float]:
    y_true_arr = np.array(y_true, dtype=float)
    y_pred_arr = np.array(y_pred, dtype=float)
    ae = np.abs(y_true_arr - y_pred_arr)
    se = (y_true_arr - y_pred_arr) ** 2
    nz = y_true_arr != 0
    if nz.any():
        mape = float(np.mean(ae[nz] / np.abs(y_true_arr[nz])))
    else:
        mape = float("nan")
    return {
        "mae": float(np.mean(ae)),
        "rmse": float(np.sqrt(np.mean(se))),
        "mape": mape,
        "bias": float(np.mean(y_pred_arr - y_true_arr)),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    repo_root = Path(__file__).resolve().parents[1]
    parser.add_argument(
        "--forecast-script",
        default=str(repo_root / "scripts" / "structured_business_forecast.py"),
    )
    parser.add_argument(
        "--matrix-csv",
        default=str(repo_root / "schema" / "index_summary_daily_matrix_2024-01-01_to_yesterday.csv"),
    )
    parser.add_argument("--lookback-recent", type=int, default=30)
    parser.add_argument("--lookback-history", type=int, default=180)
    parser.add_argument("--min-horizon", type=int, default=7)
    parser.add_argument("--max-horizon", type=int, default=56)
    parser.add_argument("--eval-days", type=int, default=365)
    parser.add_argument(
        "--output",
        default=str(repo_root / "out" / "structured_forecast_horizon_backtest.json"),
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    forecast_script = Path(str(args.forecast_script)).expanduser().resolve()
    matrix_csv = Path(str(args.matrix_csv)).expanduser().resolve()
    output_path = Path(str(args.output)).expanduser().resolve()

    module = _load_forecast_module(forecast_script)
    ts_df = module._load_matrix(matrix_csv)
    series_df = module._build_series(ts_df)
    all_dates = pd.DatetimeIndex(series_df.index).sort_values()
    if args.eval_days > 0:
        all_dates = all_dates[-args.eval_days :]

    summaries: list[dict[str, object]] = []
    for horizon in range(args.min_horizon, args.max_horizon + 1):
        y_true: list[float] = []
        y_pred_enhanced: list[float] = []
        for as_of in all_dates:
            history_df = module._window(series_df, as_of, args.lookback_history)
            recent_df = module._window(series_df, as_of, args.lookback_recent)
            if history_df.empty or recent_df.empty:
                continue

            start = as_of + pd.Timedelta(days=1)
            end = as_of + pd.Timedelta(days=horizon)
            future_df = series_df[(series_df.index >= start) & (series_df.index <= end)]
            if len(future_df) != horizon:
                continue

            recent_leads = float(recent_df["leads"].mean())
            recent_rate = float(recent_df["lock_rate"].mean())
            lead_daily_delta = module._positive_daily_trend_delta(recent_df["leads"])
            rate_daily_delta = module._positive_daily_trend_delta(recent_df["lock_rate"])
            mature_df = history_df[history_df.index <= (as_of - pd.Timedelta(days=30))].copy()
            if mature_df.empty:
                mature_df = history_df.copy()

            enhanced = module._enhanced_assumption_projection(
                recent_leads=recent_leads,
                recent_rate=recent_rate,
                forecast_days=horizon,
                mature_df=mature_df,
                lead_daily_delta=lead_daily_delta,
                rate_daily_delta=rate_daily_delta,
            )

            pred_enhanced = float(enhanced["base"]["period_lock_orders"])
            actual = float(future_df["lock_orders"].sum())

            y_true.append(actual)
            y_pred_enhanced.append(pred_enhanced)

        if len(y_true) < 30:
            continue

        summaries.append(
            {
                "horizon_days": int(horizon),
                "samples": int(len(y_true)),
                "enhanced_assumption_based": _metrics(y_true, y_pred_enhanced),
            }
        )

    if not summaries:
        raise RuntimeError("未生成可用回测结果，请检查参数")

    best_enhanced = min(summaries, key=lambda x: x["enhanced_assumption_based"]["mape"])
    best_overall = {
        "model": "enhanced_assumption_based",
        "horizon_days": int(best_enhanced["horizon_days"]),
        "samples": int(best_enhanced["samples"]),
        **best_enhanced["enhanced_assumption_based"],
    }

    result = {
        "forecast_script": str(forecast_script),
        "dataset": str(matrix_csv),
        "lookback_recent": int(args.lookback_recent),
        "lookback_history": int(args.lookback_history),
        "horizon_range": [int(args.min_horizon), int(args.max_horizon)],
        "best_horizon_by_model": {
            "enhanced_assumption_based": {
                "horizon_days": int(best_enhanced["horizon_days"]),
                "samples": int(best_enhanced["samples"]),
                **best_enhanced["enhanced_assumption_based"],
            },
        },
        "best_overall": best_overall,
        "all_horizon_metrics": summaries,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(output_path))


if __name__ == "__main__":
    main()

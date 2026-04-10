import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


TARGET = "订单分析.锁单数"

METRICS = [
    "下发线索转化率.下发线索数",
    "下发线索转化率.下发线索当日试驾率",
    "下发线索转化率.下发 (门店)线索当日锁单率",
    "下发线索转化率.下发线索当7日锁单率",
    "下发线索转化率.下发线索当30日锁单率",
    "试驾分析.有效试驾数",
    "归因分析.平均触达次数",
    "归因分析.平均转化时长(天)",
    "订单分析.share_l6",
    "订单分析.share_ls6",
    "订单分析.share_ls9",
    "订单分析.share_reev",
    "订单分析.整体ATP(用户车,万元)",
    "订单分析.在营门店数",
    "订单分析.店均锁单数",
    "订单分析.CR5门店销量集中度",
    "订单分析.CR5门店城市销量集中度",
    "订单分析.店日均下发线索数",
    "derived.主渠道_自然客流占比",
    "derived.主渠道_直接大定占比",
    "derived.用户分类_One-Touch占比",
    "derived.用户分类_Cross-Channel占比",
]


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


def _spearman_corr_without_scipy(a: pd.Series, b: pd.Series) -> float:
    return float(a.rank(method="average").corr(b.rank(method="average"), method="pearson"))


def _extract_pct_from_json_row(
    row_series: pd.Series, key_field: str, key_value: str, value_field: str = "pct"
) -> pd.Series:
    out: dict[str, float | None] = {}
    for col, raw in row_series.items():
        if pd.isna(raw):
            out[str(col)] = None
            continue
        try:
            arr = json.loads(str(raw))
        except Exception:
            out[str(col)] = None
            continue
        pct_value: float | None = None
        if isinstance(arr, list):
            for item in arr:
                if not isinstance(item, dict):
                    continue
                if str(item.get(key_field, "")).strip() != key_value:
                    continue
                pct_value = _to_float(item.get(value_field))
                break
        out[str(col)] = pct_value
    s = pd.Series(out)
    s.index = pd.to_datetime(s.index, errors="coerce")
    s = s[~s.index.isna()].sort_index()
    return s


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--matrix-csv",
        default=str(
            Path(__file__).resolve().parents[1]
            / "schema"
            / "index_summary_daily_matrix_2024-01-01_to_yesterday.csv"
        ),
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parents[1] / "out" / "lock_order_correlation_validation.json"),
    )
    parser.add_argument("--backtest-days", type=int, default=120)
    return parser.parse_args()


def _regression_metrics(y_true: pd.Series, y_pred: pd.Series) -> dict[str, float]:
    ae = (y_true - y_pred).abs()
    se = (y_true - y_pred) ** 2
    mape_base = y_true.replace(0, np.nan).abs()
    mape = (ae / mape_base).dropna()
    return {
        "mae": float(ae.mean()),
        "rmse": float(np.sqrt(se.mean())),
        "mape": float(mape.mean()) if len(mape) > 0 else float("nan"),
    }


def _run_backtest(ts: pd.DataFrame, backtest_days: int) -> dict[str, object]:
    required = [TARGET, "下发线索转化率.下发线索数", "下发线索转化率.下发线索当30日锁单率", "试驾分析.有效试驾数"]
    for metric in required:
        if metric not in ts.columns:
            return {"enabled": False, "reason": f"缺少回测指标: {metric}"}

    df = ts[required].copy()
    df = df.dropna()
    if len(df) < 240:
        return {"enabled": False, "reason": "有效样本不足，至少需要 240 天"}

    test_size = min(backtest_days, max(60, len(df) // 4))
    split_idx = len(df) - test_size
    train = df.iloc[:split_idx].copy()
    test = df.iloc[split_idx:].copy()

    y_train = train[TARGET]
    y_test = test[TARGET]

    pred_naive = df[TARGET].shift(1).reindex(test.index)
    pred_identity = (
        df["下发线索转化率.下发线索数"] * df["下发线索转化率.下发线索当30日锁单率"]
    ).reindex(test.index)

    x_train = train[["下发线索转化率.下发线索数", "下发线索转化率.下发线索当30日锁单率", "试驾分析.有效试驾数"]].copy()
    x_test = test[["下发线索转化率.下发线索数", "下发线索转化率.下发线索当30日锁单率", "试驾分析.有效试驾数"]].copy()

    x_train_np = np.column_stack([np.ones(len(x_train)), x_train.values])
    y_train_np = y_train.values
    coef, *_ = np.linalg.lstsq(x_train_np, y_train_np, rcond=None)
    x_test_np = np.column_stack([np.ones(len(x_test)), x_test.values])
    pred_ols = pd.Series(x_test_np @ coef, index=x_test.index)
    pred_ols = pred_ols.clip(lower=0)

    metrics = {
        "naive_lag1_lock_orders": _regression_metrics(y_test, pred_naive),
        "identity_leads_x_30d_rate": _regression_metrics(y_test, pred_identity),
        "ols_leads_rate30_testdrive": _regression_metrics(y_test, pred_ols),
    }

    best_model = min(metrics.items(), key=lambda kv: kv[1]["mape"] if not np.isnan(kv[1]["mape"]) else float("inf"))[0]
    return {
        "enabled": True,
        "train_days": int(len(train)),
        "test_days": int(len(test)),
        "test_start": str(test.index.min().date()),
        "test_end": str(test.index.max().date()),
        "models": metrics,
        "best_by_mape": best_model,
        "ols_coefficients": {
            "intercept": float(coef[0]),
            "leads": float(coef[1]),
            "rate30": float(coef[2]),
            "test_drive": float(coef[3]),
        },
    }


def main() -> None:
    args = _parse_args()
    matrix_csv = Path(str(args.matrix_csv)).expanduser().resolve()
    output_path = Path(str(args.output)).expanduser().resolve()

    raw_text = pd.read_csv(matrix_csv, encoding="utf-8-sig").set_index("metric")
    if hasattr(raw_text, "map"):
        data = raw_text.map(_to_float)
    else:
        data = raw_text.applymap(_to_float)

    ts = data.T.copy()
    ts.index = pd.to_datetime(ts.index, errors="coerce")
    ts = ts[~ts.index.isna()].sort_index()

    if "归因分析.锁单用户主要渠道Top5" in raw_text.index:
        channel_row = raw_text.loc["归因分析.锁单用户主要渠道Top5"]
        ts["derived.主渠道_自然客流占比"] = _extract_pct_from_json_row(
            row_series=channel_row, key_field="channel", key_value="自然客流"
        ).reindex(ts.index)
        ts["derived.主渠道_直接大定占比"] = _extract_pct_from_json_row(
            row_series=channel_row, key_field="channel", key_value="直接大定"
        ).reindex(ts.index)

    if "归因分析.锁单用户分类占比（观察口径）" in raw_text.index:
        category_row = raw_text.loc["归因分析.锁单用户分类占比（观察口径）"]
        ts["derived.用户分类_One-Touch占比"] = _extract_pct_from_json_row(
            row_series=category_row, key_field="category", key_value="One-Touch (Decisive)"
        ).reindex(ts.index)
        ts["derived.用户分类_Cross-Channel占比"] = _extract_pct_from_json_row(
            row_series=category_row,
            key_field="category",
            key_value="Cross-Channel (Comparison Shopper)",
        ).reindex(ts.index)

    if TARGET not in ts.columns:
        raise ValueError(f"目标指标不存在: {TARGET}")
    y = ts[TARGET]

    details: list[dict[str, object]] = []
    for metric in METRICS:
        if metric not in ts.columns:
            details.append({"metric": metric, "exists": False})
            continue

        x = ts[metric]
        base_df = pd.concat([y, x], axis=1).dropna()
        if len(base_df) < 30:
            details.append(
                {
                    "metric": metric,
                    "exists": True,
                    "usable_samples": int(len(base_df)),
                    "note": "sample_too_small",
                }
            )
            continue

        pearson = float(base_df.iloc[:, 0].corr(base_df.iloc[:, 1], method="pearson"))
        spearman = _spearman_corr_without_scipy(base_df.iloc[:, 0], base_df.iloc[:, 1])

        best = {
            "lag_days": 0,
            "pearson": pearson,
            "abs_pearson": abs(pearson),
            "samples": int(len(base_df)),
        }
        for lag in range(1, 8):
            lag_df = pd.concat([y, x.shift(lag)], axis=1).dropna()
            if len(lag_df) < 30:
                continue
            c = float(lag_df.iloc[:, 0].corr(lag_df.iloc[:, 1], method="pearson"))
            if abs(c) > best["abs_pearson"]:
                best = {
                    "lag_days": lag,
                    "pearson": c,
                    "abs_pearson": abs(c),
                    "samples": int(len(lag_df)),
                }

        details.append(
            {
                "metric": metric,
                "exists": True,
                "usable_samples": int(len(base_df)),
                "pearson_same_day": pearson,
                "spearman_same_day": spearman,
                "best_lag_days": int(best["lag_days"]),
                "best_lag_pearson": float(best["pearson"]),
                "best_lag_abs_pearson": float(best["abs_pearson"]),
                "best_lag_samples": int(best["samples"]),
            }
        )

    df = pd.DataFrame(details)
    existing = df[df["exists"] == True].copy()
    existing = existing.sort_values("best_lag_abs_pearson", ascending=False)

    high = existing[existing["best_lag_abs_pearson"] >= 0.5]["metric"].tolist()
    medium = existing[
        (existing["best_lag_abs_pearson"] >= 0.3) & (existing["best_lag_abs_pearson"] < 0.5)
    ]["metric"].tolist()
    low = existing[existing["best_lag_abs_pearson"] < 0.3]["metric"].tolist()

    result = {
        "target_metric": TARGET,
        "dataset": str(matrix_csv),
        "date_range": {
            "start": str(ts.index.min().date()),
            "end": str(ts.index.max().date()),
            "days": int(len(ts)),
        },
        "summary": {
            "high_correlation_candidates_abs_ge_0_5": high,
            "medium_correlation_candidates_0_3_to_0_5": medium,
            "low_correlation_candidates_abs_lt_0_3": low,
        },
        "backtest": _run_backtest(ts=ts, backtest_days=int(args.backtest_days)),
        "details": details,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(output_path))


if __name__ == "__main__":
    main()

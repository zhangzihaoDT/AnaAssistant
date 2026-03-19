from __future__ import annotations

import json
from pathlib import Path
import importlib.util

import numpy as np
import pandas as pd


def spearman_like(a: pd.Series, b: pd.Series) -> float:
    x = pd.to_numeric(a, errors="coerce")
    y = pd.to_numeric(b, errors="coerce")
    m = (~x.isna()) & (~y.isna())
    x = x[m]
    y = y[m]
    if len(x) < 3:
        return float("nan")
    xr = x.rank(method="average").to_numpy(dtype=float)
    yr = y.rank(method="average").to_numpy(dtype=float)
    xr = xr - xr.mean()
    yr = yr - yr.mean()
    denom = (np.sqrt((xr**2).sum()) * np.sqrt((yr**2).sum()))
    if denom == 0:
        return float("nan")
    return float((xr * yr).sum() / denom)


def pct_strict(series: pd.Series, value: float | None) -> float | None:
    if value is None:
        return None
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return None
    return float((s < float(value)).mean())


def pct_rank_strict(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    m = ~s.isna()
    n = int(m.sum())
    out = pd.Series(index=series.index, dtype="float64")
    if n <= 0:
        return out
    rmin = s[m].rank(method="min")
    out[m] = (rmin - 1.0) / float(n)
    return out


def scan_conv30(
    df: pd.DataFrame,
    short_ratio: dict[str, float],
    sample_p: dict[str, float | None],
    w30_min: float = 0.30,
    w30_max: float = 0.90,
    w30_step: float = 0.05,
    w7_max: float = 0.35,
    w7_step: float = 0.05,
) -> pd.DataFrame:
    rows = []
    w30_grid = np.round(np.arange(w30_min, w30_max + 1e-9, w30_step), 4)
    for w30 in w30_grid:
        max_w7 = min(float(w7_max), 1.0 - float(w30))
        w7_grid = np.round(np.arange(0.0, max_w7 + 1e-9, w7_step), 4)
        for w7 in w7_grid:
            rem = 1.0 - float(w30) - float(w7)
            if rem < -1e-9:
                continue
            w0 = rem * short_ratio["store_lock0_rate"]
            wtd = rem * short_ratio["td0_rate"]

            score = (df["p_conv30"] * w30) + (df["p_conv7"] * w7) + (df["p_store_lock0_rate"] * w0) + (df["p_td0_rate"] * wtd)
            corr30 = spearman_like(score, df["p_conv30"])
            corr7 = spearman_like(score, df["p_conv7"])
            short_series = (df["p_store_lock0_rate"] * short_ratio["store_lock0_rate"]) + (df["p_td0_rate"] * short_ratio["td0_rate"])
            corr_short = spearman_like(score, short_series)
            obj = (0.7 * corr30) + (0.15 * corr7) + (0.15 * corr_short)

            sample_score = None
            if all(sample_p.get(k) is not None for k in ["p_conv30", "p_conv7", "p_store_lock0_rate", "p_td0_rate"]):
                sample_score = (
                    float(sample_p["p_conv30"]) * float(w30)
                    + float(sample_p["p_conv7"]) * float(w7)
                    + float(sample_p["p_store_lock0_rate"]) * float(w0)
                    + float(sample_p["p_td0_rate"]) * float(wtd)
                )

            rows.append(
                {
                    "w_conv30": float(w30),
                    "w_conv7": float(w7),
                    "w_store_lock0": float(round(w0, 4)),
                    "w_td0": float(round(wtd, 4)),
                    "corr_p30": float(corr30),
                    "corr_p7": float(corr7),
                    "corr_short": float(corr_short),
                    "obj": float(obj),
                    "sample_score": (None if sample_score is None else float(sample_score)),
                }
            )

    out = pd.DataFrame(rows)
    out = out.sort_values(["obj", "corr_p30", "w_conv30"], ascending=[False, False, False]).reset_index(drop=True)
    for c in ["corr_p30", "corr_p7", "corr_short", "obj", "sample_score"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").round(4)
    return out


def scan_conv7(
    df: pd.DataFrame,
    short_ratio: dict[str, float],
    sample_p: dict[str, float | None],
    w7_min: float = 0.20,
    w7_max: float = 0.90,
    w7_step: float = 0.05,
) -> pd.DataFrame:
    rows = []
    w7_grid = np.round(np.arange(w7_min, w7_max + 1e-9, w7_step), 4)
    for w7 in w7_grid:
        rem = 1.0 - float(w7)
        if rem < -1e-9:
            continue
        w0 = rem * short_ratio["store_lock0_rate"]
        wtd = rem * short_ratio["td0_rate"]

        score = (df["p_conv7"] * w7) + (df["p_store_lock0_rate"] * w0) + (df["p_td0_rate"] * wtd)
        corr7 = spearman_like(score, df["p_conv7"])
        short_series = (df["p_store_lock0_rate"] * short_ratio["store_lock0_rate"]) + (df["p_td0_rate"] * short_ratio["td0_rate"])
        corr_short = spearman_like(score, short_series)
        obj = (0.8 * corr7) + (0.2 * corr_short)

        sample_score = None
        if all(sample_p.get(k) is not None for k in ["p_conv7", "p_store_lock0_rate", "p_td0_rate"]):
            sample_score = (float(sample_p["p_conv7"]) * float(w7)) + (float(sample_p["p_store_lock0_rate"]) * float(w0)) + (float(sample_p["p_td0_rate"]) * float(wtd))

        rows.append(
            {
                "w_conv7": float(w7),
                "w_store_lock0": float(round(w0, 4)),
                "w_td0": float(round(wtd, 4)),
                "corr_p7": float(corr7),
                "corr_short": float(corr_short),
                "obj": float(obj),
                "sample_score": (None if sample_score is None else float(sample_score)),
            }
        )

    out = pd.DataFrame(rows)
    out = out.sort_values(["obj", "corr_p7", "w_conv7"], ascending=[False, False, False]).reset_index(drop=True)
    for c in ["corr_p7", "corr_short", "obj", "sample_score"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce").round(4)
    return out


def main() -> None:
    repo = Path(__file__).resolve().parents[1]
    engine_path = repo / ".trae" / "skills" / "index-summary-eval" / "scripts" / "evaluation_engine.py"
    spec = importlib.util.spec_from_file_location("evaluation_engine", engine_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)

    history_csv = repo / "schema" / "index_summary_daily_matrix_2024-01-01_to_yesterday.csv"
    biz = repo / "schema" / "business_definition.json"
    activity_ranges = mod._load_activity_ranges(biz)
    history_matrix = mod._read_daily_matrix_csv(history_csv)
    history_df = mod._history_from_matrix(history_matrix, activity_ranges)

    cols = ["store_lock0_rate", "td0_rate", "conv7", "conv30"]
    df = history_df[["date", "regime", *cols]].copy()
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["date"])
    for c in cols:
        df["p_" + c] = pct_rank_strict(df[c])

    sample_path = repo / "out" / "index_summary_2025-08-15.json"
    payload = json.loads(sample_path.read_text(encoding="utf-8"))
    row = mod._row_from_day_json(payload)
    sample = {
        "store_lock0_rate": mod._to_rate(row.get("store_lock0_rate")),
        "td0_rate": mod._to_rate(row.get("td0_rate")),
        "conv7": mod._to_rate(row.get("conv7")),
        "conv30": mod._to_rate(row.get("conv30")),
    }
    sample_p = {
        "p_store_lock0_rate": pct_strict(df["store_lock0_rate"], sample["store_lock0_rate"]),
        "p_td0_rate": pct_strict(df["td0_rate"], sample["td0_rate"]),
        "p_conv7": pct_strict(df["conv7"], sample["conv7"]),
        "p_conv30": pct_strict(df["conv30"], sample["conv30"]),
    }

    level_short = {"store_lock0_rate": 0.6, "td0_rate": 0.4}
    end_short = {"store_lock0_rate": 0.7, "td0_rate": 0.3}

    level_out = scan_conv30(df, level_short, sample_p)
    end_out = scan_conv30(df, end_short, sample_p)
    conv7_out = scan_conv7(df, level_short, sample_p)

    print("=== TOP 10 (level, lag>30) ===")
    print(level_out.head(10).to_string(index=False))
    print("\n=== TOP 10 (end, lag>30) ===")
    print(end_out.head(10).to_string(index=False))
    print("\n=== TOP 10 (lag>7, conv7 only) ===")
    print(conv7_out.head(10).to_string(index=False))

    print("\n=== SUGGESTED (level) ===")
    print(level_out.head(1)[["w_store_lock0", "w_td0", "w_conv7", "w_conv30", "obj", "corr_p30", "corr_p7", "corr_short", "sample_score"]].to_string(index=False))

    print("\n=== SUGGESTED (end) ===")
    print(end_out.head(1)[["w_store_lock0", "w_td0", "w_conv7", "w_conv30", "obj", "corr_p30", "corr_p7", "corr_short", "sample_score"]].to_string(index=False))

    print("\n=== SUGGESTED (lag>7, conv7 only) ===")
    print(conv7_out.head(1)[["w_store_lock0", "w_td0", "w_conv7", "obj", "corr_p7", "corr_short", "sample_score"]].to_string(index=False))


if __name__ == "__main__":
    main()

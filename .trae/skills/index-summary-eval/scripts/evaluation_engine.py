import argparse
import csv
import json
from pathlib import Path

import pandas as pd


REGIME_KEYS = ["activity_high_eff", "activity_low_eff", "weekday", "weekend"]
BASE_LEVEL_CONVERSION_WEIGHTS: dict[str, float] = {
    "store_lock0_rate": 0.6,
    "td0_rate": 0.4,
}

BASE_END_CONVERSION_WEIGHTS: dict[str, float] = {
    "store_lock0_rate": 0.7,
    "td0_rate": 0.3,
}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _default_history_csv() -> Path:
    return _repo_root() / "schema" / "index_summary_daily_matrix_2024-01-01_to_yesterday.csv"


def _default_business_definition() -> Path:
    return _repo_root() / "schema" / "business_definition.json"


def _parse_date(value: object) -> pd.Timestamp | None:
    if value is None:
        return None
    parsed = pd.to_datetime(str(value), errors="coerce")
    if pd.isna(parsed):
        return None
    return pd.Timestamp(parsed).normalize()


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if pd.isna(value):
            return None
        return float(value)
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    s = s.replace(",", "").replace("，", "")
    try:
        return float(s)
    except Exception:
        return None


def _to_rate(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if pd.isna(value):
            return None
        v = float(value)
        if v > 1.0:
            return v / 100.0
        return v
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("%"):
        try:
            return float(s[:-1].replace(",", "").replace("，", "")) / 100.0
        except Exception:
            return None
    raw = _to_float(s)
    if raw is None:
        return None
    if raw > 1.0:
        return raw / 100.0
    return raw


def _safe_ratio(numer: float | None, denom: float | None) -> float | None:
    if numer is None or denom is None:
        return None
    d = float(denom)
    if d == 0.0:
        return None
    return float(numer) / d


def _safe_round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 4)


def _percentile(value: float | None, history: pd.Series) -> float | None:
    if value is None:
        return None
    s = pd.to_numeric(history, errors="coerce").dropna()
    if s.empty:
        return None
    return float((s < float(value)).mean())


def _weighted_score(items: list[tuple[float | None, float]]) -> float | None:
    valid = [(v, w) for v, w in items if v is not None and w > 0]
    if not valid:
        return None
    total_w = sum(w for _, w in valid)
    if total_w <= 0:
        return None
    score = sum(float(v) * float(w) for v, w in valid) / total_w
    return float(score)


def _conversion_score(
    values: dict[str, float | None],
    history_df: pd.DataFrame,
    weights: dict[str, float],
) -> float | None:
    parts: list[tuple[float | None, float]] = []
    for key, w in weights.items():
        if key not in history_df.columns:
            continue
        parts.append((_percentile(values.get(key), history_df[key]), float(w)))
    return _weighted_score(parts)


def _conversion_weights_for_lag(lag_days: int | None, mode: str) -> dict[str, float]:
    mode = str(mode or "level").strip().lower()
    lag = None if lag_days is None else int(lag_days)
    if lag is None:
        return BASE_END_CONVERSION_WEIGHTS if mode == "end" else BASE_LEVEL_CONVERSION_WEIGHTS

    if lag > 30:
        if mode == "end":
            return {
                "store_lock0_rate": 0.119,
                "td0_rate": 0.051,
                "conv7": 0.13,
                "conv30": 0.7,
            }
        return {
            "store_lock0_rate": 0.102,
            "td0_rate": 0.068,
            "conv7": 0.13,
            "conv30": 0.7,
        }

    if lag > 7:
        if mode == "end":
            return {
                "store_lock0_rate": 0.6,
                "td0_rate": 0.15,
                "conv7": 0.25,
            }
        return {
            "store_lock0_rate": 0.55,
            "td0_rate": 0.25,
            "conv7": 0.2,
        }

    return BASE_END_CONVERSION_WEIGHTS if mode == "end" else BASE_LEVEL_CONVERSION_WEIGHTS


def _structure_scores(
    values: dict[str, float | None],
    history_df: pd.DataFrame,
) -> dict[str, float | None]:
    out: dict[str, float | None] = {}

    lead_quality_parts: list[float | None] = []
    for k in ["store_share", "live_share", "platform_share"]:
        if k in history_df.columns:
            lead_quality_parts.append(_percentile(values.get(k), history_df[k]))
    out["LeadQuality"] = _weighted_score([(p, 1.0) for p in lead_quality_parts])

    cr_parts: list[float | None] = []
    for k in ["cr5_store", "cr5_city"]:
        if k in history_df.columns:
            cr_parts.append(_percentile(values.get(k), history_df[k]))
    out["CRConcentration"] = _weighted_score([(p, 1.0) for p in cr_parts])

    return out


def _build_structure_state(scores: dict[str, float | None]) -> str:
    parts: list[str] = []
    for k in ["LeadQuality", "CRConcentration"]:
        parts.append(f"{k} {_level(scores.get(k))}")
    return ", ".join(parts)


def _weighted_distance(
    a: dict[str, float | None],
    b: dict[str, float | None],
    weights: dict[str, float],
) -> float | None:
    num = 0.0
    denom = 0.0
    for k, w in weights.items():
        av = a.get(k)
        bv = b.get(k)
        if av is None or bv is None:
            continue
        ww = float(w)
        num += ww * (float(av) - float(bv)) ** 2
        denom += ww
    if denom <= 0.0:
        return None
    return float((num / denom) ** 0.5)


def _scores_for_history_row(
    hist_row: pd.Series,
    history_df: pd.DataFrame,
    conversion_weights: dict[str, float],
) -> dict[str, float | None]:
    lock_cnt = _to_float(hist_row.get("lock_cnt"))
    leads = _to_float(hist_row.get("leads"))
    leads_store = _to_float(hist_row.get("leads_store"))

    store_lock0_rate = _to_rate(hist_row.get("store_lock0_rate"))
    td0_rate = _to_rate(hist_row.get("td0_rate"))
    conv7 = _to_rate(hist_row.get("conv7"))
    conv30 = _to_rate(hist_row.get("conv30"))

    store_share = _to_rate(hist_row.get("store_share"))
    live_share = _to_rate(hist_row.get("live_share"))
    platform_share = _to_rate(hist_row.get("platform_share"))

    cr5_store = _to_rate(hist_row.get("cr5_store"))
    cr5_city = _to_rate(hist_row.get("cr5_city"))

    volume_score = _weighted_score(
        [
            (_percentile(lock_cnt, history_df["lock_cnt"]), 0.6),
            (_percentile(leads, history_df["leads"]), 0.4),
        ]
    )

    conversion_score = _conversion_score(
        {"store_lock0_rate": store_lock0_rate, "td0_rate": td0_rate, "conv7": conv7, "conv30": conv30},
        history_df,
        conversion_weights,
    )

    structure = _structure_scores(
        {
            "store_share": store_share,
            "live_share": live_share,
            "platform_share": platform_share,
            "cr5_store": cr5_store,
            "cr5_city": cr5_city,
        },
        history_df,
    )

    return {
        "Volume": volume_score,
        "Conversion": conversion_score,
        "LeadQuality": structure.get("LeadQuality"),
        "CRConcentration": structure.get("CRConcentration"),
    }


def _find_peer_days(
    target_scores: dict[str, float | None],
    history_df: pd.DataFrame,
    exclude_date: str | None,
    conversion_weights: dict[str, float],
    top_k: int = 3,
) -> list[dict[str, object]]:
    weights = {"Volume": 1.0, "Conversion": 1.0, "LeadQuality": 0.6, "CRConcentration": 0.4}
    out: list[dict[str, object]] = []
    for _, r in history_df.iterrows():
        d = r.get("date")
        if isinstance(d, pd.Timestamp):
            d_str = str(pd.Timestamp(d).date())
        else:
            d_str = str(d) if d is not None else ""
        if exclude_date and d_str == exclude_date:
            continue
        cand_scores = _scores_for_history_row(r, history_df, conversion_weights=conversion_weights)
        dist = _weighted_distance(target_scores, cand_scores, weights)
        if dist is None:
            continue
        out.append(
            {
                "date": d_str,
                "distance": round(float(dist), 4),
                "regime": str(r.get("regime") or ""),
            }
        )
    out.sort(key=lambda x: float(x.get("distance") or 0.0))
    return out[: int(top_k)]


def _calc_trend_delta(series: list[float | None], head_n: int = 3, tail_n: int = 3) -> float | None:
    if len(series) < head_n + tail_n:
        return None
    head = [v for v in series[:head_n] if v is not None]
    tail = [v for v in series[-tail_n:] if v is not None]
    if not head or not tail:
        return None
    head_mean = float(sum(head)) / float(len(head))
    tail_mean = float(sum(tail)) / float(len(tail))
    return float(tail_mean - head_mean)


def _classify_trend(delta: float | None, threshold: float = 0.05) -> str:
    if delta is None:
        return "unknown"
    if delta > threshold:
        return "up"
    if delta < -threshold:
        return "down"
    return "flat"


def _build_trend(series: list[float | None], head_n: int = 3, tail_n: int = 3, threshold: float = 0.05) -> dict[str, object] | None:
    if len(series) < head_n + tail_n:
        return None
    delta = _calc_trend_delta(series, head_n=head_n, tail_n=tail_n)
    return {
        "direction": _classify_trend(delta, threshold=threshold),
        "strength": (None if delta is None else round(float(delta), 3)),
    }


def _level(p: float | None) -> str:
    if p is None:
        return "Mid"
    if p >= 0.7:
        return "High"
    if p <= 0.3:
        return "Low"
    return "Mid"


def _build_state(volume_score: float | None, conversion_score: float | None) -> str:
    return f"{_level(volume_score)} Volume + {_level(conversion_score)} Conversion"


def _load_activity_ranges(path: Path) -> list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    periods = raw.get("time_periods") or {}
    out: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]] = []
    for name, p in periods.items():
        if not isinstance(p, dict):
            continue
        s = _parse_date(p.get("start"))
        e = _parse_date(p.get("end"))
        f = _parse_date(p.get("finish") or p.get("end"))
        if s is None or e is None or f is None:
            continue
        out.append((str(name), s, e, f))
    return out


def _regime_label_for_date(d: pd.Timestamp, activity_ranges: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]]) -> str:
    dn = pd.Timestamp(d).normalize()
    in_activity = False
    for _, s, e, f in activity_ranges:
        if not (s <= dn <= f):
            continue
        in_activity = True
        is_special = (
            (s <= dn <= (s + pd.Timedelta(days=2)))
            or ((e - pd.Timedelta(days=2)) <= dn <= e)
            or ((f - pd.Timedelta(days=2)) <= dn <= f)
        )
        if dn.weekday() >= 5 or is_special:
            return "activity_high_eff"
    if in_activity:
        return "activity_low_eff"
    if dn.weekday() >= 5:
        return "weekend"
    return "weekday"


def _read_daily_matrix_csv(path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            return {"columns": [], "rows": []}
        columns = [str(c).strip() for c in header[1:]]
        rows: list[dict[str, object]] = []
        for line in reader:
            if not line:
                continue
            metric = str(line[0]).strip()
            values = list(line[1:])
            if len(values) < len(columns):
                values.extend([None] * (len(columns) - len(values)))
            elif len(values) > len(columns):
                values = values[: len(columns)]
            rows.append({"metric": metric, "values": values})
    return {"columns": columns, "rows": rows}


def _rows_from_matrix(matrix: dict[str, object]) -> list[dict[str, object]]:
    columns = [str(c) for c in (matrix.get("columns") or [])]
    row_list = matrix.get("rows") or []
    metric_map: dict[str, list[object]] = {}
    if isinstance(row_list, list):
        for row in row_list:
            if not isinstance(row, dict):
                continue
            metric = str(row.get("metric") or "").strip()
            values = row.get("values")
            if not metric or not isinstance(values, list):
                continue
            metric_map[metric] = values

    out: list[dict[str, object]] = []
    for idx, date_str in enumerate(columns):
        date = _parse_date(date_str)
        if date is None:
            continue
        lock_raw = metric_map.get("订单分析.锁单数", [None] * len(columns))
        leads_raw = metric_map.get("下发线索转化率.下发线索数", [None] * len(columns))
        leads_store_raw = metric_map.get("下发线索转化率.下发线索数 (门店)", [None] * len(columns))
        store_lock0_rate_raw = metric_map.get("下发线索转化率.下发 (门店)线索当日锁单率", [None] * len(columns))
        td0_rate_raw = metric_map.get("下发线索转化率.下发线索当日试驾率", [None] * len(columns))
        conv7_raw = metric_map.get("下发线索转化率.下发线索当7日锁单率", [None] * len(columns))
        conv30_raw = metric_map.get("下发线索转化率.下发线索当30日锁单率", [None] * len(columns))
        store_share_raw = metric_map.get("下发线索转化率.门店线索占比", [None] * len(columns))
        leads_live_raw = metric_map.get("下发线索转化率.下发线索数（直播）", [None] * len(columns))
        leads_platform_raw = metric_map.get("下发线索转化率.下发线索数（平台)", [None] * len(columns))
        cr5_store_raw = metric_map.get("订单分析.CR5门店销量集中度", [None] * len(columns))
        cr5_city_raw = metric_map.get("订单分析.CR5门店城市销量集中度", [None] * len(columns))
        lock_value = lock_raw[idx] if idx < len(lock_raw) else None
        leads_value = leads_raw[idx] if idx < len(leads_raw) else None
        leads_store_value = leads_store_raw[idx] if idx < len(leads_store_raw) else None
        store_lock0_rate_value = store_lock0_rate_raw[idx] if idx < len(store_lock0_rate_raw) else None
        td0_rate_value = td0_rate_raw[idx] if idx < len(td0_rate_raw) else None
        conv7_value = conv7_raw[idx] if idx < len(conv7_raw) else None
        conv30_value = conv30_raw[idx] if idx < len(conv30_raw) else None
        store_share_value = store_share_raw[idx] if idx < len(store_share_raw) else None
        leads_live_value = leads_live_raw[idx] if idx < len(leads_live_raw) else None
        leads_platform_value = leads_platform_raw[idx] if idx < len(leads_platform_raw) else None
        cr5_store_value = cr5_store_raw[idx] if idx < len(cr5_store_raw) else None
        cr5_city_value = cr5_city_raw[idx] if idx < len(cr5_city_raw) else None
        out.append(
            {
                "date": str(date.date()),
                "lock_cnt": _to_float(lock_value),
                "leads": _to_float(leads_value),
                "leads_store": _to_float(leads_store_value),
                "store_lock0_rate": _to_rate(store_lock0_rate_value),
                "td0_rate": _to_rate(td0_rate_value),
                "conv7": _to_rate(conv7_value),
                "conv30": _to_rate(conv30_value),
                "store_share": _to_rate(store_share_value),
                "leads_live": _to_float(leads_live_value),
                "leads_platform": _to_float(leads_platform_value),
                "cr5_store": _to_rate(cr5_store_value),
                "cr5_city": _to_rate(cr5_city_value),
            }
        )
    return out


def _row_from_day_json(day_obj: dict[str, object]) -> dict[str, object]:
    order = day_obj.get("订单分析") if isinstance(day_obj.get("订单分析"), dict) else {}
    assign = day_obj.get("下发线索转化率") if isinstance(day_obj.get("下发线索转化率"), dict) else {}
    return {
        "date": str(day_obj.get("date")),
        "lock_cnt": _to_float((order or {}).get("锁单数")),
        "leads": _to_float((assign or {}).get("下发线索数")),
        "leads_store": _to_float((assign or {}).get("下发线索数 (门店)")),
        "store_lock0_rate": _to_rate((assign or {}).get("下发 (门店)线索当日锁单率")),
        "td0_rate": _to_rate((assign or {}).get("下发线索当日试驾率")),
        "conv7": _to_rate((assign or {}).get("下发线索当7日锁单率")),
        "conv30": _to_rate((assign or {}).get("下发线索当30日锁单率")),
        "store_share": _to_rate((assign or {}).get("门店线索占比")),
        "leads_live": _to_float((assign or {}).get("下发线索数（直播）")),
        "leads_platform": _to_float((assign or {}).get("下发线索数（平台)")),
        "cr5_store": _to_rate((order or {}).get("CR5门店销量集中度")),
        "cr5_city": _to_rate((order or {}).get("CR5门店城市销量集中度")),
    }


def _history_from_matrix(matrix: dict[str, object], activity_ranges: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]]) -> pd.DataFrame:
    rows = _rows_from_matrix(matrix)
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "lock_cnt",
                "leads",
                "leads_store",
                "store_lock0_rate",
                "td0_rate",
                "conv7",
                "conv30",
                "store_share",
                "leads_live",
                "leads_platform",
                "live_share",
                "platform_share",
                "cr5_store",
                "cr5_city",
                "regime",
            ]
        )
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.normalize()
    df["lock_cnt"] = pd.to_numeric(df["lock_cnt"], errors="coerce")
    df["leads"] = pd.to_numeric(df["leads"], errors="coerce")
    df["leads_store"] = pd.to_numeric(df["leads_store"], errors="coerce")
    df["store_lock0_rate"] = pd.to_numeric(df["store_lock0_rate"], errors="coerce")
    df["td0_rate"] = pd.to_numeric(df["td0_rate"], errors="coerce")
    df["conv7"] = pd.to_numeric(df.get("conv7"), errors="coerce")
    df["conv30"] = pd.to_numeric(df.get("conv30"), errors="coerce")
    df["store_share"] = pd.to_numeric(df.get("store_share"), errors="coerce")
    df["leads_live"] = pd.to_numeric(df.get("leads_live"), errors="coerce")
    df["leads_platform"] = pd.to_numeric(df.get("leads_platform"), errors="coerce")
    df["cr5_store"] = pd.to_numeric(df.get("cr5_store"), errors="coerce")
    df["cr5_city"] = pd.to_numeric(df.get("cr5_city"), errors="coerce")
    df["live_share"] = df.apply(lambda r: _safe_ratio(r.get("leads_live"), r.get("leads")), axis=1)
    df["platform_share"] = df.apply(lambda r: _safe_ratio(r.get("leads_platform"), r.get("leads")), axis=1)
    df = df.dropna(subset=["date"])
    df["regime"] = df["date"].map(lambda d: _regime_label_for_date(pd.Timestamp(d), activity_ranges))
    return df.sort_values("date").reset_index(drop=True)


def _evaluate_row(row: dict[str, object], history_df: pd.DataFrame, activity_ranges: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]]) -> dict[str, object]:
    date = _parse_date(row.get("date"))
    lock_cnt = _to_float(row.get("lock_cnt"))
    leads = _to_float(row.get("leads"))
    leads_store = _to_float(row.get("leads_store"))
    store_lock0_rate = _to_rate(row.get("store_lock0_rate"))
    td0_rate = _to_rate(row.get("td0_rate"))
    conv7 = _to_rate(row.get("conv7"))
    conv30 = _to_rate(row.get("conv30"))
    store_share = _to_rate(row.get("store_share"))
    leads_live = _to_float(row.get("leads_live"))
    leads_platform = _to_float(row.get("leads_platform"))
    live_share = _safe_ratio(leads_live, leads)
    platform_share = _safe_ratio(leads_platform, leads)
    cr5_store = _to_rate(row.get("cr5_store"))
    cr5_city = _to_rate(row.get("cr5_city"))

    conv_values: dict[str, float | None] = {
        "store_lock0_rate": store_lock0_rate,
        "td0_rate": td0_rate,
        "conv7": conv7,
        "conv30": conv30,
    }

    global_lock_p = _percentile(lock_cnt, history_df["lock_cnt"])
    global_leads_p = _percentile(leads, history_df["leads"])

    volume_score = _weighted_score([(global_lock_p, 0.6), (global_leads_p, 0.4)])
    as_of = pd.Timestamp.today().normalize()
    lag_days = None if date is None else int((as_of - pd.Timestamp(date).normalize()).days)
    conversion_score = _conversion_score(conv_values, history_df, _conversion_weights_for_lag(lag_days, mode="level"))
    current_regime = _regime_label_for_date(date, activity_ranges) if date is not None else "weekday"

    structure_scores = _structure_scores(
        {
            "store_share": store_share,
            "live_share": live_share,
            "platform_share": platform_share,
            "cr5_store": cr5_store,
            "cr5_city": cr5_city,
        },
        history_df,
    )

    regime_eval: dict[str, dict[str, float | None]] = {
        "global": {
            "Volume": _safe_round(volume_score),
            "Conversion": _safe_round(conversion_score),
        }
    }
    for regime in REGIME_KEYS:
        subset = history_df[history_df["regime"] == regime]
        if subset.empty:
            subset = history_df
        rp_lock = _percentile(lock_cnt, subset["lock_cnt"])
        rp_leads = _percentile(leads, subset["leads"])
        rp_volume = _weighted_score([(rp_lock, 0.6), (rp_leads, 0.4)])
        rp_conv = _conversion_score(conv_values, subset, _conversion_weights_for_lag(lag_days, mode="level"))
        regime_eval[regime] = {
            "Volume": _safe_round(rp_volume),
            "Conversion": _safe_round(rp_conv),
        }

    diagnosis: list[str] = []
    if volume_score is not None and volume_score > 0.7:
        diagnosis.append("流量高位")
    if conversion_score is not None and conversion_score < 0.3:
        diagnosis.append("转化偏低")
    if current_regime == "activity_high_eff" and conversion_score is not None and conversion_score < 0.3:
        diagnosis.append("活动期转化异常")

    return {
        "scope": "day",
        "date": (None if date is None else str(date.date())),
        "factor_scores": {
            "Volume": _safe_round(volume_score),
            "Conversion": _safe_round(conversion_score),
        },
        "state": _build_state(volume_score, conversion_score),
        "regime": current_regime,
        "regime_eval": regime_eval,
        "structure": {
            "scores": {k: _safe_round(v) for k, v in structure_scores.items()},
            "state": _build_structure_state(structure_scores),
        },
        "peer_days": _find_peer_days(
            {
                "Volume": volume_score,
                "Conversion": conversion_score,
                "LeadQuality": structure_scores.get("LeadQuality"),
                "CRConcentration": structure_scores.get("CRConcentration"),
            },
            history_df,
            exclude_date=(None if date is None else str(date.date())),
            conversion_weights=_conversion_weights_for_lag(lag_days, mode="level"),
        ),
        "diagnosis": diagnosis,
        "raw_metrics": {
            "lock_cnt": lock_cnt,
            "leads": leads,
            "leads_store": leads_store,
            "store_lock0_rate": _safe_round(store_lock0_rate),
            "td0_rate": _safe_round(td0_rate),
            "conv7": _safe_round(conv7),
            "conv30": _safe_round(conv30),
            "store_share": _safe_round(store_share),
            "live_share": _safe_round(live_share),
            "platform_share": _safe_round(platform_share),
            "cr5_store": _safe_round(cr5_store),
            "cr5_city": _safe_round(cr5_city),
        },
    }


def _median_or_none(values: list[float | None]) -> float | None:
    s = pd.Series(values, dtype="float64").dropna()
    if s.empty:
        return None
    return float(s.median())


def _mean_or_none(values: list[float | None]) -> float | None:
    s = pd.Series(values, dtype="float64").dropna()
    if s.empty:
        return None
    return float(s.mean())


def _evaluate_interval(rows: list[dict[str, object]], history_df: pd.DataFrame, activity_ranges: list[tuple[str, pd.Timestamp, pd.Timestamp, pd.Timestamp]]) -> dict[str, object]:
    day_evals = [_evaluate_row(r, history_df, activity_ranges) for r in rows]
    day_evals = [d for d in day_evals if isinstance(d, dict)]
    if not day_evals:
        return {
            "scope": "interval",
            "factor_scores": {"Volume": None, "Conversion": None},
            "state": "Mid Volume + Mid Conversion",
            "regime_eval": {},
            "diagnosis": [],
            "level": {"Volume": None, "Conversion": None},
            "end_state": {"Volume": None, "Conversion": None},
            "days_count": 0,
        }

    day_volume = [d.get("factor_scores", {}).get("Volume") for d in day_evals]
    day_conv = [d.get("factor_scores", {}).get("Conversion") for d in day_evals]
    day_lq = [((d.get("structure") or {}).get("scores") or {}).get("LeadQuality") for d in day_evals]
    day_cr = [((d.get("structure") or {}).get("scores") or {}).get("CRConcentration") for d in day_evals]
    level_volume = _median_or_none(day_volume)
    level_conv = _median_or_none(day_conv)
    level_lq = _median_or_none(day_lq)
    level_cr = _median_or_none(day_cr)

    trend = {
        "Volume": _build_trend(day_volume),
        "Conversion": _build_trend(day_conv),
        "LeadQuality": _build_trend(day_lq),
        "CRConcentration": _build_trend(day_cr),
    }

    last3 = day_evals[-3:]
    end_volume = _mean_or_none([d.get("factor_scores", {}).get("Volume") for d in last3])

    last3_rows = rows[-3:]
    end_conv_values: list[float | None] = []
    for r in last3_rows:
        d = _parse_date(r.get("date"))
        as_of = pd.Timestamp.today().normalize()
        lag_days = None if d is None else int((as_of - pd.Timestamp(d).normalize()).days)
        conv_values = {
            "store_lock0_rate": _to_rate(r.get("store_lock0_rate")),
            "td0_rate": _to_rate(r.get("td0_rate")),
            "conv7": _to_rate(r.get("conv7")),
            "conv30": _to_rate(r.get("conv30")),
        }
        end_conv_values.append(_conversion_score(conv_values, history_df, _conversion_weights_for_lag(lag_days, mode="end")))
    end_conv = _mean_or_none(end_conv_values)

    end_lq = _mean_or_none([v for v in day_lq[-3:]])
    end_cr = _mean_or_none([v for v in day_cr[-3:]])

    regime_names: set[str] = set()
    for d in day_evals:
        rv = d.get("regime_eval")
        if isinstance(rv, dict):
            regime_names.update(rv.keys())

    interval_regime_eval: dict[str, dict[str, float | None]] = {}
    for name in sorted(regime_names):
        v_list: list[float | None] = []
        c_list: list[float | None] = []
        for d in day_evals:
            rv = d.get("regime_eval")
            if not isinstance(rv, dict):
                continue
            item = rv.get(name)
            if not isinstance(item, dict):
                continue
            v_list.append(item.get("Volume"))
            c_list.append(item.get("Conversion"))
        interval_regime_eval[name] = {
            "Volume": _safe_round(_median_or_none(v_list)),
            "Conversion": _safe_round(_median_or_none(c_list)),
        }

    last_regime = str(day_evals[-1].get("regime") or "")
    diagnosis: list[str] = []
    if end_volume is not None and end_volume > 0.7:
        diagnosis.append("流量高位")
    if end_conv is not None and end_conv < 0.3:
        diagnosis.append("转化偏低")
    if last_regime == "activity_high_eff" and end_conv is not None and end_conv < 0.3:
        diagnosis.append("活动期转化异常")

    return {
        "scope": "interval",
        "factor_scores": {
            "Volume": _safe_round(level_volume),
            "Conversion": _safe_round(level_conv),
        },
        "state": _build_state(end_volume, end_conv),
        "regime_eval": interval_regime_eval,
        "diagnosis": diagnosis,
        "trend": trend,
        "structure": {
            "level": {"LeadQuality": _safe_round(level_lq), "CRConcentration": _safe_round(level_cr)},
            "end_state": {"LeadQuality": _safe_round(end_lq), "CRConcentration": _safe_round(end_cr)},
            "state": _build_structure_state({"LeadQuality": end_lq, "CRConcentration": end_cr}),
        },
        "peer_days": _find_peer_days(
            {
                "Volume": end_volume,
                "Conversion": end_conv,
                "LeadQuality": end_lq,
                "CRConcentration": end_cr,
            },
            history_df,
            exclude_date=str(day_evals[-1].get("date") or ""),
            conversion_weights=_conversion_weights_for_lag(
                None
                if _parse_date(day_evals[-1].get("date")) is None
                else int((pd.Timestamp.today().normalize() - pd.Timestamp(_parse_date(day_evals[-1].get("date"))).normalize()).days),
                mode="end",
            ),
        ),
        "level": {
            "Volume": _safe_round(level_volume),
            "Conversion": _safe_round(level_conv),
        },
        "end_state": {
            "Volume": _safe_round(end_volume),
            "Conversion": _safe_round(end_conv),
        },
        "days_count": len(day_evals),
        "start": day_evals[0].get("date"),
        "end": day_evals[-1].get("date"),
    }


def _load_input_json(path: Path | None) -> dict[str, object]:
    if path is not None:
        return json.loads(path.read_text(encoding="utf-8"))
    return json.loads(input())


def _detect_scope(payload: dict[str, object], preferred_scope: str) -> str:
    if preferred_scope in {"day", "interval"}:
        return preferred_scope
    if isinstance(payload.get("date"), str) and isinstance(payload.get("订单分析"), dict):
        return "day"
    if isinstance(payload.get("start"), str) and isinstance(payload.get("end"), str):
        return "interval"
    if isinstance(payload.get("daily_metrics_matrix"), dict):
        return "interval"
    raise ValueError("无法自动识别输入类型，请使用 --scope day|interval 指定")


def _rows_for_interval(payload: dict[str, object]) -> list[dict[str, object]]:
    days = payload.get("days")
    if isinstance(days, list) and days:
        out = []
        for d in days:
            if isinstance(d, dict):
                out.append(_row_from_day_json(d))
        if out:
            return out
    matrix = payload.get("daily_metrics_matrix")
    if isinstance(matrix, dict):
        return _rows_from_matrix(matrix)
    raise ValueError("区间评估需要 days 或 daily_metrics_matrix")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-json", default=None)
    parser.add_argument("--scope", choices=["auto", "day", "interval"], default="auto")
    parser.add_argument("--history-csv", default=str(_default_history_csv()))
    parser.add_argument("--business-definition", default=str(_default_business_definition()))
    args = parser.parse_args()

    payload = _load_input_json(None if args.input_json is None else Path(str(args.input_json)).expanduser().resolve())
    activity_ranges = _load_activity_ranges(Path(str(args.business_definition)).expanduser().resolve())
    history_matrix = _read_daily_matrix_csv(Path(str(args.history_csv)).expanduser().resolve())
    history_df = _history_from_matrix(history_matrix, activity_ranges)
    if history_df.empty:
        raise RuntimeError("历史矩阵为空，无法计算分位")

    scope = _detect_scope(payload, args.scope)
    if scope == "day":
        row = _row_from_day_json(payload)
        out = _evaluate_row(row, history_df, activity_ranges)
    else:
        rows = _rows_for_interval(payload)
        out = _evaluate_interval(rows, history_df, activity_ranges)

    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

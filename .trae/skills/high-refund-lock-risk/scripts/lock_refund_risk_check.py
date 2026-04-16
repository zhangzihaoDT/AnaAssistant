import argparse
import json
from pathlib import Path
import re
import glob

import pandas as pd


def _parse_target_date(value: str) -> pd.Timestamp:
    value = str(value).strip()
    m = re.fullmatch(r"(\d{4})-(\d{1,2})-(\d{1,2})", value)
    if m:
        y, mo, d = map(int, m.groups())
        return pd.Timestamp(year=y, month=mo, day=d).normalize()
    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        raise ValueError(f"无法解析日期: {value}")
    return pd.Timestamp(parsed).normalize()


def _read_data_paths(md_path: Path) -> dict[str, Path]:
    raw = md_path.read_text(encoding="utf-8").splitlines()
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


def _load_profile(profile_json: Path) -> dict:
    return json.loads(profile_json.read_text(encoding="utf-8"))


def _safe_share(mask: pd.Series) -> float | None:
    if mask is None or mask.empty:
        return None
    denom = float(mask.shape[0])
    if denom == 0.0:
        return None
    return float(mask.mean())


def _describe_hours(delta: pd.Series) -> dict[str, float | None]:
    delta = delta.dropna()
    if delta.empty:
        return {"mean": None, "median": None, "p25": None, "p75": None}
    return {
        "mean": float(delta.mean()),
        "median": float(delta.median()),
        "p25": float(delta.quantile(0.25)),
        "p75": float(delta.quantile(0.75)),
    }


def _bins_count_days(days: pd.Series) -> dict[str, int]:
    days = days.dropna()
    if days.empty:
        return {}
    bins = pd.cut(days, bins=10)
    out: dict[str, int] = {}
    vc = bins.value_counts().sort_index()
    for k, v in vc.items():
        out[str(k)] = int(v)
    return out


def _top_table(df: pd.DataFrame, group_col: str, flag_col: str, top_n: int) -> list[dict]:
    if df.empty or group_col not in df.columns:
        return []
    g = df.groupby(group_col)[flag_col].agg(["count", "sum"]).rename(
        columns={"count": "locked", "sum": "flagged"}
    )
    g["flag_share"] = g["flagged"] / g["locked"]
    g = g.sort_values(["flagged", "locked"], ascending=[False, False]).head(top_n)
    out: list[dict] = []
    for idx, row in g.reset_index().iterrows():
        out.append(
            {
                group_col: row[group_col],
                "locked": int(row["locked"]),
                "flagged": int(row["flagged"]),
                "flag_share": float(row["flag_share"]),
            }
        )
    return out


def _subset_metrics(df_user: pd.DataFrame) -> dict:
    if df_user.empty:
        return {
            "cnt": 0,
            "final_payment_way_na_share": None,
            "finance_product_na_share": None,
            "gender_unknown_share": None,
            "owner_age_mean": None,
            "owner_age_median": None,
            "intention_to_lock_hours": {"mean": None, "median": None, "p25": None, "p75": None},
        }

    intention_time = pd.to_datetime(df_user["intention_payment_time"], errors="coerce")
    intention_to_lock_hours = (
        (pd.to_datetime(df_user["lock_time"], errors="coerce") - intention_time).dt.total_seconds()
        / 3600.0
    )
    return {
        "cnt": int(df_user["order_number"].nunique()) if "order_number" in df_user.columns else int(df_user.shape[0]),
        "final_payment_way_na_share": _safe_share(df_user["final_payment_way_na"]),
        "finance_product_na_share": _safe_share(df_user["finance_product_na"]),
        "gender_unknown_share": _safe_share(df_user["gender_unknown"]),
        "owner_age_mean": float(df_user["owner_age"].mean()) if "owner_age" in df_user.columns else None,
        "owner_age_median": float(df_user["owner_age"].median()) if "owner_age" in df_user.columns else None,
        "intention_to_lock_hours": _describe_hours(intention_to_lock_hours),
    }


def _top_cancel_rate_table(df: pd.DataFrame, group_col: str, cancel_col: str, top_n: int) -> list[dict]:
    if df.empty or group_col not in df.columns:
        return []
    g = df.groupby(group_col)[cancel_col].agg(["count", "sum"]).rename(
        columns={"count": "locked", "sum": "canceled"}
    )
    g["cancel_rate"] = g["canceled"] / g["locked"]
    g = g.sort_values(["canceled", "cancel_rate", "locked"], ascending=[False, False, False]).head(top_n)
    out: list[dict] = []
    for _, row in g.reset_index().iterrows():
        out.append(
            {
                group_col: row[group_col],
                "locked": int(row["locked"]),
                "canceled": int(row["canceled"]),
                "cancel_rate": float(row["cancel_rate"]),
            }
        )
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    parser.add_argument("--order-parquet", default=None)
    parser.add_argument("--data-path-md", default="schema/data_path.md")
    parser.add_argument(
        "--profile-json",
        default=".trae/skills/high-refund-lock-risk/references/profile_2025-11-12.json",
    )
    parser.add_argument("--print-json", action="store_true")
    args = parser.parse_args()

    target_date = _parse_target_date(args.date)
    start = target_date.normalize()
    end = start + pd.Timedelta(days=1)

    profile_path = Path(args.profile_json)
    profile = _load_profile(profile_path)
    thresholds = profile.get("thresholds", {}) or {}
    watchlist_stores = set(profile.get("watchlist_stores", []) or [])

    if args.order_parquet:
        order_path = Path(args.order_parquet)
    else:
        data_paths = _read_data_paths(Path(args.data_path_md))
        if "订单分析" not in data_paths:
            raise ValueError(f"在 {args.data_path_md} 中找不到 订单分析 的数据路径")
        order_path = data_paths["订单分析"]

    cols = [
        "order_number",
        "lock_time",
        "approve_refund_time",
        "order_type",
        "series",
        "product_name",
        "store_name",
        "store_city",
        "final_payment_way",
        "finance_product",
        "order_gender",
        "owner_age",
        "intention_payment_time",
    ]
    df = pd.read_parquet(order_path, columns=cols)
    df["lock_time"] = pd.to_datetime(df["lock_time"], errors="coerce")

    lock_mask = df["lock_time"].notna() & (df["lock_time"] >= start) & (df["lock_time"] < end)
    locked = df.loc[lock_mask].copy()
    locked_user = locked.loc[locked["order_type"] == "用户车"].copy()

    locked_user["gender_unknown"] = locked_user["order_gender"].isna() | (
        locked_user["order_gender"].astype("string") == "默认未知"
    )
    locked_user["final_payment_way_na"] = locked_user["final_payment_way"].isna()
    locked_user["finance_product_na"] = locked_user["finance_product"].isna()
    locked_user["in_watchlist_store"] = locked_user["store_name"].isin(watchlist_stores)
    locked_user["in_lhasa_hq_store"] = (
        locked_user["store_city"].astype("string") == "拉萨市"
    ) & (locked_user["store_name"].astype("string") == "总部主理店")

    intention_time = pd.to_datetime(locked_user["intention_payment_time"], errors="coerce")
    intention_to_lock_hours = (
        (locked_user["lock_time"] - intention_time).dt.total_seconds() / 3600.0
    )

    metrics = {
        "date": str(target_date.date()),
        "lock_cnt": int(locked["order_number"].nunique()),
        "lock_cnt_user_car": int(locked_user["order_number"].nunique()),
        "final_payment_way_na_share": _safe_share(locked_user["final_payment_way_na"]),
        "finance_product_na_share": _safe_share(locked_user["finance_product_na"]),
        "gender_unknown_share": _safe_share(locked_user["gender_unknown"]),
        "owner_age_mean": float(locked_user["owner_age"].mean()) if not locked_user.empty else None,
        "owner_age_median": float(locked_user["owner_age"].median()) if not locked_user.empty else None,
        "intention_to_lock_hours": _describe_hours(intention_to_lock_hours),
        "watchlist_store_lock_share": _safe_share(locked_user["in_watchlist_store"]),
        "lhasa_hq_store_lock_share": _safe_share(locked_user["in_lhasa_hq_store"]),
        "lhasa_hq_store_lock_cnt": int(locked_user.loc[locked_user["in_lhasa_hq_store"], "order_number"].nunique()),
    }

    retrospective = {}
    if "approve_refund_time" in locked.columns:
        locked_user["is_canceled"] = locked_user["approve_refund_time"].notna()
        canceled = locked.loc[locked["approve_refund_time"].notna()].copy()
        retrospective["observed_cancel_cnt"] = int(canceled["order_number"].nunique())
        retrospective["observed_cancel_rate"] = (
            float(retrospective["observed_cancel_cnt"]) / float(metrics["lock_cnt"])
            if metrics["lock_cnt"]
            else None
        )
        approve_time = pd.to_datetime(canceled["approve_refund_time"], errors="coerce")
        refund_days = (approve_time - canceled["lock_time"]).dt.total_seconds() / (3600.0 * 24.0)
        retrospective["refund_days_bins"] = _bins_count_days(refund_days)
        retrospective["refund_days_top"] = (
            refund_days.round().value_counts().head(10).to_dict() if not refund_days.empty else {}
        )
        canceled_user = locked_user.loc[locked_user["is_canceled"]].copy()
        retained_user = locked_user.loc[~locked_user["is_canceled"]].copy()
        retrospective["by_status"] = {
            "canceled": _subset_metrics(canceled_user),
            "retained": _subset_metrics(retained_user),
        }
        if (
            retrospective["by_status"]["canceled"]["cnt"] is not None
            and retrospective["by_status"]["retained"]["cnt"] is not None
        ):
            retrospective["delta_canceled_minus_retained"] = {
                "final_payment_way_na_share": (
                    (retrospective["by_status"]["canceled"]["final_payment_way_na_share"] or 0.0)
                    - (retrospective["by_status"]["retained"]["final_payment_way_na_share"] or 0.0)
                )
                if retrospective["by_status"]["canceled"]["final_payment_way_na_share"] is not None
                and retrospective["by_status"]["retained"]["final_payment_way_na_share"] is not None
                else None,
                "finance_product_na_share": (
                    (retrospective["by_status"]["canceled"]["finance_product_na_share"] or 0.0)
                    - (retrospective["by_status"]["retained"]["finance_product_na_share"] or 0.0)
                )
                if retrospective["by_status"]["canceled"]["finance_product_na_share"] is not None
                and retrospective["by_status"]["retained"]["finance_product_na_share"] is not None
                else None,
                "gender_unknown_share": (
                    (retrospective["by_status"]["canceled"]["gender_unknown_share"] or 0.0)
                    - (retrospective["by_status"]["retained"]["gender_unknown_share"] or 0.0)
                )
                if retrospective["by_status"]["canceled"]["gender_unknown_share"] is not None
                and retrospective["by_status"]["retained"]["gender_unknown_share"] is not None
                else None,
                "owner_age_mean": (
                    (retrospective["by_status"]["canceled"]["owner_age_mean"] or 0.0)
                    - (retrospective["by_status"]["retained"]["owner_age_mean"] or 0.0)
                )
                if retrospective["by_status"]["canceled"]["owner_age_mean"] is not None
                and retrospective["by_status"]["retained"]["owner_age_mean"] is not None
                else None,
                "intention_to_lock_median_hours": (
                    (retrospective["by_status"]["canceled"]["intention_to_lock_hours"]["median"] or 0.0)
                    - (retrospective["by_status"]["retained"]["intention_to_lock_hours"]["median"] or 0.0)
                )
                if retrospective["by_status"]["canceled"]["intention_to_lock_hours"]["median"] is not None
                and retrospective["by_status"]["retained"]["intention_to_lock_hours"]["median"] is not None
                else None,
            }

    risk_flags: list[dict] = []

    def _flag(name: str, value: float | None, rule: str, hit: bool) -> None:
        risk_flags.append({"name": name, "value": value, "rule": rule, "hit": bool(hit)})

    fpw = metrics["final_payment_way_na_share"]
    fna = metrics["finance_product_na_share"]
    gna = metrics["gender_unknown_share"]
    itl_median = metrics["intention_to_lock_hours"]["median"]
    wl = metrics["watchlist_store_lock_share"]
    lhasa_hq = metrics["lhasa_hq_store_lock_share"]

    _flag(
        "final_payment_way_na_share",
        fpw,
        f">={thresholds.get('final_payment_way_na_share')}",
        fpw is not None and fpw >= float(thresholds.get("final_payment_way_na_share", 1.0)),
    )
    _flag(
        "finance_product_na_share",
        fna,
        f">={thresholds.get('finance_product_na_share')}",
        fna is not None and fna >= float(thresholds.get("finance_product_na_share", 1.0)),
    )
    _flag(
        "gender_unknown_share",
        gna,
        f">={thresholds.get('gender_unknown_share')}",
        gna is not None and gna >= float(thresholds.get("gender_unknown_share", 1.0)),
    )
    _flag(
        "intention_to_lock_median_hours",
        itl_median,
        f"<={thresholds.get('intention_to_lock_median_hours_max')}",
        itl_median is not None
        and itl_median <= float(thresholds.get("intention_to_lock_median_hours_max", 0.0)),
    )
    _flag(
        "watchlist_store_lock_share",
        wl,
        f">={thresholds.get('watchlist_store_lock_share')}",
        wl is not None and wl >= float(thresholds.get("watchlist_store_lock_share", 1.0)),
    )
    if thresholds.get("lhasa_hq_store_lock_share") is None:
        _flag(
            "lhasa_hq_store_lock_share",
            lhasa_hq,
            ">0",
            lhasa_hq is not None and lhasa_hq > 0.0,
        )
    else:
        _flag(
            "lhasa_hq_store_lock_share",
            lhasa_hq,
            f">={thresholds.get('lhasa_hq_store_lock_share')}",
            lhasa_hq is not None
            and lhasa_hq >= float(thresholds.get("lhasa_hq_store_lock_share", 1.0)),
        )

    hit_cnt = sum(1 for x in risk_flags if x["hit"])
    overall = "high" if hit_cnt >= 3 else "mid" if hit_cnt == 2 else "low"

    suspicious = {
        "stores_top": _top_table(locked_user, "store_name", "in_watchlist_store", 20),
        "cities_top": _top_table(locked_user, "store_city", "in_watchlist_store", 20),
        "lhasa_hq_store_lock_cnt": metrics["lhasa_hq_store_lock_cnt"],
        "lhasa_hq_store_lock_share": metrics["lhasa_hq_store_lock_share"],
    }
    if "is_canceled" in locked_user.columns:
        suspicious["stores_by_cancel_rate_top"] = _top_cancel_rate_table(
            locked_user, "store_name", "is_canceled", 20
        )
        suspicious["cities_by_cancel_rate_top"] = _top_cancel_rate_table(
            locked_user, "store_city", "is_canceled", 20
        )
    if not locked_user.empty:
        tmp = locked_user.copy()
        tmp["fast_lock"] = intention_to_lock_hours.notna() & (intention_to_lock_hours <= 120.0)
        suspicious["fast_lock_stores_top"] = _top_table(tmp, "store_name", "fast_lock", 20)

    retrospective_risk = None
    retro_thr = profile.get("retrospective_thresholds", {}) or {}
    if retrospective.get("observed_cancel_rate") is not None and retrospective.get("delta_canceled_minus_retained"):
        delta = retrospective["delta_canceled_minus_retained"]

        retro_flags: list[dict] = []

        def _rflag(name: str, value: float | None, rule: str, hit: bool) -> None:
            retro_flags.append({"name": name, "value": value, "rule": rule, "hit": bool(hit)})

        ocr = retrospective["observed_cancel_rate"]
        _rflag(
            "observed_cancel_rate",
            ocr,
            f">={retro_thr.get('observed_cancel_rate_min')}",
            ocr is not None and ocr >= float(retro_thr.get("observed_cancel_rate_min", 1.0)),
        )
        _rflag(
            "delta_final_payment_way_na_share",
            delta.get("final_payment_way_na_share"),
            f">={retro_thr.get('delta_final_payment_way_na_share_min')}",
            delta.get("final_payment_way_na_share") is not None
            and delta["final_payment_way_na_share"]
            >= float(retro_thr.get("delta_final_payment_way_na_share_min", 1.0)),
        )
        _rflag(
            "delta_finance_product_na_share",
            delta.get("finance_product_na_share"),
            f">={retro_thr.get('delta_finance_product_na_share_min')}",
            delta.get("finance_product_na_share") is not None
            and delta["finance_product_na_share"]
            >= float(retro_thr.get("delta_finance_product_na_share_min", 1.0)),
        )
        _rflag(
            "delta_gender_unknown_share",
            delta.get("gender_unknown_share"),
            f">={retro_thr.get('delta_gender_unknown_share_min')}",
            delta.get("gender_unknown_share") is not None
            and delta["gender_unknown_share"]
            >= float(retro_thr.get("delta_gender_unknown_share_min", 1.0)),
        )
        _rflag(
            "delta_owner_age_mean",
            delta.get("owner_age_mean"),
            f"<={retro_thr.get('delta_owner_age_mean_max')}",
            delta.get("owner_age_mean") is not None
            and delta["owner_age_mean"] <= float(retro_thr.get("delta_owner_age_mean_max", 0.0)),
        )
        _rflag(
            "delta_intention_to_lock_median_hours",
            delta.get("intention_to_lock_median_hours"),
            f"<={retro_thr.get('delta_intention_to_lock_median_hours_max')}",
            delta.get("intention_to_lock_median_hours") is not None
            and delta["intention_to_lock_median_hours"]
            <= float(retro_thr.get("delta_intention_to_lock_median_hours_max", 0.0)),
        )

        retro_hit_cnt = sum(1 for x in retro_flags if x["hit"])
        retro_overall = "high" if retro_hit_cnt >= 4 else "mid" if retro_hit_cnt >= 3 else "low"
        retrospective_risk = {"hit_cnt": int(retro_hit_cnt), "overall": retro_overall, "risk_flags": retro_flags}

    out = {
        "profile": {"profile_name": profile.get("profile_name"), "lock_date": profile.get("lock_date")},
        "metrics": metrics,
        "retrospective": retrospective,
        "risk": {
            "prospective": {"hit_cnt": int(hit_cnt), "overall": overall, "risk_flags": risk_flags},
            "retrospective": retrospective_risk,
        },
        "suspicious": suspicious,
    }

    if args.print_json:
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        print(f"date={out['metrics']['date']} lock_cnt={out['metrics']['lock_cnt']} user_car_lock_cnt={out['metrics']['lock_cnt_user_car']}")
        print(f"prospective_risk_overall={out['risk']['prospective']['overall']} hit_cnt={out['risk']['prospective']['hit_cnt']}")
        for f in out["risk"]["prospective"]["risk_flags"]:
            status = "HIT" if f["hit"] else "OK"
            print(f"- {f['name']}: {f['value']} rule={f['rule']} => {status}")
        if out["risk"].get("retrospective") is not None:
            rr = out["risk"]["retrospective"]
            print(f"retrospective_risk_overall={rr['overall']} hit_cnt={rr['hit_cnt']}")
            for f in rr["risk_flags"]:
                status = "HIT" if f["hit"] else "OK"
                print(f"- {f['name']}: {f['value']} rule={f['rule']} => {status}")


if __name__ == "__main__":
    main()

"""
对比 CM2纯电 / CM2增程 / LS8 / LS9 在“上市后 N 天”窗口的锁单表现，并按新/老门店拆分；可选按大区进一步拆分，并输出城市榜单。

输入依赖：
- --data-path-md（默认 schema/data_path.md）
  - 订单分析：订单分析 parquet 路径（必需）
  - 智己大区分布：TSV 路径（可选，字段 store_city / parent_region_name）
- --business-def（默认 schema/business_definition.json）
  - time_periods：每个车系的 start / end（end 作为上市日）
  - series_group_logic：各车系 product_name 命中逻辑
  - product_type_logic：纯电/增程拆分逻辑（用于拆分 CM2）

窗口口径：
- 预售期：[start, end+1)（包含 end 当天）
- 上市日：listing_day = end
- 上市后 N 天：[listing_day, listing_day+N)（包含上市日到上市+N-1 天）
- N 默认：max(4, 今天 - max(time_periods[*].end) + 1)

指标口径：
- 门店新老：以上市日为参照，(上市日 - 门店开业日).days > 300 为老门店，否则新门店；门店开业日取 store_name 维度 store_create_date 最小值
- 预售期留存小订数：预售期内 intention_payment_time 支付，且在 end+1 前未退订（intention_refund_time 为空或 >= end+1）
- 本车系锁单数：上市后 N 天内 lock_time，且 product_name 命中 series_group_logic
- 其他车系锁单数：上市后 N 天内 lock_time，且 product_name 不命中 series_group_logic
- 在营门店数：以上市日为参照，上市日往前 29 天~上市日（共 30 天）内有下单创建（order_create_date）且开业日 <= 上市日，按新/老拆分
- 店均锁单数：(本车系锁单数 + 其他车系锁单数) / 在营门店数

输出内容（stdout）：
- 新门店 / 老门店：CM2纯电、CM2增程、LS8、LS9 对比表
- 可选 --by-region：仅输出按 parent_region_name 拆分的大区汇总表，并额外输出车系城市气泡图 HTML（同一文件内分四段：CM2纯电/CM2增程/LS8/LS9）
- 城市榜单：每个车系输出“店均锁单 TOPN 城市”和“本车系锁单总数 TOPN 城市”
- 可选 --series-city-scatter-out：输出城市散点图 HTML（同一文件内分四段：CM2纯电/CM2增程/LS8/LS9；需要环境中已安装 plotly）

运行示例：
python3 scripts/compare_store_lock.py
python3 scripts/compare_store_lock.py --by-region
python3 scripts/compare_store_lock.py --by-region --region-city-bubble-out scripts/reports/region_city_bubbles.html
python3 scripts/compare_store_lock.py --city-topn 15 --city-total-topn 15
python3 scripts/compare_store_lock.py --series-city-scatter-out scripts/reports/series_city_scatter.html
"""

import argparse
import json
import math
import re
from pathlib import Path

import pandas as pd


def _read_data_paths(md_path: Path) -> dict[str, Path]:
    out: dict[str, Path] = {}
    for line in md_path.read_text(encoding="utf-8").splitlines():
        line = str(line).strip()
        if not line or line.startswith("---"):
            continue
        if "：" not in line:
            continue
        k, v = line.split("：", 1)
        v = v.strip().replace("\\_", "_")
        out[k.strip()] = Path(v).expanduser().resolve()
    return out


def _load_parent_region_map(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    df = None
    for enc in ["utf-8", "utf-16", "utf-16le", "gbk"]:
        try:
            df = pd.read_csv(path, sep="\t", encoding=enc)
            break
        except Exception:
            df = None
    if df is None:
        return {}
    if "store_city" not in df.columns or "parent_region_name" not in df.columns:
        return {}
    df = df.loc[:, ["store_city", "parent_region_name"]].dropna()
    df["store_city"] = df["store_city"].astype("string").str.strip()
    df["parent_region_name"] = df["parent_region_name"].astype("string").str.strip()
    df = df.drop_duplicates(subset=["store_city"], keep="first")
    return dict(zip(df["store_city"].tolist(), df["parent_region_name"].tolist()))


def _parse_sql_condition(df: pd.DataFrame, condition_str: str) -> pd.Series:
    def not_like_replacer(match):
        val = match.group(1)
        return f"~df['product_name'].str.contains('{val}', na=False, regex=False)"

    condition_str = re.sub(
        r"product_name\s+NOT\s+LIKE\s+'%([^%]+)%+'",
        not_like_replacer,
        condition_str,
    )

    def like_replacer(match):
        val = match.group(1)
        return f"df['product_name'].str.contains('{val}', na=False, regex=False)"

    condition_str = re.sub(
        r"product_name\s+LIKE\s+'%([^%]+)%+'",
        like_replacer,
        condition_str,
    )

    condition_str = condition_str.replace(" AND ", " & ").replace(" OR ", " | ")
    return eval(condition_str)


def _to_rate(numer: int, denom: int) -> str:
    if denom <= 0:
        return "-"
    return f"{(numer / float(denom)):.1%}"


def _store_type(store_create_date: pd.Series, ref_date: pd.Timestamp) -> pd.Series:
    scd = pd.to_datetime(store_create_date, errors="coerce").dt.normalize()
    age_days = (pd.Timestamp(ref_date).normalize() - scd).dt.days
    out = pd.Series(pd.NA, index=store_create_date.index, dtype="string")
    out = out.mask(age_days <= 300, "新门店")
    out = out.mask(age_days > 300, "老门店")
    return out


def _calc_active_store_counts(df: pd.DataFrame, ref_date: pd.Timestamp) -> dict[str, int]:
    df_store = df.loc[:, ["store_name", "store_open_date", "order_create_date"]].copy()
    df_store = df_store.dropna(subset=["store_name", "store_open_date", "order_create_date"])
    if df_store.empty:
        return {"新门店": 0, "老门店": 0}

    d = pd.Timestamp(ref_date).normalize()
    window_start = d - pd.Timedelta(days=29)
    activity = df_store[(df_store["order_create_date"] >= window_start) & (df_store["order_create_date"] <= d)]
    if activity.empty:
        return {"新门店": 0, "老门店": 0}

    stores = pd.Index(activity["store_name"].dropna().unique())
    store_open_dates = (
        df_store.drop_duplicates(subset=["store_name"], keep="first")
        .set_index("store_name")["store_open_date"]
        .reindex(stores)
    )
    is_open = (store_open_dates <= d).fillna(False)
    open_stores = stores[is_open.to_numpy()]
    if open_stores.empty:
        return {"新门店": 0, "老门店": 0}

    open_dates = store_open_dates.reindex(open_stores)
    store_type = _store_type(open_dates, d)
    return {
        "新门店": int((store_type == "新门店").sum()),
        "老门店": int((store_type == "老门店").sum()),
    }


def _calc_active_store_counts_by_city(df: pd.DataFrame, ref_date: pd.Timestamp) -> pd.Series:
    df_store = df.loc[:, ["store_name", "store_open_date", "order_create_date", "store_city"]].copy()
    df_store = df_store.dropna(subset=["store_name", "store_open_date", "order_create_date"])
    if df_store.empty:
        return pd.Series(dtype="int64")

    d = pd.Timestamp(ref_date).normalize()
    window_start = d - pd.Timedelta(days=29)
    activity = df_store[(df_store["order_create_date"] >= window_start) & (df_store["order_create_date"] <= d)]
    if activity.empty:
        return pd.Series(dtype="int64")

    stores = pd.Index(activity["store_name"].dropna().unique())
    store_open_dates = (
        df_store.drop_duplicates(subset=["store_name"], keep="first")
        .set_index("store_name")["store_open_date"]
        .reindex(stores)
    )
    is_open = (store_open_dates <= d).fillna(False)
    open_stores = stores[is_open.to_numpy()]
    if open_stores.empty:
        return pd.Series(dtype="int64")

    df_city = (
        df_store.dropna(subset=["store_city"])
        .drop_duplicates(subset=["store_name"], keep="first")
        .set_index("store_name")["store_city"]
        .reindex(open_stores)
        .fillna("未知")
        .astype("string")
    )
    return df_city.value_counts().astype("int64")


def _calc_lock_counts_by_city(df: pd.DataFrame, start: pd.Timestamp, end_excl: pd.Timestamp) -> pd.Series:
    m_lock = df["lock_time"].notna() & (df["lock_time"] >= start) & (df["lock_time"] < end_excl)
    lock_win = df.loc[m_lock, ["order_number", "store_city"]].copy()
    if lock_win.empty:
        return pd.Series(dtype="int64")
    lock_win["order_number"] = lock_win["order_number"].astype("string")
    lock_win["store_city"] = lock_win["store_city"].astype("string").fillna("未知")
    lock_win = lock_win.dropna(subset=["order_number"]).drop_duplicates(subset=["order_number"], keep="first")
    return lock_win.groupby("store_city")["order_number"].nunique().astype("int64")


def _calc_listing_plus_days(time_periods: dict[str, object], default_days: int = 4) -> int:
    if not isinstance(time_periods, dict) or not time_periods:
        return int(default_days)

    end_days: list[pd.Timestamp] = []
    for _, tp in time_periods.items():
        if not isinstance(tp, dict):
            continue
        end = tp.get("end")
        if not end:
            continue
        end_days.append(pd.Timestamp(str(end)).normalize())

    if not end_days:
        return int(default_days)

    max_end_day = max(end_days)
    today = pd.Timestamp.now().normalize()
    n = int((today - max_end_day).days) + 1
    return max(int(default_days), n)


def _calc_one_series(
    df: pd.DataFrame,
    start: str,
    end: str,
    group: str,
    logic: str,
    listing_plus_days: int,
) -> dict[str, dict[str, object]]:

    df = df.copy()
    df["product_name"] = df["product_name"].astype("string").fillna("")
    m_group = _parse_sql_condition(df, logic).fillna(False)

    start_day = pd.Timestamp(start).normalize()
    presale_end_day = pd.Timestamp(end).normalize()
    window_end_excl = presale_end_day + pd.Timedelta(days=1)
    listing_day = presale_end_day
    listing_plus_end_excl = listing_day + pd.Timedelta(days=int(listing_plus_days))
    lock_label = f"上市后{int(listing_plus_days)}天锁单数"

    df["store_open_date"] = df.groupby("store_name", dropna=False)["store_create_date"].transform("min")
    df["store_type"] = _store_type(df["store_open_date"], listing_day)

    m_presale = (
        m_group
        & df["intention_payment_time"].notna()
        & (df["intention_payment_time"] >= start_day)
        & (df["intention_payment_time"] < window_end_excl)
    )
    m_retained = df["intention_refund_time"].isna() | (df["intention_refund_time"] >= window_end_excl)
    presale = df.loc[m_presale & m_retained, ["order_number", "store_type"]].copy()
    presale["order_number"] = presale["order_number"].astype("string")
    presale = presale.dropna(subset=["order_number"]).drop_duplicates(subset=["order_number"])

    m_lock = (
        df["lock_time"].notna()
        & (df["lock_time"] >= listing_day)
        & (df["lock_time"] < listing_plus_end_excl)
    )
    locks = df.loc[m_lock, ["order_number", "store_type"]].copy()
    locks["order_number"] = locks["order_number"].astype("string")
    locks = locks.dropna(subset=["order_number"]).drop_duplicates(subset=["order_number"])

    group_locks = df.loc[m_lock & m_group, ["order_number", "store_type"]].copy()
    group_locks["order_number"] = group_locks["order_number"].astype("string")
    group_locks = group_locks.dropna(subset=["order_number"]).drop_duplicates(subset=["order_number"])

    other_locks = df.loc[m_lock & (~m_group), ["order_number", "store_type"]].copy()
    other_locks["order_number"] = other_locks["order_number"].astype("string")
    other_locks = other_locks.dropna(subset=["order_number"]).drop_duplicates(subset=["order_number"])

    active_store_counts = _calc_active_store_counts(df, listing_day)
    active_store_total = int(sum(active_store_counts.values()))

    out: dict[str, dict[str, object]] = {}
    for st in ["新门店", "老门店"]:
        retained_cnt = int(presale.loc[presale["store_type"] == st, "order_number"].nunique())
        lock_cnt = int(group_locks.loc[group_locks["store_type"] == st, "order_number"].nunique())
        other_lock_cnt = int(other_locks.loc[other_locks["store_type"] == st, "order_number"].nunique())
        active_store_cnt = int(active_store_counts.get(st) or 0)
        avg_locks_per_store = (
            None
            if active_store_cnt <= 0
            else round((lock_cnt + other_lock_cnt) / float(active_store_cnt), 2)
        )
        out[st] = {
            "预售期留存小订数": retained_cnt,
            lock_label: lock_cnt,
            "转化率": _to_rate(lock_cnt, retained_cnt),
            "其他车系锁单数": other_lock_cnt,
            "在营门店数": active_store_cnt,
            "店均锁单数": avg_locks_per_store,
        }

    retained_total = int(presale["order_number"].nunique())
    lock_total = int(group_locks["order_number"].nunique())
    other_lock_total = int(other_locks["order_number"].nunique())
    avg_total = (
        None
        if active_store_total <= 0
        else round((lock_total + other_lock_total) / float(active_store_total), 2)
    )
    out["全部"] = {
        "预售期留存小订数": retained_total,
        lock_label: lock_total,
        "转化率": _to_rate(lock_total, retained_total),
        "其他车系锁单数": other_lock_total,
        "在营门店数": active_store_total,
        "店均锁单数": avg_total,
    }
    out["_meta"] = {
        "group": group,
        "presale": f"{start_day.date().isoformat()}~{presale_end_day.date().isoformat()}",
        f"listing_plus{int(listing_plus_days)}": f"{listing_day.date().isoformat()}~{(listing_plus_end_excl - pd.Timedelta(days=1)).date().isoformat()}",
    }
    return out


def _fmt(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "-"
    if isinstance(v, float) and abs(v - round(v)) < 1e-9:
        return str(int(round(v)))
    return str(v)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-path-md", default="schema/data_path.md")
    parser.add_argument("--business-def", default="schema/business_definition.json")
    parser.add_argument("--by-region", action="store_true")
    parser.add_argument("--region-city-bubble-out", default="scripts/reports/region_city_bubbles.html")
    parser.add_argument("--city-topn", type=int, default=10)
    parser.add_argument("--city-total-topn", type=int, default=10)
    parser.add_argument("--series-city-scatter-out", default="")
    args = parser.parse_args()

    business_def = json.loads(Path(str(args.business_def)).read_text(encoding="utf-8"))
    time_periods = business_def.get("time_periods") or {}
    series_logic = business_def.get("series_group_logic") or {}
    product_type_logic = business_def.get("product_type_logic") or {}

    listing_plus_days = _calc_listing_plus_days(time_periods, default_days=4)

    for k in ["CM2", "LS8", "LS9"]:
        if k not in time_periods:
            raise KeyError(f"business_definition.time_periods 缺少 {k}")
        if k not in series_logic:
            raise KeyError(f"business_definition.series_group_logic 缺少 {k}")
    for k in ["纯电", "增程"]:
        if k not in product_type_logic:
            raise KeyError(f"business_definition.product_type_logic 缺少 {k}")

    group_specs = [
        {
            "name": "CM2纯电",
            "base": "CM2",
            "logic": f"({str(series_logic['CM2'])}) AND ({str(product_type_logic['纯电'])})",
        },
        {
            "name": "CM2增程",
            "base": "CM2",
            "logic": f"({str(series_logic['CM2'])}) AND ({str(product_type_logic['增程'])})",
        },
        {"name": "LS8", "base": "LS8", "logic": str(series_logic["LS8"])},
        {"name": "LS9", "base": "LS9", "logic": str(series_logic["LS9"])},
    ]

    data_paths = _read_data_paths(Path(str(args.data_path_md)))
    order_path = data_paths["订单分析"]
    region_path = data_paths.get("智己大区分布")

    cols = [
        "order_number",
        "product_name",
        "store_city",
        "store_name",
        "store_create_date",
        "order_create_date",
        "intention_payment_time",
        "intention_refund_time",
        "lock_time",
    ]
    df = pd.read_parquet(order_path, columns=cols)
    for c in ["store_create_date", "order_create_date", "intention_payment_time", "intention_refund_time", "lock_time"]:
        df[c] = pd.to_datetime(df[c], errors="coerce")
    df["order_create_date"] = df["order_create_date"].dt.normalize()
    df["store_city"] = df["store_city"].astype("string").str.strip().fillna("未知")
    df["store_open_date"] = df.groupby("store_name", dropna=False)["store_create_date"].transform("min")

    parent_region_map = {} if region_path is None else _load_parent_region_map(Path(region_path))
    if parent_region_map:
        df["parent_region_name"] = df["store_city"].map(parent_region_map).fillna("未知")
    else:
        df["parent_region_name"] = "未知"

    lock_label = f"上市后{listing_plus_days}天锁单数"
    headers = [
        "车系",
        "预售期留存小订数",
        lock_label,
        "其他车系锁单数",
        "转化率",
        "在营门店数",
        "店均锁单数",
    ]

    if args.by_region:
        try:
            import plotly.graph_objects as go
            import plotly.io as pio
        except Exception:
            go = None
            pio = None

        regions = sorted(pd.Index(df["parent_region_name"].dropna().unique()).astype(str).tolist())

        region_headers = ["大区"] + headers
        region_rows: list[list[str]] = []
        for region in regions:
            df_r = df[df["parent_region_name"].astype("string") == region].copy()
            r_results: dict[str, dict[str, dict[str, object]]] = {}
            for spec in group_specs:
                g = str(spec["name"])
                tp = time_periods[str(spec["base"])]
                r_results[g] = _calc_one_series(
                    df=df_r,
                    start=str(tp["start"]),
                    end=str(tp["end"]),
                    group=g,
                    logic=str(spec["logic"]),
                    listing_plus_days=listing_plus_days,
                )

            for spec in group_specs:
                g = str(spec["name"])
                r = r_results[g]["全部"]
                region_rows.append(
                    [
                        str(region),
                        g,
                        _fmt(r["预售期留存小订数"]),
                        _fmt(r[lock_label]),
                        _fmt(r["其他车系锁单数"]),
                        _fmt(r["转化率"]),
                        _fmt(r["在营门店数"]),
                        _fmt(r["店均锁单数"]),
                    ]
                )

        col_widths = [len(h) for h in region_headers]
        for r in region_rows:
            for i, cell in enumerate(r):
                col_widths[i] = max(col_widths[i], len(str(cell)))

        def fmt_row(r: list[str]) -> str:
            return " | ".join(
                str(c).rjust(col_widths[i]) if i else str(c).ljust(col_widths[i])
                for i, c in enumerate(r)
            )

        print("大区汇总")
        print(fmt_row(region_headers))
        print("-+-".join("-" * w for w in col_widths))
        for r in region_rows:
            print(fmt_row(r))

        if go is not None and pio is not None:
            palette = [
                "#006BA4",
                "#FF800E",
                "#ABABAB",
                "#595959",
                "#5F9ED1",
                "#C85200",
                "#898989",
                "#A2C8EC",
                "#FFBC79",
                "#CFCFCF",
            ]

            def _region_group(v: object) -> str:
                s = str(v).strip()
                m = re.match(r"^(一区|二区|上海区|三区|华南特区)", s)
                return m.group(1) if m else "其他"

            region_order = ["一区", "二区", "上海区", "三区", "华南特区", "其他"]
            region_colors = {
                "一区": palette[3],
                "二区": palette[1],
                "上海区": palette[2],
                "三区": palette[0],
                "华南特区": palette[4],
                "其他": palette[5],
            }
            group_rank = {k: i for i, k in enumerate(region_order)}

            out_path = Path(str(args.region_city_bubble_out)).expanduser().resolve()
            out_path.parent.mkdir(parents=True, exist_ok=True)

            city_region = (
                df.loc[:, ["store_city", "parent_region_name"]]
                .dropna(subset=["store_city"])
                .drop_duplicates(subset=["store_city"], keep="first")
                .set_index("store_city")["parent_region_name"]
                .astype("string")
            )

            def _bubble_for_group(group: str):
                bubble_days = 6
                spec = next((x for x in group_specs if x["name"] == group), None)
                if spec is None:
                    return None
                tp = time_periods[str(spec["base"])]
                start_day = pd.Timestamp(str(tp["start"])).normalize()
                presale_end_day = pd.Timestamp(str(tp["end"])).normalize()
                presale_end_excl = presale_end_day + pd.Timedelta(days=1)

                listing_day = presale_end_day
                end_excl = listing_day + pd.Timedelta(days=int(bubble_days))

                m_group = _parse_sql_condition(df, str(spec["logic"])).fillna(False)
                m_presale = (
                    m_group
                    & df["intention_payment_time"].notna()
                    & (df["intention_payment_time"] >= start_day)
                    & (df["intention_payment_time"] < presale_end_excl)
                )
                m_retained = df["intention_refund_time"].isna() | (df["intention_refund_time"] >= presale_end_excl)
                presale = df.loc[m_presale & m_retained, ["order_number", "store_city"]].copy()
                presale["order_number"] = presale["order_number"].astype("string")
                presale["store_city"] = presale["store_city"].astype("string").fillna("未知")
                presale = presale.dropna(subset=["order_number"]).drop_duplicates(subset=["order_number"], keep="first")
                retained_by_city = presale.groupby("store_city")["order_number"].nunique().astype("int64")

                m_lock = df["lock_time"].notna() & (df["lock_time"] >= listing_day) & (df["lock_time"] < end_excl)
                group_locks = df.loc[m_lock & m_group, ["order_number", "store_city"]].copy()
                group_locks["order_number"] = group_locks["order_number"].astype("string")
                group_locks["store_city"] = group_locks["store_city"].astype("string").fillna("未知")
                group_locks = group_locks.dropna(subset=["order_number"]).drop_duplicates(subset=["order_number"], keep="first")
                group_locks_by_city = group_locks.groupby("store_city")["order_number"].nunique().astype("int64")

                locks_all_by_city = _calc_lock_counts_by_city(df, listing_day, end_excl)
                active_by_city = _calc_active_store_counts_by_city(df, listing_day)

                lock_col = f"上市后{bubble_days}天锁单数"
                city = (
                    pd.DataFrame(
                        {
                            "预售期留存小订数": retained_by_city,
                            lock_col: group_locks_by_city,
                            "锁单数(全车系)": locks_all_by_city,
                            "在营门店数": active_by_city,
                        }
                    )
                    .fillna(0)
                    .astype(
                        {
                            "预售期留存小订数": "int64",
                            lock_col: "int64",
                            "锁单数(全车系)": "int64",
                            "在营门店数": "int64",
                        }
                    )
                )
                city = city[(city["预售期留存小订数"] > 0) & (city["在营门店数"] > 0)].copy()
                if city.empty:
                    return None

                city["店均锁单数"] = (city["锁单数(全车系)"] / city["在营门店数"]).astype("float64")
                city["转化率"] = (city[lock_col] / city["预售期留存小订数"]).astype("float64")
                city["大区"] = pd.Series(city.index.astype("string"), index=city.index).map(city_region).fillna("未知")
                city["大区分组"] = city["大区"].apply(_region_group).astype("string")

                hover = []
                size = []
                for city_name, row in city.iterrows():
                    lock_cnt = int(row[lock_col])
                    size.append(max(6.0, min(40.0, (float(max(lock_cnt, 1)) ** 0.5) * 6.0)))
                    hover.append(
                        "<br>".join(
                            [
                                f"城市: {city_name}",
                                f"大区: {str(row['大区'])}",
                                f"分组: {str(row['大区分组'])}",
                                f"预售期留存小订数: {int(row['预售期留存小订数'])}",
                                f"{lock_col}: {lock_cnt}",
                                f"在营门店数: {int(row['在营门店数'])}",
                                f"店均锁单数: {float(row['店均锁单数']):.2f}",
                                f"转化率: {float(row['转化率'])*100.0:.1f}%",
                            ]
                        )
                    )
                city["hover"] = hover
                city["size"] = pd.Series(size, index=city.index, dtype="float64")

                traces = []
                full_regions = sorted(
                    pd.Index(city["大区"].dropna().astype(str).unique()).tolist(),
                    key=lambda x: (group_rank.get(_region_group(x), 999), str(x)),
                )
                for region_full in full_regions:
                    sub = city[city["大区"].astype(str) == str(region_full)].copy()
                    if sub.empty:
                        continue
                    rg = _region_group(region_full)
                    traces.append(
                        go.Scatter(
                            name=str(region_full),
                            x=sub["店均锁单数"].astype("float64"),
                            y=sub["转化率"].astype("float64"),
                            mode="markers",
                            marker={
                                "size": sub["size"].astype("float64"),
                                "color": region_colors.get(str(rg), "#7f7f7f"),
                                "opacity": 0.8,
                                "line": {"color": palette[-1], "width": 1},
                            },
                            hovertext=sub["hover"].tolist(),
                            hoverinfo="text",
                        )
                    )

                label_topn = 5
                label_df = city.sort_values(["店均锁单数", lock_col], ascending=[False, False]).head(label_topn).copy()
                if not label_df.empty:
                    traces.append(
                        go.Scatter(
                            name=f"TOP{label_topn}城市",
                            x=label_df["店均锁单数"].astype("float64"),
                            y=label_df["转化率"].astype("float64"),
                            mode="text",
                            text=[str(x) for x in label_df.index.tolist()],
                            textposition="top center",
                            textfont={"size": 11, "color": "#111111"},
                            hoverinfo="skip",
                            showlegend=False,
                        )
                    )

                if not traces:
                    return None

                fig = go.Figure(data=traces)
                fig.update_layout(
                    title=f"{group} 上市后{bubble_days}天 城市气泡图（X=店均锁单数，Y=转化率）",
                    template="plotly_white",
                    xaxis_title=f"店均锁单数（上市后{bubble_days}天，全车系锁单/在营门店数）",
                    yaxis_title="转化率（本车系锁单/预售期留存小订）",
                    yaxis_tickformat=".1%",
                    legend_title_text="大区",
                    height=720,
                )
                grid_color = "rgba(0,0,0,0.08)"
                fig.update_xaxes(showgrid=True, gridcolor=grid_color)
                fig.update_yaxes(showgrid=True, gridcolor=grid_color)
                return fig

            figs = []
            for spec in group_specs:
                fig = _bubble_for_group(str(spec["name"]))
                if fig is not None:
                    figs.append((str(spec["name"]), fig))

            if figs:
                html_parts = [
                    "<!doctype html>",
                    "<html>",
                    "<head>",
                    "<meta charset='utf-8' />",
                    "<meta name='viewport' content='width=device-width, initial-scale=1' />",
                    "<title>城市气泡图</title>",
                    "<style>body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,'Noto Sans',sans-serif;margin:24px}h2{margin:28px 0 12px}</style>",
                    "</head>",
                    "<body>",
                    "<h1>城市气泡图（按大区配色）</h1>",
                ]
                for i, (g, fig) in enumerate(figs):
                    html_parts.append(f"<h2>{g}</h2>")
                    html_parts.append(
                        pio.to_html(
                            fig,
                            include_plotlyjs="cdn" if i == 0 else False,
                            full_html=False,
                        )
                    )
                html_parts.append("</body></html>")
                out_path.write_text("\n".join(html_parts) + "\n", encoding="utf-8")
                print()
                print(f"城市气泡图已写入: {out_path}")
        return

    results: dict[str, dict[str, dict[str, object]]] = {}
    for spec in group_specs:
        g = str(spec["name"])
        tp = time_periods[str(spec["base"])]
        results[g] = _calc_one_series(
            df=df,
            start=str(tp["start"]),
            end=str(tp["end"]),
            group=g,
            logic=str(spec["logic"]),
            listing_plus_days=listing_plus_days,
        )

    meta = {str(spec["name"]): (results[str(spec["name"])].get("_meta") or {}) for spec in group_specs}
    for spec in group_specs:
        g = str(spec["name"])
        print(
            g,
            "预售期:",
            meta[g].get("presale"),
            f"上市后{listing_plus_days}天:",
            meta[g].get(f"listing_plus{listing_plus_days}"),
        )

    def print_table(store_type: str) -> None:
        rows = []
        for spec in group_specs:
            g = str(spec["name"])
            r = results[g][store_type]
            rows.append(
                [
                    g,
                    _fmt(r["预售期留存小订数"]),
                    _fmt(r[lock_label]),
                    _fmt(r["其他车系锁单数"]),
                    _fmt(r["转化率"]),
                    _fmt(r["在营门店数"]),
                    _fmt(r["店均锁单数"]),
                ]
            )

        col_widths = [len(h) for h in headers]
        for r in rows:
            for i, cell in enumerate(r):
                col_widths[i] = max(col_widths[i], len(str(cell)))

        def fmt_row(r: list[str]) -> str:
            return " | ".join(
                str(c).rjust(col_widths[i]) if i else str(c).ljust(col_widths[i])
                for i, c in enumerate(r)
            )

        print()
        print(store_type)
        print(fmt_row(headers))
        print("-+-".join("-" * w for w in col_widths))
        for r in rows:
            print(fmt_row(r))

    print_table("新门店")
    print_table("老门店")

    topn = max(1, int(args.city_topn or 10))
    city_headers = ["城市", "锁单数(全车系)", "在营门店数", "店均锁单数", "本车系锁单占比"]

    def print_city_top10(spec: dict[str, object]) -> None:
        group = str(spec["name"])
        tp = time_periods[str(spec["base"])]
        listing_day = pd.Timestamp(str(tp["end"])).normalize()
        end_excl = listing_day + pd.Timedelta(days=int(listing_plus_days))

        m_group = _parse_sql_condition(df, str(spec["logic"])).fillna(False)
        locks_by_city = _calc_lock_counts_by_city(df, listing_day, end_excl)
        group_locks_by_city = _calc_lock_counts_by_city(df[m_group].copy(), listing_day, end_excl)
        active_by_city = _calc_active_store_counts_by_city(df, listing_day)
        city = (
            pd.DataFrame(
                {
                    "锁单数(全车系)": locks_by_city,
                    "本车系锁单数": group_locks_by_city,
                    "在营门店数": active_by_city,
                }
            )
            .fillna(0)
            .astype({"锁单数(全车系)": "int64", "本车系锁单数": "int64", "在营门店数": "int64"})
        )
        city = city[city["在营门店数"] > 0].copy()
        if city.empty:
            return
        city["店均锁单数"] = (city["锁单数(全车系)"] / city["在营门店数"]).round(2)
        city["本车系锁单占比"] = (
            (city["本车系锁单数"] / city["锁单数(全车系)"]).where(city["锁单数(全车系)"] > 0, 0.0) * 100.0
        ).round(1)
        city = city.sort_values(["店均锁单数", "锁单数(全车系)"], ascending=[False, False]).head(topn)

        rows = []
        for city_name, row in city.iterrows():
            rows.append(
                [
                    str(city_name),
                    _fmt(int(row["锁单数(全车系)"])),
                    _fmt(int(row["在营门店数"])),
                    _fmt(float(row["店均锁单数"])),
                    f'{float(row["本车系锁单占比"]):.1f}%',
                ]
            )

        col_widths = [len(h) for h in city_headers]
        for r in rows:
            for i, cell in enumerate(r):
                col_widths[i] = max(col_widths[i], len(str(cell)))

        def fmt_row(r: list[str]) -> str:
            return " | ".join(
                str(c).rjust(col_widths[i]) if i else str(c).ljust(col_widths[i])
                for i, c in enumerate(r)
            )

        print()
        print(f"{group} 上市后{listing_plus_days}天 店均锁单 TOP{topn} 城市")
        print(fmt_row(city_headers))
        print("-+-".join("-" * w for w in col_widths))
        for r in rows:
            print(fmt_row(r))

    for spec in group_specs:
        print_city_top10(spec)

    total_topn = max(1, int(args.city_total_topn or 10))
    total_headers = ["城市", "本车系锁单数", "锁单数(全车系)", "本车系锁单占比", "在营门店数", "本车系店均锁单"]

    def print_city_total_top(spec: dict[str, object]) -> None:
        group = str(spec["name"])
        tp = time_periods[str(spec["base"])]
        listing_day = pd.Timestamp(str(tp["end"])).normalize()
        end_excl = listing_day + pd.Timedelta(days=int(listing_plus_days))

        m_group = _parse_sql_condition(df, str(spec["logic"])).fillna(False)
        locks_by_city = _calc_lock_counts_by_city(df, listing_day, end_excl)
        group_locks_by_city = _calc_lock_counts_by_city(df[m_group].copy(), listing_day, end_excl)
        active_by_city = _calc_active_store_counts_by_city(df, listing_day)

        city = (
            pd.DataFrame(
                {
                    "本车系锁单数": group_locks_by_city,
                    "锁单数(全车系)": locks_by_city,
                    "在营门店数": active_by_city,
                }
            )
            .fillna(0)
            .astype({"本车系锁单数": "int64", "锁单数(全车系)": "int64", "在营门店数": "int64"})
        )
        city = city[(city["在营门店数"] > 0) & (city["本车系锁单数"] > 0)].copy()
        if city.empty:
            return
        city["本车系锁单占比"] = (
            (city["本车系锁单数"] / city["锁单数(全车系)"]).where(city["锁单数(全车系)"] > 0, 0.0) * 100.0
        ).round(1)
        city["本车系店均锁单"] = (city["本车系锁单数"] / city["在营门店数"]).round(2)
        city = city.sort_values(["本车系锁单数", "本车系锁单占比"], ascending=[False, False]).head(total_topn)

        rows = []
        for city_name, row in city.iterrows():
            rows.append(
                [
                    str(city_name),
                    _fmt(int(row["本车系锁单数"])),
                    _fmt(int(row["锁单数(全车系)"])),
                    f'{float(row["本车系锁单占比"]):.1f}%',
                    _fmt(int(row["在营门店数"])),
                    _fmt(float(row["本车系店均锁单"])),
                ]
            )

        col_widths = [len(h) for h in total_headers]
        for r in rows:
            for i, cell in enumerate(r):
                col_widths[i] = max(col_widths[i], len(str(cell)))

        def fmt_row(r: list[str]) -> str:
            return " | ".join(
                str(c).rjust(col_widths[i]) if i else str(c).ljust(col_widths[i])
                for i, c in enumerate(r)
            )

        print()
        print(f"{group} 上市后{listing_plus_days}天 本车系锁单总数 TOP{total_topn} 城市")
        print(fmt_row(total_headers))
        print("-+-".join("-" * w for w in col_widths))
        for r in rows:
            print(fmt_row(r))

    for spec in group_specs:
        print_city_total_top(spec)

    if str(args.series_city_scatter_out or "").strip():
        import plotly.graph_objects as go
        import plotly.io as pio

        palette = [
            "#006BA4",
            "#FF800E",
            "#ABABAB",
            "#595959",
            "#5F9ED1",
            "#C85200",
            "#898989",
            "#A2C8EC",
            "#FFBC79",
            "#CFCFCF",
        ]
        region_order = ["一区", "二区", "上海区", "三区", "华南特区", "其他"]
        region_colors = {
            "一区": palette[3],
            "二区": palette[1],
            "上海区": palette[2],
            "三区": palette[0],
            "华南特区": palette[4],
            "其他": palette[5],
        }
        group_rank = {k: i for i, k in enumerate(region_order)}

        city_region = (
            df.loc[:, ["store_city", "parent_region_name"]]
            .dropna(subset=["store_city"])
            .drop_duplicates(subset=["store_city"], keep="first")
            .set_index("store_city")["parent_region_name"]
            .astype("string")
        )

        def _region_group(v: object) -> str:
            s = str(v).strip()
            m = re.match(r"^(一区|二区|上海区|三区|华南特区)", s)
            return m.group(1) if m else "其他"

        def _scatter_for_group(spec: dict[str, object]):
            group = str(spec["name"])
            tp = time_periods[str(spec["base"])]
            listing_day = pd.Timestamp(str(tp["end"])).normalize()
            end_excl = listing_day + pd.Timedelta(days=int(listing_plus_days))

            m_group = _parse_sql_condition(df, str(spec["logic"])).fillna(False)
            locks_all_by_city = _calc_lock_counts_by_city(df, listing_day, end_excl)
            locks_group_by_city = _calc_lock_counts_by_city(df[m_group].copy(), listing_day, end_excl)
            active_by_city = _calc_active_store_counts_by_city(df, listing_day)

            lock_col = f"锁单数({group})"
            avg_col = f"店均锁单数({group})"
            city = (
                pd.DataFrame(
                    {
                        "锁单数(全车系)": locks_all_by_city,
                        lock_col: locks_group_by_city,
                        "在营门店数": active_by_city,
                    }
                )
                .fillna(0)
                .astype({"锁单数(全车系)": "int64", lock_col: "int64", "在营门店数": "int64"})
            )
            city = city[(city["锁单数(全车系)"] > 0) & (city[lock_col] > 0) & (city["在营门店数"] > 0)].copy()
            if city.empty:
                return None

            denom = float(city["锁单数(全车系)"].sum())
            national_share = float(city[lock_col].sum()) / denom if denom > 0 else 0.0
            if national_share <= 0:
                return None

            city[f"本城{group}占比"] = (city[lock_col] / city["锁单数(全车系)"]).where(city["锁单数(全车系)"] > 0, 0.0)
            city[avg_col] = (city[lock_col] / city["在营门店数"]).round(4)
            city["偏好度"] = (city[f"本城{group}占比"] / float(national_share)).astype("float64")
            city = city.dropna(subset=["偏好度", avg_col]).copy()
            city["偏好度log"] = city["偏好度"].apply(lambda v: math.log(v) if v and v > 0 else pd.NA).astype("float64")
            city = city.dropna(subset=["偏好度log"]).copy()
            if city.empty:
                return None

            city = city.sort_values([lock_col], ascending=[False])
            city["大区"] = (
                pd.Series(city.index.astype("string"), index=city.index).map(city_region).fillna("未知").astype("string")
            )
            city["大区分组"] = city["大区"].apply(_region_group).astype("string")

            hover = []
            for city_name, row in city.iterrows():
                hover.append(
                    "<br>".join(
                        [
                            f"城市: {city_name}",
                            f"大区: {str(row['大区'])}",
                            f"分组: {str(row['大区分组'])}",
                            f"{group}锁单数: {int(row[lock_col])}",
                            f"{group}店均锁单: {float(row[avg_col]):.2f}",
                            f"全车系锁单数: {int(row['锁单数(全车系)'])}",
                            f"{group}占比: {float(row[f'本城{group}占比'])*100.0:.1f}%",
                            f"偏好度(份额Lift): {float(row['偏好度']):.2f}",
                            f"在营门店数: {int(row['在营门店数'])}",
                        ]
                    )
                )
            city["hover"] = hover

            traces = []
            full_regions = sorted(
                pd.Index(city["大区"].dropna().astype(str).unique()).tolist(),
                key=lambda x: (group_rank.get(_region_group(x), 999), str(x)),
            )
            for region_full in full_regions:
                sub = city[city["大区"].astype(str) == str(region_full)].copy()
                if sub.empty:
                    continue
                region_group = _region_group(region_full)
                traces.append(
                    go.Scatter(
                        name=str(region_full),
                        x=sub[avg_col].astype("float64"),
                        y=sub["偏好度"].astype("float64"),
                        mode="markers",
                        marker={
                            "size": (
                                sub[lock_col]
                                .clip(lower=1)
                                .astype("float64")
                                .pow(0.5)
                                * 6.0
                            ).clip(lower=6.0, upper=40.0),
                            "color": region_colors.get(str(region_group), "#7f7f7f"),
                            "opacity": 0.8,
                            "line": {"color": palette[-1], "width": 1},
                        },
                        hovertext=sub["hover"].tolist(),
                        hoverinfo="text",
                    )
                )

            label_topn = 10
            label_df = city.sort_values([avg_col, lock_col], ascending=[False, False]).head(label_topn).copy()
            if not label_df.empty:
                traces.append(
                    go.Scatter(
                        name=f"TOP{label_topn}城市",
                        x=label_df[avg_col].astype("float64"),
                        y=label_df["偏好度"].astype("float64"),
                        mode="text",
                        text=[str(x) for x in label_df.index.tolist()],
                        textposition="top center",
                        textfont={"size": 11, "color": "#111111"},
                        hoverinfo="skip",
                        showlegend=False,
                    )
                )

            if not traces:
                return None

            fig = go.Figure(data=traces)
            fig.update_layout(
                title=f"{group} 上市后{listing_plus_days}天 城市店均锁单数 vs 偏好度（份额Lift，按大区）",
                template="plotly_white",
                xaxis_title=f"城市{group}店均锁单数（上市后{listing_plus_days}天）",
                yaxis_title=f"偏好度（本城{group}占比 / 全国{group}占比，log轴）",
                yaxis_type="log",
                legend_title_text="大区",
                height=720,
            )
            grid_color = "rgba(0,0,0,0.08)"
            fig.update_xaxes(showgrid=True, gridcolor=grid_color)
            fig.update_yaxes(showgrid=True, gridcolor=grid_color)
            return fig

        figs = []
        for spec in group_specs:
            fig = _scatter_for_group(spec)
            if fig is not None:
                figs.append((str(spec["name"]), fig))

        if figs:
            html_parts = [
                "<!doctype html>",
                "<html>",
                "<head>",
                "<meta charset='utf-8' />",
                "<meta name='viewport' content='width=device-width, initial-scale=1' />",
                "<title>城市散点图</title>",
                "<style>body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,'Noto Sans',sans-serif;margin:24px}h2{margin:28px 0 12px}</style>",
                "</head>",
                "<body>",
                "<h1>城市散点图（按大区配色）</h1>",
            ]
            for i, (g, fig) in enumerate(figs):
                html_parts.append(f"<h2>{g}</h2>")
                html_parts.append(
                    pio.to_html(
                        fig,
                        include_plotlyjs="cdn" if i == 0 else False,
                        full_html=False,
                    )
                )
            html_parts.append("</body></html>")

            out_path = Path(str(args.series_city_scatter_out)).expanduser().resolve()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text("\n".join(html_parts) + "\n", encoding="utf-8")
            print()
            print(f"城市店均锁单数 vs 偏好度 散点图已写入: {out_path}")


if __name__ == "__main__":
    main()

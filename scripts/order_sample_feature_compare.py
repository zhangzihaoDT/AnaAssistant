"""
---
name: order_sample_feature_compare
type: script
path: scripts/order_sample_feature_compare.py
updated_at: "2026-04-28 00:00"
summary: 基于订单号清单或自然语言条件筛选订单（A/B），按业务定义计算 series_group_logic，并与选配表合并成宽表；输出 A 组画像或 A vs B 核心差异摘要（占比/均值/SMD/缺失率）。
inputs:
  - schema/data_path.md (optional, via --data-path-md)
  - schema/business_definition.json (optional, via --business-definition)
  - order_data.parquet
  - config_attribute.parquet (optional, via --no-config-attribute)
outputs:
  - stdout: A 组画像或 A vs B 差异摘要
  - optional: 宽表 parquet/csv (via --wide-out)
  - optional: markdown 摘要 (via --md-out)
cli:
  - python3 scripts/order_sample_feature_compare.py --a-nl "LS8 用户性别为男" --md-out out/LS8_男_画像.md
  - python3 scripts/order_sample_feature_compare.py --a-nl "LS8 用户性别为男" --b-nl "LS8 用户性别为女" --md-out out/LS8_男_vs_女.md
  - python3 scripts/order_sample_feature_compare.py --a-nl "LS8 Attribute 的 IM 智控地暖系统，value=是" --md-out out/LS8_地暖_是_画像.md
  - python3 scripts/order_sample_feature_compare.py --a-nl "LS8 Attribute 的 IM 智控地暖系统，value=是 且锁单的用户" --b-nl "LS8 锁单的用户" --exclude-features "IM 智控地暖系统" --md-out out/LS8_地暖锁单_vs_LS8整体锁单.md
  - python3 scripts/order_sample_feature_compare.py --orders-a-list 1001,1002 --orders-b-list 2001,2002 --wide-out out/wide.parquet --md-out out/summary.md
---
"""

import argparse
import json
import math
import re
import sys
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from operators.series_group_logic import apply_series_group_logic


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _load_json(path: Path) -> dict:
    return json.loads(_read_text(path))


def _normalize_data_path(s: str) -> str:
    return str(s).strip().replace("\\_", "_").replace("\\*", "*")


def _load_data_paths_from_md(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in _read_text(path).splitlines():
        line = line.strip()
        if not line or "：" not in line:
            continue
        k, v = line.split("：", 1)
        k = k.strip()
        v = _normalize_data_path(v)
        if v:
            out[k] = v
    return out


def _parse_order_numbers_from_text(s: str) -> list[str]:
    raw = re.split(r"[,\s]+", str(s).strip())
    return [x.strip() for x in raw if x.strip()]


def _load_order_numbers(path: Path) -> list[str]:
    ext = path.suffix.lower()
    if ext in {".txt"}:
        return _parse_order_numbers_from_text(_read_text(path))
    if ext in {".csv"}:
        df = pd.read_csv(path)
    elif ext in {".xlsx", ".xls"}:
        df = pd.read_excel(path)
    elif ext in {".json"}:
        raw = json.loads(_read_text(path))
        if isinstance(raw, list):
            return [str(x).strip() for x in raw if str(x).strip()]
        if isinstance(raw, dict) and "order_numbers" in raw and isinstance(raw["order_numbers"], list):
            return [str(x).strip() for x in raw["order_numbers"] if str(x).strip()]
        raise ValueError("JSON 输入仅支持 list[str] 或 {'order_numbers': list[str]}")
    else:
        raise ValueError(f"不支持的订单清单文件类型: {path}")

    for col in ["order_number", "Order Number", "订单号", "OrderNumber"]:
        if col in df.columns:
            s = df[col].astype("string").dropna().astype(str)
            return [x.strip() for x in s.tolist() if x.strip()]
    raise ValueError(f"未在 {path} 中找到订单号列（尝试过 order_number / Order Number / 订单号）")


def _norm_text(s: str | None) -> str:
    return str(s or "").strip()


def _extract_series_group_logic(text: str) -> str | None:
    s = _norm_text(text).upper()
    for code in ["LS8", "LS9", "LS7", "L7", "CM2", "CM1", "CM0", "DM1", "DM0"]:
        if re.search(rf"\b{re.escape(code)}\b", s):
            return code
        if code in s:
            return code
    return None


def _extract_gender(text: str, default_col: str) -> tuple[str | None, str | None]:
    s = _norm_text(text)
    if not s:
        return None, None

    col = default_col
    if re.search(r"(购车人|下单人)", s):
        col = "order_gender"
    if re.search(r"(车主)", s):
        col = "owner_gender"

    m = re.search(r"(性别|gender)[^男女性]*([男女])", s, flags=re.IGNORECASE)
    if m:
        return col, m.group(2)
    if "男" in s and "女" not in s:
        return col, "男"
    if "女" in s and "男" not in s:
        return col, "女"
    return None, None


def _extract_attribute_value(text: str) -> tuple[str | None, str | None]:
    s = _norm_text(text)
    if not s:
        return None, None

    attr = None
    m = re.search(r"(?:Attribute|选配|配置)\s*(?:的|=|为)?\s*([^，,]+)", s, flags=re.IGNORECASE)
    if m:
        attr = m.group(1).strip()
        for stop in ["value", "取值", "为", "=", "订单", "特征"]:
            if stop in attr:
                attr = attr.split(stop, 1)[0].strip()
        attr = attr.strip("“”\"' ").strip()
        if not attr:
            attr = None

    val = None
    mv = re.search(r"(?:value|取值)\s*(?:=|为)\s*([^，,]+)", s, flags=re.IGNORECASE)
    if mv:
        val = mv.group(1).strip().strip("“”\"' ").strip()
    else:
        mv2 = re.search(r"(?:为|=)\s*(是|否|有|无)(?:的)?", s)
        if mv2:
            val = mv2.group(1).strip()
    if val is not None:
        val = val.rstrip("的").strip()
        for sep in ["且", "并且", "同时", "并", "和", "，", ",", "；", ";"]:
            if sep in val:
                val = val.split(sep, 1)[0].strip()
        if not val:
            val = None

    return attr, val


def _extract_lock_status(text: str) -> bool | None:
    s = _norm_text(text)
    if not s:
        return None
    if re.search(r"(未锁单|未锁|没锁单|未锁定)", s):
        return False
    if re.search(r"(已锁单|锁单|锁定|锁单用户|锁单的用户)", s):
        return True
    return None


def _extract_lock_date_range(text: str) -> tuple[pd.Timestamp, pd.Timestamp] | None:
    s = _norm_text(text)
    if not s:
        return None

    low = s.lower()
    if "yesterday_lock" in low or (("yesterday" in low or "昨日" in s or "昨天" in s) and ("锁单" in s or "锁定" in s)):
        end = pd.Timestamp.now().normalize()
        start = end - pd.Timedelta(days=1)
        return start, end

    if "today_lock" in low or (("today" in low or "今日" in s or "今天" in s) and ("锁单" in s or "锁定" in s)):
        start = pd.Timestamp.now().normalize()
        end = start + pd.Timedelta(days=1)
        return start, end

    m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
    if m and ("锁单" in s or "锁定" in s):
        try:
            start = pd.Timestamp(m.group(1)).normalize()
            end = start + pd.Timedelta(days=1)
            return start, end
        except Exception:
            return None

    return None


def _filter_orders_by_nl(
    order_data_path: Path,
    config_attribute_path: Path | None,
    business_definition: dict,
    text: str,
    default_gender_col: str,
) -> list[str]:
    series_group = _extract_series_group_logic(text)
    gender_col, gender_val = _extract_gender(text, default_col=default_gender_col)
    attr, val = _extract_attribute_value(text)
    lock_required = _extract_lock_status(text)
    lock_range = _extract_lock_date_range(text)

    base_cols = [
        "order_number",
        "product_name",
        "series",
        "order_type",
        "store_city",
        "parent_region_name",
        "license_city",
        "order_gender",
        "owner_gender",
        "lock_time",
    ]
    try:
        odf = pd.read_parquet(order_data_path, columns=base_cols)
    except Exception:
        odf = pd.read_parquet(order_data_path)

    if odf.empty:
        return []
    if "order_number" not in odf.columns:
        raise ValueError("order_data.parquet 缺少 order_number 字段")
    if "product_name" not in odf.columns:
        raise ValueError("order_data.parquet 缺少 product_name 字段（series_group_logic 依赖）")

    odf = odf.copy()
    odf["order_number"] = odf["order_number"].astype("string")
    odf = apply_series_group_logic(odf, business_definition)

    m = pd.Series([True] * len(odf), index=odf.index)
    if series_group is not None and "series_group_logic" in odf.columns:
        m = m & (odf["series_group_logic"].astype("string").str.upper() == str(series_group).upper())
    if gender_col is not None and gender_val is not None and gender_col in odf.columns:
        m = m & odf[gender_col].astype("string").fillna("").str.contains(str(gender_val), na=False, regex=False)

    if lock_required is not None or lock_range is not None:
        lock_time = pd.to_datetime(odf["lock_time"], errors="coerce") if "lock_time" in odf.columns else pd.Series([pd.NaT] * len(odf), index=odf.index)
        if lock_range is not None:
            start, end = lock_range
            m = m & lock_time.notna() & (lock_time >= start) & (lock_time < end)
        elif lock_required is not None:
            m = m & (lock_time.notna() if lock_required else lock_time.isna())

    order_numbers = odf.loc[m, "order_number"].dropna().astype("string").unique().tolist()
    if not order_numbers:
        return []

    if attr is not None:
        if config_attribute_path is None:
            raise ValueError("自然语言条件包含 Attribute，但未提供 config_attribute.parquet（或已指定 --no-config-attribute）")
        cdf = pd.read_parquet(config_attribute_path, columns=["Order Number", "Attribute", "value"])
        cdf = cdf.copy()
        cdf["Order Number"] = cdf["Order Number"].astype("string")
        cdf["Attribute"] = cdf["Attribute"].astype("string")
        cdf["value"] = cdf["value"].astype("string")

        mm = cdf["Order Number"].isin(pd.Series(order_numbers, dtype="string"))
        attr_norm = re.sub(r"\s+", "", str(attr))
        attr_s = cdf["Attribute"].fillna("").astype("string").str.replace(r"\s+", "", regex=True)
        mm = mm & attr_s.str.contains(attr_norm, na=False, regex=False)
        if val is not None:
            val_norm = re.sub(r"\s+", "", str(val))
            val_s = cdf["value"].fillna("").astype("string").str.replace(r"\s+", "", regex=True)
            mm = mm & val_s.str.contains(val_norm, na=False, regex=False)
        order_numbers = cdf.loc[mm, "Order Number"].dropna().astype("string").unique().tolist()

    return [str(x).strip() for x in order_numbers if str(x).strip()]


def _read_orders_df(args) -> pd.DataFrame:
    a: list[str] = []
    b: list[str] = []

    if args.orders_a is not None:
        a = _load_order_numbers(Path(args.orders_a))
    elif args.orders_a_list is not None:
        a = _parse_order_numbers_from_text(args.orders_a_list)

    if args.orders_b is not None:
        b = _load_order_numbers(Path(args.orders_b))
    elif args.orders_b_list is not None:
        b = _parse_order_numbers_from_text(args.orders_b_list)

    if not a or not b:
        raise ValueError("必须同时提供 A/B 两组订单号（--orders-a/--orders-a-list 与 --orders-b/--orders-b-list）")

    df_a = pd.DataFrame({"order_number": pd.Series(a, dtype="string"), "group": "A"})
    df_b = pd.DataFrame({"order_number": pd.Series(b, dtype="string"), "group": "B"})
    df = pd.concat([df_a, df_b], ignore_index=True)
    df["order_number"] = df["order_number"].astype("string").str.strip()
    df = df[df["order_number"].notna() & (df["order_number"] != "")]
    df = df.drop_duplicates(subset=["order_number", "group"], keep="first")
    return df


def _read_order_data(order_data_path: Path, order_numbers: list[str]) -> pd.DataFrame:
    cols = [
        "order_number",
        "product_name",
        "series",
        "order_type",
        "invoice_amount",
        "parent_region_name",
        "store_name",
        "store_city",
        "license_city",
        "order_create_date",
        "first_assign_time",
        "lock_time",
        "delivery_date",
        "invoice_upload_time",
        "intention_payment_time",
        "deposit_payment_time",
        "final_payment_time",
        "finance_product",
        "final_payment_way",
        "buyer_age",
        "owner_age",
        "order_gender",
        "owner_gender",
        "buyer_identity_no",
        "owner_identity_no",
        "vin",
    ]

    try:
        df = pd.read_parquet(order_data_path, columns=cols, filters=[("order_number", "in", order_numbers)])
    except Exception:
        try:
            df = pd.read_parquet(order_data_path, columns=cols)
        except Exception:
            df = pd.read_parquet(order_data_path)

        if "order_number" not in df.columns:
            raise ValueError("order_data.parquet 缺少 order_number 字段")
        df["order_number"] = df["order_number"].astype("string")
        df = df[df["order_number"].isin(pd.Series(order_numbers, dtype="string"))].copy()

    if df.empty:
        return df
    df = df.copy()
    df["order_number"] = df["order_number"].astype("string")
    return df


def _add_first_assign_lock_time(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "first_assign_time" not in df.columns or "lock_time" not in df.columns:
        return df

    out = df.copy()
    out["first_assign_time"] = pd.to_datetime(out["first_assign_time"], errors="coerce")
    out["lock_time"] = pd.to_datetime(out["lock_time"], errors="coerce")
    delta = out["lock_time"] - out["first_assign_time"]
    days = (delta.dt.total_seconds() / 86400.0).astype("Float64")
    out["first_assign_lock_time"] = days.where(days.ge(0))
    return out


def _read_config_attribute(config_attribute_path: Path, order_numbers: list[str]) -> pd.DataFrame:
    cols = ["Order Number", "Attribute", "value", "is_staff"]
    try:
        df = pd.read_parquet(config_attribute_path, columns=cols, filters=[("Order Number", "in", order_numbers)])
    except Exception:
        try:
            df = pd.read_parquet(config_attribute_path, columns=cols)
        except Exception:
            df = pd.read_parquet(config_attribute_path)

        if "Order Number" not in df.columns:
            raise ValueError("config_attribute.parquet 缺少 Order Number 字段")
        df["Order Number"] = df["Order Number"].astype("string")
        df = df[df["Order Number"].isin(pd.Series(order_numbers, dtype="string"))].copy()

    if df.empty:
        return df
    df = df.copy()
    df["Order Number"] = df["Order Number"].astype("string")
    df["Attribute"] = df["Attribute"].astype("string")
    df["value"] = df["value"].astype("string")
    return df


def _pivot_config_attribute(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(index=pd.Index([], name="order_number"))

    staff = None
    if "is_staff" in df.columns:
        staff = (
            df[["Order Number", "is_staff"]]
            .dropna(subset=["Order Number"])
            .drop_duplicates(subset=["Order Number", "is_staff"], keep="last")
            .groupby("Order Number", dropna=False, sort=False)["is_staff"]
            .max()
            .astype("boolean")
            .rename("is_staff")
            .to_frame()
        )

    core = df.dropna(subset=["Order Number", "Attribute"]).copy()
    core = core.sort_values(["Order Number", "Attribute"], kind="mergesort")
    core = core.drop_duplicates(subset=["Order Number", "Attribute"], keep="last")
    wide = core.pivot(index="Order Number", columns="Attribute", values="value")
    wide.index = wide.index.astype("string")
    wide.columns = wide.columns.astype("string")

    out = wide
    if staff is not None and not staff.empty:
        out = out.join(staff, how="left")
    out = out.reset_index().rename(columns={"Order Number": "order_number"})
    out["order_number"] = out["order_number"].astype("string")
    return out


def _fmt_pct(x: float | None) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "-"
    return f"{x * 100:.1f}%"


def _fmt_num(x: float | None) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "-"
    if abs(x) >= 100:
        return f"{x:.1f}"
    return f"{x:.3f}".rstrip("0").rstrip(".")


def _summarize_categorical(df: pd.DataFrame, group_col: str, col: str, top_k: int) -> list[dict]:
    s = df[col].astype("string")
    a = s[df[group_col] == "A"]
    b = s[df[group_col] == "B"]

    da = a.value_counts(dropna=False)
    db = b.value_counts(dropna=False)
    cats = pd.Index(da.index).union(db.index)
    if len(cats) > top_k:
        score = (da.reindex(cats, fill_value=0) / max(len(a), 1) - db.reindex(cats, fill_value=0) / max(len(b), 1)).abs()
        cats = score.sort_values(ascending=False).head(top_k).index

    out: list[dict] = []
    for c in cats.tolist():
        pa = float(da.get(c, 0)) / max(len(a), 1)
        pb = float(db.get(c, 0)) / max(len(b), 1)
        out.append({"feature": col, "value": None if pd.isna(c) else str(c), "pA": pa, "pB": pb, "diff": pa - pb})
    out.sort(key=lambda x: abs(float(x["diff"])), reverse=True)
    return out


def _summarize_numeric(df: pd.DataFrame, group_col: str, col: str) -> dict | None:
    s = pd.to_numeric(df[col], errors="coerce")
    a = s[df[group_col] == "A"]
    b = s[df[group_col] == "B"]
    if a.notna().sum() + b.notna().sum() == 0:
        return None

    ma = float(a.mean()) if a.notna().any() else None
    mb = float(b.mean()) if b.notna().any() else None
    sa = float(a.std(ddof=0)) if a.notna().any() else None
    sb = float(b.std(ddof=0)) if b.notna().any() else None
    pooled = None
    if sa is not None and sb is not None:
        pooled = math.sqrt((sa * sa + sb * sb) / 2) if (sa * sa + sb * sb) > 0 else 0.0
    smd = None
    if ma is not None and mb is not None and pooled not in {None, 0.0}:
        smd = (ma - mb) / pooled
    mr_a = 1 - float(a.notna().mean()) if len(a) else None
    mr_b = 1 - float(b.notna().mean()) if len(b) else None
    return {"feature": col, "meanA": ma, "meanB": mb, "diff": None if ma is None or mb is None else (ma - mb), "smd": smd, "missA": mr_a, "missB": mr_b}


def _parse_exclude_features(s: str | None) -> set[str]:
    raw = _norm_text(s)
    if not raw:
        return set()
    parts = [x.strip() for x in re.split(r"[,;\n]+", raw) if x.strip()]
    return set(parts)


def _build_summary_md(df: pd.DataFrame, group_col: str, max_features: int, exclude_features: set[str] | None = None) -> str:
    base_cols = [
        "series_group_logic",
        "product_name",
        "series",
        "order_type",
        "store_city",
        "parent_region_name",
        "license_city",
        "finance_product",
        "final_payment_way",
        "is_staff",
    ]
    num_cols = ["invoice_amount", "buyer_age", "owner_age"]

    exclude = set(exclude_features or set())
    existing_base = [c for c in base_cols if c in df.columns and c not in exclude]
    existing_num = [c for c in num_cols if c in df.columns and c not in exclude]
    extra_cols = [c for c in df.columns if c not in set(existing_base + existing_num + [group_col, "order_number"]) and c not in exclude]

    cat_summaries: list[dict] = []
    for col in existing_base + extra_cols:
        if col not in df.columns:
            continue
        s = df[col]
        if pd.api.types.is_numeric_dtype(s):
            continue
        if s.nunique(dropna=False) <= 1:
            continue
        cat_summaries.extend(_summarize_categorical(df, group_col, col, top_k=6))

    cat_summaries.sort(key=lambda x: abs(float(x["diff"])), reverse=True)
    cat_summaries = cat_summaries[: max_features * 2]

    num_summaries = []
    for col in existing_num:
        r = _summarize_numeric(df, group_col, col)
        if r is not None:
            num_summaries.append(r)
    num_summaries.sort(key=lambda x: abs(float(x["smd"] or 0.0)), reverse=True)

    n_a = int((df[group_col] == "A").sum())
    n_b = int((df[group_col] == "B").sum())

    lines: list[str] = []
    lines.append(f"# 订单抽样特征对比")
    lines.append("")
    lines.append(f"- A 组订单数：{n_a}")
    lines.append(f"- B 组订单数：{n_b}")
    lines.append("")

    if cat_summaries:
        lines.append("## 类别特征差异（Top）")
        lines.append("")
        lines.append("| 特征 | 取值 | A占比 | B占比 | A-B |")
        lines.append("| --- | --- | ---: | ---: | ---: |")
        for r in cat_summaries[:max_features]:
            v = r["value"] if r["value"] is not None else "(缺失)"
            lines.append(f"| {r['feature']} | {v} | {_fmt_pct(r['pA'])} | {_fmt_pct(r['pB'])} | {_fmt_pct(r['diff'])} |")
        lines.append("")

    if num_summaries:
        lines.append("## 数值特征差异")
        lines.append("")
        lines.append("| 特征 | A均值 | B均值 | A-B | SMD | A缺失率 | B缺失率 |")
        lines.append("| --- | ---: | ---: | ---: | ---: | ---: | ---: |")
        for r in num_summaries[:max_features]:
            lines.append(
                f"| {r['feature']} | {_fmt_num(r['meanA'])} | {_fmt_num(r['meanB'])} | {_fmt_num(r['diff'])} | {_fmt_num(r['smd'])} | {_fmt_pct(r['missA'])} | {_fmt_pct(r['missB'])} |"
            )
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def _build_profile_md(df: pd.DataFrame, group_name: str, max_features: int, exclude_features: set[str] | None = None) -> str:
    base_cols = [
        "series_group_logic",
        "product_name",
        "series",
        "order_type",
        "store_city",
        "parent_region_name",
        "license_city",
        "finance_product",
        "final_payment_way",
        "is_staff",
        "owner_gender",
        "order_gender",
    ]
    num_cols = ["invoice_amount", "buyer_age", "owner_age"]

    exclude = set(exclude_features or set())
    existing_base = [c for c in base_cols if c in df.columns and c not in exclude]
    existing_num = [c for c in num_cols if c in df.columns and c not in exclude]
    extra_cols = [c for c in df.columns if c not in set(existing_base + existing_num + ["order_number", "group"]) and c not in exclude]

    lines: list[str] = []
    lines.append("# 订单抽样特征画像")
    lines.append("")
    lines.append(f"- 订单组：{group_name}")
    lines.append(f"- 订单数：{len(df)}")
    lines.append("")

    cat_rows = []
    base_set = set(existing_base)
    for col in existing_base + extra_cols:
        if col not in df.columns:
            continue
        s = df[col]
        if pd.api.types.is_numeric_dtype(s):
            continue
        if col not in base_set and s.nunique(dropna=False) <= 1:
            continue
        vc = s.astype("string").value_counts(dropna=False).head(6)
        for k, v in vc.items():
            cat_rows.append(
                {
                    "feature": col,
                    "value": None if pd.isna(k) else str(k),
                    "pct": float(v) / max(len(df), 1),
                }
            )
    cat_rows.sort(key=lambda x: float(x["pct"]), reverse=True)

    if cat_rows:
        lines.append("## 类别特征（Top）")
        lines.append("")
        lines.append("| 特征 | 取值 | 占比 |")
        lines.append("| --- | --- | ---: |")
        for r in cat_rows[:max_features]:
            v = r["value"] if r["value"] is not None else "(缺失)"
            lines.append(f"| {r['feature']} | {v} | {_fmt_pct(r['pct'])} |")
        lines.append("")

    num_rows = []
    for col in existing_num:
        s = pd.to_numeric(df[col], errors="coerce")
        if s.notna().sum() == 0:
            continue
        num_rows.append(
            {
                "feature": col,
                "mean": float(s.mean()) if s.notna().any() else None,
                "miss": 1 - float(s.notna().mean()) if len(s) else None,
            }
        )

    if num_rows:
        lines.append("## 数值特征")
        lines.append("")
        lines.append("| 特征 | 均值 | 缺失率 |")
        lines.append("| --- | ---: | ---: |")
        for r in num_rows[:max_features]:
            lines.append(f"| {r['feature']} | {_fmt_num(r['mean'])} | {_fmt_pct(r['miss'])} |")
        lines.append("")

    return "\n".join(lines).strip() + "\n"


def _save_table(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".parquet":
        import pyarrow as pa
        import pyarrow.parquet as pq

        table = pa.Table.from_pandas(df, preserve_index=False)
        with path.open("wb") as f:
            pq.write_table(table, f)
        return
    if path.suffix.lower() == ".csv":
        path.write_text(df.to_csv(index=False), encoding="utf-8")
        return
    raise ValueError("仅支持输出为 .parquet 或 .csv")


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description="订单抽样特征对比：打通宽表并输出 A vs B 差异摘要")
    p.add_argument("--a-nl", help="A 组自然语言筛选条件（不提供订单号时使用）")
    p.add_argument("--b-nl", help="B 组自然语言筛选条件（不提供则只输出 A 组画像）")
    p.add_argument(
        "--gender-default",
        choices=["owner", "order"],
        default="owner",
        help="自然语言里未指明“购车人/车主”时默认使用的性别字段（默认 owner）",
    )
    p.add_argument("--orders-a", help="A 组订单号文件（csv/xlsx/txt/json）")
    p.add_argument("--orders-b", help="B 组订单号文件（csv/xlsx/txt/json）")
    p.add_argument("--orders-a-list", help="A 组订单号（逗号/空格分隔）")
    p.add_argument("--orders-b-list", help="B 组订单号（逗号/空格分隔）")
    p.add_argument("--data-path-md", default="schema/data_path.md", help="数据路径配置（默认 schema/data_path.md）")
    p.add_argument("--business-definition", default="schema/business_definition.json", help="业务定义（默认 schema/business_definition.json）")
    p.add_argument("--order-data", help="订单表 parquet 路径（默认从 data_path.md 读取“订单分析”）")
    p.add_argument("--config-attribute", help="选配表 parquet 路径（默认从 data_path.md 读取“选配信息”）")
    p.add_argument("--no-config-attribute", action="store_true", help="不加载选配表（只用订单表特征）")
    p.add_argument("--wide-out", help="输出打通后的宽表（.parquet 或 .csv）")
    p.add_argument("--md-out", help="输出 markdown 摘要（.md）")
    p.add_argument("--max-features", type=int, default=12, help="摘要中最多展示的特征条数")
    p.add_argument("--exclude-features", help="不在摘要中展示的列名（逗号分隔），例如：\"IM 智控地暖系统,lock_time\"")
    args = p.parse_args(argv)

    data_path_md = Path(args.data_path_md)
    data_paths = _load_data_paths_from_md(data_path_md) if data_path_md.exists() else {}

    order_data_path = Path(args.order_data or data_paths.get("订单分析") or "")
    if not str(order_data_path):
        raise ValueError("未提供 order_data.parquet 路径（--order-data 或 data_path.md 的“订单分析”）")
    if not order_data_path.exists():
        raise FileNotFoundError(f"找不到订单表: {order_data_path}")

    business_definition_path = Path(args.business_definition)
    if not business_definition_path.exists():
        raise FileNotFoundError(f"找不到业务定义: {business_definition_path}")
    business_definition = _load_json(business_definition_path)

    cfg_path = None
    if not args.no_config_attribute:
        p0 = Path(args.config_attribute or data_paths.get("选配信息") or "")
        if str(p0) and p0.exists():
            cfg_path = p0

    default_gender_col = "owner_gender" if args.gender_default == "owner" else "order_gender"

    compare_mode = bool(args.orders_a or args.orders_a_list or args.orders_b or args.orders_b_list)
    if compare_mode:
        orders_df = _read_orders_df(args)
    else:
        if not args.a_nl:
            raise ValueError("未提供订单号，也未提供 --a-nl；请二选一输入订单号清单或自然语言筛选条件")
        a_orders = _filter_orders_by_nl(
            order_data_path=order_data_path,
            config_attribute_path=cfg_path,
            business_definition=business_definition,
            text=str(args.a_nl),
            default_gender_col=default_gender_col,
        )
        if not a_orders:
            raise ValueError("A 组条件未筛到任何订单")
        df_a = pd.DataFrame({"order_number": pd.Series(a_orders, dtype="string"), "group": "A"})

        if args.b_nl:
            b_orders = _filter_orders_by_nl(
                order_data_path=order_data_path,
                config_attribute_path=cfg_path,
                business_definition=business_definition,
                text=str(args.b_nl),
                default_gender_col=default_gender_col,
            )
            if not b_orders:
                raise ValueError("B 组条件未筛到任何订单")
            df_b = pd.DataFrame({"order_number": pd.Series(b_orders, dtype="string"), "group": "B"})
            orders_df = pd.concat([df_a, df_b], ignore_index=True)
        else:
            orders_df = df_a

        orders_df["order_number"] = orders_df["order_number"].astype("string").str.strip()
        orders_df = orders_df[orders_df["order_number"].notna() & (orders_df["order_number"] != "")]
        orders_df = orders_df.drop_duplicates(subset=["order_number", "group"], keep="first")

    all_orders = orders_df["order_number"].astype("string").dropna().unique().tolist()
    order_df = _read_order_data(order_data_path, all_orders)
    order_df = _add_first_assign_lock_time(order_df)
    order_df = apply_series_group_logic(order_df, business_definition)

    wide_cfg = pd.DataFrame({"order_number": pd.Series([], dtype="string")})
    if cfg_path is not None:
        cfg_long = _read_config_attribute(cfg_path, all_orders)
        wide_cfg = _pivot_config_attribute(cfg_long)

    wide = order_df.merge(wide_cfg, how="left", on="order_number")
    wide = orders_df.merge(wide, how="left", on="order_number")

    if args.wide_out:
        _save_table(wide, Path(args.wide_out))

    max_features = max(1, int(args.max_features))
    exclude_features = _parse_exclude_features(args.exclude_features)
    if "B" in set(orders_df["group"].astype("string").unique().tolist()):
        md = _build_summary_md(wide, group_col="group", max_features=max_features, exclude_features=exclude_features)
    else:
        md = _build_profile_md(wide[wide["group"] == "A"].copy(), group_name="A", max_features=max_features, exclude_features=exclude_features)
    sys.stdout.write(md)

    if args.md_out:
        out_path = Path(args.md_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(md, encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

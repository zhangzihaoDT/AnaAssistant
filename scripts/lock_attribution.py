import argparse
import glob
import json
from pathlib import Path
import re

import pandas as pd


def _parse_target_date(value: str) -> pd.Timestamp:
    parsed = pd.to_datetime(str(value), errors="coerce")
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


def _safe_ratio(numer: float | int, denom: float | int) -> float | None:
    denom = float(denom)
    if denom == 0.0:
        return None
    return float(numer) / denom


def _to_percent_1dp(value: float | None) -> str | None:
    if value is None:
        return None
    return f"{round(value * 100.0, 1):.1f}%"


def _pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols_lower = {str(c).lower(): c for c in df.columns}
    for cand in candidates:
        key = str(cand).lower()
        if key in cols_lower:
            return str(cols_lower[key])
    return None


def _resolve_journey_columns(df: pd.DataFrame) -> dict[str, str | None]:
    return {
        "user_phone_md5": _pick_column(df, ["lc_user_phone_md5", "ic_user_phone_md5", "user_phone_md5", "phone_md5"]),
        "main_code": _pick_column(df, ["lc_main_code", "ic_main_code", "main_code", "clue_code"]),
        "channel": _pick_column(
            df,
            [
                "lc_small_channel_name",
                "ic_small_channel_name",
                "lc_small_channel",
                "small_channel_name",
                "channel_name",
            ],
        ),
        "create_time": _pick_column(df, ["lc_create_time", "ic_create_time", "create_time", "created_time"]),
        "lock_time": _pick_column(
            df,
            ["lc_order_lock_time_min", "ic_order_lock_time_min", "order_lock_time_min", "order_lock_time", "lock_time"],
        ),
    }


def _tokenize_logic(expr: str) -> list[str]:
    token_re = re.compile(r"\s*(\(|\)|AND|OR|NOT|LIKE|[A-Za-z_][A-Za-z0-9_]*|'[^']*')\s*", re.IGNORECASE)
    tokens: list[str] = []
    pos = 0
    while pos < len(expr):
        m = token_re.match(expr, pos)
        if not m:
            raise ValueError(f"无法解析表达式: {expr}")
        tok = m.group(1)
        up = tok.upper()
        if up in {"AND", "OR", "NOT", "LIKE"}:
            tokens.append(up)
        else:
            tokens.append(tok)
        pos = m.end()
    return tokens


def _parse_logic_expr(tokens: list[str]) -> object:
    i = 0

    def peek() -> str | None:
        nonlocal i
        if i >= len(tokens):
            return None
        return tokens[i]

    def take(expected: str | None = None) -> str:
        nonlocal i
        tok = peek()
        if tok is None:
            raise ValueError("表达式意外结束")
        if expected is not None and tok != expected:
            raise ValueError(f"期望 {expected}，但得到 {tok}")
        i += 1
        return tok

    def parse_expr() -> object:
        return parse_or()

    def parse_or() -> object:
        node = parse_and()
        while peek() == "OR":
            take("OR")
            rhs = parse_and()
            node = ("OR", node, rhs)
        return node

    def parse_and() -> object:
        node = parse_not()
        while peek() == "AND":
            take("AND")
            rhs = parse_not()
            node = ("AND", node, rhs)
        return node

    def parse_not() -> object:
        if peek() == "NOT":
            take("NOT")
            node = parse_not()
            return ("NOT", node)
        return parse_primary()

    def parse_primary() -> object:
        if peek() == "(":
            take("(")
            node = parse_expr()
            take(")")
            return node
        return parse_predicate()

    def parse_predicate() -> object:
        ident = take()
        neg = False
        if peek() == "NOT":
            take("NOT")
            neg = True
        take("LIKE")
        pat = take()
        if not (len(pat) >= 2 and pat[0] == "'" and pat[-1] == "'"):
            raise ValueError("LIKE 右侧必须是单引号字符串")
        return ("PRED", str(ident), str(pat[1:-1]), neg)

    ast = parse_expr()
    if i != len(tokens):
        raise ValueError("表达式解析未消费全部 token")
    return ast


def _sql_like_mask(series: pd.Series, pattern: str) -> pd.Series:
    s = series.astype("string").fillna("")
    if "%" not in pattern and "_" not in pattern:
        return s.eq(pattern)
    parts: list[str] = []
    for ch in str(pattern):
        if ch == "%":
            parts.append(".*")
        elif ch == "_":
            parts.append(".")
        else:
            parts.append(re.escape(ch))
    regex = "^" + "".join(parts) + "$"
    return s.str.match(regex, na=False)


def _eval_logic_ast(ast: object, product_name: pd.Series) -> pd.Series:
    if isinstance(ast, tuple) and ast:
        op = ast[0]
        if op == "AND":
            return _eval_logic_ast(ast[1], product_name) & _eval_logic_ast(ast[2], product_name)
        if op == "OR":
            return _eval_logic_ast(ast[1], product_name) | _eval_logic_ast(ast[2], product_name)
        if op == "NOT":
            return ~_eval_logic_ast(ast[1], product_name)
        if op == "PRED":
            _, ident, pat, neg = ast
            if str(ident).lower() != "product_name":
                raise ValueError(f"仅支持 product_name 规则，当前为: {ident}")
            m = _sql_like_mask(product_name, str(pat))
            return (~m) if bool(neg) else m
    raise ValueError("表达式 AST 不合法")


def _load_series_group_logic(business_definition_path: Path) -> dict[str, str]:
    raw = json.loads(business_definition_path.read_text(encoding="utf-8"))
    logic = raw.get("series_group_logic")
    if not isinstance(logic, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in logic.items():
        if isinstance(k, str) and isinstance(v, str):
            out[k] = v
    return out


def _prepare_attribution_df(attribution_path: Path) -> tuple[pd.DataFrame, dict[str, str | None]]:
    cols = [
        "lc_main_code",
        "lc_user_phone_md5",
        "lc_create_time",
        "lc_order_lock_time_min",
        "lc_small_channel_name",
    ]
    try:
        df = pd.read_parquet(attribution_path, columns=cols)
    except Exception:
        df = pd.read_parquet(attribution_path)

    cols_map = _resolve_journey_columns(df)
    main_code_col = cols_map["main_code"] or "lc_main_code"
    user_col = cols_map["user_phone_md5"] or "lc_user_phone_md5"
    create_time_col = cols_map["create_time"] or "lc_create_time"
    lock_time_col = cols_map["lock_time"] or "lc_order_lock_time_min"

    if user_col not in df.columns or create_time_col not in df.columns or lock_time_col not in df.columns:
        return df, cols_map

    df = df.copy()
    df[create_time_col] = pd.to_datetime(df[create_time_col], errors="coerce")
    df[lock_time_col] = pd.to_datetime(df[lock_time_col], errors="coerce")

    if main_code_col in df.columns:
        df = df.drop_duplicates(subset=[main_code_col], keep="first").copy()

    df = df.dropna(subset=[user_col, create_time_col]).copy()
    sort_cols = [user_col, create_time_col]
    if main_code_col in df.columns:
        sort_cols.append(main_code_col)
    df = df.sort_values(sort_cols, kind="mergesort")
    df["touch_index"] = df.groupby(user_col, dropna=False).cumcount() + 1
    delta = df[lock_time_col] - df[create_time_col]
    df["time_to_lock_days"] = (delta.dt.total_seconds() / 86400).astype("Float64")
    return df, cols_map


def _calc_order_lock_people_non_test_drive_for_range(
    order_table_path: Path,
    start: pd.Timestamp,
    end_exclusive: pd.Timestamp,
    series_group: str | None,
    series_group_logic: dict[str, str] | None,
) -> int | None:
    cols = ["lock_time", "owner_cell_phone", "order_type"]
    if series_group is not None:
        cols.append("product_name")
    try:
        df = pd.read_parquet(order_table_path, columns=cols)
    except Exception:
        return None

    if "lock_time" not in df.columns or "owner_cell_phone" not in df.columns:
        return None
    if series_group is not None and "product_name" not in df.columns:
        raise ValueError("订单表缺少 product_name，无法按车系分组计算数据完整度分母")

    df = df.copy()
    df["lock_time"] = pd.to_datetime(df["lock_time"], errors="coerce")
    lock_mask = df["lock_time"].notna() & (df["lock_time"] >= start) & (df["lock_time"] < end_exclusive)
    if "order_type" in df.columns:
        lock_mask = lock_mask & df["owner_cell_phone"].notna() & (df["order_type"] != "试驾车")
    else:
        lock_mask = lock_mask & df["owner_cell_phone"].notna()

    df_lock = df.loc[lock_mask].copy()
    if series_group is not None:
        logic = series_group_logic or {}
        if not logic:
            raise ValueError("business_definition.json 中缺少 series_group_logic")
        if series_group not in logic:
            valid = sorted(list(logic.keys()))
            raise ValueError(f"未知车系分组: {series_group}，可选: {valid}")
        expr = str(logic.get(series_group) or "")
        pn = df_lock["product_name"].astype("string").fillna("")
        if expr.strip().upper() == "ELSE":
            union = pd.Series(False, index=df_lock.index)
            for _, v in logic.items():
                if str(v).strip().upper() == "ELSE":
                    continue
                tokens = _tokenize_logic(str(v))
                ast = _parse_logic_expr(tokens)
                union = union | _eval_logic_ast(ast, pn)
            series_mask = ~union
        else:
            tokens = _tokenize_logic(expr)
            ast = _parse_logic_expr(tokens)
            series_mask = _eval_logic_ast(ast, pn)
        df_lock = df_lock.loc[series_mask].copy()

    return int(df_lock["owner_cell_phone"].nunique())


def _calc_main_code_whitelist_from_order_analysis_for_range(
    order_analysis_path: Path,
    start: pd.Timestamp,
    end_exclusive: pd.Timestamp,
    series_group: str,
    series_group_logic: dict[str, str],
) -> set[str]:
    cols = ["lock_time", "order_type", "product_name", "main_lead_id"]
    try:
        df = pd.read_parquet(order_analysis_path, columns=cols)
    except Exception:
        df = pd.read_parquet(order_analysis_path)

    missing: list[str] = []
    for c in ["lock_time", "product_name", "main_lead_id"]:
        if c not in df.columns:
            missing.append(c)
    if missing:
        raise ValueError(f"订单分析表缺少字段 {missing}，无法按车系分组过滤")

    df = df.copy()
    df["lock_time"] = pd.to_datetime(df["lock_time"], errors="coerce")
    lock_mask = df["lock_time"].notna() & (df["lock_time"] >= start) & (df["lock_time"] < end_exclusive)
    if "order_type" in df.columns:
        lock_mask = lock_mask & (df["order_type"] != "试驾车")

    df_lock = df.loc[lock_mask, ["product_name", "main_lead_id"]].copy()
    df_lock = df_lock[df_lock["main_lead_id"].notna()].copy()

    logic = series_group_logic
    if series_group not in logic:
        valid = sorted(list(logic.keys()))
        raise ValueError(f"未知车系分组: {series_group}，可选: {valid}")
    expr = str(logic.get(series_group) or "")
    pn = df_lock["product_name"].astype("string").fillna("")
    if expr.strip().upper() == "ELSE":
        union = pd.Series(False, index=df_lock.index)
        for _, v in logic.items():
            if str(v).strip().upper() == "ELSE":
                continue
            tokens = _tokenize_logic(str(v))
            ast = _parse_logic_expr(tokens)
            union = union | _eval_logic_ast(ast, pn)
        series_mask = ~union
    else:
        tokens = _tokenize_logic(expr)
        ast = _parse_logic_expr(tokens)
        series_mask = _eval_logic_ast(ast, pn)
    df_lock = df_lock.loc[series_mask].copy()

    return set(df_lock["main_lead_id"].astype("string").dropna().unique().tolist())


def _calc_attribution_metrics_for_range(
    df: pd.DataFrame,
    cols_map: dict[str, str | None],
    start: pd.Timestamp,
    end_exclusive: pd.Timestamp,
    order_lock_people_non_test_drive: int | None,
    lock_channel_filter: str | None,
    lock_main_code_whitelist: set[str] | None,
) -> dict[str, object]:
    main_code_col = cols_map["main_code"] or "lc_main_code"
    user_col = cols_map["user_phone_md5"] or "lc_user_phone_md5"
    create_time_col = cols_map["create_time"] or "lc_create_time"
    lock_time_col = cols_map["lock_time"] or "lc_order_lock_time_min"
    channel_col = cols_map["channel"]

    if user_col not in df.columns or create_time_col not in df.columns or lock_time_col not in df.columns:
        return {
            "锁单用户数": 0,
            "数据完整度": None,
            "平均触达次数": None,
            "平均转化时长(天)": None,
            "锁单用户主要渠道Top5": [],
            "锁单用户分类占比（观察口径）": [],
            "跨渠道锁单用户主要助攻渠道Top5": [],
        }

    locked = df[df[lock_time_col].notna() & (df[lock_time_col] >= start) & (df[lock_time_col] < end_exclusive)].copy()
    if locked.empty:
        return {
            "锁单用户数": 0,
            "数据完整度": (None if order_lock_people_non_test_drive is None else _to_percent_1dp(_safe_ratio(0, order_lock_people_non_test_drive))),
            "平均触达次数": None,
            "平均转化时长(天)": None,
            "锁单用户主要渠道Top5": [],
            "锁单用户分类占比（观察口径）": [],
            "跨渠道锁单用户主要助攻渠道Top5": [],
        }

    sort_cols = [user_col, lock_time_col]
    if main_code_col in locked.columns:
        sort_cols.append(main_code_col)
    locked = locked.sort_values(sort_cols, kind="mergesort")
    per_user = locked.groupby(user_col, dropna=False, as_index=False).first()

    if lock_channel_filter is not None and channel_col is not None and channel_col in per_user.columns:
        target = str(lock_channel_filter).strip()
        chan = per_user[channel_col].astype("string").fillna("").str.strip()
        per_user = per_user[chan == target].copy()
        if per_user.empty:
            return {
                "锁单用户数": 0,
                "数据完整度": (
                    None
                    if order_lock_people_non_test_drive is None
                    else _to_percent_1dp(_safe_ratio(0, order_lock_people_non_test_drive))
                ),
                "平均触达次数": None,
                "平均转化时长(天)": None,
                "锁单用户主要渠道Top5": [],
                "锁单用户分类占比（观察口径）": [],
                "跨渠道锁单用户主要助攻渠道Top5": [],
            }

    if lock_main_code_whitelist is not None:
        if main_code_col not in per_user.columns:
            raise ValueError("锁单归因数据缺少 main_code 字段，无法按车系分组过滤")
        whitelist = {str(x) for x in lock_main_code_whitelist if x is not None}
        c = per_user[main_code_col].astype("string")
        per_user = per_user[c.isin(whitelist)].copy()
        if per_user.empty:
            return {
                "锁单用户数": 0,
                "数据完整度": (
                    None
                    if order_lock_people_non_test_drive is None
                    else _to_percent_1dp(_safe_ratio(0, order_lock_people_non_test_drive))
                ),
                "平均触达次数": None,
                "平均转化时长(天)": None,
                "锁单用户主要渠道Top5": [],
                "锁单用户分类占比（观察口径）": [],
                "跨渠道锁单用户主要助攻渠道Top5": [],
            }

    locked_users = int(per_user[user_col].nunique(dropna=True))
    touch_mean = float(per_user["touch_index"].mean()) if not per_user.empty else None
    ttl = per_user["time_to_lock_days"].dropna()
    ttl_mean = (float(ttl.mean()) if not ttl.empty else None)

    channel_top_out: list[dict[str, object]] = []
    lens_out_records: list[dict[str, object]] = []
    assist_out_records: list[dict[str, object]] = []

    if channel_col is not None and channel_col in df.columns and main_code_col in df.columns:
        lock_time_by_user = per_user[[user_col, lock_time_col, channel_col, "time_to_lock_days", main_code_col]].copy()
        lock_time_by_user = lock_time_by_user.rename(
            columns={
                user_col: "user",
                lock_time_col: "first_lock_time",
                channel_col: "lock_channel",
                "time_to_lock_days": "ttl_days",
                main_code_col: "lock_main_code",
            }
        )
        lock_time_by_user["user"] = lock_time_by_user["user"].astype("string")
        lock_time_by_user["lock_channel"] = lock_time_by_user["lock_channel"].astype("string")

        touches = df[[user_col, main_code_col, channel_col, create_time_col]].copy()
        touches = touches[touches[create_time_col].notna()].copy()
        touches["user"] = touches[user_col].astype("string")
        touches = touches.merge(lock_time_by_user[["user", "first_lock_time"]], on="user", how="inner")
        touches = touches[touches[create_time_col].le(touches["first_lock_time"])].copy()

        touch_agg = touches.groupby("user", dropna=False).agg(
            touches_to_lock=(create_time_col, "size"),
            distinct_channels_to_lock=(channel_col, lambda s: int(pd.Series(s.dropna()).nunique())),
        )
        touch_agg = touch_agg.reset_index()

        user_summary = lock_time_by_user.merge(touch_agg, on="user", how="left")
        user_summary["touches_to_lock"] = user_summary["touches_to_lock"].fillna(0).astype(int)
        user_summary["distinct_channels_to_lock"] = user_summary["distinct_channels_to_lock"].fillna(0).astype(int)

        channel_series = user_summary["lock_channel"].astype("string").fillna("(missing)")
        vc = channel_series.value_counts(dropna=False)
        total_users = int(vc.sum())
        top_n = 5
        top = vc.head(max(int(top_n), 1))
        channel_top = pd.DataFrame({"channel": top.index.astype("string"), "locked_users": top.values})
        other_cnt = int(vc.iloc[len(top) :].sum())
        if other_cnt:
            channel_top = pd.concat(
                [channel_top, pd.DataFrame([{"channel": "其他", "locked_users": other_cnt}])],
                ignore_index=True,
            )
        channel_top["pct"] = channel_top["locked_users"] / max(total_users, 1)
        channel_top["pct"] = channel_top["pct"].map(lambda x: _to_percent_1dp(float(x)) if pd.notna(x) else None)
        channel_top_out = channel_top.to_dict(orient="records")

        one_touch_users = int(user_summary["touches_to_lock"].astype(int).eq(1).sum())
        same_channel_multi_users = int(
            (user_summary["touches_to_lock"].astype(int).gt(1) & user_summary["distinct_channels_to_lock"].astype(int).eq(1)).sum()
        )
        cross_channel_users = int(user_summary["distinct_channels_to_lock"].astype(int).gt(1).sum())
        long_users = int(user_summary["ttl_days"].astype("Float64").gt(14).fillna(False).sum())
        long_14_60_users = int(
            (user_summary["ttl_days"].astype("Float64").gt(14) & user_summary["ttl_days"].astype("Float64").lt(60)).fillna(False).sum()
        )
        prior_lock_users = df.loc[df[lock_time_col].notna() & df[lock_time_col].lt(start), user_col].astype("string")
        prior_lock_users = set(prior_lock_users.dropna().tolist())
        repeat_lock_users = int(user_summary["user"].astype("string").isin(prior_lock_users).sum())

        lens_out = pd.DataFrame(
            [
                {"category": "One-Touch (Decisive)", "users": one_touch_users},
                {"category": "Hesitant (Same Channel, Multiple Touches)", "users": same_channel_multi_users},
                {"category": "Cross-Channel (Comparison Shopper)", "users": cross_channel_users},
                {"category": "Long Consideration (>14 Days)", "users": long_users},
                {"category": "Long Consideration (>14 Days & <60 Days)", "users": long_14_60_users},
                {"category": "Repeat Lockers (Had Prior Locks)", "users": repeat_lock_users},
            ]
        )
        lens_out["pct"] = lens_out["users"] / max(int(user_summary.shape[0]), 1)
        lens_out["pct"] = lens_out["pct"].map(lambda x: _to_percent_1dp(float(x)) if pd.notna(x) else None)
        lens_out_records = lens_out.to_dict(orient="records")

        if cross_channel_users <= 0:
            assist_out = pd.DataFrame(columns=["assist_channel", "assist_touches", "pct"])
        else:
            cross_users = user_summary.loc[user_summary["distinct_channels_to_lock"].gt(1), "user"].copy()
            assist = touches[touches["user"].astype("string").isin(cross_users.astype("string"))].copy()
            assist = assist.merge(lock_time_by_user[["user", "lock_channel", "lock_main_code"]], on="user", how="left")
            assist["assist_channel"] = assist[channel_col].astype("string")
            assist = assist[
                assist["assist_channel"].notna() & assist["lock_channel"].notna() & assist["lock_main_code"].notna()
            ].copy()
            assist = assist[assist["assist_channel"] != assist["lock_channel"]].copy()
            assist = assist[assist[main_code_col] != assist["lock_main_code"]].copy()

            vc_assist = assist["assist_channel"].value_counts(dropna=False)
            assist_total = int(len(assist))
            top_assist = vc_assist.head(max(int(top_n), 1))
            assist_out = pd.DataFrame({"assist_channel": top_assist.index.astype("string"), "assist_touches": top_assist.values})
            assist_out["pct"] = assist_out["assist_touches"] / max(assist_total, 1)
            assist_out["pct"] = assist_out["pct"].map(lambda x: _to_percent_1dp(float(x)) if pd.notna(x) else None)

        assist_out_records = assist_out.to_dict(orient="records")

    return {
        "锁单用户数": locked_users,
        "数据完整度": (
            None
            if order_lock_people_non_test_drive is None
            else _to_percent_1dp(_safe_ratio(locked_users, order_lock_people_non_test_drive))
        ),
        "平均触达次数": (None if touch_mean is None else round(touch_mean, 2)),
        "平均转化时长(天)": (None if ttl_mean is None else round(ttl_mean, 2)),
        "锁单用户主要渠道Top5": channel_top_out,
        "锁单用户分类占比（观察口径）": lens_out_records,
        "跨渠道锁单用户主要助攻渠道Top5": assist_out_records,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="起始日期，YYYY-MM-DD（含）")
    parser.add_argument("--end", required=True, help="结束日期，YYYY-MM-DD（含）")
    parser.add_argument("--channel", default=None, help="按锁单渠道过滤（Ic_small_channel_name/lc_small_channel_name）")
    parser.add_argument("--series-group", default=None, help="按锁单车系分组过滤（business_definition.json: series_group_logic）")
    parser.add_argument("--business-definition-json", default="schema/business_definition.json", help="业务定义文件")
    parser.add_argument("--data-path-md", default="schema/data_path.md", help="数据路径配置文件")
    parser.add_argument("--json-out", default=None, help="输出 JSON 文件路径；不传则打印到 stdout")
    args = parser.parse_args()

    start_date = _parse_target_date(str(args.start))
    end_date = _parse_target_date(str(args.end))
    if end_date < start_date:
        raise ValueError("--end 不能早于 --start")

    start = start_date.normalize()
    end_exclusive = end_date.normalize() + pd.Timedelta(days=1)

    data_paths = _read_data_paths(Path(args.data_path_md))
    attribution_path = data_paths.get("锁单归因")
    if attribution_path is None:
        raise ValueError(f"在 {args.data_path_md} 中找不到 锁单归因 的数据路径")

    order_table_path = data_paths.get("订单表")
    order_analysis_path = data_paths.get("订单分析")
    main_code_whitelist = None
    series_group_logic = None
    if args.series_group is not None:
        if order_analysis_path is None:
            raise ValueError("指定 --series-group 时，data_path.md 中必须提供 订单分析 路径")
        if order_table_path is None:
            raise ValueError("指定 --series-group 时，data_path.md 中必须提供 订单表 路径（用于计算数据完整度分母）")
        series_group_logic = _load_series_group_logic(Path(args.business_definition_json))
        if not series_group_logic:
            raise ValueError("business_definition.json 中缺少 series_group_logic")
        main_code_whitelist = _calc_main_code_whitelist_from_order_analysis_for_range(
            order_analysis_path=order_analysis_path,
            start=start,
            end_exclusive=end_exclusive,
            series_group=str(args.series_group),
            series_group_logic=series_group_logic,
        )

    denom = None
    if order_table_path is not None:
        denom = _calc_order_lock_people_non_test_drive_for_range(
            order_table_path,
            start,
            end_exclusive,
            series_group=(None if args.series_group is None else str(args.series_group)),
            series_group_logic=series_group_logic,
        )

    attr_df, cols_map = _prepare_attribution_df(attribution_path)
    metrics = _calc_attribution_metrics_for_range(
        df=attr_df,
        cols_map=cols_map,
        start=start,
        end_exclusive=end_exclusive,
        order_lock_people_non_test_drive=denom,
        lock_channel_filter=(None if args.channel is None else str(args.channel)),
        lock_main_code_whitelist=main_code_whitelist,
    )

    result = {
        "start": str(start_date.date()),
        "end": str(end_date.date()),
        "channel": (None if args.channel is None else str(args.channel)),
        "series_group": (None if args.series_group is None else str(args.series_group)),
        "归因分析": metrics,
    }

    if args.json_out:
        out_path = Path(str(args.json_out)).expanduser().resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(str(out_path))
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

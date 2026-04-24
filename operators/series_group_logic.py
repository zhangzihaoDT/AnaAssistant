import pandas as pd


def _eval_series_group_logic_expr(product_name: pd.Series, expr: str) -> pd.Series:
    s = product_name.astype("string").fillna("")
    expr = str(expr or "").strip()
    if not expr or expr.upper() == "ELSE":
        return pd.Series([False] * len(s), index=s.index)

    expr = expr.replace("(", " ").replace(")", " ")
    or_terms = [t.strip() for t in expr.split(" OR ") if t.strip()]

    out = pd.Series([False] * len(s), index=s.index)
    for term in or_terms:
        and_terms = [t.strip() for t in term.split(" AND ") if t.strip()]
        m = pd.Series([True] * len(s), index=s.index)
        for cond in and_terms:
            cond = cond.strip()
            if " NOT LIKE " in cond:
                tok = cond.split(" NOT LIKE ", 1)[1].strip().strip("'").strip('"')
                tok = tok.strip("%")
                m = m & (~s.str.contains(tok, na=False, regex=False))
            elif " LIKE " in cond:
                tok = cond.split(" LIKE ", 1)[1].strip().strip("'").strip('"')
                tok = tok.strip("%")
                m = m & (s.str.contains(tok, na=False, regex=False))
        out = out | m
    return out


def apply_series_group_logic(df: pd.DataFrame, business_definition: dict) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "series_group_logic" in df.columns:
        return df
    if "product_name" not in df.columns:
        df["series_group_logic"] = pd.NA
        return df

    logic = (business_definition or {}).get("series_group_logic") or {}
    if not isinstance(logic, dict) or not logic:
        df["series_group_logic"] = pd.NA
        return df

    out = pd.Series(["其他"] * len(df), index=df.index, dtype="string")
    product_name = df["product_name"]
    for key, expr in logic.items():
        if str(key) == "其他":
            continue
        mask = _eval_series_group_logic_expr(product_name, str(expr))
        if mask.any():
            out = out.where(~mask, other=str(key))
    df["series_group_logic"] = out
    return df


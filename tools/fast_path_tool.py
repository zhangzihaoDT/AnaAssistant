import datetime


class FastPathTool:
    def run(self, config: dict, user_query: str) -> dict:
        kind = str((config or {}).get("type") or "")
        if kind == "current_iso_week":
            today = datetime.date.today()
            iso = today.isocalendar()
            return {
                "type": "fast_path",
                "kind": "current_iso_week",
                "date": today.isoformat(),
                "iso_year": int(iso.year),
                "iso_week": int(iso.week),
                "iso_weekday": int(iso.weekday),
                "answer": f"今天是 {today.isoformat()}，ISO 周数为 {int(iso.year)}-W{int(iso.week):02d}。",
                "question": str(user_query or ""),
            }
        if kind != "numeric_ratio":
            return {"type": "fast_path", "error": "unsupported_type", "message": f"不支持的 fast_path 类型: {kind}"}
        try:
            current = float(config.get("current"))
            base = float(config.get("base"))
        except Exception:
            return {"type": "fast_path", "error": "invalid_config", "message": "fast_path 参数无效"}

        delta = current - base
        ratio = None if base == 0 else (delta / base)
        direction = "提升" if delta >= 0 else "下降"
        ratio_pct = None if ratio is None else round(ratio * 100, 2)
        return {
            "type": "fast_path",
            "kind": "numeric_ratio",
            "current": current,
            "base": base,
            "delta": round(delta, 6),
            "direction": direction,
            "ratio": ratio,
            "ratio_pct": ratio_pct,
            "answer": (
                f"{current:g} 相比 {base:g}{direction}"
                + ("无法计算百分比（基数为0）。" if ratio_pct is None else f" {abs(ratio_pct):g}%")
            ),
            "question": str(user_query or ""),
        }


FAST_PATH_TOOL_SCHEMA = {
    "type": "function",
    "function": {
        "name": "run_fast_path",
        "description": "执行轻量 Fast Path 计算（如数字环比提升，或获取当前日期 ISO 周数）。",
        "parameters": {
            "type": "object",
            "properties": {
                "config": {
                    "type": "object",
                    "properties": {
                        "type": {"type": "string", "enum": ["numeric_ratio", "current_iso_week"]},
                        "current": {"type": "number"},
                        "base": {"type": "number"},
                    },
                    "required": ["type"],
                },
                "user_query": {"type": "string"},
            },
            "required": ["config"],
        },
    },
}

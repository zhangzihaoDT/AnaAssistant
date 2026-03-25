import datetime


class FastPathTool:
    def run(
        self,
        config: dict,
        user_query: str,
        memory_context: dict | None = None,
    ) -> dict:
        kind = str((config or {}).get("type") or "")
        if kind == "small_talk_contextual":
            ctx = memory_context if isinstance(memory_context, dict) else {}
            facts = ctx.get("facts") if isinstance(ctx.get("facts"), dict) else {}
            working = ctx.get("working_memory") if isinstance(ctx.get("working_memory"), dict) else {}
            logs = ctx.get("execution_log") if isinstance(ctx.get("execution_log"), list) else []
            recent_queries: list[str] = []
            for item in logs[-3:]:
                if not isinstance(item, dict):
                    continue
                q = str(item.get("query") or "").strip()
                if q:
                    recent_queries.append(q)
            memory_preview = "；".join(recent_queries) if recent_queries else ""
            fact_keys = [str(k) for k in list(facts.keys())[:3]]
            fact_preview = "、".join(fact_keys)
            focus_dimension = str(working.get("focus_dimension") or "").strip()
            if memory_preview and fact_preview and focus_dimension:
                answer = (
                    f"收到，干得漂亮！你最近在关注：{memory_preview}。"
                    f"我们当前焦点是 {focus_dimension}，已沉淀结论包括：{fact_preview}。要继续深入吗？"
                )
            elif memory_preview and fact_preview:
                answer = f"收到，干得漂亮！你最近在关注：{memory_preview}。已沉淀结论包括：{fact_preview}。要继续深入吗？"
            elif memory_preview:
                answer = f"收到，干得漂亮！我记得你最近在关注：{memory_preview}。需要我继续沿这个方向分析吗？"
            elif fact_preview:
                answer = f"收到，干得漂亮！当前已沉淀结论包括：{fact_preview}。要不要继续深入下一层？"
            else:
                answer = "收到，干得漂亮！如果你愿意，我可以继续帮你做下一步分析。"
            return {
                "type": "fast_path",
                "kind": "small_talk_contextual",
                "recent_queries": recent_queries,
                "facts_snapshot": facts,
                "working_memory_snapshot": working,
                "answer": answer,
                "question": str(user_query or ""),
            }
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
                        "type": {"type": "string", "enum": ["numeric_ratio", "current_iso_week", "small_talk_contextual"]},
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

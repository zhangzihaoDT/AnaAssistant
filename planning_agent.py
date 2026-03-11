import json
import datetime
import re

from openai import OpenAI


PLANNING_TOOL_SCHEMA = {
    "type": "function",
    "function": {
        "name": "create_planning_dsl",
        "description": "将自然语言问题转为规划 DSL（可拆解为多个子问题对应多个 plan）。",
        "parameters": {
            "type": "object",
            "properties": {
                "plans": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "question": {"type": "string"},
                            "dataset": {"type": "string"},
                            "metric": {
                                "type": "object",
                                "properties": {
                                    "field": {"type": "string"},
                                    "agg": {"type": "string", "enum": ["sum", "mean", "count", "min", "max"]},
                                    "alias": {"type": "string"},
                                    "business_name": {"type": "string"},
                                },
                                "required": ["field", "agg"],
                            },
                            "time": {
                                "type": "object",
                                "properties": {
                                    "field": {"type": "string"},
                                    "start": {"type": "string"},
                                    "end": {"type": "string"},
                                },
                                "required": ["field", "start", "end"],
                            },
                            "dimensions": {"type": "array", "items": {"type": "string"}},
                            "filters": {"type": "array"},
                            "comparison": {
                                "type": "object",
                                "properties": {
                                    "type": {"type": "string", "enum": ["none", "yoy", "wow"]},
                                },
                                "required": ["type"],
                            },
                            "ranking": {
                                "type": "object",
                                "properties": {
                                    "order": {"type": "string", "enum": ["asc", "desc"]},
                                    "top_k": {"type": "integer"},
                                },
                            },
                        },
                        "required": ["dataset", "metric", "time", "comparison"],
                    },
                },
            },
            "required": ["plans"],
        },
    },
}


class PlanningAgent:
    def __init__(self, client: OpenAI, schema_md: str, business_definition: str):
        self.client = client
        self.schema_md = schema_md or ""
        self.business_definition = business_definition or ""

    @staticmethod
    def _parse_comparison_type(user_query: str) -> str:
        q = user_query or ""
        if "同比" in q or "年同比" in q:
            return "yoy"
        if "环比" in q or "周环比" in q:
            return "wow"
        return "none"

    @staticmethod
    def _parse_time_window(user_query: str, today: datetime.date) -> tuple[str, str] | None:
        q = user_query or ""
        if "昨天" in q or "昨日" in q:
            start = today - datetime.timedelta(days=1)
            end = today
            return (start.isoformat(), end.isoformat())

        def _normalize_year(y: str) -> int | None:
            if not y:
                return None
            y = str(y).strip()
            if not y.isdigit():
                return None
            if len(y) == 2:
                return 2000 + int(y)
            if len(y) == 4:
                return int(y)
            return None

        def _safe_date(y: int, m: int, d: int) -> datetime.date | None:
            try:
                return datetime.date(int(y), int(m), int(d))
            except Exception:
                return None

        def _month_window(year: int, month: int) -> tuple[str, str] | None:
            if month < 1 or month > 12:
                return None
            start = _safe_date(year, month, 1)
            if not start:
                return None
            if month == 12:
                end = _safe_date(year + 1, 1, 1)
            else:
                end = _safe_date(year, month + 1, 1)
            if not end:
                return None
            return (start.isoformat(), end.isoformat())

        m = re.search(
            r"(?P<y>\d{2,4})年\s*(?P<m>\d{1,2})月\s*(?P<d>\d{1,2})[日号]?\s*(?:到|至|[-~—–－])\s*"
            r"(?:(?P<y2>\d{2,4})年\s*)?(?:(?P<m2>\d{1,2})月\s*)?(?P<d2>\d{1,2})[日号]?",
            q,
        )
        if m:
            y1 = _normalize_year(m.group("y")) or today.year
            m1 = int(m.group("m"))
            d1 = int(m.group("d"))
            y2 = _normalize_year(m.group("y2")) or y1
            m2 = int(m.group("m2") or m1)
            d2 = int(m.group("d2"))
            start_date = _safe_date(y1, m1, d1)
            end_date = _safe_date(y2, m2, d2)
            if start_date and end_date:
                end_open = end_date + datetime.timedelta(days=1)
                return (start_date.isoformat(), end_open.isoformat())

        m = re.search(
            r"(?P<y>\d{2,4})年\s*(?P<m>\d{1,2})月\s*(?:整月|全月|整个月)",
            q,
        )
        if m:
            year = _normalize_year(m.group("y")) or today.year
            month = int(m.group("m"))
            window = _month_window(year, month)
            if window:
                return window

        m = re.search(r"(?P<y>\d{2,4})年\s*(?P<m>\d{1,2})月(?!\d)", q)
        if m and ("整月" in q or "全月" in q or "整个月" in q):
            year = _normalize_year(m.group("y")) or today.year
            month = int(m.group("m"))
            window = _month_window(year, month)
            if window:
                return window

        m = re.search(r"(?P<y>\d{2,4})年\s*(?P<m>\d{1,2})月\s*(?P<d>\d{1,2})[日号]?", q)
        if m:
            year = _normalize_year(m.group("y")) or today.year
            month = int(m.group("m"))
            day = int(m.group("d"))
            start_date = _safe_date(year, month, day)
            if start_date:
                end_open = start_date + datetime.timedelta(days=1)
                return (start_date.isoformat(), end_open.isoformat())

        m = re.search(r"(\d{4}-\d{2}-\d{2})\s*(?:到|至|[-~—–－])\s*(\d{4}-\d{2}-\d{2})", q)
        if m:
            start_date = datetime.date.fromisoformat(m.group(1))
            end_date = datetime.date.fromisoformat(m.group(2))
            end_open = end_date + datetime.timedelta(days=1)
            return (start_date.isoformat(), end_open.isoformat())

        m = re.search(r"(\d{4}-\d{2}-\d{2})", q)
        if m:
            start = datetime.date.fromisoformat(m.group(1))
            end = start + datetime.timedelta(days=1)
            return (start.isoformat(), end.isoformat())

        return None

    @staticmethod
    def _metric_defaults(user_query: str) -> dict | None:
        q = user_query or ""
        if "锁单" in q:
            return {
                "dataset": "order_full_data",
                "metric": {"field": "order_number", "agg": "count", "alias": "锁单数", "business_name": "锁单量"},
                "time_field": "lock_time",
                "non_null_field": "lock_time",
            }
        if "交付" in q:
            return {
                "dataset": "order_full_data",
                "metric": {"field": "order_number", "agg": "count", "alias": "交付数", "business_name": "交付数"},
                "time_field": "delivery_date",
                "non_null_field": "delivery_date",
            }
        if "开票金额" in q or ("开票" in q and "金额" in q):
            return {
                "dataset": "order_full_data",
                "metric": {"field": "invoice_amount", "agg": "sum", "alias": "开票金额", "business_name": "开票金额"},
                "time_field": "invoice_upload_time",
                "non_null_field": "invoice_upload_time",
            }
        if "开票" in q:
            return {
                "dataset": "order_full_data",
                "metric": {"field": "order_number", "agg": "count", "alias": "开票数", "business_name": "开票数"},
                "time_field": "invoice_upload_time",
                "non_null_field": "invoice_upload_time",
            }
        if "小订" in q or "意向金" in q:
            return {
                "dataset": "order_full_data",
                "metric": {"field": "order_number", "agg": "count", "alias": "小订数", "business_name": "小订数"},
                "time_field": "intention_payment_time",
                "non_null_field": "intention_payment_time",
            }
        return None

    @staticmethod
    def _parse_series(user_query: str) -> str | None:
        q = (user_query or "").upper()
        for candidate in ["LS6", "L6", "LS9", "LS7", "L7"]:
            if candidate in q:
                return candidate
        return None

    @staticmethod
    def _parse_product_type(user_query: str) -> str | None:
        q = user_query or ""
        if "增程" in q:
            return "增程"
        if "纯电" in q:
            return "纯电"
        return None

    @staticmethod
    def _append_filter(filters: list[dict], new_filter: dict) -> list[dict]:
        if not isinstance(new_filter, dict):
            return filters
        field = new_filter.get("field")
        op = new_filter.get("op")
        value = new_filter.get("value", None)
        for f in filters:
            if not isinstance(f, dict):
                continue
            if f.get("field") == field and f.get("op") == op and f.get("value", None) == value:
                return filters
        return [*filters, new_filter]

    @staticmethod
    def _normalize_plan(plan: dict) -> dict:
        dataset = plan.get("dataset")
        if isinstance(dataset, str):
            lowered = dataset.lower()
            if lowered.endswith(".parquet") or lowered.endswith(".csv"):
                plan["dataset"] = dataset.rsplit(".", 1)[0]

        filters = plan.get("filters")
        if not isinstance(filters, list):
            filters = []

        normalized_filters: list[dict] = []
        for f in filters:
            if not isinstance(f, dict):
                continue
            field = f.get("field")
            op = f.get("op")
            value = f.get("value", None)

            if op in {"not null", "not_null", "is not null", "is_not_null"}:
                normalized_filters.append({"field": field, "op": "!=", "value": None})
                continue
            if op in {"null", "is null", "is_null"}:
                normalized_filters.append({"field": field, "op": "==", "value": None})
                continue

            if op == "!=" and "value" not in f:
                normalized_filters.append({"field": field, "op": "!=", "value": None})
                continue

            normalized_filters.append({"field": field, "op": op, "value": value})

        plan["filters"] = normalized_filters

        dims = plan.get("dimensions")
        if dims is None:
            plan["dimensions"] = []
        elif not isinstance(dims, list):
            plan["dimensions"] = [str(dims)]

        comparison = plan.get("comparison")
        if not isinstance(comparison, dict):
            plan["comparison"] = {"type": "none"}
        else:
            ctype = comparison.get("type")
            if ctype not in {"none", "yoy", "wow"}:
                plan["comparison"] = {"type": "none"}

        time = plan.get("time")
        if not isinstance(time, dict):
            plan["time"] = {}

        metric = plan.get("metric")
        if not isinstance(metric, dict):
            plan["metric"] = {}

        return plan

    def _rule_based_plan(self, user_query: str) -> dict | None:
        today = datetime.date.today()
        comparison_type = self._parse_comparison_type(user_query)
        metric_defaults = self._metric_defaults(user_query)
        if not metric_defaults:
            return None

        time_window = self._parse_time_window(user_query, today) or (
            (today - datetime.timedelta(days=1)).isoformat(),
            today.isoformat(),
        )

        start, end = time_window
        time_field = metric_defaults["time_field"]
        non_null_field = metric_defaults.get("non_null_field")

        plan = {
            "dataset": metric_defaults["dataset"],
            "metric": metric_defaults["metric"],
            "time": {"field": time_field, "start": start, "end": end},
            "dimensions": [],
            "filters": [],
            "comparison": {"type": comparison_type},
        }

        if non_null_field:
            plan["filters"].append({"field": non_null_field, "op": "!=", "value": None})

        series = self._parse_series(user_query)
        if series:
            plan["filters"] = self._append_filter(plan["filters"], {"field": "series", "op": "==", "value": series})

        product_type = self._parse_product_type(user_query)
        if product_type == "增程":
            plan["filters"] = self._append_filter(
                plan["filters"], {"field": "product_name", "op": "matches", "value": "52|66"}
            )
        elif product_type == "纯电":
            plan["filters"] = self._append_filter(
                plan["filters"], {"field": "product_name", "op": "not matches", "value": "52|66"}
            )

        return self._normalize_plan(plan)

    @staticmethod
    def _split_user_query(user_query: str) -> list[str]:
        q = (user_query or "").strip()
        if not q:
            return []
        parts = re.split(r"[？?\n；;]+", q)
        return [p.strip() for p in parts if p and p.strip()]

    def create_plans(self, user_query: str) -> list[dict]:
        parts = self._split_user_query(user_query) or [user_query]
        base_defaults = self._metric_defaults(user_query)

        rule_plans: list[dict] = []
        for part in parts:
            effective_part = part
            if base_defaults and not self._metric_defaults(part):
                metric_hint = base_defaults.get("metric", {}).get("business_name") or base_defaults.get("metric", {}).get("alias")
                if metric_hint:
                    effective_part = f"{metric_hint} {part}"
            plan = self._rule_based_plan(effective_part)
            if isinstance(plan, dict) and plan:
                plan["question"] = part
                rule_plans.append(plan)
        if rule_plans:
            return rule_plans

        current_date = datetime.date.today().isoformat()
        messages = [
            {
                "role": "system",
                "content": (
                    "你是一个智能数据分析助手 (Planning Agent)。"
                    "你的任务是把用户问题转成可执行前的规划 DSL（包含时间范围、对比类型、拆解维度、过滤口径）。"
                    "不要直接回答结论，必须调用 create_planning_dsl 工具返回 plans。\n\n"
                    f"今天是: {current_date}\n\n"
                    "数据集与 Schema:\n"
                    f"{self.schema_md}\n\n"
                    "业务定义:\n"
                    f"{self.business_definition}\n\n"
                    "约束:\n"
                    "- 规划 DSL 中 time.start/time.end 必须是 YYYY-MM-DD，且 end 为开区间。\n"
                    "- 如果问题涉及同比/年同比，comparison.type = yoy；涉及环比/周环比，comparison.type = wow。\n"
                    "- 如果用户一句话包含多个子问题，请拆成多个 plan，按出现顺序返回。\n"
                    "- 如果你拆了子问题，请为每个 plan 填写 question 字段用于回显。\n"
                    "- 锁单量的统计口径：order_number count 且 lock_time 非空，时间筛选基于 lock_time。\n"
                ),
            },
            {"role": "user", "content": user_query},
        ]

        response = self.client.chat.completions.create(
            model="deepseek-chat",
            messages=messages,
            tools=[PLANNING_TOOL_SCHEMA],
            tool_choice={"type": "function", "function": {"name": "create_planning_dsl"}},
        )

        message = response.choices[0].message
        tool_calls = message.tool_calls or []
        for tool_call in tool_calls:
            if tool_call.function.name != "create_planning_dsl":
                continue
            args = json.loads(tool_call.function.arguments or "{}")
            raw_plans = args.get("plans")
            if isinstance(raw_plans, list):
                plans: list[dict] = []
                for p in raw_plans:
                    if not isinstance(p, dict):
                        continue
                    q = p.get("question") or user_query
                    plans.append(self._fill_defaults(self._normalize_plan(p), q))
                return [p for p in plans if isinstance(p, dict) and p]
            raw_plan = args.get("plan")
            if isinstance(raw_plan, dict):
                q = raw_plan.get("question") or user_query
                return [self._fill_defaults(self._normalize_plan(raw_plan), q)]

        content = message.content or ""
        try:
            obj = json.loads(content)
            if isinstance(obj, dict) and isinstance(obj.get("plans"), list):
                plans: list[dict] = []
                for p in obj["plans"]:
                    if not isinstance(p, dict):
                        continue
                    q = p.get("question") or user_query
                    plans.append(self._fill_defaults(self._normalize_plan(p), q))
                return [p for p in plans if isinstance(p, dict) and p]
            if isinstance(obj, dict) and isinstance(obj.get("plan"), dict):
                p = obj["plan"]
                q = p.get("question") or user_query
                return [self._fill_defaults(self._normalize_plan(p), q)]
        except Exception:
            pass

        return []

    def _fill_defaults(self, plan: dict, user_query: str) -> dict:
        metric_defaults = self._metric_defaults(user_query)
        today = datetime.date.today()
        default_start = (today - datetime.timedelta(days=1)).isoformat()
        default_end = today.isoformat()

        if metric_defaults:
            if not plan.get("dataset"):
                plan["dataset"] = metric_defaults["dataset"]

            metric = plan.get("metric")
            if not isinstance(metric, dict):
                metric = {}
            if not metric.get("field") or not metric.get("agg"):
                plan["metric"] = metric_defaults["metric"]

            time = plan.get("time")
            if not isinstance(time, dict):
                time = {}
            if not time.get("field"):
                time["field"] = metric_defaults["time_field"]
            if not time.get("start") or not time.get("end"):
                time["start"] = time.get("start") or default_start
                time["end"] = time.get("end") or default_end
            plan["time"] = time

            non_null_field = metric_defaults.get("non_null_field")
            if non_null_field:
                filters = plan.get("filters")
                if not isinstance(filters, list):
                    filters = []
                has_non_null = any(
                    isinstance(f, dict) and f.get("field") == non_null_field and f.get("op") == "!=" and f.get("value") is None
                    for f in filters
                )
                if not has_non_null:
                    filters.append({"field": non_null_field, "op": "!=", "value": None})
                plan["filters"] = filters

        filters = plan.get("filters")
        if not isinstance(filters, list):
            filters = []

        series = self._parse_series(user_query)
        if series:
            filters = self._append_filter(filters, {"field": "series", "op": "==", "value": series})

        product_type = self._parse_product_type(user_query)
        if product_type == "增程":
            filters = self._append_filter(filters, {"field": "product_name", "op": "matches", "value": "52|66"})
        elif product_type == "纯电":
            filters = self._append_filter(filters, {"field": "product_name", "op": "not matches", "value": "52|66"})

        plan["filters"] = filters

        comparison = plan.get("comparison")
        if not isinstance(comparison, dict) or comparison.get("type") not in {"none", "yoy", "wow"}:
            plan["comparison"] = {"type": self._parse_comparison_type(user_query)}

        time = plan.get("time")
        if isinstance(time, dict):
            if not time.get("start") or not time.get("end"):
                time["start"] = time.get("start") or default_start
                time["end"] = time.get("end") or default_end

        return self._normalize_plan(plan)

    def create_plan(self, user_query: str) -> dict:
        plans = self.create_plans(user_query)
        if plans:
            return plans[0]
        return {}

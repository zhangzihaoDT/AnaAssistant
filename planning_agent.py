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
                            "statistics": {
                                "type": "object",
                                "properties": {
                                    "type": {"type": "string", "enum": ["weekly_decline_ratio", "daily_threshold_count"]},
                                    "time_field": {"type": "string"},
                                    "window_weeks": {"type": "integer"},
                                    "window_days": {"type": "integer"},
                                    "weekdays": {"type": "array", "items": {"type": "integer"}},
                                    "op": {"type": "string", "enum": [">", ">=", "<", "<=", "==", "!="]},
                                    "threshold": {"type": "number"},
                                    "numerator_metric": {
                                        "type": "object",
                                        "properties": {
                                            "field": {"type": "string"},
                                            "agg": {"type": "string", "enum": ["sum", "mean", "count", "min", "max"]},
                                            "alias": {"type": "string"},
                                        },
                                    },
                                    "denominator_metric": {
                                        "type": "object",
                                        "properties": {
                                            "field": {"type": "string"},
                                            "agg": {"type": "string", "enum": ["sum", "mean", "count", "min", "max"]},
                                            "alias": {"type": "string"},
                                        },
                                    },
                                    "value_metric": {
                                        "type": "object",
                                        "properties": {
                                            "field": {"type": "string"},
                                            "agg": {"type": "string", "enum": ["sum", "mean", "count", "min", "max"]},
                                            "alias": {"type": "string"},
                                        },
                                    },
                                },
                            },
                        },
                        "required": ["dataset", "metric", "time", "comparison"],
                    },
                },
                "clarification": {
                    "type": "object",
                    "properties": {
                        "need": {"type": "boolean"},
                        "question": {"type": "string"},
                        "options": {"type": "array", "items": {"type": "string"}},
                        "context": {"type": "object"},
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
    def _classify_intent(user_query: str) -> str:
        q = (user_query or "").strip()
        if not q:
            return "query"

        stat_keywords = ["有多少天", "多少天", "几天", "多少周", "几周", "连续", "超过", "大于", "小于", "高于", "低于", "阈值", "下降"]
        has_stat_keyword = any(k in q for k in stat_keywords)
        has_time_window = bool(re.search(r"近\s*\d+\s*(日|天|周|月)", q)) or any(
            k in q for k in ["昨天", "昨日", "本周", "上周", "本月", "上月", "今年", "去年"]
        )
        if has_stat_keyword and has_time_window:
            return "statistics"
        if any(k in q for k in ["同比", "年同比", "环比", "周环比"]):
            return "comparison"
        return "query"

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

        def _year_window(year: int) -> tuple[str, str] | None:
            start = _safe_date(year, 1, 1)
            end = _safe_date(year + 1, 1, 1)
            if not start or not end:
                return None
            return (start.isoformat(), end.isoformat())

        if "前年" in q:
            window = _year_window(today.year - 2)
            if window:
                return window
        if "去年" in q:
            window = _year_window(today.year - 1)
            if window:
                return window
        if "今年" in q:
            window = _year_window(today.year)
            if window:
                return window

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

        m = re.search(r"(?P<y>\d{2,4})年(?!\s*\d|\s*月)", q)
        if m:
            year = _normalize_year(m.group("y")) or today.year
            window = _year_window(year)
            if window:
                return window

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
    def _should_sales_clarify(user_query: str) -> bool:
        q = user_query or ""
        if not q:
            return False
        if not any(k in q for k in ["销量", "卖了多少", "成交量"]):
            return False
        if any(k in q for k in ["锁单", "交付", "开票"]):
            return False
        return True

    @staticmethod
    def _sales_clarification(original_question: str) -> dict:
        return {
            "need": True,
            "question": "你提到的“销量”具体是指哪个业务口径？",
            "options": ["锁单量", "交付数", "开票数"],
            "context": {"original_question": original_question},
        }

    @staticmethod
    def _extract_city_token(user_query: str) -> str | None:
        q = (user_query or "").strip().lstrip(" \t\r\n\"'“”‘’")
        if not q:
            return None
        stop = {"查询", "统计", "汇总", "查看", "分析", "对比", "输出", "导出", "列出", "展示", "打印", "生成"}
        m = re.search(r"([\u4e00-\u9fff]{2,8})市", q)
        if m:
            city = (m.group(1) or "").strip()
            if not city or city in stop:
                return None
            if any(bad in city for bad in stop):
                return None
            return city
        m = re.search(r"^([\u4e00-\u9fff]{2,8})(?=(昨天|昨日|今日|今天|本周|上周|本月|上月|今年|去年|前年|\d{2,4}年))", q)
        if m:
            city = (m.group(1) or "").strip()
            if not city or city in stop:
                return None
            if any(bad in city for bad in stop):
                return None
            return city
        return None

    @staticmethod
    def _should_city_clarify(user_query: str) -> tuple[bool, str | None]:
        q = user_query or ""
        if not q:
            return (False, None)
        if any(k in q for k in ["门店城市", "store_city", "上牌城市", "license_city"]):
            return (False, None)
        if not any(k in q for k in ["锁单", "交付", "开票", "小订", "意向金", "订单"]):
            return (False, None)
        city = PlanningAgent._extract_city_token(q)
        if not city:
            return (False, None)
        return (True, city)

    @staticmethod
    def _city_clarification(city: str, original_question: str) -> dict:
        return {
            "need": True,
            "question": f"你问的“{city}”是指门店城市(store_city)，还是上牌城市(license_city)？",
            "options": ["门店城市", "上牌城市", "两者都要"],
            "context": {"city": city, "original_question": original_question},
        }

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
    def _parse_recent_weeks(user_query: str) -> int | None:
        q = user_query or ""
        m = re.search(r"近\s*(\d{1,3})\s*周", q)
        if not m:
            return None
        try:
            v = int(m.group(1))
            return v if v > 0 else None
        except Exception:
            return None

    @staticmethod
    def _parse_recent_days(user_query: str) -> int | None:
        q = user_query or ""
        m = re.search(r"近\s*(\d{1,3})\s*(?:日|天)", q)
        if not m:
            return None
        try:
            v = int(m.group(1))
            return v if v > 0 else None
        except Exception:
            return None

    @staticmethod
    def _parse_threshold_condition(user_query: str) -> tuple[str, float] | None:
        q = (user_query or "").replace(" ", "")
        pattern_map = [
            (r"(?:大于等于|不低于|不少于|>=)\s*(\d+(?:\.\d+)?)", ">="),
            (r"(?:小于等于|不高于|不大于|<=)\s*(\d+(?:\.\d+)?)", "<="),
            (r"(?:大于|高于|超过|>)\s*(\d+(?:\.\d+)?)", ">"),
            (r"(?:小于|低于|<)\s*(\d+(?:\.\d+)?)", "<"),
            (r"(?:等于|==)\s*(\d+(?:\.\d+)?)", "=="),
            (r"(?:不等于|!=)\s*(\d+(?:\.\d+)?)", "!="),
        ]
        for pattern, op in pattern_map:
            m = re.search(pattern, q)
            if not m:
                continue
            try:
                return (op, float(m.group(1)))
            except Exception:
                continue
        return None

    @staticmethod
    def _parse_weekdays(user_query: str) -> list[int]:
        q = user_query or ""
        mapping = {
            "周一": 1,
            "星期一": 1,
            "周二": 2,
            "星期二": 2,
            "周三": 3,
            "星期三": 3,
            "周四": 4,
            "星期四": 4,
            "周五": 5,
            "星期五": 5,
            "周六": 6,
            "星期六": 6,
            "周日": 7,
            "周天": 7,
            "星期日": 7,
            "星期天": 7,
        }
        out: list[int] = []
        for k, v in mapping.items():
            if k in q:
                out.append(v)
        return sorted(list(dict.fromkeys(out)))

    @staticmethod
    def _is_weekly_decline_ratio_query(user_query: str) -> bool:
        q = user_query or ""
        if not q:
            return False
        has_rate = "锁单率" in q
        has_decline = ("下降" in q and "多少" in q) or "下降周数" in q
        has_week_window = "近" in q and "周" in q
        has_source = "下发线索" in q and "门店" in q
        has_weekday = ("周四" in q or "星期四" in q or "周五" in q or "星期五" in q)
        return has_rate and has_decline and has_week_window and has_source and has_weekday

    @staticmethod
    def _is_daily_threshold_count_query(user_query: str) -> bool:
        q = user_query or ""
        if not q:
            return False
        has_day_count = ("多少天" in q) or ("几天" in q)
        has_recent_day = ("近" in q) and (("日" in q) or ("天" in q))
        has_threshold = PlanningAgent._parse_threshold_condition(q) is not None
        return has_day_count and has_recent_day and has_threshold

    def _build_weekly_decline_ratio_plan(self, user_query: str) -> dict:
        today = datetime.date.today()
        weeks = self._parse_recent_weeks(user_query) or 10
        start = today - datetime.timedelta(days=weeks * 7)
        end = today
        weekdays = self._parse_weekdays(user_query) or [4, 5]
        time_field = "Assign Time 年/月/日"
        numerator = {"field": "下发线索当日锁单数 (门店)", "agg": "sum", "alias": "门店当日锁单数"}
        denominator = {"field": "下发线索数 (门店)", "agg": "sum", "alias": "门店线索数"}
        plan = {
            "dataset": "assign_data",
            "metric": numerator,
            "time": {"field": time_field, "start": start.isoformat(), "end": end.isoformat()},
            "dimensions": [time_field],
            "filters": [],
            "comparison": {"type": "none"},
            "statistics": {
                "type": "weekly_decline_ratio",
                "time_field": time_field,
                "window_weeks": weeks,
                "weekdays": weekdays,
                "numerator_metric": numerator,
                "denominator_metric": denominator,
            },
        }
        return self._normalize_plan(plan)

    def _build_daily_threshold_count_plan(self, user_query: str) -> dict | None:
        metric_defaults = self._metric_defaults(user_query)
        threshold_cond = self._parse_threshold_condition(user_query)
        if not metric_defaults or not threshold_cond:
            return None
        op, threshold = threshold_cond
        today = datetime.date.today()
        days = self._parse_recent_days(user_query) or 30
        start = today - datetime.timedelta(days=days)
        end = today
        time_field = metric_defaults["time_field"]
        value_metric = metric_defaults["metric"]

        plan = {
            "dataset": metric_defaults["dataset"],
            "metric": value_metric,
            "time": {"field": time_field, "start": start.isoformat(), "end": end.isoformat()},
            "dimensions": [time_field],
            "filters": [{"field": time_field, "op": "!=", "value": None}],
            "comparison": {"type": "none"},
            "statistics": {
                "type": "daily_threshold_count",
                "time_field": time_field,
                "window_days": days,
                "op": op,
                "threshold": threshold,
                "value_metric": value_metric,
            },
        }
        return self._normalize_plan(plan)

    @staticmethod
    def _statistics_plan_valid(statistics: dict, plan: dict) -> bool:
        stype = statistics.get("type")
        if stype == "weekly_decline_ratio":
            time_field = statistics.get("time_field") or (plan.get("time", {}) or {}).get("field")
            weekdays = statistics.get("weekdays")
            numerator = statistics.get("numerator_metric")
            denominator = statistics.get("denominator_metric")
            if not isinstance(time_field, str) or not time_field:
                return False
            if not isinstance(weekdays, list) or not weekdays:
                return False
            if not isinstance(numerator, dict) or not isinstance(denominator, dict):
                return False
            if not numerator.get("field") or not denominator.get("field"):
                return False
            if not numerator.get("agg") or not denominator.get("agg"):
                return False
            if (
                numerator.get("field") == denominator.get("field")
                and numerator.get("agg") == denominator.get("agg")
                and (numerator.get("alias") or "") == (denominator.get("alias") or "")
            ):
                return False
            return True

        if stype == "daily_threshold_count":
            time_field = statistics.get("time_field") or (plan.get("time", {}) or {}).get("field")
            op = statistics.get("op")
            threshold = statistics.get("threshold")
            value_metric = statistics.get("value_metric")
            if not isinstance(time_field, str) or not time_field:
                return False
            if op not in {">", ">=", "<", "<=", "==", "!="}:
                return False
            if not isinstance(value_metric, dict):
                return False
            if not value_metric.get("field") or not value_metric.get("agg"):
                return False
            try:
                float(threshold)
            except Exception:
                return False
            return True

        return False

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
        seen_filters: set[tuple[str, str, str]] = set()
        for f in filters:
            if not isinstance(f, dict):
                continue
            field = f.get("field")
            op = f.get("op")
            value = f.get("value", None)

            if field in {"store_city", "license_city"} and isinstance(value, str):
                raw = value.strip()
                if "市" in raw:
                    raw = raw.split("市", 1)[0] + "市"
                else:
                    m_city = re.search(r"([\u4e00-\u9fff]{2,8})", raw)
                    if m_city:
                        raw = m_city.group(1)

                base = raw[:-1] if raw.endswith("市") and len(raw) > 1 else raw
                if base:
                    value = base

            if op in {"not null", "not_null", "is not null", "is_not_null"}:
                normalized = {"field": field, "op": "!=", "value": None}
                key = (str(field), "!=", "None")
                if key not in seen_filters:
                    normalized_filters.append(normalized)
                    seen_filters.add(key)
                continue
            if op in {"null", "is null", "is_null"}:
                normalized = {"field": field, "op": "==", "value": None}
                key = (str(field), "==", "None")
                if key not in seen_filters:
                    normalized_filters.append(normalized)
                    seen_filters.add(key)
                continue
            if op in {"=", "eq"}:
                normalized = {"field": field, "op": "==", "value": value}
                key = (str(field), "==", repr(value))
                if key not in seen_filters:
                    normalized_filters.append(normalized)
                    seen_filters.add(key)
                continue

            if field in {"store_city", "license_city"} and op == "==" and isinstance(value, str) and value:
                variants = []
                base = value[:-1] if value.endswith("市") and len(value) > 1 else value
                if base:
                    variants.append(base)
                    variants.append(f"{base}市")
                variants = list(dict.fromkeys(variants))
                normalized = {"field": field, "op": "in", "value": variants}
                key = (str(field), "in", repr(variants))
                if key not in seen_filters:
                    normalized_filters.append(normalized)
                    seen_filters.add(key)
                continue

            if op == "!=" and "value" not in f:
                normalized = {"field": field, "op": "!=", "value": None}
                key = (str(field), "!=", "None")
                if key not in seen_filters:
                    normalized_filters.append(normalized)
                    seen_filters.add(key)
                continue

            normalized = {"field": field, "op": op, "value": value}
            key = (str(field), str(op), repr(value))
            if key not in seen_filters:
                normalized_filters.append(normalized)
                seen_filters.add(key)

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

        statistics = plan.get("statistics")
        if statistics is not None and not isinstance(statistics, dict):
            plan["statistics"] = {}
        elif isinstance(statistics, dict):
            stype = statistics.get("type")
            if stype not in {"weekly_decline_ratio", "daily_threshold_count"}:
                plan["statistics"] = {}
            elif stype == "weekly_decline_ratio":
                wdays = statistics.get("weekdays")
                if isinstance(wdays, list):
                    normalized_wdays = []
                    for w in wdays:
                        if isinstance(w, (int, float, str)) and str(w).isdigit():
                            iv = int(w)
                            if 1 <= iv <= 7:
                                normalized_wdays.append(iv)
                    statistics["weekdays"] = sorted(list(dict.fromkeys(normalized_wdays)))
                wweeks = statistics.get("window_weeks")
                if isinstance(wweeks, str) and wweeks.isdigit():
                    statistics["window_weeks"] = int(wweeks)
            elif stype == "daily_threshold_count":
                op = statistics.get("op")
                if op not in {">", ">=", "<", "<=", "==", "!="}:
                    statistics["op"] = ">"
                threshold = statistics.get("threshold")
                try:
                    statistics["threshold"] = float(threshold)
                except Exception:
                    statistics["threshold"] = 0.0
                wdays = statistics.get("window_days")
                if isinstance(wdays, str) and wdays.isdigit():
                    statistics["window_days"] = int(wdays)
            if not PlanningAgent._statistics_plan_valid(statistics, plan):
                plan["statistics"] = {}

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
        for part in parts:
            intent = self._classify_intent(part)
            if intent == "statistics" and self._is_weekly_decline_ratio_query(part):
                p = self._build_weekly_decline_ratio_plan(part)
                p["question"] = part
                return [p]
            if intent == "statistics" and self._is_daily_threshold_count_query(part):
                p = self._build_daily_threshold_count_plan(part)
                if isinstance(p, dict) and p:
                    p["question"] = part
                    return [p]
        for part in parts:
            if self._should_sales_clarify(part):
                return [{"question": part, "clarification": self._sales_clarification(part)}]
            need, city = self._should_city_clarify(part)
            if need and city:
                return [{"question": part, "clarification": self._city_clarification(city, part)}]

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
                    "- 如果问题涉及同比/年同比，comparison.type = yoy；涉及跨窗口环比（非统计计数问题）时，comparison.type = wow。\n"
                    "- 如果用户一句话包含多个子问题，请拆成多个 plan，按出现顺序返回。\n"
                    "- 如果你拆了子问题，请为每个 plan 填写 question 字段用于回显。\n"
                    "- 遇到歧义必须返回 clarification.need=true，而不是自行猜测。\n"
                    "- 澄清规则与口径定义以 Schema 文档为准。\n"
                    "- 锁单量的统计口径：order_number count 且 lock_time 非空，时间筛选基于 lock_time。\n"
                    "- 若问题是时序统计类（如近N周、指定周内日、多少周下降），请在 plan.statistics 中输出 weekly_decline_ratio 配置；该类型表示单窗口内按周聚合后做周环比序列统计。\n"
                    "- weekly_decline_ratio 必须包含: time_field/window_weeks/weekdays/numerator_metric/denominator_metric。\n"
                    "- 若问题是阈值计数类（如近N日有多少天指标大于X），请在 plan.statistics 中输出 daily_threshold_count 配置。\n"
                    "- daily_threshold_count 必须包含: time_field/window_days/op/threshold/value_metric。\n"
                ),
            },
            {"role": "user", "content": user_query},
        ]

        try:
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
                clarification = args.get("clarification")
                if isinstance(clarification, dict) and clarification.get("need"):
                    return [{"question": user_query, "clarification": clarification}]
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
            obj = json.loads(content)
            if isinstance(obj, dict) and isinstance(obj.get("plans"), list):
                plans: list[dict] = []
                for p in obj["plans"]:
                    if not isinstance(p, dict):
                        continue
                    q = p.get("question") or user_query
                    plans.append(self._fill_defaults(self._normalize_plan(p), q))
                return [p for p in plans if isinstance(p, dict) and p]
            if isinstance(obj, dict) and isinstance(obj.get("clarification"), dict) and obj["clarification"].get("need"):
                return [{"question": user_query, "clarification": obj["clarification"]}]
            if isinstance(obj, dict) and isinstance(obj.get("plan"), dict):
                p = obj["plan"]
                q = p.get("question") or user_query
                return [self._fill_defaults(self._normalize_plan(p), q)]
        except Exception:
            pass

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
        return rule_plans

    def _fill_defaults(self, plan: dict, user_query: str) -> dict:
        metric_defaults = self._metric_defaults(user_query)
        today = datetime.date.today()
        default_start = (today - datetime.timedelta(days=1)).isoformat()
        default_end = today.isoformat()

        if not plan.get("dataset"):
            plan["dataset"] = metric_defaults["dataset"] if metric_defaults else "order_full_data"

        metric = plan.get("metric")
        if not isinstance(metric, dict):
            metric = {}
        if not metric.get("field") or not metric.get("agg"):
            if metric_defaults:
                plan["metric"] = metric_defaults["metric"]

        time = plan.get("time")
        if not isinstance(time, dict):
            time = {}
        if not time.get("start") or not time.get("end"):
            time["start"] = time.get("start") or default_start
            time["end"] = time.get("end") or default_end
        if not time.get("field") and metric_defaults:
            time["field"] = metric_defaults["time_field"]
        plan["time"] = time

        filters = plan.get("filters")
        if not isinstance(filters, list):
            filters = []

        time_field = time.get("field")
        if isinstance(time_field, str) and time_field in {"lock_time", "delivery_date", "invoice_upload_time", "intention_payment_time"}:
            has_non_null = any(
                isinstance(f, dict) and f.get("field") == time_field and f.get("op") == "!=" and f.get("value") is None
                for f in filters
            )
            if not has_non_null:
                filters.append({"field": time_field, "op": "!=", "value": None})
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

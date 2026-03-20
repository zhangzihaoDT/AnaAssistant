import json
import os
import sys
import datetime

from dotenv import load_dotenv
from openai import OpenAI

from planning_agent import PlanningAgent
from tools import QueryTool, ComparisonTool, StatisticsTool


def _load_api_key() -> str | None:
    load_dotenv()
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if api_key:
        return api_key
    env_file = ".env"
    if not os.path.exists(env_file):
        return None
    with open(env_file, "r", encoding="utf-8") as file:
        for line in file:
            if line.startswith("DEEPSEEK_API_KEY="):
                return line.strip().split("=", 1)[1]
    return None


def _memory_file() -> str:
    return os.path.join(os.path.dirname(__file__), ".query_agent_memory.json")


def _load_memory() -> dict:
    path = _memory_file()
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_memory(obj: dict) -> None:
    path = _memory_file()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _clear_memory() -> None:
    path = _memory_file()
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def _merge_pending_context(user_query: str, memory: dict) -> str | None:
    pending = memory.get("pending")
    if not isinstance(pending, dict):
        return None
    pending_type = pending.get("type")

    reply = (user_query or "").strip()
    if not reply:
        return None

    if pending_type == "clarification":
        original_question = pending.get("original_question")
        clarification = pending.get("clarification")
        if not isinstance(original_question, str) or not isinstance(clarification, dict):
            return None
        question = (
            str(clarification.get("question") or "")
            .replace("\n", " ")
            .replace("？", "")
            .replace("?", "")
            .strip()
        )
        options = clarification.get("options")
        options_text = ""
        if isinstance(options, list) and options:
            options_text = " / ".join(str(o) for o in options)
        base_original = (
            original_question.strip()
            .replace("\n", " ")
            .replace("？", "")
            .replace("?", "")
            .strip()
            .rstrip("。；;")
        )
        base_reply = reply.replace("\n", " ").strip().rstrip("？?。；;")
        payload = (
            "澄清上下文: "
            f"原始问题={base_original} "
            f"澄清问题={question} "
            f"可选项={options_text} "
            f"用户回复={base_reply}。"
            "请基于上述上下文生成 plans；如仍不明确，请返回 clarification.need=true。"
        )
        return payload

    return None


def _looks_like_new_question(user_query: str) -> bool:
    q = (user_query or "").strip()
    if not q:
        return False
    if len(q) >= 12:
        return True
    keywords = ["锁单", "交付", "开票", "小订", "意向金", "金额", "试驾", "同比", "环比", "昨天", "去年", "今年", "按", "分"]
    return any(k in q for k in keywords)


def _matches_pending_option(user_query: str, memory: dict) -> bool:
    pending = memory.get("pending")
    if not isinstance(pending, dict):
        return False
    reply = (user_query or "").strip()
    if not reply:
        return False
    normalized_reply = reply.replace(" ", "")
    ptype = pending.get("type")
    if ptype == "clarification":
        clarification = pending.get("clarification")
        if not isinstance(clarification, dict):
            return False
        options = clarification.get("options")
        if isinstance(options, list):
            normalized_options = {str(o).replace(" ", "") for o in options}
            if normalized_reply in normalized_options:
                return True
            for opt in normalized_options:
                if opt and opt in normalized_reply:
                    return True
            tokens = set()
            for o in options:
                s = str(o).strip()
                if not s:
                    continue
                for sep in ["（", "(", " "]:
                    if sep in s:
                        s = s.split(sep, 1)[0].strip()
                if s:
                    tokens.add(s)
            for t in tokens:
                if t and t in reply:
                    return True
            relaxed_tokens = set()
            for t in tokens:
                base = str(t).replace("数量", "").replace("数目", "").replace("数量", "").strip()
                for suffix in ["量", "数"]:
                    if base.endswith(suffix) and len(base) > 1:
                        base = base[: -len(suffix)]
                if base:
                    relaxed_tokens.add(base)
            for t in relaxed_tokens:
                if t and t in reply:
                    return True
        if normalized_reply in {"1", "2", "3", "4"}:
            return True
        return False
    return False


def _looks_like_clarification_answer(user_query: str) -> bool:
    q = (user_query or "").strip()
    if not q:
        return False
    if len(q) <= 6:
        return True
    return False


def run_main_agent(user_query: str) -> str:
    print(f"\n{'='*60}")
    print(f"用户提问: '{user_query}'")

    memory = _load_memory()
    merged = None
    if memory.get("pending") and (
        _matches_pending_option(user_query, memory)
        or _looks_like_clarification_answer(user_query)
        or not _looks_like_new_question(user_query)
    ):
        merged = _merge_pending_context(user_query, memory)
    if merged:
        _clear_memory()
        user_query = merged
        print(f"\n{'='*60}")
        print("已合并上一轮澄清上下文，继续规划...")
    elif memory.get("pending") and _looks_like_new_question(user_query):
        _clear_memory()

    api_key = _load_api_key()
    if not api_key:
        return "Error: Could not find API key in .env"

    client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
    query_tool = QueryTool(
        data_path_file="/Users/zihao_/Documents/github/26W06_Tool_calls/schema/data_path.md",
        schema_dir="/Users/zihao_/Documents/github/26W06_Tool_calls/schema",
    )

    schema_context = query_tool._schema_context()

    planning_agent = PlanningAgent(
        client=client,
        schema_md=schema_context.get("schema_md", ""),
        business_definition=schema_context.get("business_definition", ""),
    )
    comparison_tool = ComparisonTool(query_tool=query_tool)
    statistics_tool = StatisticsTool()

    print("\n[Thinking] PlanningAgent 正在构建规划 DSL...")
    plans = planning_agent.create_plans(user_query) or [planning_agent.create_plan(user_query)]

    plans = [p for p in plans if isinstance(p, dict) and p]
    if not plans:
        return "未能生成有效的规划 DSL。"

    if len(plans) == 1 and isinstance(plans[0], dict) and isinstance(plans[0].get("clarification"), dict):
        clarification = plans[0]["clarification"]
        if clarification.get("need"):
            _save_memory(
                {
                    "pending": {
                        "type": "clarification",
                        "clarification": clarification,
                        "original_question": plans[0].get("question") or user_query,
                        "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
                    }
                }
            )
            opts = clarification.get("options") or []
            opts_text = " / ".join([str(o) for o in opts]) if isinstance(opts, list) else ""
            qtext = clarification.get("question") or "需要你补充信息后才能继续。"
            if opts_text:
                return f"{qtext}\n请选择其一回复：{opts_text}"
            return str(qtext)

    result_blocks: list[str] = []
    for idx, plan in enumerate(plans):
        print(f"\n  ➡️  规划 DSL[{idx+1}/{len(plans)}]: {json.dumps(plan, ensure_ascii=False)}")

        dataset = plan.get("dataset")
        metric = plan.get("metric", {}) or {}
        time = plan.get("time", {}) or {}
        dimensions = plan.get("dimensions", []) or []
        filters = plan.get("filters", []) or []
        comparison = plan.get("comparison", {}) or {}
        statistics = plan.get("statistics", {}) or {}

        time_field = time.get("field")
        time_start = time.get("start")
        time_end = time.get("end")

        filters_without_time = []
        for f in filters:
            if not isinstance(f, dict):
                continue
            if f.get("field") != time_field:
                filters_without_time.append(f)
                continue
            if f.get("op") in {">=", "<"} and str(f.get("value")) in {str(time_start), str(time_end)}:
                continue
            filters_without_time.append(f)

        comparison_type = comparison.get("type")
        stats_type = statistics.get("type") if isinstance(statistics, dict) else None

        tool_result = None
        comparison_df = None
        if comparison_type in {"yoy", "wow"}:
            if comparison_type == "wow" and stats_type == "weekly_decline_ratio":
                print("\n[Thinking] 执行共享周序列算子（Comparison → Weekly WoW Series）...")
                comparison_result = comparison_tool.build_weekly_wow_series(
                    {
                        "dataset": dataset,
                        "filters": filters_without_time,
                        "time": {"field": time_field, "start": time_start, "end": time_end},
                        "statistics": statistics,
                    }
                )
                if isinstance(comparison_result, str):
                    tool_result = comparison_result
                else:
                    comparison_df = comparison_result
            else:
                print(f"\n[Thinking] 执行派生指标对比计算: {comparison_type}")
                comparison_result = comparison_tool.perform_comparison_df(
                    {
                        "dataset": dataset,
                        "metrics": [
                            {
                                "field": metric.get("field"),
                                "agg": metric.get("agg"),
                                "alias": metric.get("alias") or metric.get("business_name") or "value",
                            }
                        ],
                        "dimensions": dimensions,
                        "filters": filters_without_time,
                        "time": {"field": time_field, "start": time_start, "end": time_end},
                        "comparison": {"type": comparison_type},
                    }
                )
                if isinstance(comparison_result, str):
                    tool_result = comparison_result
                else:
                    comparison_df = comparison_result
                    if not stats_type:
                        tool_result = comparison_df.to_string(index=False)

        if stats_type == "weekly_decline_ratio" and tool_result is None:
            print("\n[Thinking] 执行统计型序列分析...")
            if comparison_df is not None:
                if comparison_type != "wow":
                    tool_result = {
                        "type": "weekly_decline_ratio",
                        "error": "unsupported_pipeline_input",
                        "message": "weekly_decline_ratio 仅支持与 wow 周序列算子联动。",
                    }
                else:
                    stat_request = {
                        "type": "weekly_decline_ratio",
                        "series_input": True,
                        "window_weeks": statistics.get("window_weeks") or 10,
                        "weekdays": statistics.get("weekdays") or [4, 5],
                    }
                    try:
                        tool_result = statistics_tool.perform_statistics(stat_request, comparison_df)
                    except Exception as e:
                        tool_result = {"type": "weekly_decline_ratio", "error": "statistics_execution_failed", "message": str(e)}
            else:
                numerator_metric = statistics.get("numerator_metric", {}) if isinstance(statistics, dict) else {}
                denominator_metric = statistics.get("denominator_metric", {}) if isinstance(statistics, dict) else {}
                query_plan = {
                    "dataset": dataset,
                    "metrics": [
                        {
                            "field": numerator_metric.get("field") or metric.get("field"),
                            "agg": numerator_metric.get("agg") or metric.get("agg") or "sum",
                            "alias": numerator_metric.get("alias") or "门店当日锁单数",
                        },
                        {
                            "field": denominator_metric.get("field") or "下发线索数 (门店)",
                            "agg": denominator_metric.get("agg") or "sum",
                            "alias": denominator_metric.get("alias") or "门店线索数",
                        },
                    ],
                    "dimensions": dimensions if dimensions else ([time_field] if time_field else []),
                    "filters": [
                        *filters_without_time,
                        {"field": time_field, "op": ">=", "value": time_start},
                        {"field": time_field, "op": "<", "value": time_end},
                    ]
                    if time_field and time_start and time_end
                    else filters_without_time,
                }
                raw_df = query_tool.execute_analysis_df(query_plan)
                if isinstance(raw_df, str):
                    tool_result = raw_df
                else:
                    numerator_alias = numerator_metric.get("alias") or "门店当日锁单数"
                    denominator_alias = denominator_metric.get("alias") or "门店线索数"
                    weekly_missing_cols = [
                        c for c in [statistics.get("time_field") or time_field, numerator_alias, denominator_alias] if c not in raw_df.columns
                    ]
                    if weekly_missing_cols:
                        print(f"  ⚠️  weekly_decline_ratio 输入列缺失，降级为基础查询输出: {weekly_missing_cols}")
                        tool_result = {
                            "type": "weekly_decline_ratio",
                            "error": "invalid_statistics_input_schema",
                            "missing_columns": weekly_missing_cols,
                        }
                    else:
                        stat_request = {
                            "type": "weekly_decline_ratio",
                            "time_field": statistics.get("time_field") or time_field,
                            "window_weeks": statistics.get("window_weeks") or 10,
                            "weekdays": statistics.get("weekdays") or [4, 5],
                            "numerator_alias": numerator_alias,
                            "denominator_alias": denominator_alias,
                        }
                        try:
                            tool_result = statistics_tool.perform_statistics(stat_request, raw_df)
                        except Exception as e:
                            tool_result = {"type": "weekly_decline_ratio", "error": "statistics_execution_failed", "message": str(e)}
        elif stats_type == "daily_threshold_count" and tool_result is None:
            print("\n[Thinking] 执行统计型阈值计数分析...")
            value_metric = statistics.get("value_metric", {}) if isinstance(statistics, dict) else {}
            metric_alias = value_metric.get("alias") or metric.get("alias") or "value"
            if comparison_df is not None:
                comparison_metric_alias = f"{metric_alias}_diff"
                stat_time_field = (
                    statistics.get("time_field")
                    or (time_field if time_field in comparison_df.columns else None)
                    or (dimensions[0] if isinstance(dimensions, list) and dimensions else None)
                )
                stat_metric_alias = statistics.get("metric_alias") or comparison_metric_alias
                pipeline_missing_cols = [c for c in [stat_time_field, stat_metric_alias] if c and c not in comparison_df.columns]
                if not stat_time_field:
                    tool_result = {
                        "type": "daily_threshold_count",
                        "error": "invalid_statistics_input_schema",
                        "missing_columns": ["time_field"],
                    }
                elif pipeline_missing_cols:
                    print(f"  ⚠️  comparison→statistics 输入列缺失，返回结构化错误: {pipeline_missing_cols}")
                    tool_result = {
                        "type": "daily_threshold_count",
                        "error": "invalid_statistics_input_schema",
                        "missing_columns": pipeline_missing_cols,
                    }
                else:
                    stat_request = {
                        "type": "daily_threshold_count",
                        "time_field": stat_time_field,
                        "window_days": statistics.get("window_days") or 30,
                        "op": statistics.get("op") or ">",
                        "threshold": statistics.get("threshold") if isinstance(statistics, dict) else 0,
                        "metric_alias": stat_metric_alias,
                    }
                    try:
                        tool_result = statistics_tool.perform_statistics(stat_request, comparison_df)
                    except Exception as e:
                        tool_result = {"type": "daily_threshold_count", "error": "statistics_execution_failed", "message": str(e)}
            else:
                query_plan = {
                    "dataset": dataset,
                    "metrics": [
                        {
                            "field": value_metric.get("field") or metric.get("field"),
                            "agg": value_metric.get("agg") or metric.get("agg") or "count",
                            "alias": metric_alias,
                        }
                    ],
                    "dimensions": dimensions if dimensions else ([time_field] if time_field else []),
                    "filters": [
                        *filters_without_time,
                        {"field": time_field, "op": ">=", "value": time_start},
                        {"field": time_field, "op": "<", "value": time_end},
                    ]
                    if time_field and time_start and time_end
                    else filters_without_time,
                }
                raw_df = query_tool.execute_analysis_df(query_plan)
                if isinstance(raw_df, str):
                    tool_result = raw_df
                else:
                    daily_missing_cols = [c for c in [statistics.get("time_field") or time_field, metric_alias] if c not in raw_df.columns]
                    if daily_missing_cols:
                        print(f"  ⚠️  daily_threshold_count 输入列缺失，返回结构化错误: {daily_missing_cols}")
                        tool_result = {
                            "type": "daily_threshold_count",
                            "error": "invalid_statistics_input_schema",
                            "missing_columns": daily_missing_cols,
                        }
                    else:
                        stat_request = {
                            "type": "daily_threshold_count",
                            "time_field": statistics.get("time_field") or time_field,
                            "window_days": statistics.get("window_days") or 30,
                            "op": statistics.get("op") or ">",
                            "threshold": statistics.get("threshold") if isinstance(statistics, dict) else 0,
                            "metric_alias": metric_alias,
                        }
                        try:
                            tool_result = statistics_tool.perform_statistics(stat_request, raw_df)
                        except Exception as e:
                            tool_result = {"type": "daily_threshold_count", "error": "statistics_execution_failed", "message": str(e)}
        elif stats_type == "daily_mean" and tool_result is None:
            print("\n[Thinking] 执行统计型日均分析...")
            value_metric = statistics.get("value_metric", {}) if isinstance(statistics, dict) else {}
            metric_alias = value_metric.get("alias") or metric.get("alias") or "value"
            if comparison_df is not None:
                tool_result = {
                    "type": "daily_mean",
                    "error": "unsupported_pipeline_input",
                    "message": "daily_mean 暂不支持 comparison 联动，请使用单窗口查询。",
                }
            else:
                query_plan = {
                    "dataset": dataset,
                    "metrics": [
                        {
                            "field": value_metric.get("field") or metric.get("field"),
                            "agg": value_metric.get("agg") or metric.get("agg") or "count",
                            "alias": metric_alias,
                        }
                    ],
                    "dimensions": dimensions if dimensions else ([time_field] if time_field else []),
                    "filters": [
                        *filters_without_time,
                        {"field": time_field, "op": ">=", "value": time_start},
                        {"field": time_field, "op": "<", "value": time_end},
                    ]
                    if time_field and time_start and time_end
                    else filters_without_time,
                }
                raw_df = query_tool.execute_analysis_df(query_plan)
                if isinstance(raw_df, str):
                    tool_result = raw_df
                else:
                    daily_missing_cols = [c for c in [statistics.get("time_field") or time_field, metric_alias] if c not in raw_df.columns]
                    if daily_missing_cols:
                        print(f"  ⚠️  daily_mean 输入列缺失，返回结构化错误: {daily_missing_cols}")
                        tool_result = {
                            "type": "daily_mean",
                            "error": "invalid_statistics_input_schema",
                            "missing_columns": daily_missing_cols,
                        }
                    else:
                        stat_request = {
                            "type": "daily_mean",
                            "time_field": statistics.get("time_field") or time_field,
                            "window_days": statistics.get("window_days") or 30,
                            "metric_alias": metric_alias,
                        }
                        try:
                            tool_result = statistics_tool.perform_statistics(stat_request, raw_df)
                        except Exception as e:
                            tool_result = {"type": "daily_mean", "error": "statistics_execution_failed", "message": str(e)}
        elif stats_type == "daily_percentile_rank" and tool_result is None:
            print("\n[Thinking] 执行统计型分位分析...")
            value_metric = statistics.get("value_metric", {}) if isinstance(statistics, dict) else {}
            metric_alias = value_metric.get("alias") or metric.get("alias") or "value"
            if comparison_df is not None:
                tool_result = {
                    "type": "daily_percentile_rank",
                    "error": "unsupported_pipeline_input",
                    "message": "daily_percentile_rank 暂不支持 comparison 联动，请使用单窗口查询。",
                }
            else:
                query_plan = {
                    "dataset": dataset,
                    "metrics": [
                        {
                            "field": value_metric.get("field") or metric.get("field"),
                            "agg": value_metric.get("agg") or metric.get("agg") or "count",
                            "alias": metric_alias,
                        }
                    ],
                    "dimensions": dimensions if dimensions else ([time_field] if time_field else []),
                    "filters": [
                        *filters_without_time,
                        {"field": time_field, "op": ">=", "value": time_start},
                        {"field": time_field, "op": "<", "value": time_end},
                    ]
                    if time_field and time_start and time_end
                    else filters_without_time,
                }
                raw_df = query_tool.execute_analysis_df(query_plan)
                if isinstance(raw_df, str):
                    tool_result = raw_df
                else:
                    daily_missing_cols = [c for c in [statistics.get("time_field") or time_field, metric_alias] if c not in raw_df.columns]
                    if daily_missing_cols:
                        print(f"  ⚠️  daily_percentile_rank 输入列缺失，返回结构化错误: {daily_missing_cols}")
                        tool_result = {
                            "type": "daily_percentile_rank",
                            "error": "invalid_statistics_input_schema",
                            "missing_columns": daily_missing_cols,
                        }
                    else:
                        stat_request = {
                            "type": "daily_percentile_rank",
                            "time_field": statistics.get("time_field") or time_field,
                            "window_days": statistics.get("window_days") or 30,
                            "reference_date": statistics.get("reference_date"),
                            "metric_alias": metric_alias,
                        }
                        try:
                            tool_result = statistics_tool.perform_statistics(stat_request, raw_df)
                        except Exception as e:
                            tool_result = {"type": "daily_percentile_rank", "error": "statistics_execution_failed", "message": str(e)}

        if tool_result is None:
            print("\n[Thinking] 执行单次查询...")
            query_plan = {
                "dataset": dataset,
                "metrics": [
                    {
                        "field": metric.get("field"),
                        "agg": metric.get("agg"),
                        "alias": metric.get("alias") or metric.get("business_name") or "value",
                    }
                ],
                "dimensions": dimensions,
                "filters": [
                    *filters_without_time,
                    {"field": time_field, "op": ">=", "value": time_start},
                    {"field": time_field, "op": "<", "value": time_end},
                ]
                if time_field and time_start and time_end
                else filters_without_time,
            }
            tool_result = query_tool.execute_analysis(query_plan)

        if isinstance(tool_result, str) and ("找不到数据集" in tool_result or "聚合计算失败" in tool_result):
            print("  ⚠️  执行异常，尝试回退到关键词匹配...")
            fallback_question = plan.get("question") or user_query
            fallback_result = query_tool.answer_question(fallback_question)
            tool_result = f"执行遇到问题: {tool_result}\n\n尝试关键词匹配结果:\n{fallback_result}"

        sub_query = plan.get("question") or user_query
        tool_result_text = (
            json.dumps(tool_result, ensure_ascii=False, indent=2)
            if isinstance(tool_result, dict)
            else str(tool_result)
        )
        result_blocks.append(
            f"子问题 {idx+1}: {sub_query}\n规划 DSL: {json.dumps(plan, ensure_ascii=False)}\n执行结果:\n{tool_result_text}"
        )

    print("\n[Thinking] AnalysisAgent 正在生成最终回答...")
    messages = [
        {
            "role": "system",
            "content": "你是一个智能数据分析助手。请基于给定的规划 DSL 与执行结果，直接回答用户问题，语言简洁，给出关键数值与同比/环比方向与幅度。",
        },
        {"role": "user", "content": f"用户问题: {user_query}\n\n{'\n\n---\n\n'.join(result_blocks)}"},
    ]

    final_response = client.chat.completions.create(model="deepseek-chat", messages=messages)
    final_text = final_response.choices[0].message.content or ""
    print(f"\n{'='*60}")
    return final_text


if __name__ == "__main__":
    query = " ".join(sys.argv[1:]).strip()
    if not query:
        query = "下发线索数 (门店) 的平均值是多少？"
    answer = run_main_agent(query)
    print(answer)

import json
import os
import sys
import datetime

from dotenv import load_dotenv
from openai import OpenAI

from planning_agent import PlanningAgent
from tools import QueryTool, ComparisonTool


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


def run_query_agent(user_query: str) -> str:
    print(f"\n{'='*60}")
    print(f"用户提问: '{user_query}'")
    
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

    print("\n[Thinking] PlanningAgent 正在构建规划 DSL...")
    plans = planning_agent.create_plans(user_query) or [planning_agent.create_plan(user_query)]

    plans = [p for p in plans if isinstance(p, dict) and p]
    if not plans:
        return "未能生成有效的规划 DSL。"

    result_blocks: list[str] = []
    for idx, plan in enumerate(plans):
        print(f"\n  ➡️  规划 DSL[{idx+1}/{len(plans)}]: {json.dumps(plan, ensure_ascii=False)}")

        dataset = plan.get("dataset")
        metric = plan.get("metric", {}) or {}
        time = plan.get("time", {}) or {}
        dimensions = plan.get("dimensions", []) or []
        filters = plan.get("filters", []) or []
        comparison = plan.get("comparison", {}) or {}

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

        if comparison_type in {"yoy", "wow"}:
            print(f"\n[Thinking] 执行派生指标对比计算: {comparison_type}")
            tool_result = comparison_tool.perform_comparison(
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
        else:
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

        if "找不到数据集" in tool_result or "聚合计算失败" in tool_result:
            print(f"  ⚠️  执行异常，尝试回退到关键词匹配...")
            fallback_question = plan.get("question") or user_query
            fallback_result = query_tool.answer_question(fallback_question)
            tool_result = f"执行遇到问题: {tool_result}\n\n尝试关键词匹配结果:\n{fallback_result}"

        sub_query = plan.get("question") or user_query
        result_blocks.append(
            f"子问题 {idx+1}: {sub_query}\n规划 DSL: {json.dumps(plan, ensure_ascii=False)}\n执行结果:\n{tool_result}"
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
    answer = run_query_agent(query)
    print(answer)

import json
import os
import sys
import datetime

from dotenv import load_dotenv
from openai import OpenAI

from tools import QUERY_TOOL_SCHEMA, QueryTool


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
    
    # 获取 schema 内容作为 prompt 的一部分
    schema_context = query_tool._schema_context()
    current_date = datetime.date.today().isoformat()
    
    messages = [
        {
            "role": "system",
            "content": (
                "你是一个智能数据分析助手 (Planning Agent)。"
                "你的核心职责是将用户的自然语言问题转化为结构化的 BI 分析计划 (DSL)。"
                "你不需要编写 Python 代码，而是生成一个符合 `perform_analysis` 工具定义的 JSON 计划。\n\n"
                
                f"### 当前时间上下文\n- 今天是: {current_date}\n\n"
                
                "### 数据集与 Schema 信息\n"
                f"{schema_context.get('schema_md', '')}\n\n"
                
                "### 任务流程\n"
                "1. 理解用户问题，识别分析目标 (Metric) 和维度 (Dimension)。\n"
                "2. 根据 Schema 映射到正确的数据集 (assign_data 或 order_full_data) 和字段名。\n"
                "3. 构造 BI DSL 计划，调用 `perform_analysis` 工具。\n"
                "4. 必须使用工具，不要直接回答。\n\n"
                
                "### DSL 构造指南\n"
                "- `dataset`: 选择最相关的数据集。\n"
                "- `metrics`: 定义要计算的指标，如 `{'field': 'sales', 'agg': 'sum'}`。\n"
                "- `dimensions`: 定义分组维度。\n"
                "- `filters`: 定义过滤条件。\n"
                "- `sort` & `limit`: 定义排序和限制。\n"
            ),
        },
        {"role": "user", "content": user_query},
    ]
    
    print("\n[Thinking] Agent 正在分析问题并构建查询计划...")
    
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=messages,
        tools=[QUERY_TOOL_SCHEMA],
        tool_choice="auto",
    )
    
    message = response.choices[0].message
    tool_calls = message.tool_calls
    
    if not tool_calls:
        print("\n[Info] 模型决定不调用工具，直接回答。")
        return message.content or "未获得有效回答 (模型未调用工具)。"
    
    messages.append(message)
    tool_outputs: list[str] = []
    
    print(f"\n[✅ 模型决定调用工具] 识别到 {len(tool_calls)} 个分析步骤")

    for i, tool_call in enumerate(tool_calls):
        if tool_call.function.name != "perform_analysis":
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "content": f"未知工具: {tool_call.function.name}",
                }
            )
            continue
            
        try:
            args = json.loads(tool_call.function.arguments or "{}")
        except json.JSONDecodeError:
            args = {}
            
        plan = args.get("plan", {})
        
        # 打印 DSL 计划
        print(f"\n  ➡️  步骤 {i+1}: 执行分析计划 (DSL)")
        print(f"      数据集: {plan.get('dataset', 'unknown')}")
        if plan.get('metrics'):
            metrics_str = ", ".join([f"{m.get('agg')}({m.get('field')})" for m in plan.get('metrics', [])])
            print(f"      指标: {metrics_str}")
        if plan.get('dimensions'):
            print(f"      维度: {plan.get('dimensions')}")
        if plan.get('filters'):
            filters_str = ", ".join([f"{f.get('field')} {f.get('op')} {f.get('value')}" for f in plan.get('filters', [])])
            print(f"      过滤: {filters_str}")
        
        # 执行 DSL 计划
        tool_result = query_tool.execute_analysis(plan)
        
        # 打印部分执行结果
        result_lines = tool_result.split('\n')
        preview_lines = result_lines[:5]
        print(f"  ✅  执行结果预览:")
        for line in preview_lines:
             print(f"      {line}")
        if len(result_lines) > 5:
             print(f"      ... (共 {len(result_lines)} 行)")
        
        # 简单的回退机制 (如果 DSL 执行出错，尝试简单关键词搜索)
        if "找不到数据集" in tool_result or "聚合计算失败" in tool_result:
             print(f"  ⚠️  DSL 执行异常，尝试回退到关键词匹配...")
             fallback_result = query_tool.answer_question(user_query)
             tool_result = f"DSL 执行遇到问题: {tool_result}\n\n尝试关键词匹配结果:\n{fallback_result}"

        tool_outputs.append(tool_result)
        messages.append(
            {
                "role": "tool",
                "tool_call_id": tool_call.id,
                "content": tool_result,
            }
        )
    
    messages.append(
        {
            "role": "user",
            "content": "请基于以上数据分析结果直接回答原问题。用简洁的语言总结结论。",
        }
    )
    
    print("\n[Thinking] Agent 正在根据分析结果生成最终回答...")
    
    final_response = client.chat.completions.create(
        model="deepseek-chat",
        messages=messages,
    )
    
    final_text = final_response.choices[0].message.content or ""
    print(f"\n{'='*60}")
    return final_text


if __name__ == "__main__":
    query = " ".join(sys.argv[1:]).strip()
    if not query:
        query = "下发线索数 (门店) 的平均值是多少？"
    answer = run_query_agent(query)
    print(answer)

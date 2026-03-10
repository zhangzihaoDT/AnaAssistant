import os
import json
from dotenv import load_dotenv
from openai import OpenAI
from tools import get_current_time, TIME_TOOL_SCHEMA, CodeInterpreterTool, CODE_INTERPRETER_SCHEMA

# 1. 加载环境变量 (Load Environment Variables)
# 这一步是为了获取DeepSeek API Key
load_dotenv()
api_key = os.getenv("deepseek")
e2b_api_key = os.getenv("E2B_API_KEY") or os.getenv("E2B_development")

if not api_key:
    # 尝试手动读取 .env 文件
    try:
        with open(".env", "r") as f:
            for line in f:
                if line.startswith("deepseek="):
                    api_key = line.strip().split("=")[1]
                if line.startswith("E2B_API_KEY="):
                    e2b_api_key = line.strip().split("=")[1]
                if line.startswith("E2B_development="):
                    e2b_api_key = line.strip().split("=")[1]
    except Exception:
        pass

if not api_key:
    print("Error: Could not find API key in .env")
    exit(1)

# 2. 初始化 OpenAI 客户端 (Initialize Client)
# DeepSeek API 兼容 OpenAI SDK，只需设置 base_url 为 https://api.deepseek.com
client = OpenAI(
    api_key=api_key,
    base_url="https://api.deepseek.com",
)

# 3. 注册工具 (Register Tools)
# 初始化代码解释器 (如果提供了 E2B_API_KEY)
code_tool = CodeInterpreterTool(api_key=e2b_api_key)

# 将工具描述放在列表中
tools = [TIME_TOOL_SCHEMA, CODE_INTERPRETER_SCHEMA]

def run_conversation(user_query):
    print(f"\n{'='*60}")
    print(f"用户提问: '{user_query}'")
    
    messages = [{"role": "user", "content": user_query}]
    
    # 5. 发送请求给模型 (Send Request to Model)
    # tool_choice="auto" 让模型自己决定是否使用工具
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=messages,
            tools=tools,
            tool_choice="auto", # <--- 这里是关键
        )
    except Exception as e:
        print(f"调用 API 出错: {e}")
        return
    
    message = response.choices[0].message
    tool_calls = message.tool_calls
    
    # 6. 处理模型响应 (Handle Model Response)
    if tool_calls:
        print("\n[✅ 模型决定调用工具]")
        # 将带有 tool_calls 的消息添加到历史记录中 (必须添加，否则模型不知道它发起了工具调用)
        messages.append(message)

        for tool_call in tool_calls:
            print(f"  ➡️  工具名称: {tool_call.function.name}")
            print(f"  ➡️  参数: {tool_call.function.arguments}")
            
            if tool_call.function.name == "get_current_time":
                # 执行工具函数
                try:
                    args = json.loads(tool_call.function.arguments)
                    function_response = get_current_time(**args)
                    print(f"  ✅ 工具运行结果: {function_response}")
                    
                    # 将工具结果添加回消息历史
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": function_response,
                    })
                except Exception as e:
                    print(f"  ❌ 工具执行出错: {e}")

            elif tool_call.function.name == "execute_python_code":
                try:
                    args = json.loads(tool_call.function.arguments)
                    print(f"  📝 执行代码:\n{args.get('code')}")
                    function_response = code_tool.execute_code(args['code'])
                    print(f"  ✅ 代码执行结果:\n{function_response}")
                    
                    messages.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": function_response,
                    })
                except Exception as e:
                    print(f"  ❌ 代码执行出错: {e}")
        
        # 再次调用模型，让它根据工具结果生成最终回答
        print("\n[模型根据工具结果生成最终回答...]")
        second_response = client.chat.completions.create(
            model="deepseek-chat",
            messages=messages,
        )
        print(f"🤖 最终回答: {second_response.choices[0].message.content}")
        
    else:
        print("\n[🚫 模型决定不调用工具]")
        print(f"🤖 直接回答: {message.content}")

if __name__ == "__main__":
    # 场景 1: 用户询问时间 -> 模型应该调用 get_current_time
    # run_conversation("请问现在几点了？")
    
    # 场景 2: 代码执行 (Pandas Demo)
    run_conversation("创建一个包含姓名(Alice, Bob, Charlie)和分数(85, 92, 78)的Pandas DataFrame，计算平均分并打印出来。")
    
    # 关闭沙箱 (Clean up sandbox)
    if code_tool.sandbox:
        code_tool.stop()
    
    # 场景 3: 用户闲聊 -> 模型不应该调用工具
    # run_conversation("你好，请介绍一下你自己。")

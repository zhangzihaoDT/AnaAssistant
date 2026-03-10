import os
import json
from dotenv import load_dotenv
from openai import OpenAI
from tools import get_current_time, TIME_TOOL_SCHEMA

# Load environment variables
load_dotenv()
api_key = os.getenv("deepseek")
if not api_key:
    # Fallback manual read
    try:
        with open(".env", "r") as f:
            for line in f:
                if line.startswith("deepseek="):
                    api_key = line.strip().split("=")[1]
                    break
    except Exception:
        pass

client = OpenAI(
    api_key=api_key,
    base_url="https://api.deepseek.com",
)

# Use shared tool schema
tools = [TIME_TOOL_SCHEMA]

def test_query(query):
    print(f"\n测试提问: '{query}'")
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[{"role": "user", "content": query}],
            tools=tools,
            tool_choice="auto", # 关键参数：自动判断
        )
        
        tool_calls = response.choices[0].message.tool_calls
        if tool_calls:
            print(f"  -> 模型决定调用工具: {tool_calls[0].function.name}")
        else:
            print("  -> 模型决定不调用工具")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    print("验证 tool_choice='auto' 的动态判断能力：")
    test_query("现在几点了？")           # 应该调用
    test_query("今天是几号？")           # 应该调用
    test_query("你好，讲个笑话。")        # 不应该调用
    test_query("1+1等于几？")            # 不应该调用
    test_query("帮我查一下现在的时间")     # 应该调用

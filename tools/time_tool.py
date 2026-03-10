from datetime import datetime

# 3. 定义工具函数 (Define Tool Function)
# 这是一个简单的获取当前时间的函数
def get_current_time(format="%Y-%m-%d %H:%M:%S"):
    """Get the current time."""
    return datetime.now().strftime(format)

# 4. 定义工具描述 (Define Tool Schema)
# 告诉模型有哪些工具可用，以及如何调用它们
TIME_TOOL_SCHEMA = {
    "type": "function",
    "function": {
        "name": "get_current_time",
        "description": "获取当前时间。当用户询问现在几点、日期或时间时使用。",
        "parameters": {
            "type": "object",
            "properties": {
                "format": {
                    "type": "string",
                    "description": "时间格式，例如 '%Y-%m-%d %H:%M:%S'。默认为 '%Y-%m-%d %H:%M:%S'。",
                }
            },
            "required": [],
        },
    },
}

# DeepSeek Tool Calling Demo

此项目演示了如何使用 DeepSeek API (OpenAI 兼容接口) 进行工具调用 (Function Calling)。
脚本展示了模型如何根据用户的问题，智能地判断是否需要调用外部工具。

## 功能演示

脚本 `demo.py` 包含两个场景：

1. **需要调用工具**：用户询问时间 -> 模型调用 `get_current_time` 工具 -> 返回准确时间。
2. **不需要调用工具**：用户进行闲聊 -> 模型直接回答 -> 不调用任何工具。

## 环境准备

### 1. 配置 API Key

确保项目根目录下存在 `.env` 文件，并包含您的 DeepSeek API Key：

```env
deepseek=sk-your-api-key-here
```

_(注意：本项目已适配读取 `/Users/zihao_/Documents/github/W2606_Tool_calls/.env`)_

### 2. 创建虚拟环境并安装依赖

```bash
# 创建虚拟环境
python3 -m venv venv

# 激活虚拟环境
# macOS / Linux:
source venv/bin/activate
# Windows:
# venv\Scripts\activate

# 安装依赖
pip install -r requirements.txt
```

## 运行脚本

```bash
python3 demo.py
```

## 运行结果示例

```text
============================================================
用户提问: '请问现在几点了？'

[✅ 模型决定调用工具]
  ➡️  工具名称: get_current_time
  ➡️  参数: {}
  ✅ 工具运行结果: 2026-03-05 09:34:46

[模型根据工具结果生成最终回答...]
🤖 最终回答: 现在是 **2026年3月5日 上午9点34分**。

============================================================
用户提问: '你好，请介绍一下你自己。'

[🚫 模型决定不调用工具]
🤖 直接回答: 你好！我是DeepSeek...
```

## 参考文档

- [DeepSeek Tool Calls 指南](https://api-docs.deepseek.com/zh-cn/guides/tool_calls)

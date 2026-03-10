# DeepSeek Tool Calling Demo

此项目演示了如何使用 DeepSeek API (OpenAI 兼容接口) 进行工具调用 (Function Calling)，并构建了一个具备 **Agentic BI** 能力的智能数据查询助手。

## 核心功能

### 1. 基础工具调用 (`demo.py`)

- **智能判断**：根据用户问题动态决定是否调用工具。
- **时间查询**：调用 `get_current_time` 获取实时时间。
- **代码执行**：调用 `CodeInterpreter` 在沙箱中执行 Python 代码。

### 2. Agentic BI 数据查询 (`query_agent.py`)

这是一个基于 **NL → BI DSL → Execution** 架构的智能数据分析 Agent。

- **Planning Agent**：将自然语言问题转化为结构化的 BI 分析计划 (DSL)。
- **Query Tool**：执行 BI DSL 计划，支持过滤、分组、聚合、排序等 OLAP 操作。
- **Schema Aware**：感知数据集 Schema 和业务定义，准确识别指标和维度。

**工作流示例：**

```
用户提问: "2026年3月到目前为止锁单总数是多少？"
   ↓
Planning Agent: 生成 DSL
   {
     "dataset": "order_full_data",
     "metrics": [{"field": "lock_time", "agg": "count"}],
     "filters": [{"field": "lock_time", "op": ">=", "value": "2026-03-01"}]
   }
   ↓
Query Tool: 执行 DSL (Pandas Aggregation)
   ↓
Result: 1664
   ↓
Agent: "2026年3月到目前为止的锁单总数为 1,664单。"
```

## 环境准备

### 1. 配置 API Key

确保项目根目录下存在 `.env` 文件，并包含您的 DeepSeek API Key：

```env
deepseek=sk-your-api-key-here
```

_(注意：本项目已适配读取 `/Users/zihao_/Documents/github/W2606*Tool_calls/.env`)*

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

### 运行基础演示

```bash
python3 demo.py
```

### 运行数据查询 Agent

```bash
python3 query_agent.py "下发线索数 (门店) 的平均值是多少？"
```

## 项目结构

- `demo.py`: 基础工具调用演示入口。
- `query_agent.py`: Agentic BI 查询助手入口。
- `tools/`: 工具库
  - `query_tool.py`: **核心** BI 查询工具，实现了 DSL 执行引擎。
  - `time_tool.py`: 时间查询工具。
  - `code_interpreter.py`: 代码解释器工具。
- `schema/`: 数据集定义
  - `schema.md`: 数据集字段、指标和维度的详细文档。
  - `data_path.md`: 数据文件路径配置。
  - `business_definition.json`: 业务规则定义。

## 参考文档

- [DeepSeek Tool Calls 指南](https://api-docs.deepseek.com/zh-cn/guides/tool_calls)

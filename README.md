# DeepSeek Tool Calling Demo

此项目演示了如何使用 DeepSeek API (OpenAI 兼容接口) 进行工具调用 (Function Calling)，并构建了一个具备 **Agentic BI** 能力的智能数据查询助手。

## 核心功能

### 1. 基础工具调用 (`demo.py`)

- **智能判断**：根据用户问题动态决定是否调用工具。
- **时间查询**：调用 `get_current_time` 获取实时时间。
- **代码执行**：调用 `CodeInterpreter` 在沙箱中执行 Python 代码。

### 2. Agentic BI 数据查询 (`query_agent.py`)

这是一个基于 **NL → Planning DSL → Execution DSL → Execution** 架构的智能数据分析 Agent。

- **Planning Agent**：将自然语言问题转化为结构化的规划 DSL（时间范围、对比类型、拆解维度、过滤口径）。
- **Query Tool**：执行查询类 DSL（过滤、分组、聚合、排序等 OLAP 操作）。
- **Comparison Tool**：执行派生指标类计算（同比/环比等），通过多个查询窗口对齐后计算差值与涨幅。
- **Schema Aware**：感知数据集 Schema 和业务定义，准确识别指标和维度。
- **多子问题拆解**：支持把一句话里多个子问题拆成多个 `plan` 并逐条执行。
- **中文时间窗解析**：支持 `25年3月1日-3月10日`、`25年3月整月/全月` 等表达，并统一为开区间 `end`（`[start, end)`）。

**工作流示例：**

```
用户提问: "昨天的锁单数周环比如何？"
   ↓
Planning Agent: 生成规划 DSL
  {
    "plans": [
      {
        "question": "昨天的锁单数周环比如何？",
        "dataset": "order_full_data",
        "metric": {"field": "order_number", "agg": "count", "alias": "锁单数"},
        "time": {"field": "lock_time", "start": "2026-03-10", "end": "2026-03-11"},
        "comparison": {"type": "wow"},
        "filters": [{"field": "lock_time", "op": "!=", "value": null}]
      }
    ]
  }
   ↓
Comparison Tool: 生成当前期/对比期执行计划并计算
   ↓
Result: {锁单数_current, 锁单数_compare, 锁单数_diff, 锁单数_diff_pct}
   ↓
Agent: "昨天锁单数为 X，相比上周同期变化 Y，周环比 Z%。"
```

## 环境准备

### 1. 配置 API Key

确保项目根目录下存在 `.env` 文件，并包含您的 DeepSeek API Key：

```env
DEEPSEEK_API_KEY=sk-your-api-key-here
```

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

多子问题示例：

```bash
python3 query_agent.py "25年3月1日-3月10日锁单数多少？25年3月整月锁单数多少？"
```

## 项目结构

- `demo.py`: 基础工具调用演示入口。
- `query_agent.py`: Agentic BI 查询助手入口。
- `planning_agent.py`: PlanningAgent（输出 `plans` 列表 + 意图解析与规范化）。
- `tools/`: 工具库
  - `query_tool.py`: **核心** BI 查询工具，实现了 DSL 执行引擎。
  - `comparison_tool.py`: 派生指标工具（同比/环比），通过对齐两个时间窗口计算差值与涨幅。
  - `time_tool.py`: 时间查询工具。
  - `code_interpreter.py`: 代码解释器工具。
- `schema/`: 数据集定义
  - `schema.md`: 数据集字段、指标和维度的详细文档。
  - `data_path.md`: 数据文件路径配置。
  - `business_definition.json`: 业务规则定义。

## 参考文档

- [DeepSeek Tool Calls 指南](https://api-docs.deepseek.com/zh-cn/guides/tool_calls)

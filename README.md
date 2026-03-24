# DeepSeek Tool Calling Demo

此项目演示了如何使用 DeepSeek API（OpenAI 兼容接口）进行工具调用（Function Calling），并实现一个可落地的 **Agentic BI 查询系统**。

## 系统定位

- 通过 **NL → Planning DSL → Execution → Analysis** 的分层流程处理业务问题
- 支持普通聚合查询、同比/周环比派生对比、统计序列分析
- 通过澄清机制处理“销量口径”“城市口径”等业务歧义
- 规划与执行解耦：LLM 负责规划，代码负责确定性执行

## 核心能力

### 1) 基础工具调用（`demo.py`）

- **智能判断**：根据用户问题动态决定是否调用工具。
- **时间查询**：调用 `get_current_time` 获取实时时间。
- **代码执行**：调用 `CodeInterpreter` 在沙箱中执行 Python 代码。

### 2) Agentic BI 数据查询（`agent/agent_loop.py`）

这是一个基于 **NL → Planning DSL → Execution DSL → Execution** 架构的智能数据分析 Agent。

- **Planning Agent**：把自然语言转成结构化 `plan`（时间范围、过滤口径、比较类型、统计类型、fast_path）。
- **Fast Path Tool**：处理纯数字比较类问题（如“405 环比 382 提升多少”），直接计算差值与涨跌幅。
- **Query Tool**：执行基础查询 DSL（过滤、分组、聚合、排序）。
- **Comparison Tool**：执行跨时间窗口对比（yoy/wow，双窗口对齐 + 差值 + 涨幅）。
- **Statistics Tool**：执行单窗口统计后处理（周环比序列统计、周下降占比、日阈值计数、日均值、分位值排名）。
- **Operators Layer**：承接强口径指标固定算子（如在营门店数），避免通用 DSL 口径漂移。
- **Schema Aware**：规划阶段显式注入 schema 与业务定义，提高字段/指标映射准确性。
- **Clarification Memory**：需要澄清时暂存上下文，下一轮自动合并后继续规划。

## 执行流程

```text
用户问题
  ↓
PlanningAgent.create_plan()
  ↓
plan（含 dataset/metric/time/filters/comparison/statistics/fast_path）
  ↓
agent/tool_router.py 路由执行：
  - fast_path.type == numeric_ratio → FastPathTool（数字环比/同比直算）
  - fast_path.type == current_iso_week → FastPathTool（当前日期 ISO 周数）
  - 命中固定口径问题 → operators.registry（如 active_store）
  - comparison.type in {yoy,wow} → ComparisonTool（周序列场景复用共享算子）
  - statistics.type == weekly_decline_ratio → QueryTool + StatisticsTool 或 ComparisonTool + StatisticsTool
  - statistics.type == daily_threshold_count → QueryTool + StatisticsTool 或 ComparisonTool + StatisticsTool
  - statistics.type == daily_mean → QueryTool + StatisticsTool
  - statistics.type == daily_percentile_rank → QueryTool + StatisticsTool
  - 其他 → QueryTool
  ↓
执行结果（字符串或结构化 JSON）
  ↓
AnalysisAgent 生成最终自然语言回答
```

## 统计型查询说明

当前已支持四类统计 DSL：

- `weekly_decline_ratio`：用于“周环比统计（单窗口周序列）+ 下降周数占比”
  - 示例：查询近 10 周，周四+周五门店锁单率环比变化，有多少周下降
- `daily_threshold_count`：用于“近N日有多少天 metric > x”
  - 示例：近30日有多少天锁单数大于120
- `daily_mean`：用于“近N日/指定时间窗按日聚合后的日均值”
  - 示例：2025年8月1日~10日锁单数日均值是多少
- `daily_percentile_rank`：用于“参考日指标在近N日分布中的分位位置”
  - 示例：昨天的锁单数在近30日的锁单数中处于什么分位值

能力边界：

- `ComparisonTool`：处理“当前窗口 vs 对比窗口”的双窗口对比（如本周 vs 上周、今年同月 vs 去年同月）。
- `StatisticsTool`：处理“单一窗口内部序列”的统计后处理（如近10周逐周 delta、下降周数占比）。
- 当同一 plan 同时包含 `comparison` 与 `statistics` 时，执行器按“Comparison → Statistics”顺序串联。
- `weekly_decline_ratio` 与 `comparison.type=wow` 通过共享周序列算子联动，先生成周序列环比，再统计下降周数占比。
- `daily_mean` 与 `daily_percentile_rank` 当前仅支持单窗口统计，不与 comparison 联动。

`StatisticsTool` 返回结构化 JSON（供 AnalysisAgent 直接消费）：

```json
{
  "type": "weekly_decline_ratio",
  "window_weeks": 10,
  "weekdays": [4, 5],
  "decline_weeks": 6,
  "total_weeks": 10,
  "decline_ratio": 0.6,
  "weekly_rows": [
    {
      "week_start": "2026-01-12",
      "numerator": 123.0,
      "denominator": 4567.0,
      "lock_rate": 0.0269,
      "delta": -0.0018,
      "is_decline": true
    }
  ]
}
```

`daily_threshold_count` 返回结构化 JSON：

```json
{
  "type": "daily_threshold_count",
  "window_days": 30,
  "op": ">",
  "threshold": 120.0,
  "metric_alias": "锁单数",
  "matched_days": 17,
  "total_days": 30,
  "matched_ratio": 0.567,
  "daily_rows": [
    {
      "date": "2026-03-01",
      "value": 132.0,
      "matched": true
    }
  ]
}
```

`daily_mean` 返回结构化 JSON：

```json
{
  "type": "daily_mean",
  "window_days": 10,
  "metric_alias": "锁单数",
  "daily_mean": 152.4,
  "total_days": 10,
  "daily_rows": [
    {
      "date": "2025-08-01",
      "value": 147.0
    }
  ]
}
```

`daily_percentile_rank` 返回结构化 JSON：

```json
{
  "type": "daily_percentile_rank",
  "window_days": 30,
  "metric_alias": "锁单数",
  "reference_date": "2026-03-19",
  "reference_value": 109.0,
  "less_count": 21,
  "le_count": 23,
  "total_days": 30,
  "percentile_rank": 0.767,
  "percentile_pct": 76.7,
  "daily_rows": [
    {
      "date": "2026-03-01",
      "value": 132.0
    }
  ]
}
```

## 机制说明（规划与兜底）

- **规划流程**（LLM 优先 + 规则兜底）：
  - 优先调用 LLM 输出 `plan`，再做规范化与合法性校验
  - 对“日均/分位”等语义执行后处理修正（`_finalize_plans`），避免退化为累计查询
  - LLM 异常或输出不可用时，回退到规则规划分支
- **统计类型细分**：
  - `weekly_decline_ratio` 仅用于单窗口内周环比序列统计与下降占比统计
  - `daily_threshold_count` 用于近 N 日阈值计数问题
  - `daily_mean` 用于近 N 日或显式时间窗的日均值问题
  - `daily_percentile_rank` 用于参考日在近 N 日分布中的分位值问题
- **plan 合法性校验**：
  - statistics 计划会在规范化阶段校验必要字段与语义
  - 不合法时会清空 `statistics`，避免误路由到错误统计分支
- **路由优先级**：
  - 先尝试 Fast Path（纯数字比较）
  - 再尝试 Operators（固定算子）
  - 未命中时进入 comparison / statistics / query 通用链路
- **agent/tool_router.py 执行兜底**：
  - 执行 statistics 前校验输入 DataFrame 所需列
  - 列缺失或执行异常时返回结构化错误对象，不直接抛异常

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

## 运行方式

### 运行基础演示

```bash
python3 demo.py
```

### 运行飞书消息接收测试

```bash
python3 feishu_test.py
```

### 运行飞书 Agent Bot

```bash
python3 feishu_bot.py
```

### 运行数据查询 Agent

```bash
python3 -m agent.agent_loop "下发线索数 (门店) 的平均值是多少？"
```

多子问题示例：

```bash
python3 -m agent.agent_loop "25年3月1日-3月10日锁单数多少？25年3月整月锁单数多少？"
```

统计型示例：

```bash
python3 -m agent.agent_loop "查询近10周的周四、周五的下发线索（门店）锁单率环比变化，看有多少是下降的？"
```

```bash
python3 -m agent.agent_loop "近30日有多少天锁单数是大于120的？"
```

```bash
python3 -m agent.agent_loop "2025年8月1日~10日锁单数日均值是多少？"
```

```bash
python3 -m agent.agent_loop "昨天的锁单数在近30日的锁单数中,处于什么分位值？"
```

作为模块调用：

```python
from agent.agent_loop import run_main_agent

answer = run_main_agent("昨天锁单数周环比如何？")
print(answer)
```

## 项目结构

- `demo.py`: 基础工具调用演示入口。
- `agent/`: Agent 运行时目录
  - `agent_loop.py`: 核心循环入口（状态驱动、多步执行、最终回答）。
  - `planner.py`: 规划与 Loop 决策模块（PlanningAgent + 运行时 action 决策）。
  - `tool_router.py`: 工具路由与 DSL 执行编排。
  - `state.py`: Agent 状态管理（history/iteration/done/result_blocks）。
  - `tools/`: 工具导出层（与根目录 `tools/` 保持一致）。
  - `schema/`: schema 路径导出层（指向根目录 `schema/`）。
- `operators/`: 固定算子层
  - `registry.py`: 算子注册中心（按问题语义路由到固定算子）。
  - `active_store.py`: 在营门店口径算子（30天活动窗口 + 开店日约束）。
- `main.py`: 兼容入口（转发至 `agent.agent_loop.run_main_agent`）。
- `tools/`: 工具库
  - `fast_path_tool.py`: Fast Path 计算工具（纯数字比较直算）。
  - `query_tool.py`: 查询执行引擎（过滤、分组、聚合、排序）。
  - `comparison_tool.py`: 双窗口对比引擎（同比/周环比）。
  - `statistics_tool.py`: 序列统计引擎（weekly_decline_ratio / daily_threshold_count / daily_mean / daily_percentile_rank，结构化 JSON 输出）。
  - `time_tool.py`: 时间查询工具。
  - `code_interpreter.py`: 代码解释器工具。
- `schema/`: 数据集定义
  - `schema.md`: 数据集字段、指标和维度的详细文档。
  - `data_path.md`: 数据文件路径配置。
  - `business_definition.json`: 业务规则定义。

## 参考文档

- [DeepSeek Tool Calls 指南](https://api-docs.deepseek.com/zh-cn/guides/tool_calls)

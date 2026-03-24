目标很明确：

> 在你现有 DeepSeek + DSL tool calling 基础上，最小改动 → 升级为 **可持续执行的 DSL Agent**

我会给你一套**完整可运行骨架**（你可以直接塞进你 repo 改）。

---

# 一、最终结构（先看全局）

```text
agent/
├── agent_loop.py        # ⭐ 核心循环
├── planner.py           # LLM 决策
├── tools.py             # DSL 执行
├── state.py             # 状态管理
└── prompt.py            # prompt模板
```

---

# 二、核心思想（你要记住这句）

> **不是“调用一次 DSL”，而是“不断决定是否继续调用 DSL”**

---

# 三、代码实现（最小可运行版本）

## 1️⃣ state.py（状态定义）

```python
class AgentState:
    def __init__(self, goal):
        self.goal = goal
        self.history = []   # 每一步 action + result
        self.iteration = 0
        self.done = False

    def add_step(self, action, result):
        self.history.append({
            "action": action,
            "result": result
        })
        self.iteration += 1
```

---

## 2️⃣ tools.py（DSL 工具）

```python
def run_dsl_query(dsl: str):
    """
    你原来的 DSL 执行逻辑放这里
    """

    # mock 示例
    result = {
        "data": [{"date": "2026-03-01", "sales": 100}],
        "summary": "销量下降",
        "row_count": 30,
        "quality": "enough"  # ⭐ 关键字段
    }

    return result
```

---

## 3️⃣ prompt.py（核心提示词）

这是整个 Agent 的“大脑约束”，非常关键：

```python
SYSTEM_PROMPT = """
你是一个数据分析Agent，你的目标是逐步完成任务。

你必须每一步输出 JSON，格式如下：

{
  "action": "run_dsl / finish",
  "reason": "为什么这么做",
  "dsl": "DSL语句（如果需要）",
  "analysis": "对当前情况的理解"
}

规则：
1. 不要一次完成任务，要分步骤
2. 每次查询后，根据结果决定下一步
3. 如果信息足够，输出 finish
4. 最多执行 5 步
"""
```

---

## 4️⃣ planner.py（LLM 决策）

```python
import json

def planner(llm, state):
    prompt = f"""
{SYSTEM_PROMPT}

目标：
{state.goal}

历史步骤：
{state.history}

请决定下一步。
"""

    response = llm(prompt)

    try:
        action = json.loads(response)
    except:
        action = {
            "action": "finish",
            "reason": "解析失败",
            "analysis": response
        }

    return action
```

---

## 5️⃣ agent_loop.py（🔥核心循环）

```python
from state import AgentState
from planner import planner
from tools import run_dsl_query

def run_agent(goal, llm, max_steps=5):

    state = AgentState(goal)

    while not state.done and state.iteration < max_steps:

        print(f"\n=== Step {state.iteration} ===")

        # 1️⃣ LLM 决策
        action = planner(llm, state)
        print("Action:", action)

        # 2️⃣ 执行
        if action["action"] == "run_dsl":
            result = run_dsl_query(action["dsl"])

        elif action["action"] == "finish":
            result = action.get("analysis", "完成")
            state.done = True

        else:
            result = "未知 action"
            state.done = True

        # 3️⃣ 写入状态
        state.add_step(action, result)

        print("Result:", result)

    return state
```

---

# 四、如何接入你现在的 DeepSeek demo

你现在应该已经有：

```python
def llm(prompt):
    # deepseek.chat.completions.create(...)
```

👉 只需要：

```python
state = run_agent(
    goal="分析最近销量下降原因",
    llm=llm
)
```

---

# 五、这个版本已经具备什么能力？

✅ 多步 DSL 查询
✅ 根据结果继续分析
✅ 自动决定何时结束
✅ 有完整执行轨迹（history）

---

👉 已经是一个：

> **最小可用 Agent（MVP版 Claude Code 思路）**

---

# 六、接下来你可以做的 3 个增强（强烈建议）

---

## ✅ 1. 让 DSL 结果“更可判断”

现在：

```python
"quality": "enough"
```

升级为：

```python
{
  "trend": "下降",
  "by_channel": {...},
  "confidence": 0.7,
  "need_drill_down": True
}
```

👉 LLM 才知道要不要继续

---

## ✅ 2. 加一个“分析工具”

```python
def analyze_data(data):
    return {
        "insight": "...",
        "next_suggestion": "按城市拆分"
    }
```

👉 变成：

- run_dsl
- analyze

---

## ✅ 3. 加“强约束 DSL 校验层”（你最有优势）

```python
def validate_dsl(dsl):
    # 禁止全表扫描 / 强制limit / 必须带时间
    return True
```

👉 这是你区别于 SQL Agent 的核心壁垒

---

# 七、你现在的阶段（非常清晰）

你已经完成：

✅ Tool Calling
✅ DSL 抽象

正在进入：

👉 **Agent Runtime（最关键一层）**

---

# 八、最后一句（帮你打穿）

> Claude Code 的秘密不在模型，而在这个 while loop

---

如果你下一步想继续升级，我可以帮你做一个更狠的版本：

👉 **LangGraph版 DSL Agent（带状态节点 + 条件分支 + 可视化执行图）**

这个会直接让你的系统进入“可产品化”的层级。

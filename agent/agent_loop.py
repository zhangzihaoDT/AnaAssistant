import json
import os
import sys
import datetime

from dotenv import load_dotenv
from openai import OpenAI

from agent.planner import PlanningAgent, plan_runtime_action
from agent.schema import DATA_PATH_FILE, SCHEMA_DIR
from agent.state import AgentRuntimeState
from agent.tool_router import run_dsl_step
from tools import QueryTool, ComparisonTool, StatisticsTool

FINAL_ANSWER_SYSTEM_PROMPT = "你是一个智能数据分析助手。请基于给定的规划 DSL 与执行结果，直接回答用户问题，语言简洁，给出关键数值与同比/环比方向与幅度。"


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


def _memory_file() -> str:
    return os.path.join(os.path.dirname(__file__), ".query_agent_memory.json")


def _load_memory() -> dict:
    path = _memory_file()
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _save_memory(obj: dict) -> None:
    path = _memory_file()
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _clear_memory() -> None:
    path = _memory_file()
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


def _merge_pending_context(user_query: str, memory: dict) -> str | None:
    pending = memory.get("pending")
    if not isinstance(pending, dict):
        return None
    pending_type = pending.get("type")

    reply = (user_query or "").strip()
    if not reply:
        return None

    if pending_type == "clarification":
        original_question = pending.get("original_question")
        clarification = pending.get("clarification")
        if not isinstance(original_question, str) or not isinstance(clarification, dict):
            return None
        question = (
            str(clarification.get("question") or "")
            .replace("\n", " ")
            .replace("？", "")
            .replace("?", "")
            .strip()
        )
        options = clarification.get("options")
        options_text = ""
        if isinstance(options, list) and options:
            options_text = " / ".join(str(o) for o in options)
        base_original = (
            original_question.strip()
            .replace("\n", " ")
            .replace("？", "")
            .replace("?", "")
            .strip()
            .rstrip("。；;")
        )
        base_reply = reply.replace("\n", " ").strip().rstrip("？?。；;")
        payload = (
            "澄清上下文: "
            f"原始问题={base_original} "
            f"澄清问题={question} "
            f"可选项={options_text} "
            f"用户回复={base_reply}。"
            "请基于上述上下文生成 plans；如仍不明确，请返回 clarification.need=true。"
        )
        return payload

    return None


def _looks_like_new_question(user_query: str) -> bool:
    q = (user_query or "").strip()
    if not q:
        return False
    if len(q) >= 12:
        return True
    keywords = ["锁单", "交付", "开票", "小订", "意向金", "金额", "试驾", "同比", "环比", "昨天", "去年", "今年", "按", "分"]
    return any(k in q for k in keywords)


def _matches_pending_option(user_query: str, memory: dict) -> bool:
    pending = memory.get("pending")
    if not isinstance(pending, dict):
        return False
    reply = (user_query or "").strip()
    if not reply:
        return False
    normalized_reply = reply.replace(" ", "")
    ptype = pending.get("type")
    if ptype == "clarification":
        clarification = pending.get("clarification")
        if not isinstance(clarification, dict):
            return False
        options = clarification.get("options")
        if isinstance(options, list):
            normalized_options = {str(o).replace(" ", "") for o in options}
            if normalized_reply in normalized_options:
                return True
            for opt in normalized_options:
                if opt and opt in normalized_reply:
                    return True
            tokens = set()
            for o in options:
                s = str(o).strip()
                if not s:
                    continue
                for sep in ["（", "(", " "]:
                    if sep in s:
                        s = s.split(sep, 1)[0].strip()
                if s:
                    tokens.add(s)
            for t in tokens:
                if t and t in reply:
                    return True
            relaxed_tokens = set()
            for t in tokens:
                base = str(t).replace("数量", "").replace("数目", "").replace("数量", "").strip()
                for suffix in ["量", "数"]:
                    if base.endswith(suffix) and len(base) > 1:
                        base = base[: -len(suffix)]
                if base:
                    relaxed_tokens.add(base)
            for t in relaxed_tokens:
                if t and t in reply:
                    return True
        if normalized_reply in {"1", "2", "3", "4"}:
            return True
        return False
    return False


def _looks_like_clarification_answer(user_query: str) -> bool:
    q = (user_query or "").strip()
    if not q:
        return False
    if len(q) <= 6:
        return True
    return False


def _trim_text(text: str, limit: int = 1800) -> str:
    if len(text) <= limit:
        return text
    return text[:limit] + "...(truncated)"


def _generate_final_answer(client: OpenAI, user_query: str, result_blocks: list[str]) -> str:
    print("\n[Thinking] AnalysisAgent 正在生成最终回答...")
    messages = [
        {
            "role": "system",
            "content": FINAL_ANSWER_SYSTEM_PROMPT,
        },
        {"role": "user", "content": f"用户问题: {user_query}\n\n{'\n\n---\n\n'.join(result_blocks)}"},
    ]
    final_response = client.chat.completions.create(model="deepseek-chat", messages=messages)
    return final_response.choices[0].message.content or ""


def _build_finish_grounded_answer(action: dict) -> str:
    reason = str(action.get("reason") or "").strip()
    analysis = str(action.get("analysis") or "").strip()
    if reason and analysis:
        return f"{reason}\n\n{analysis}"
    return reason or analysis


def run_main_agent(user_query: str) -> str:
    print(f"\n{'='*60}")
    print(f"用户提问: '{user_query}'")

    memory = _load_memory()
    merged = None
    if memory.get("pending") and (
        _matches_pending_option(user_query, memory)
        or _looks_like_clarification_answer(user_query)
        or not _looks_like_new_question(user_query)
    ):
        merged = _merge_pending_context(user_query, memory)
    if merged:
        _clear_memory()
        user_query = merged
        print(f"\n{'='*60}")
        print("已合并上一轮澄清上下文，继续规划...")
    elif memory.get("pending") and _looks_like_new_question(user_query):
        _clear_memory()

    api_key = _load_api_key()
    if not api_key:
        return "Error: Could not find API key in .env"

    client = OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
    query_tool = QueryTool(
        data_path_file=str(DATA_PATH_FILE),
        schema_dir=str(SCHEMA_DIR),
    )

    schema_context = query_tool._schema_context()

    planning_agent = PlanningAgent(
        client=client,
        schema_md=schema_context.get("schema_md", ""),
        business_definition=schema_context.get("business_definition", ""),
    )
    comparison_tool = ComparisonTool(query_tool=query_tool)
    statistics_tool = StatisticsTool()
    state = AgentRuntimeState(goal=user_query, max_steps=5)
    finish_grounded_answer = ""
    while not state.done and state.iteration < state.max_steps:
        print(f"\n=== Loop Step {state.iteration + 1}/{state.max_steps} ===")
        action = plan_runtime_action(client, state)
        print(f"[Loop] action={json.dumps(action, ensure_ascii=False)}")

        if action.get("action") == "run_dsl":
            action_query = str(action.get("query") or state.goal).strip() or state.goal
            step_result = run_dsl_step(
                action_query=action_query,
                planning_agent=planning_agent,
                query_tool=query_tool,
                comparison_tool=comparison_tool,
                statistics_tool=statistics_tool,
            )
            status = step_result.get("status")
            if status == "clarification":
                clarification = step_result.get("clarification") or {}
                _save_memory(
                    {
                        "pending": {
                            "type": "clarification",
                            "clarification": clarification,
                            "original_question": step_result.get("original_question") or action_query,
                            "created_at": datetime.datetime.now().isoformat(timespec="seconds"),
                        }
                    }
                )
                opts = clarification.get("options") or []
                opts_text = " / ".join([str(o) for o in opts]) if isinstance(opts, list) else ""
                qtext = clarification.get("question") or "需要你补充信息后才能继续。"
                state.add_step(action, _trim_text(f"clarification: {qtext}"))
                if opts_text:
                    return f"{qtext}\n请选择其一回复：{opts_text}"
                return str(qtext)
            if status == "error":
                err_text = str(step_result.get("message") or "执行失败")
                state.add_step(action, _trim_text(err_text))
                state.done = True
                break

            step_blocks = step_result.get("result_blocks") or []
            state.result_blocks.extend(step_blocks)
            merged_step_text = "\n\n---\n\n".join(step_blocks)
            state.add_step(action, _trim_text(merged_step_text))
        elif action.get("action") == "finish":
            finish_grounded_answer = _build_finish_grounded_answer(action)
            state.add_step(action, _trim_text(str(action.get("analysis") or "完成")))
            state.done = True
        else:
            state.add_step(action, "未知 action，终止。")
            state.done = True

    if not state.result_blocks:
        fallback = "未产出可用查询结果。"
        if state.history:
            last_result = str(state.history[-1].get("result") or "")
            if last_result:
                fallback = last_result
        print(f"\n{'='*60}")
        return fallback

    if finish_grounded_answer:
        print(f"\n{'='*60}")
        return finish_grounded_answer

    final_text = _generate_final_answer(client=client, user_query=user_query, result_blocks=state.result_blocks)
    print(f"\n{'='*60}")
    return final_text


if __name__ == "__main__":
    query = " ".join(sys.argv[1:]).strip()
    if not query:
        query = "下发线索数 (门店) 的平均值是多少？"
    answer = run_main_agent(query)
    print(answer)

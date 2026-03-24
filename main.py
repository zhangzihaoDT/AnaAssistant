import sys

from agent.agent_loop import run_main_agent


if __name__ == "__main__":
    query = " ".join(sys.argv[1:]).strip()
    if not query:
        query = "下发线索数 (门店) 的平均值是多少？"
    answer = run_main_agent(query)
    print(answer)

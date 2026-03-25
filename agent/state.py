from dataclasses import dataclass, field


@dataclass
class AgentRuntimeState:
    goal: str
    max_steps: int = 5
    history: list[dict] = field(default_factory=list)
    facts: dict = field(default_factory=dict)
    working_memory: dict = field(
        default_factory=lambda: {
            "current_hypothesis": None,
            "focus_dimension": None,
            "analysis_stage": "init",
        }
    )
    iteration: int = 0
    done: bool = False
    result_blocks: list[str] = field(default_factory=list)

    def add_step(self, action: dict, result: str) -> None:
        self.history.append({"action": action, "result": result})
        self.iteration += 1

    def merge_facts(self, new_facts: dict) -> None:
        if not isinstance(new_facts, dict):
            return
        for k, v in new_facts.items():
            if isinstance(k, str) and k.strip():
                self.facts[k] = v

    def update_working_memory(self, updates: dict) -> None:
        if not isinstance(updates, dict):
            return
        if not isinstance(self.working_memory, dict):
            self.working_memory = {}
        for k, v in updates.items():
            if isinstance(k, str) and k.strip():
                self.working_memory[k] = v

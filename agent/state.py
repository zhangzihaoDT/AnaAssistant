from dataclasses import dataclass, field


@dataclass
class AgentRuntimeState:
    goal: str
    max_steps: int = 5
    history: list[dict] = field(default_factory=list)
    iteration: int = 0
    done: bool = False
    result_blocks: list[str] = field(default_factory=list)

    def add_step(self, action: dict, result: str) -> None:
        self.history.append({"action": action, "result": result})
        self.iteration += 1

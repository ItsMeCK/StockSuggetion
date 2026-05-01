from langgraph.graph import StateGraph, END
from langgraph.checkpoint.postgres import PostgresSaver

from midnight_sovereign.core.state import SovereignState

from midnight_sovereign.agents.heuristic_dtw import run_heuristic_pre_processor as heuristic_pre_processor
from midnight_sovereign.agents.meta_gate import run_meta_gate as meta_gate_experience_check
from midnight_sovereign.agents.pattern_agent import run_pattern_agent as pattern_agent_vision
from midnight_sovereign.agents.risk_agent import run_risk_agent as risk_and_position_sizing
from midnight_sovereign.agents.execution_agent import run_execution_agent as execution_agent
from midnight_sovereign.agents.reflection_engine import run_reflection_engine as reflection_engine_post_mortem
from midnight_sovereign.agents.entry_trigger_agent import run_entry_trigger_agent as entry_trigger_agent
from midnight_sovereign.agents.critic_agent import run_critic_agent as critic_agent
from midnight_sovereign.agents.watcher_agent import run_watcher_agent as watcher_agent
from midnight_sovereign.agents.sector_agent import run_sector_agent as sector_agent

def should_execute(state: SovereignState) -> str:
    """Conditional edge router: proceed to execution if we have approved allocations, else END."""
    if state.get("approved_allocations"):
        return "execution_agent"
    return END

# --- LangGraph Topology Builder ---

def build_sovereign_graph(connection_kwargs: dict) -> StateGraph:
    return build_sovereign_graph_with_checkpointer(None)

def build_sovereign_graph_with_checkpointer(checkpointer) -> StateGraph:
    workflow = StateGraph(SovereignState)

    # 1. Add all nodes
    workflow.add_node("heuristic_pre_processor", heuristic_pre_processor)
    workflow.add_node("meta_gate_experience_check", meta_gate_experience_check)
    workflow.add_node("entry_trigger_agent", entry_trigger_agent)
    workflow.add_node("pattern_agent_vision", pattern_agent_vision)
    workflow.add_node("sector_agent", sector_agent)
    workflow.add_node("watcher_agent", watcher_agent)
    workflow.add_node("critic_agent", critic_agent)
    workflow.add_node("risk_and_position_sizing", risk_and_position_sizing)
    workflow.add_node("execution_agent", execution_agent)
    workflow.add_node("reflection_engine_post_mortem", reflection_engine_post_mortem)

    # 2. Define the flow
    workflow.set_entry_point("heuristic_pre_processor")
    
    workflow.add_edge("heuristic_pre_processor", "meta_gate_experience_check")
    workflow.add_edge("meta_gate_experience_check", "entry_trigger_agent")
    workflow.add_edge("entry_trigger_agent", "watcher_agent")
    workflow.add_edge("watcher_agent", "pattern_agent_vision")
    workflow.add_edge("pattern_agent_vision", "sector_agent")
    workflow.add_edge("sector_agent", "critic_agent")
    
    def critic_debate_router(state: SovereignState) -> str:
        count = state.get("debate_count", 0)
        results = state.get("critic_results", {})
        
        # Check if any vetoes happened
        has_veto = any(res.get("veto", False) for res in results.values())
        
        # Allow 1 correction cycle
        if has_veto and count < 1:
            return "proposer"
        return "risk"

    workflow.add_conditional_edges(
        "critic_agent",
        critic_debate_router,
        {
            "proposer": "pattern_agent_vision",
            "risk": "risk_and_position_sizing"
        }
    )
    
    # Branching logic: Only execute if trades are approved by the Risk Agent
    workflow.add_conditional_edges(
        "risk_and_position_sizing",
        should_execute,
        {
            "execution_agent": "execution_agent",
            END: END
        }
    )

    # Execution feeds directly into the Reflection Engine for continuous learning
    workflow.add_edge("execution_agent", "reflection_engine_post_mortem")
    workflow.add_edge("reflection_engine_post_mortem", END)

    return workflow.compile(checkpointer=checkpointer)

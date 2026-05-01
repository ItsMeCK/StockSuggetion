import operator
from typing import TypedDict, Annotated, List, Dict, Any

def merge_dicts(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """
    Reducer function to merge dictionaries.
    Essential for the LangGraph reducer pattern to ensure immutability 
    and prevent race conditions when multiple agents update the state.
    """
    if not a:
        a = {}
    if not b:
        b = {}
    c = a.copy()
    c.update(b)
    return c

def add_lists(a: List[Any], b: List[Any]) -> List[Any]:
    """
    Reducer function to append lists cleanly and ensure uniqueness.
    """
    if not a:
        a = []
    if not b:
        b = []
    return list(set(a + b))

class SovereignState(TypedDict):
    """
    The immutable State object for the LangGraph orchestrator.
    Using Annotated reducers ensures that delta updates from parallel agents
    (e.g., Risk Agent and Pattern Agent) are safely merged rather than overwritten.
    """
    
    # --- Phase 1: Macro & Deterministic Output ---
    macro_regime: str 
    fii_net: float
    dii_net: float
    india_vix: float
    dxy: float
    # Using add_lists in case we append candidates from multiple sector screeners.
    candidates: Annotated[List[str], add_lists]
    incubator: Annotated[List[str], add_lists]
    breakouts: Annotated[List[str], add_lists]
    
    # Scoreboards (0-100 ranking)
    base_scores: Annotated[Dict[str, float], merge_dicts]
    conviction_scores: Annotated[Dict[str, float], merge_dicts]
    
    # --- Phase 2: Cognitive Engine Delta Updates ---
    # Maps ticker -> dict of identified patterns via DTW (Dynamic Time Warping)
    heuristic_flags: Annotated[Dict[str, Dict[str, Any]], merge_dicts]
    
    # Gear 2: Entry Trigger results (Volume Thrust, 10-SMA Floor, etc.)
    entry_trigger_results: Annotated[Dict[str, Dict[str, Any]], merge_dicts]

    # Maps ticker -> list of warnings retrieved from pgvector Experience DB
    experience_warnings: Annotated[Dict[str, List[str]], merge_dicts]
    
    # Maps ticker -> vision results
    vision_validations: Annotated[Dict[str, Dict[str, Any]], merge_dicts]
    
    # Maps ticker -> agent scores (e.g., {"entry": 85, "vision": 90, "critic": 75})
    agent_scores: Annotated[Dict[str, Dict[str, float]], merge_dicts]
    
    # Maps ticker -> Critic Agent validations
    critic_results: Annotated[Dict[str, Dict[str, Any]], merge_dicts]
    
    # Maps ticker -> Watcher Agent notes
    incubator_notes: Annotated[Dict[str, str], merge_dicts]
    
    # Maps ticker -> allocation details (size, entry, stop, Kelly fraction)
    approved_allocations: Annotated[Dict[str, Dict[str, float]], merge_dicts]
    
    # --- Phase 3: Telemetry & Execution ---
    # Maps ticker -> TWAP/VWAP execution metadata (arrival price, fill price, slippage bps)
    execution_telemetry: Annotated[Dict[str, Any], merge_dicts]
    
    # Append-only error logging for the overarching handle_error node
    error_log: Annotated[List[str], add_lists]
    
    # Debate counter for Proposer-Critic cyclic edge logic
    debate_count: Annotated[int, operator.add]


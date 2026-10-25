from typing import Dict, Any, Tuple

def evaluate_bid_ask_spread(
    depth: Dict[str, Any],
    max_spread_pct: float = 3.5
) -> Tuple[bool, float, str]:
    """
    Evaluates whether an option contract has sufficient liquidity to prevent market-maker gouging
    as defined in Claude Framework Requirement V3 / Gate 4.
    
    Formula:
    Mid = (Best Bid + Best Ask) / 2.0
    Spread Pct = ((Best Ask - Best Bid) / Mid) * 100.0
    
    Returns:
    (is_liquid: bool, spread_pct: float, reason: str)
    """
    if not depth:
        return True, 0.0, "No depth data provided, defaulting to pass"

    buy_depth = depth.get("buy", [])
    sell_depth = depth.get("sell", [])

    if not buy_depth or not sell_depth:
        return False, 100.0, "Missing buy or sell depth"

    best_bid = float(buy_depth[0].get("price", 0.0))
    best_ask = float(sell_depth[0].get("price", 0.0))

    if best_bid <= 0 or best_ask <= 0:
        return False, 100.0, f"Non-positive quote prices (Bid: {best_bid}, Ask: {best_ask})"

    if best_bid > best_ask:
        return False, 0.0, f"Crossed book: Bid ({best_bid}) > Ask ({best_ask})"

    mid = (best_bid + best_ask) / 2.0
    spread_pct = ((best_ask - best_bid) / mid) * 100.0

    if spread_pct > max_spread_pct:
        return False, spread_pct, f"Spread {spread_pct:.2f}% exceeds max threshold {max_spread_pct}%"

    return True, spread_pct, f"Spread {spread_pct:.2f}% is within acceptable threshold {max_spread_pct}%"

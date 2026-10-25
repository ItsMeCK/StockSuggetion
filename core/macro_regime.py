from typing import Dict, Any, List

def classify_macro_regime(
    nifty_close: float,
    nifty_sma20: float,
    nifty_sma50: float = None,
    vix: float = None
) -> Dict[str, Any]:
    """
    Macro Regime & Directional Alignment Engine (Claude Requirement R5 / Taleb)
    Guards against the Single-Regime Put Trap:
    - In BULL_EXPANSION: Call buying has positive edge; Put buying bleeds theta.
    - In BEAR_DISTRIBUTION: Put buying has positive edge; Call buying is catching falling knives.
    - In NEUTRAL_CHOP: Directional edge is compressed; size at 50% with strict time-stops.
    """
    if nifty_sma50 is None:
        nifty_sma50 = nifty_sma20

    dist_20 = ((nifty_close - nifty_sma20) / nifty_sma20) * 100.0 if nifty_sma20 > 0 else 0.0

    # High Volatility Panic Override (Taleb: Beware the Distribution)
    is_high_vix = vix is not None and vix > 22.0
    if is_high_vix:
        return {
            "regime": "BEAR_DISTRIBUTION",
            "allowed_directions": ["SHORT"],
            "allow_ce": False,
            "allow_pe": True,
            "size_multiplier": 1.0,
            "description": f"India VIX ({vix:.1f}) > 22.0 indicates extreme tail risk / market panic. Strict Put focus.",
            "max_holding_hours": 2.0
        }

    if nifty_close > nifty_sma20 and nifty_sma20 >= nifty_sma50:
        return {
            "regime": "BULL_EXPANSION",
            "allowed_directions": ["LONG"],
            "allow_ce": True,
            "allow_pe": False,
            "size_multiplier": 1.0,
            "description": f"Nifty {dist_20:+.2f}% above 20 SMA with bullish SMA alignment. Full Long CE focus.",
            "max_holding_hours": 3.0
        }
    elif nifty_close < nifty_sma20 and nifty_sma20 <= nifty_sma50:
        return {
            "regime": "BEAR_DISTRIBUTION",
            "allowed_directions": ["SHORT"],
            "allow_ce": False,
            "allow_pe": True,
            "size_multiplier": 1.0,
            "description": f"Nifty {dist_20:+.2f}% below 20 SMA. Full Short PE focus (gravity tailwind).",
            "max_holding_hours": 3.0
        }
    else:
        return {
            "regime": "NEUTRAL_CHOP",
            "allowed_directions": ["LONG", "SHORT"],
            "allow_ce": True,
            "allow_pe": True,
            "size_multiplier": 0.5,
            "description": f"Nifty {dist_20:+.2f}% near 20 SMA in consolidation. 50% position sizing with 90m decay cut.",
            "max_holding_hours": 1.5
        }

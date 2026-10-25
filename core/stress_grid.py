import math
from typing import Dict, Any, List, Optional

def standard_normal_cdf(x: float) -> float:
    """Cumulative distribution function for standard normal distribution using math.erf."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def black_scholes_price(
    spot: float,
    strike: float,
    time_to_exp_years: float,
    iv: float,
    r: float = 0.07,
    is_call: bool = True
) -> float:
    """
    Standard Black-Scholes pricing for European options on NSE.
    """
    if time_to_exp_years <= 0.0:
        return max(0.0, spot - strike) if is_call else max(0.0, strike - spot)
    
    if iv <= 0.001:
        disc_k = strike * math.exp(-r * time_to_exp_years)
        return max(0.0, spot - disc_k) if is_call else max(0.0, disc_k - spot)

    d1 = (math.log(spot / strike) + (r + 0.5 * iv * iv) * time_to_exp_years) / (iv * math.sqrt(time_to_exp_years))
    d2 = d1 - iv * math.sqrt(time_to_exp_years)

    if is_call:
        price = spot * standard_normal_cdf(d1) - strike * math.exp(-r * time_to_exp_years) * standard_normal_cdf(d2)
    else:
        price = strike * math.exp(-r * time_to_exp_years) * standard_normal_cdf(-d2) - spot * standard_normal_cdf(-d1)

    return max(0.0, price)

def calculate_overnight_stress_grid(
    spot_price: float,
    strike: float,
    dte_days: float,
    iv: float,
    risk_free_rate: float = 0.07,
    is_call: bool = True,
    qty: int = 1,
    max_allowable_loss_pct: float = -50.0,
    max_rupee_risk: Optional[float] = None
) -> Dict[str, Any]:
    """
    Computes a 3x3 Spot x IV overnight stress grid as mandated by Claude Requirement R2 & Taleb:
    - Spot shifts: -5%, 0%, +5%
    - IV shifts: -20%, 0%, +30%
    - Overnight Theta: DTE advances by 1 day (dte - 1)
    
    Evaluates worst-case loss and returns approval status against account risk budget.
    """
    t0 = max(0.5, dte_days) / 365.0
    t1 = max(0.5, dte_days - 1.0) / 365.0

    baseline_price = black_scholes_price(spot_price, strike, t0, iv, risk_free_rate, is_call)
    
    spot_shifts = [-5.0, 0.0, 5.0]
    iv_shifts = [-20.0, 0.0, 30.0]
    
    scenarios: List[Dict[str, Any]] = []

    for s_shift in spot_shifts:
        for v_shift in iv_shifts:
            stressed_spot = spot_price * (1.0 + s_shift / 100.0)
            stressed_iv = max(0.05, iv * (1.0 + v_shift / 100.0))
            stressed_price = black_scholes_price(
                stressed_spot, strike, t1, stressed_iv, risk_free_rate, is_call
            )
            pnl_pct = ((stressed_price - baseline_price) / baseline_price) * 100.0 if baseline_price > 0 else 0.0
            pnl_rupees = (stressed_price - baseline_price) * qty

            scenarios.append({
                "spot_shift_pct": s_shift,
                "iv_shift_pct": v_shift,
                "stressed_spot": round(stressed_spot, 2),
                "stressed_iv": round(stressed_iv, 4),
                "stressed_price": round(stressed_price, 2),
                "pnl_pct": round(pnl_pct, 2),
                "pnl_rupees": round(pnl_rupees, 2)
            })

    worst_scenario = min(scenarios, key=lambda s: s["pnl_pct"])
    best_scenario = max(scenarios, key=lambda s: s["pnl_pct"])
    worst_case_loss_pct = worst_scenario["pnl_pct"]
    worst_case_loss_rupees = worst_scenario["pnl_rupees"]

    is_approved = True
    rejection_reason = "Stress test passed within risk tolerances"

    if worst_case_loss_pct < max_allowable_loss_pct:
        is_approved = False
        rejection_reason = f"Worst-case loss {worst_case_loss_pct:.2f}% exceeds tolerance {max_allowable_loss_pct:.2f}%"
    elif max_rupee_risk is not None and abs(worst_case_loss_rupees) > max_rupee_risk:
        is_approved = False
        rejection_reason = f"Worst-case rupee loss ₹{abs(worst_case_loss_rupees):.2f} exceeds rupee budget ₹{max_rupee_risk:.2f}"

    return {
        "baseline_price": round(baseline_price, 2),
        "worst_case_loss_pct": round(worst_case_loss_pct, 2),
        "worst_case_loss_rupees": round(worst_case_loss_rupees, 2),
        "worst_case_scenario": worst_scenario,
        "best_case_scenario": best_scenario,
        "scenarios": scenarios,
        "is_approved": is_approved,
        "reason": rejection_reason
    }

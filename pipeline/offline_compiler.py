import json
import os
from pathlib import Path

def compile_institutional_context() -> dict:
    """
    Synthesizes the core technical rules, psychological rationales, and pattern geometries
    from Brian Shannon's and Martin Pring's works into a static JSON schema.
    This eliminates the need for expensive RAG vector searches during the daily execution hot-path.
    """
    
    context = {
        "metadata": {
            "version": "v2.1",
            "sources": [
                "Technical Analysis Using Multiple Timeframes by Brian Shannon",
                "Pring on Price Patterns by Martin Pring"
            ],
            "description": "Static Rulebook for Midnight Sovereign Pattern Agent"
        },
        "shannon_stage_analysis": {
            "stage_1_accumulation": {
                "math": "Price crossing 50 SMA; 200 SMA flattening.",
                "psychology": "Boredom, skepticism. Institutional stealth buying.",
                "action": "Monitor for AVWAP breakout."
            },
            "stage_2_markup": {
                "math": "Price > 10 SMA > 20 SMA > 50 SMA > 200 SMA. 50 SMA slope > 0.",
                "psychology": "Greed, building confidence. Trend becomes obvious.",
                "action": "Aggressively scale positions via Fractional Kelly."
            },
            "stage_3_distribution": {
                "math": "Price breaks 20/50 SMA; 200 SMA flattening. High volatility.",
                "psychology": "Euphoria, then denial. Institutional unloading to retail.",
                "action": "Lock profits. Veto new long positions."
            },
            "stage_4_decline": {
                "math": "Price < 50 SMA < 200 SMA. 200 SMA slope < 0.",
                "psychology": "Panic, capitulation, despair.",
                "action": "Maintain cash or initiate short setups."
            }
        },
        "pring_pattern_geometries": {
            "rectangle": {
                "structure": "Horizontal range bounded by parallel resistance and support lines.",
                "psychology": "Protracted trench warfare between accumulation and distribution.",
                "volume_confirmation": "Volume must dry up during consolidation and spike >150% of average on breakout.",
                "vision_failure_markers": ["Pinocchio Bar (Whipsaw)", "Breakout on declining volume"]
            },
            "symmetrical_triangle": {
                "structure": "Converging higher lows and lower highs indicating a price coil.",
                "psychology": "Diminishing volatility leading to a dramatic imbalance between buyers and sellers.",
                "volume_confirmation": "Progressive volume contraction into the apex. Explosion on breakout.",
                "vision_failure_markers": ["Breakout occurring too deep into apex (>75%)", "Lack of volume on thrust"]
            },
            "ascending_triangle": {
                "structure": "Flat upper resistance with rising lower support line.",
                "psychology": "Buyers are increasingly aggressive, willing to buy at higher prices, while sellers hold firm at a specific level.",
                "volume_confirmation": "Drying up on pullbacks, expanding on advances toward resistance.",
                "vision_failure_markers": ["Spring/Upthrust failing to hold breakout level"]
            },
            "head_and_shoulders": {
                "structure": "Final rally (Head) flanked by two smaller rallies (Shoulders) on a common neckline.",
                "psychology": "Exhaustion of buying power. Transition from advancing peaks to declining peaks.",
                "volume_confirmation": "Heaviest volume on the left shoulder, lighter on head, lightest on right shoulder. Heavy volume on neckline break.",
                "vision_failure_markers": ["Neckline break fails to hold (Bear Trap)"]
            }
        },
        "vision_agent_directives": {
            "skeptical_auditor_prompt": "You are a ruthless institutional auditor. Look for reasons to reject this chart. Specifically hunt for 'Pinocchio Bars' (long wicks indicating fakeouts) and confirm that volume structurally diminishes during consolidation phases.",
            "avwap_validation": "Ensure current price is respecting the AVWAP anchored to the most recent earnings gap or multi-month low."
        }
    }
    return context

def main():
    # Ensure core directory exists
    core_dir = Path(__file__).parent.parent / "core"
    core_dir.mkdir(parents=True, exist_ok=True)
    
    file_path = core_dir / "context_rules.json"
    
    rules = compile_institutional_context()
    
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(rules, f, indent=4)
        
    print(f"✅ Successfully compiled Midnight Sovereign Offline Context to: {file_path}")

if __name__ == "__main__":
    main()

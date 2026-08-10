"""
Runs the SovereignScreener on the latest ingested database state (Monday EOD)
and prints all technical candidates, incubator stocks, and flagged momentum breakouts.
"""
from pipeline.screener import SovereignScreener

def main():
    print("Initializing Sovereign Screener...")
    screener = SovereignScreener()
    
    print("Running screener pipeline on the latest EOD data...")
    # Passing target_date=None runs it on the absolute latest database state
    candidates, incubator, flagged, base_scores, regime = screener.run_pipeline(target_date=None)
    
    print("\n=========================================================================")
    print("MONDAY EOD TECHNICAL SCREENER RESULTS (FOR TUESDAY)")
    print("=========================================================================")
    print(f"Market Regime: {regime['regime']} (Nifty Close: {regime['nifty_close']:.2f})")
    print(f"Total Screened Stocks: {screener.total_screened}")
    print("=========================================================================")
    
    print(f"\n1. Standard Stage 2 Candidates ({len(candidates)}):")
    print(f"   {candidates}")
    
    print(f"\n2. Incubator (Shannon Stage Transition) ({len(incubator)}):")
    print(f"   {incubator}")
    
    print(f"\n3. Flagged Momentum Breakouts ({len(flagged)}):")
    print(f"   {flagged}")
    
    print("\n4. Top 15 Candidates ranked by Base Score:")
    # Sort candidates by their base score
    ranked = sorted(base_scores.items(), key=lambda x: x[1], reverse=True)
    for i, (sym, score) in enumerate(ranked[:15], 1):
        print(f"   {i:2d}. {sym:12s} | Score: {score:.1f}")
        
    print("=========================================================================\n")

if __name__ == "__main__":
    main()

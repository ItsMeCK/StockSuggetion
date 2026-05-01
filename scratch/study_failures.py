import json
from pathlib import Path

def study_failures():
    results_path = Path("backtest_results.json")
    if not results_path.exists():
        print("Results not found.")
        return

    with open(results_path, 'r') as f:
        data = json.load(f)

    losses = []
    total_trades = 0
    
    for day in data:
        regime = day.get("market_regime", "UNKNOWN")
        agent_scores = day.get("agent_scores", {})
        trades = day.get("trades", [])
        total_trades += len(trades)
        for t in trades:
            if t['pnl'] <= -5:
                symbol = t['symbol']
                scores = agent_scores.get(symbol, {})
                losses.append({
                    "date": day['date'],
                    "symbol": symbol,
                    "pnl": t['pnl'],
                    "regime": regime,
                    "vision_score": scores.get("vision", 0),
                    "entry_score": scores.get("entry", 0),
                    "dtw_score": scores.get("dtw", 0)
                })

    print(f"Total Trades Analyzed: {total_trades}")
    print(f"Total Failures (SL Hit): {len(losses)}")
    
    bear_losses = [l for l in losses if l['regime'] == 'BEARISH']
    bull_losses = [l for l in losses if l['regime'] == 'BULLISH']
    
    print("\n--- Failure Distribution by Regime ---")
    print(f"BEARISH Market Losses: {len(bear_losses)}")
    print(f"BULLISH Market Losses: {len(bull_losses)}")
    
    # Calculate Average Scores of Failures
    avg_vision = sum([l['vision_score'] for l in losses]) / len(losses) if losses else 0
    avg_entry = sum([l['entry_score'] for l in losses]) / len(losses) if losses else 0
    
    print(f"\n--- Cognitive Profile of Failures ---")
    print(f"Avg Vision Score of Losers: {avg_vision:.1f}")
    print(f"Avg Entry Score of Losers: {avg_entry:.1f}")
    
    if avg_vision > 80 and avg_entry < 50:
        print("\nHYPOTHESIS: 'The Visual Trap'. GPT-4o is falling for patterns that lack technical momentum.")
    elif avg_entry > 80 and avg_vision < 50:
        print("\nHYPOTHESIS: 'The Momentum Trap'. We are buying vertical spikes that are visually overextended.")

    # Group by symbol to see repeat offenders
    from collections import Counter
    symbols = [l['symbol'] for l in losses]
    repeat = Counter(symbols).most_common(5)
    print(f"\nTop 5 Failing Symbols: {repeat}")

if __name__ == "__main__":
    study_failures()

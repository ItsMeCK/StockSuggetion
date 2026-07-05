"""
Tests whether requiring a REAL catalyst (news or fundamental) improves the
directional hit rate over pure technicals - smart about cost: June is
already extensively cached (2028 news entries, 642 audit entries) from
earlier session runs, so this reuses that cache and only pays for genuine
gaps, using gpt-4o-mini (TRADING_MODE=HISTORICAL default).

Scope: June-only, bullish (CE) candidates - where cache coverage is real.
Bearish/PE catalyst-gating and May coverage are natural follow-ups once this
proves (or disproves) the hypothesis, not worth paying for upfront.
"""
import os
import datetime
import statistics
from dotenv import load_dotenv

load_dotenv()
os.environ["TRADING_MODE"] = "HISTORICAL"
import sys
sys.path.insert(0, "/Users/poonamsalke/Workplace/StockSuggetion")
os.chdir("/Users/poonamsalke/Workplace/StockSuggetion")
import polars as pl

from scripts.gate1_widening_lab import load_universe, load_and_compute, baseline_candidates, build_series_cache
from agents.news_catalyst_agent import NewsCatalystAgent
from agents.fundamental_audit import FundamentalAuditAgent

POSITIVE_GRADES = {"A", "B"}
POSITIVE_ACTIONS = {"DEPLOY", "ACCUMULATE"}


def hit_rate(rows, holding_days):
    """rows: list of (symbol, date). Returns (n, hit_rate_pct, avg_move)."""
    hits, moves = 0, []
    for sym, date, series in rows:
        dl = [b["d"] for b in series]
        if date not in dl:
            continue
        idx = dl.index(date)
        entry_idx = idx + 1
        exit_idx = entry_idx + holding_days
        if exit_idx >= len(series):
            continue
        entry_price = series[entry_idx]["o"]
        exit_price = series[exit_idx]["c"]
        if entry_price <= 0:
            continue
        pct = (exit_price - entry_price) / entry_price * 100
        moves.append(pct)
        if pct >= 2.0:
            hits += 1
    n = len(moves)
    if n == 0:
        return 0, 0, 0
    return n, 100 * hits / n, statistics.mean(moves)


def main():
    universe = load_universe()
    print(f"Universe: {len(universe)} symbols. Loading indicators (June only)...")
    df = load_and_compute(universe, "2026-04-15", "2026-06-30")
    window_df = df.filter((pl.col("time") >= datetime.date(2026, 6, 1)) & (pl.col("time") <= datetime.date(2026, 6, 30)))
    series_cache = build_series_cache(df)

    bull_cands = baseline_candidates(window_df)
    print(f"June bullish candidate-days: {len(bull_cands)}")

    news_agent = NewsCatalystAgent()
    fund_agent = FundamentalAuditAgent()
    print(f"News cache entries available: {len(news_agent.cache)}   Audit cache entries available: {len(fund_agent.cache)}")

    has_news_catalyst, no_news_catalyst = [], []
    has_positive_fund, no_positive_fund = [], []
    both_positive, neither_positive = [], []
    cache_hits_news, fresh_calls_news = 0, 0

    rows = list(bull_cands.to_dicts())
    print(f"Evaluating {len(rows)} candidate-days for real news/fundamental signals...")
    for i, row in enumerate(rows):
        sym, date = row["symbol"], row["time"].strftime("%Y-%m-%d")
        series = series_cache.get(sym)
        if not series:
            continue

        key = f"{sym}_{date}"
        was_cached = key in news_agent.cache
        try:
            news_result = news_agent.evaluate_news_catalysts(sym, date)
        except Exception as e:
            print(f"  news error {sym} {date}: {e}")
            continue
        if was_cached:
            cache_hits_news += 1
        else:
            fresh_calls_news += 1

        catalyst = news_result.get("catalyst_detected", False) and news_result.get("score_boost", 0) > 0

        # Fundamental: only check cache (skip fresh paid calls here to control cost -
        # audit cache is sparser; use what exists, don't force new spend for this pass)
        fund_key = f"{sym}_{date}"
        fund_positive = None
        if fund_key in fund_agent.cache:
            fr = fund_agent.cache[fund_key]
            fund_positive = fr.get("grade") in POSITIVE_GRADES or fr.get("action") in POSITIVE_ACTIONS

        entry = (sym, date, series)
        if catalyst:
            has_news_catalyst.append(entry)
        else:
            no_news_catalyst.append(entry)

        if fund_positive is True:
            has_positive_fund.append(entry)
        elif fund_positive is False:
            no_positive_fund.append(entry)

        if catalyst and fund_positive is True:
            both_positive.append(entry)
        if not catalyst and fund_positive is False:
            neither_positive.append(entry)

        if (i + 1) % 200 == 0:
            print(f"  ...{i+1}/{len(rows)} processed")

    print(f"\nNews: {cache_hits_news} cache hits, {fresh_calls_news} fresh calls "
          f"(fresh calls used gpt-4o-mini, TRADING_MODE=HISTORICAL)")
    print(f"Fundamental: checked cache only ({len(fund_agent.cache)} entries available), no fresh spend this pass")

    print("\n" + "=" * 78)
    print("DIRECTIONAL HIT RATE: technical-only vs catalyst-gated (June 2026, buy CE)")
    print("=" * 78)
    for holding_days in (2, 3, 5):
        print(f"\n--- Holding period: {holding_days} trading days ---")
        for label, rows_ in [
            ("ALL bullish candidates (technical only, baseline)", has_news_catalyst + no_news_catalyst),
            ("HAS real news catalyst", has_news_catalyst),
            ("NO news catalyst", no_news_catalyst),
            ("HAS positive fundamental grade", has_positive_fund),
            ("NO positive fundamental grade (or ungraded->skip)", no_positive_fund),
            ("BOTH catalyst AND positive fundamental", both_positive),
            ("NEITHER (pure technical noise)", neither_positive),
        ]:
            n, hr, avg = hit_rate(rows_, holding_days)
            print(f"  {label:52s} n={n:4d}  HIT RATE(>=2%)={hr:5.1f}%  avg_move={avg:+.2f}%")


if __name__ == "__main__":
    main()

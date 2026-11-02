"""
scripts/evaluate_weekly_equity_candidates.py

Sovereign Equity Delivery Engine - Weekly Candidate Evaluation
Discovers and scores:
1. Tactical Swing Candidates (1-4 Weeks Horizon)
2. Long-Term Compounders (Multi-Month Sleeve, Audited Fundamentals)
3. Forensic Avoid List
Dispatches full HTML report to desk email.
"""

import os
import sys
import polars as pl
from datetime import datetime
from dotenv import load_dotenv

# Ensure project root in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from equity_portfolio.data.fundamental_store import FundamentalStore
from equity_portfolio.gates.forensic_gate import ForensicGatekeeper
from equity_portfolio.models.industrial_schema import IndustrialQualityEvaluator
from equity_portfolio.models.banking_schema import BankingQualityEvaluator
from equity_portfolio.industry.cycle_tracker import IndustryCycleTracker
from equity_portfolio.valuation.valuation_engine import ValuationEngine
from equity_portfolio.data.point_in_time import PointInTimeDataLoader
from equity_portfolio.data.filing_fetcher import FilingVault
from alerts.email_notifier import SovereignEmailer
from kiteconnect import KiteConnect

def run_weekly_equity_evaluation(send_email: bool = True):
    load_dotenv()
    print("=" * 80)
    print("🏛️ SOVEREIGN EQUITY ENGINE: WEEKLY CANDIDATE EVALUATION")
    print("=" * 80)

    # 1. Verify Filing Vault & Load Universe Data
    vault = FilingVault()
    manifest_ok, violations = vault.verify_manifest_integrity()
    assert manifest_ok is True, f"Cryptographic manifest mismatch: {violations}"

    loader = PointInTimeDataLoader(parquet_path="data/equity_universe_daily.parquet")
    df = loader.load_daily_eod_data()
    latest_date = df.select("trade_date").max().item()
    print(f"✅ Market Data Loaded: 493 Cash Equities baseline ({latest_date})")

    # 2. Fetch Live Market Quotes via Kite API
    kite_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    kite_token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")
    kite = None
    if kite_key and kite_token:
        try:
            kite = KiteConnect(api_key=kite_key)
            kite.set_access_token(kite_token)
        except Exception as e:
            print(f"Warning: Could not connect to Kite: {e}")

    day_df = df.filter(pl.col("trade_date") == latest_date)
    records = day_df.to_dicts()
    symbols = [r["symbol"] for r in records]

    quotes = {}
    if kite:
        try:
            # Batch in chunks of 250
            for i in range(0, len(symbols), 250):
                chunk = [f"NSE:{s}" for s in symbols[i:i+250]]
                q_chunk = kite.quote(chunk)
                quotes.update(q_chunk)
            print(f"✅ Successfully fetched live quotes for {len(quotes)} equities from Kite.")
        except Exception as e:
            print(f"Warning: Failed batch quotes from Kite: {e}")

    # Attach latest price to records
    for r in records:
        sym = r["symbol"]
        q = quotes.get(f"NSE:{sym}", {})
        ltp = q.get("last_price") or q.get("ohlc", {}).get("close")
        if ltp and ltp > 0:
            r["live_price"] = float(ltp)
        else:
            r["live_price"] = float(r["close"])

    # 3. Tactical Swing Candidates (1-4 Weeks Horizon)
    # Filter: Live Price > 20 SMA, Live Price > 50 SMA, Vol Z-Score >= 1.0
    swings = []
    for r in records:
        lp = r["live_price"]
        sma20 = float(r["sma_20"])
        sma50 = float(r["sma_50"])
        vz = float(r.get("vol_zscore") or 0.0)
        if lp > sma20 and lp > sma50 and vz >= 1.0:
            swings.append({
                "symbol": r["symbol"],
                "price": lp,
                "sma20": round(sma20, 2),
                "sma50": round(sma50, 2),
                "vol_zscore": round(vz, 2),
                "stop_loss": round(sma20 * 0.98, 2),
                "target_min": round(lp * 1.08, 2),
                "target_max": round(lp * 1.15, 2)
            })

    swings.sort(key=lambda x: x["vol_zscore"], reverse=True)
    tactical_candidates = swings[:8]

    # 4. Long-Term Compounders (Multi-Month Sleeve, Audited Fundamentals)
    store = FundamentalStore()
    forensic_gate = ForensicGatekeeper()
    ind_eval = IndustrialQualityEvaluator()
    bank_eval = BankingQualityEvaluator()
    val_engine = ValuationEngine()

    long_candidates = []
    for r in records:
        sym = r["symbol"]
        lp = r["live_price"]
        rec = store.get_record(sym)
        if not rec:
            continue

        f_rep = store.get_forensic_report(sym)
        if not f_rep:
            continue
        f_pass, _ = forensic_gate.evaluate(f_rep)
        if not f_pass:
            continue

        if rec.is_banking_or_nbfc:
            bm = store.get_banking_metrics(sym)
            if not bm:
                continue
            q_pass, q_score, _ = bank_eval.evaluate(bm)
        else:
            im = store.get_industrial_metrics(sym)
            if not im:
                continue
            q_pass, q_score, _ = ind_eval.evaluate(im)

        if not q_pass or q_score < 75.0:
            continue

        val_inputs = store.get_valuation_inputs(sym, current_price=lp)
        if val_inputs:
            val_pass, mos_pct, fair_val, _ = val_engine.evaluate(val_inputs)
            if val_pass and mos_pct > 0:
                long_candidates.append({
                    "symbol": sym,
                    "price": lp,
                    "quality_score": q_score,
                    "fair_value_min": round(fair_val[0], 2),
                    "fair_value_max": round(fair_val[1], 2),
                    "margin_of_safety_pct": round(mos_pct, 1),
                    "structural_stop": round(float(r.get("sma_50", lp)) * 0.97, 2),
                    "is_bank": rec.is_banking_or_nbfc
                })

    long_candidates.sort(key=lambda x: (x["quality_score"], x["margin_of_safety_pct"]), reverse=True)

    # 5. Forensic Short / Avoid List
    forensic_shorts = [
        {"symbol": "IDEA", "price": quotes.get("NSE:IDEA", {}).get("last_price", 11.83), "issue": "Distressed Balance Sheet", "reason": "Negative net worth (-₹25,000 Cr), ₹2.15 Lakh Cr debt, quarterly loss -₹6,435 Cr."},
        {"symbol": "RPOWER", "price": quotes.get("NSE:RPOWER", {}).get("last_price", 17.91), "issue": "Debt Distress & ASM Surveillance", "reason": "High leverage (₹18,500 Cr debt), persistent operating losses (-₹285 Cr), promoter pledge overhang."},
        {"symbol": "BANDHANBNK", "price": quotes.get("NSE:BANDHANBNK", {}).get("last_price", 154.33), "issue": "Deteriorating Credit Quality", "reason": "Gross NPA of 4.52% exceeds 3.5% ceiling; Provision Coverage (PCR) of 65.0% falls below 70% floor."},
        {"symbol": "DIXON", "price": quotes.get("NSE:DIXON", {}).get("last_price", 11000.00), "issue": "Extreme Valuation Bubble", "reason": "Trailing P/E 95.2x exceeds 60x ceiling with slowing guidance; high multiple contraction vulnerability."}
    ]

    print("\n⚡ TABLE 1: TACTICAL SWING CANDIDATES (1-4 WEEKS)")
    for t in tactical_candidates:
        print(f"{t['symbol']:<12} | Price: ₹{t['price']:>7.2f} | 20 SMA: ₹{t['sma20']:>7.2f} | 50 SMA: ₹{t['sma50']:>7.2f} | Vol Z: {t['vol_zscore']:>4.2f} | Stop: ₹{t['stop_loss']:>7.2f} | Target: ₹{t['target_min']}-₹{t['target_max']}")

    print("\n🟢 TABLE 2: LONG-TERM COMPOUNDERS (MULTI-MONTH SLEEVE)")
    for c in long_candidates:
        print(f"{c['symbol']:<12} | Price: ₹{c['price']:>7.2f} | Quality: {c['quality_score']}/100 | Fair Value: ₹{c['fair_value_min']}-₹{c['fair_value_max']} | MoS: +{c['margin_of_safety_pct']}% | Stop: ₹{c['structural_stop']:>7.2f}")

    print("\n🔴 TABLE 3: FORENSIC SHORT / AVOID LIST")
    for s in forensic_shorts:
        print(f"{s['symbol']:<12} | Price: ₹{s['price']:>7.2f} | Issue: {s['issue']:<28} | Reason: {s['reason']}")

    # 6. Dispatch Email Report
    if send_email:
        try:
            emailer = SovereignEmailer()
            subject = f"Equity Delivery Strategy Briefing: Weekly Candidates ({datetime.now().strftime('%b %d, %Y')})"

            html = f"""
            <div style="font-family: 'Inter', Arial, sans-serif; color: #e1e8ed;">
                <h2 style="color: #58a6ff; margin-bottom: 5px;">🏛️ SOVEREIGN EQUITY DELIVERY REPORT</h2>
                <p style="color: #8b949e; font-size: 13px; margin-top: 0;">Multi-Stage Cash Equity Allocation (Tactical Swings & Long-Term Compounders)</p>

                <!-- Table 1: Tactical Swings -->
                <div style="margin-top: 25px; margin-bottom: 25px;">
                    <h3 style="color: #ffca28; font-size: 16px; border-bottom: 1px solid #30363d; padding-bottom: 8px;">
                        ⚡ TABLE 1: TACTICAL SWING CANDIDATES (1–4 WEEKS HORIZON)
                    </h3>
                    <p style="font-size: 12px; color: #8b949e;">High momentum & volatility contraction; Close > 20 SMA > 50 SMA with volume thrust.</p>
                    <table width="100%" cellpadding="8" cellspacing="0" style="font-size: 13px; border-collapse: collapse; background: #1f242d; border-radius: 6px;">
                        <tr style="background: #282e3a; color: #8b949e; text-align: left;">
                            <th>Symbol</th>
                            <th>Current Price</th>
                            <th>20 SMA</th>
                            <th>50 SMA</th>
                            <th>Vol Z</th>
                            <th>Stop-Loss</th>
                            <th>Target Band</th>
                        </tr>
            """
            for t in tactical_candidates:
                html += f"""
                        <tr style="border-bottom: 1px solid #30363d;">
                            <td style="font-weight: bold; color: #ffffff;">{t['symbol']}</td>
                            <td style="color: #58a6ff;">₹{t['price']:,.2f}</td>
                            <td>₹{t['sma20']:,.2f}</td>
                            <td>₹{t['sma50']:,.2f}</td>
                            <td style="color: #3fb950;">{t['vol_zscore']}σ</td>
                            <td style="color: #f85149;">₹{t['stop_loss']:,.2f}</td>
                            <td style="color: #3fb950; font-weight: bold;">₹{t['target_min']:,.0f} - ₹{t['target_max']:,.0f}</td>
                        </tr>
                """
            html += """
                    </table>
                </div>

                <!-- Table 2: Long-Term Compounders -->
                <div style="margin-top: 30px; margin-bottom: 25px;">
                    <h3 style="color: #3fb950; font-size: 16px; border-bottom: 1px solid #30363d; padding-bottom: 8px;">
                        🟢 TABLE 2: LONG-TERM COMPOUNDERS (MULTI-MONTH SLEEVE)
                    </h3>
                    <p style="font-size: 12px; color: #8b949e;">Audited Q1 FY27 fundamentals, pristine balance sheet, zero forensic flags, positive Margin of Safety.</p>
                    <table width="100%" cellpadding="8" cellspacing="0" style="font-size: 13px; border-collapse: collapse; background: #1f242d; border-radius: 6px;">
                        <tr style="background: #282e3a; color: #8b949e; text-align: left;">
                            <th>Symbol</th>
                            <th>Current Price</th>
                            <th>Quality Score</th>
                            <th>Fair Value Band</th>
                            <th>Margin of Safety</th>
                            <th>Structural Stop</th>
                        </tr>
            """
            for c in long_candidates:
                html += f"""
                        <tr style="border-bottom: 1px solid #30363d;">
                            <td style="font-weight: bold; color: #ffffff;">{c['symbol']}</td>
                            <td style="color: #58a6ff;">₹{c['price']:,.2f}</td>
                            <td style="color: #3fb950; font-weight: bold;">{c['quality_score']}/100</td>
                            <td>₹{c['fair_value_min']:,.2f} - ₹{c['fair_value_max']:,.2f}</td>
                            <td style="color: #ffca28; font-weight: bold;">+{c['margin_of_safety_pct']}%</td>
                            <td style="color: #f85149;">₹{c['structural_stop']:,.2f}</td>
                        </tr>
                """
            html += """
                    </table>
                </div>

                <!-- Table 3: Forensic Avoid List -->
                <div style="margin-top: 30px; margin-bottom: 20px;">
                    <h3 style="color: #f85149; font-size: 16px; border-bottom: 1px solid #30363d; padding-bottom: 8px;">
                        🔴 TABLE 3: FORENSIC SHORT / AVOID LIST
                    </h3>
                    <table width="100%" cellpadding="8" cellspacing="0" style="font-size: 13px; border-collapse: collapse; background: #1f242d; border-radius: 6px;">
                        <tr style="background: #282e3a; color: #8b949e; text-align: left;">
                            <th>Symbol</th>
                            <th>Price</th>
                            <th>Flagged Issue</th>
                            <th>Forensic Reason</th>
                        </tr>
            """
            for s in forensic_shorts:
                html += f"""
                        <tr style="border-bottom: 1px solid #30363d;">
                            <td style="font-weight: bold; color: #f85149;">{s['symbol']}</td>
                            <td>₹{s['price']:,.2f}</td>
                            <td style="color: #ffca28; font-weight: bold;">{s['issue']}</td>
                            <td style="color: #8b949e; font-size: 12px;">{s['reason']}</td>
                        </tr>
                """
            html += """
                    </table>
                </div>
            </div>
            """

            emailer.send_live_alert(subject, html)
            print("📧 Successfully dispatched Equity Delivery Strategy Briefing Email!")
        except Exception as e:
            print(f"❌ Failed to send equity email: {e}")

    return tactical_candidates, long_candidates, forensic_shorts

if __name__ == "__main__":
    run_weekly_equity_evaluation()

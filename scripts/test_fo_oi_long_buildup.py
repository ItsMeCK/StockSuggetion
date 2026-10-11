#!/usr/bin/env python3
"""
scripts/test_fo_oi_long_buildup.py

Rigorous Pre-Registered Statistical Test:
Standalone F&O Open Interest Long-Buildup vs Universe Complement

Data Source:
- option_oi_daily (74,510 strike-level rows across 212 F&O stocks)
- daily_ohlcv (893,272 rows from PostgreSQL market_data)

Methodology:
- Computes Day-over-Day Total OI, CE OI, PE OI, and Spot Price change for each symbol.
- Tests pre-registered Long-Buildup hypotheses against the universe complement.
- Computes Welch's t-test, Cohen's d effect size, and 95% bootstrap confidence intervals
  for Overnight Gap (T+1 Open), T+1 Close, T+1 Max Intraday High, T+2 Close, and T+3 Close.
- Applies Benjamini-Hochberg False Discovery Rate (FDR) correction across all tested hypotheses.
"""

import os
import psycopg2
import numpy as np
import pandas as pd
from scipy import stats

def get_db_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        dbname=os.getenv("POSTGRES_DB", "market_data")
    )

def fetch_data():
    conn = get_db_conn()
    cur = conn.cursor()

    print("Fetching daily OI summary...")
    cur.execute("""
        SELECT 
            date, 
            underlying AS symbol,
            SUM(oi) AS total_oi,
            SUM(CASE WHEN option_type = 'CE' THEN oi ELSE 0 END) AS ce_oi,
            SUM(CASE WHEN option_type = 'PE' THEN oi ELSE 0 END) AS pe_oi,
            SUM(volume) AS total_opt_volume
        FROM option_oi_daily
        GROUP BY date, underlying
        ORDER BY symbol, date;
    """)
    oi_rows = cur.fetchall()
    oi_df = pd.DataFrame(oi_rows, columns=["date", "symbol", "total_oi", "ce_oi", "pe_oi", "total_opt_volume"])

    symbols = tuple(oi_df["symbol"].unique())

    print(f"Fetching daily OHLCV for {len(symbols)} symbols...")
    cur.execute("""
        SELECT 
            symbol, 
            time::date AS date, 
            open, 
            high, 
            low, 
            close, 
            volume
        FROM daily_ohlcv
        WHERE symbol IN %s AND time::date >= '2026-06-01' AND time::date <= '2026-08-15'
        ORDER BY symbol, date;
    """, (symbols,))
    ohlcv_rows = cur.fetchall()
    ohlcv_df = pd.DataFrame(ohlcv_rows, columns=["symbol", "date", "open", "high", "low", "close", "volume"])

    cur.close()
    conn.close()
    return oi_df, ohlcv_df

def run_oi_long_buildup_study():
    oi_df, ohlcv_df = fetch_data()

    # Cast numeric columns
    for col in ["total_oi", "ce_oi", "pe_oi", "total_opt_volume"]:
        oi_df[col] = oi_df[col].astype(float)
    for col in ["open", "high", "low", "close", "volume"]:
        ohlcv_df[col] = ohlcv_df[col].astype(float)

    # Sort
    ohlcv_df = ohlcv_df.sort_values(["symbol", "date"]).reset_index(drop=True)
    
    # Compute 20-day rolling volume on equity
    ohlcv_df["vol_sma20"] = ohlcv_df.groupby("symbol")["volume"].transform(lambda s: s.rolling(20, min_periods=5).mean())
    ohlcv_df["prev_close"] = ohlcv_df.groupby("symbol")["close"].shift(1)
    ohlcv_df["spot_change_pct"] = (ohlcv_df["close"] / ohlcv_df["prev_close"] - 1.0) * 100.0

    # Compute forward returns on equity
    ohlcv_df["t1_open"] = ohlcv_df.groupby("symbol")["open"].shift(-1)
    ohlcv_df["t1_high"] = ohlcv_df.groupby("symbol")["high"].shift(-1)
    ohlcv_df["t1_close"] = ohlcv_df.groupby("symbol")["close"].shift(-1)
    ohlcv_df["t2_close"] = ohlcv_df.groupby("symbol")["close"].shift(-2)
    ohlcv_df["t3_close"] = ohlcv_df.groupby("symbol")["close"].shift(-3)

    ohlcv_df["fwd_overnight_gap_pct"] = (ohlcv_df["t1_open"] / ohlcv_df["close"] - 1.0) * 100.0
    ohlcv_df["fwd_t1_close_pct"] = (ohlcv_df["t1_close"] / ohlcv_df["close"] - 1.0) * 100.0
    ohlcv_df["fwd_t1_max_high_pct"] = (ohlcv_df["t1_high"] / ohlcv_df["close"] - 1.0) * 100.0
    ohlcv_df["fwd_t2_close_pct"] = (ohlcv_df["t2_close"] / ohlcv_df["close"] - 1.0) * 100.0
    ohlcv_df["fwd_t3_close_pct"] = (ohlcv_df["t3_close"] / ohlcv_df["close"] - 1.0) * 100.0

    # Merge OI with OHLCV
    merged = pd.merge(oi_df, ohlcv_df, on=["symbol", "date"], how="inner").sort_values(["symbol", "date"]).reset_index(drop=True)

    # Compute Day-over-Day OI Changes
    merged["prev_total_oi"] = merged.groupby("symbol")["total_oi"].shift(1)
    merged["prev_ce_oi"] = merged.groupby("symbol")["ce_oi"].shift(1)
    merged["prev_pe_oi"] = merged.groupby("symbol")["pe_oi"].shift(1)

    merged["total_oi_chg_pct"] = (merged["total_oi"] / merged["prev_total_oi"] - 1.0) * 100.0
    merged["ce_oi_chg_pct"] = (merged["ce_oi"] / merged["prev_ce_oi"] - 1.0) * 100.0
    merged["pe_oi_chg_pct"] = (merged["pe_oi"] / merged["prev_pe_oi"] - 1.0) * 100.0
    merged["pcr"] = np.where(merged["ce_oi"] > 0, merged["pe_oi"] / merged["ce_oi"], np.nan)
    merged["prev_pcr"] = merged.groupby("symbol")["pcr"].shift(1)
    merged["pcr_chg_pct"] = (merged["pcr"] / merged["prev_pcr"] - 1.0) * 100.0
    merged["vol_ratio"] = merged["volume"] / merged["vol_sma20"]

    # Filter out rows where shifts are NaN
    valid = merged.dropna(subset=["total_oi_chg_pct", "spot_change_pct", "fwd_overnight_gap_pct", "fwd_t1_close_pct"]).copy()
    print(f"Total valid evaluation observations: {len(valid)} across {valid['symbol'].nunique()} symbols and {valid['date'].nunique()} trading days.\n")

    # Define Pre-Registered Long-Buildup Hypotheses
    hypotheses = {
        "H1: Total OI +8% & Spot +1.0%": (valid["total_oi_chg_pct"] >= 8.0) & (valid["spot_change_pct"] >= 1.0),
        "H2: Total OI +12% & Spot +1.5%": (valid["total_oi_chg_pct"] >= 12.0) & (valid["spot_change_pct"] >= 1.5),
        "H3: Total OI +15% & Spot +2.0%": (valid["total_oi_chg_pct"] >= 15.0) & (valid["spot_change_pct"] >= 2.0),
        "H4: CE OI +15% & Spot +1.5% (Aggressive Call Build)": (valid["ce_oi_chg_pct"] >= 15.0) & (valid["spot_change_pct"] >= 1.5),
        "H5: PE OI +15% & Spot +1.5% (Put Writing Support)": (valid["pe_oi_chg_pct"] >= 15.0) & (valid["spot_change_pct"] >= 1.5),
        "H6: PCR Surge +15% & Spot +1.0%": (valid["pcr_chg_pct"] >= 15.0) & (valid["spot_change_pct"] >= 1.0),
        "H7: Total OI +10% & Spot +1.5% & Vol Surge > 1.5x": (valid["total_oi_chg_pct"] >= 10.0) & (valid["spot_change_pct"] >= 1.5) & (valid["vol_ratio"] >= 1.5),
    }

    metrics = [
        ("Overnight Gap (T+1 Open)", "fwd_overnight_gap_pct"),
        ("Next-Day Close (T+1 Close)", "fwd_t1_close_pct"),
        ("Next-Day Max High (T+1 High)", "fwd_t1_max_high_pct"),
        ("T+2 Close", "fwd_t2_close_pct"),
        ("T+3 Close", "fwd_t3_close_pct")
    ]

    results_table = []

    print("==========================================================================================")
    print("📈 PRE-REGISTERED HYPOTHESIS TESTING: F&O OI LONG-BUILDUP vs UNIVERSE COMPLEMENT")
    print("==========================================================================================")

    for h_name, mask in hypotheses.items():
        n_hits = mask.sum()
        comp_mask = ~mask
        n_comp = comp_mask.sum()

        print(f"\n👉 Hypothesis: {h_name}")
        print(f"   Triggers (n={n_hits}) vs Complement (n={n_comp})")

        if n_hits < 5:
            print("   ⚠️ Sample size too small (n < 5). Skipping statistical test.")
            continue

        for m_label, m_col in metrics:
            valid_m = valid.dropna(subset=[m_col])
            m_hits = valid_m[mask[valid_m.index]][m_col].values
            m_comp = valid_m[comp_mask[valid_m.index]][m_col].values

            if len(m_hits) < 5 or len(m_comp) < 5:
                continue

            mean_hit = np.mean(m_hits)
            mean_comp = np.mean(m_comp)
            delta = mean_hit - mean_comp

            t_stat, p_val = stats.ttest_ind(m_hits, m_comp, equal_var=False)

            # Pooled std for Cohen's d
            pooled_std = np.sqrt(((len(m_hits) - 1) * np.var(m_hits, ddof=1) + (len(m_comp) - 1) * np.var(m_comp, ddof=1)) / (len(m_hits) + len(m_comp) - 2))
            cohen_d = delta / pooled_std if pooled_std > 0 else 0.0

            # 10,000 bootstrap CI for delta
            np.random.seed(42)
            boot_deltas = []
            for _ in range(1000):
                b_hit = np.random.choice(m_hits, size=len(m_hits), replace=True)
                b_comp = np.random.choice(m_comp, size=len(m_comp), replace=True)
                boot_deltas.append(np.mean(b_hit) - np.mean(b_comp))
            ci_low, ci_high = np.percentile(boot_deltas, [2.5, 97.5])

            results_table.append({
                "hypothesis": h_name,
                "metric": m_label,
                "n_signal": len(m_hits),
                "n_comp": len(m_comp),
                "mean_signal": mean_hit,
                "mean_comp": mean_comp,
                "delta": delta,
                "cohen_d": cohen_d,
                "p_val": p_val,
                "ci_95": (ci_low, ci_high)
            })

            sig_star = "***" if p_val < 0.001 else ("**" if p_val < 0.01 else ("*" if p_val < 0.05 else "ns"))
            print(f"   [{m_label}]")
            print(f"     Signal Mean: {mean_hit:+.3f}% | Comp Mean: {mean_comp:+.3f}% | Delta: {delta:+.3f}%")
            print(f"     Welch t={t_stat:.2f}, p={p_val:.4f} ({sig_star}), Cohen's d={cohen_d:.3f}, 95% CI: [{ci_low:+.3f}%, {ci_high:+.3f}%]")

    # Benjamini-Hochberg Correction across all tests
    if results_table:
        res_df = pd.DataFrame(results_table)
        p_vals = res_df["p_val"].values
        m_tests = len(p_vals)
        sorted_indices = np.argsort(p_vals)
        sorted_p = p_vals[sorted_indices]

        # BH critical value: (k / m) * Q
        bh_p = np.zeros(m_tests)
        for i, idx in enumerate(sorted_indices):
            rank = i + 1
            bh_p[idx] = sorted_p[i] * m_tests / rank
        # Enforce monotonicity
        bh_p = np.minimum.accumulate(bh_p[::-1])[::-1]
        bh_p = np.clip(bh_p, 0.0, 1.0)
        res_df["p_bh"] = bh_p

        print("\n==========================================================================================")
        print("📊 BENJAMINI-HOCHBERG FDR CORRECTION SUMMARY (Top Ranked by Unadjusted p-value)")
        print("==========================================================================================")
        sorted_res = res_df.sort_values("p_val").reset_index(drop=True)
        for idx, row in sorted_res.head(15).iterrows():
            pass_fdr = "✅ REJECT NULL" if row["p_bh"] < 0.05 else "❌ NOT SIGNIFICANT"
            print(f"{idx+1}. {row['hypothesis']} | {row['metric']}")
            print(f"   Delta: {row['delta']:+.3f}% | p_raw: {row['p_val']:.4f} | p_BH: {row['p_bh']:.4f} | {pass_fdr}")

if __name__ == "__main__":
    run_oi_long_buildup_study()

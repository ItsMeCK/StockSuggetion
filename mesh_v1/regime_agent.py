"""
Regime Agent — classifies each trading day as TRENDING_UP, TRENDING_DOWN, or
CHOPPY, using NIFTY trend + breadth + realized volatility. Not a mover
detector; feeds the router's dynamic weighting.

Rationale (this session's own findings): pure trend-following signals
(ma_stack_rising etc.) decay hard in a choppy/stale-rally tape (validated:
June 15-17 fresh-turn WR 56-63% vs June 22+ stale WR 29%). News/event-driven
catalysts are regime-agnostic. So: in CHOPPY regime, downweight trend agents
and upweight news/event agents; in a clean TRENDING regime, trend agents get
full weight.

Choppiness proxy: NIFTY's realized volatility (ATR/price) relative to its own
recent history, AND how far NIFTY is from its 20-SMA (weak trend = close to
its MA = ambiguous/choppy).
"""
import polars as pl


def classify_regime(nif_rows):
    """nif_rows: list of dicts sorted by date with nif_close, nif_sma20, nif_atr14.
    Returns dict date_str -> {'regime': ..., 'trend_strength': ..., 'choppiness': ...}"""
    out = {}
    for i, r in enumerate(nif_rows):
        d = r["time"].strftime("%Y-%m-%d")
        close, sma20, atr = r.get("nif_close"), r.get("nif_sma20"), r.get("nif_atr14")
        if close is None or sma20 is None or atr is None or sma20 == 0:
            out[d] = {"regime": "UNKNOWN", "trend_strength": 0.0, "choppiness": 0.5}
            continue
        dist_pct = (close - sma20) / sma20 * 100
        vol_pct = atr / close * 100  # realized vol as % of price

        # trend strength: how far price is from its 20sma, normalized by ATR
        trend_strength = dist_pct / (vol_pct if vol_pct else 1)

        # choppiness: recent 10-day directional consistency (fraction of days
        # NIFTY moved the SAME direction as the 10-day trend) - low = choppy
        if i >= 10:
            recent = nif_rows[i - 10:i + 1]
            closes = [x["nif_close"] for x in recent if x.get("nif_close") is not None]
            if len(closes) >= 5:
                diffs = [closes[k] - closes[k - 1] for k in range(1, len(closes))]
                net_dir = 1 if sum(diffs) > 0 else -1
                consistent = sum(1 for x in diffs if (x > 0) == (net_dir > 0))
                choppiness = 1 - (consistent / len(diffs))
            else:
                choppiness = 0.5
        else:
            choppiness = 0.5

        if choppiness >= 0.45:
            regime = "CHOPPY"
        elif dist_pct > 0.3:
            regime = "TRENDING_UP"
        elif dist_pct < -0.3:
            regime = "TRENDING_DOWN"
        else:
            regime = "CHOPPY"

        out[d] = {"regime": regime, "trend_strength": round(trend_strength, 3), "choppiness": round(choppiness, 3)}
    return out


def build_regime_map(df):
    """df: the full stock frame (has nif_* columns joined). Extract unique
    per-date NIFTY rows and classify."""
    nif_rows = (df.select(["time", "nif_close", "nif_sma20", "nif_atr14"])
                .unique(subset=["time"]).sort("time").to_dicts())
    return classify_regime(nif_rows)

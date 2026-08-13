"""
Shared feature computation for the V5 agent mesh. Fresh build — computes the
technical indicators every detector agent needs, once, vectorized with polars.
Not part of the old agents/ pipeline.
"""
import os
import csv
import psycopg2
import polars as pl
from dotenv import load_dotenv
load_dotenv()


def conn():
    return psycopg2.connect(host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"),
                            user=os.getenv("POSTGRES_USER", "quant"), password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                            dbname=os.getenv("POSTGRES_DB", "market_data"))


def load_universe():
    with open("pipeline/master_universe.csv") as f:
        return [r["Symbol"] for r in csv.DictReader(f) if r.get("Symbol")]


def load_company_map():
    m = {}
    with open("pipeline/master_universe.csv") as f:
        for r in csv.DictReader(f):
            if r.get("Symbol"):
                m[r["Symbol"]] = r.get("Company Name", "")
    return m


def build_stock_frame(universe, start=None, end=None):
    """Full per-stock daily feature frame: MAs, ATR, volume, RS vs NIFTY, gap."""
    c = conn()
    df = pl.read_database(
        "SELECT time::date as time, symbol, open, high, low, close, volume FROM daily_ohlcv "
        "WHERE symbol = ANY(%(s)s) ORDER BY symbol, time",
        c, execute_options={"parameters": {"s": universe}})
    c.close()

    c = conn()
    nif = pl.read_database("SELECT time::date as time, close as nif_close, high as nif_high, low as nif_low "
                           "FROM daily_ohlcv WHERE symbol='NIFTY 50' ORDER BY time", c)
    c.close()
    nif = nif.with_columns([
        (((pl.col("nif_close") - pl.col("nif_close").shift(10)) / pl.col("nif_close").shift(10)) * 100).alias("nif_roc10"),
        pl.col("nif_close").rolling_mean(20).alias("nif_sma20"),
        pl.max_horizontal([pl.col("nif_high") - pl.col("nif_low"),
                           (pl.col("nif_high") - pl.col("nif_close").shift(1)).abs(),
                           (pl.col("nif_low") - pl.col("nif_close").shift(1)).abs()]).alias("nif_tr"),
    ])
    nif = nif.with_columns(pl.col("nif_tr").rolling_mean(14).alias("nif_atr14"))

    o = pl.col("symbol")
    df = df.with_columns([
        pl.col("close").ewm_mean(span=10).over(o).alias("ema10"),
        pl.col("close").ewm_mean(span=20).over(o).alias("ema20"),
        pl.col("close").ewm_mean(span=50).over(o).alias("ema50"),
        pl.col("close").rolling_mean(20).over(o).alias("sma20"),
        pl.col("close").rolling_mean(100).over(o).alias("sma100"),
        pl.col("volume").rolling_mean(20).over(o).alias("vol_avg20"),
        pl.col("high").rolling_max(20).shift(1).over(o).alias("prior_high20"),
        pl.col("high").rolling_max(10).shift(1).over(o).alias("hi10"),
        pl.col("low").rolling_min(10).shift(1).over(o).alias("lo10"),
        pl.col("low").rolling_min(5).shift(1).over(o).alias("lo5"),
        pl.col("low").rolling_min(3).shift(1).over(o).alias("lo3"),
        pl.col("close").shift(1).over(o).alias("prev_close"),
        pl.col("close").shift(10).over(o).alias("close_10ago"),
        pl.max_horizontal([pl.col("high") - pl.col("low"),
                           (pl.col("high") - pl.col("close").shift(1).over(o)).abs(),
                           (pl.col("low") - pl.col("close").shift(1).over(o)).abs()]).alias("tr"),
    ])
    df = df.with_columns([
        pl.col("tr").rolling_mean(14).over(o).alias("atr14"),
        pl.col("ema20").shift(5).over(o).alias("ema20_5ago"),
        pl.col("sma20").shift(5).over(o).alias("sma20_5ago"),
        pl.col("sma100").shift(5).over(o).alias("sma100_5ago"),
        (((pl.col("close") - pl.col("close").shift(10).over(o)) / pl.col("close").shift(10).over(o)) * 100).alias("roc10"),
        (((pl.col("open") - pl.col("close").shift(1).over(o)) / pl.col("close").shift(1).over(o)) * 100).alias("gap_pct"),
    ])
    df = df.join(nif.select(["time", "nif_roc10", "nif_close", "nif_sma20", "nif_atr14"]), on="time", how="left")

    if start:
        df = df.filter(pl.col("time") >= pl.lit(start).str.to_date())
    if end:
        df = df.filter(pl.col("time") <= pl.lit(end).str.to_date())
    return df


def frame_lookup(df):
    """dict[(symbol,date_str)] -> row dict, and per-symbol sorted list for scanning."""
    from collections import defaultdict
    by_sym = defaultdict(list)
    for r in df.sort(["symbol", "time"]).to_dicts():
        by_sym[r["symbol"]].append(r)
    lut = {(r["symbol"], r["time"].strftime("%Y-%m-%d")): r for s in by_sym for r in by_sym[s]}
    return by_sym, lut

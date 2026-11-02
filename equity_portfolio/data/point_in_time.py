"""
Point-in-Time Data Layer & Daily EOD Pipeline
Requirements EQ4, EQ5, EQ6, EQ46
"""

import os
import polars as pl
from datetime import datetime, date
from typing import Dict, Any, List, Optional, Tuple

class PointInTimeDataLoader:
    """
    Constructs Point-in-Time daily OHLCV and volume footprints
    from daily or intraday parquet data without look-ahead bias.
    """
    def __init__(self, parquet_path: Optional[str] = None):
        if parquet_path is not None:
            self.parquet_path = parquet_path
        elif os.path.exists("data/equity_universe_daily.parquet"):
            self.parquet_path = "data/equity_universe_daily.parquet"
        else:
            self.parquet_path = "data/intraday_ohlcv.parquet"
        self._daily_df: Optional[pl.DataFrame] = None

    def load_daily_eod_data(self) -> pl.DataFrame:
        """
        Aggregates OHLCV into clean daily EOD OHLCV bars for all NSE universe stocks.
        Stamps IST trading dates cleanly.
        """
        if self._daily_df is not None:
            return self._daily_df

        if not os.path.exists(self.parquet_path):
            raise FileNotFoundError(f"Parquet dataset not found at {self.parquet_path}")

        df = pl.read_parquet(self.parquet_path)
        
        if "trade_date" in df.columns:
            # Already formatted daily EOD bars from Zerodha ingestion
            if "traded_value" not in df.columns:
                df = df.with_columns((pl.col("close") * pl.col("volume")).alias("traded_value"))
            daily = df.sort(["symbol", "trade_date"])
        else:
            # Aggregate hourly/intraday candles to daily EOD bars
            df = df.with_columns([
                pl.col("time").dt.convert_time_zone("Asia/Kolkata").alias("ist_time")
            ])
            df = df.with_columns([
                pl.col("ist_time").dt.date().alias("trade_date")
            ])

            daily = df.group_by(["symbol", "trade_date"]).agg([
                pl.col("open").first().alias("open"),
                pl.col("high").max().alias("high"),
                pl.col("low").min().alias("low"),
                pl.col("close").last().alias("close"),
                pl.col("volume").sum().alias("volume"),
                (pl.col("close") * pl.col("volume")).sum().alias("traded_value")
            ]).sort(["symbol", "trade_date"])

        # Compute rolling indicators: 20-day SMA, 50-day SMA, ATR%, and Volume Z-scores
        # Uses min_samples so indicators are populated reliably across rolling partitions
        daily = daily.with_columns([
            pl.col("close").rolling_mean(window_size=20, min_samples=5).over("symbol").alias("sma_20"),
            pl.col("close").rolling_mean(window_size=50, min_samples=10).over("symbol").alias("sma_50"),
            pl.col("volume").rolling_mean(window_size=20, min_samples=5).over("symbol").alias("vol_sma_20"),
            pl.col("volume").rolling_std(window_size=20, min_samples=5).over("symbol").alias("vol_std_20"),
            (pl.col("high") - pl.col("low")).alias("daily_range"),
            ((pl.col("high") - pl.col("low")) / pl.col("close") * 100.0).alias("daily_range_pct")
        ])

        daily = daily.with_columns([
            pl.col("daily_range_pct").rolling_mean(window_size=20, min_samples=5).over("symbol").alias("atr_pct_20"),
            ((pl.col("volume") - pl.col("vol_sma_20")) / (pl.col("vol_std_20") + 1.0)).alias("vol_zscore")
        ])

        # Corwin-Schultz (Journal of Finance 2012) High-Low Effective Bid-Ask Spread Estimator
        # Eliminates arbitrary turnover buckets by estimating effective bid-ask spread directly from price volatility
        c_denom = 3.0 - 2.0 * (2.0 ** 0.5)
        sqrt_2_minus_1 = (2.0 ** 0.5) - 1.0

        daily = daily.with_columns([
            pl.col("high").shift(1).over("symbol").alias("prev_high"),
            pl.col("low").shift(1).over("symbol").alias("prev_low")
        ])

        daily = daily.with_columns([
            ((pl.col("high") / pl.col("low")).log() ** 2 + (pl.col("prev_high") / pl.col("prev_low")).log() ** 2).alias("cs_beta"),
            (((pl.max_horizontal("high", "prev_high") / pl.min_horizontal("low", "prev_low")).log()) ** 2).alias("cs_gamma")
        ])

        daily = daily.with_columns([
            ((sqrt_2_minus_1 * (pl.col("cs_beta") ** 0.5) / c_denom) - ((pl.col("cs_gamma") / c_denom) ** 0.5)).alias("cs_alpha")
        ])

        daily = daily.with_columns([
            pl.when(pl.col("cs_alpha") > 0)
            .then(2.0 * ((pl.col("cs_alpha").exp() - 1.0) / (pl.col("cs_alpha").exp() + 1.0)) * 100.0)
            .otherwise(0.08)
            .clip(0.02, 3.0)
            .rolling_mean(window_size=5, min_samples=1)
            .over("symbol")
            .alias("corwin_schultz_spread_pct")
        ])

        self._daily_df = daily
        return self._daily_df

    def get_symbols(self) -> List[str]:
        df = self.load_daily_eod_data()
        return df.select(pl.col("symbol").unique()).to_series().to_list()

    def get_trading_dates(self) -> List[date]:
        df = self.load_daily_eod_data()
        return sorted(df.select(pl.col("trade_date").unique()).to_series().to_list())

    def get_snapshot_as_of(self, as_of_date: date) -> pl.DataFrame:
        """
        Point-in-Time slice: guarantees no future bars beyond as_of_date are visible.
        """
        df = self.load_daily_eod_data()
        return df.filter(pl.col("trade_date") <= as_of_date)

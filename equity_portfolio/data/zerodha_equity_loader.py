"""
Zerodha Kite Ingestion Engine for 1,000+ NSE Cash Equities
Requirements EQ4, EQ5, EQ6
"""

import os
import time
import logging
from datetime import datetime, timedelta, date
from typing import List, Dict, Any, Optional
import polars as pl
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("ZerodhaEquityLoader")

class ZerodhaEquityLoader:
    """
    Ingests 1,000+ NSE Cash Equities (segment='NSE', instrument_type='EQ')
    via Zerodha KiteConnect Historical API.
    Saves and updates data/equity_universe_daily.parquet.
    """
    def __init__(
        self,
        api_key: Optional[str] = None,
        access_token: Optional[str] = None,
        parquet_path: str = "data/equity_universe_daily.parquet"
    ):
        self.api_key = (api_key or os.getenv("KITE_API_KEY", "")).strip("'\"")
        self.access_token = (access_token or os.getenv("KITE_ACCESS_TOKEN", "")).strip("'\"")
        self.parquet_path = parquet_path
        self._kite = None

    @property
    def kite(self):
        if self._kite is None:
            try:
                from kiteconnect import KiteConnect
                self._kite = KiteConnect(api_key=self.api_key)
                self._kite.set_access_token(self.access_token)
            except Exception as e:
                logger.warning(f"Could not initialize KiteConnect client: {e}")
                self._kite = None
        return self._kite

    @staticmethod
    def filter_cash_equities(instruments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Pure deterministic filter for NSE cash equities.
        Filters out derivatives (FUT/CE/PE), debt/bonds, and non-NSE segments.
        """
        return [
            {
                "symbol": inst["tradingsymbol"],
                "instrument_token": inst["instrument_token"],
                "name": inst.get("name", inst["tradingsymbol"]),
                "segment": inst["segment"],
                "instrument_type": inst["instrument_type"]
            }
            for inst in instruments
            if inst.get("segment") == "NSE" and inst.get("instrument_type") == "EQ"
        ]

    def fetch_nse_cash_instruments(self, min_market_cap_rank: int = 1000) -> List[Dict[str, Any]]:
        """
        Discovers all cash equity instruments on NSE (EQ segment).
        Returns list of {symbol, instrument_token, name}.
        """
        if not self.kite:
            logger.warning("Kite client not initialized. Cannot fetch instruments from live API.")
            return []

        try:
            instruments = self.kite.instruments("NSE")
            cash_equities = self.filter_cash_equities(instruments)
            logger.info(f"Discovered {len(cash_equities)} NSE cash equity instruments (EQ).")
            return cash_equities[:min_market_cap_rank]
        except Exception as e:
            logger.error(f"Error fetching NSE instruments: {e}")
            return []

    def fetch_historical_daily_bars(
        self,
        instrument_token: int,
        symbol: str,
        from_date: date,
        to_date: date
    ) -> List[Dict[str, Any]]:
        """
        Fetches daily historical EOD candles for a single equity scrip from Zerodha.
        """
        if not self.kite:
            return []

        try:
            records = self.kite.historical_data(
                instrument_token=instrument_token,
                from_date=from_date.strftime("%Y-%m-%d"),
                to_date=to_date.strftime("%Y-%m-%d"),
                interval="day"
            )
            # Add symbol tag
            for r in records:
                r["symbol"] = symbol
            return records
        except Exception as e:
            logger.warning(f"Failed to fetch daily bars for {symbol} ({instrument_token}): {e}")
            return []

    def sync_universe_to_parquet(
        self,
        lookback_days: int = 120,
        max_symbols: int = 1000,
        rate_limit_delay_sec: float = 0.35 # Kite API allows 3 req/sec
    ) -> pl.DataFrame:
        """
        Iterates over 1,000+ NSE cash equities, downloads historical daily data,
        and saves incrementally to parquet.
        """
        instruments = self.fetch_nse_cash_instruments(min_market_cap_rank=max_symbols)
        if not instruments:
            logger.warning("No instruments available. Returning existing parquet or empty DataFrame.")
            if os.path.exists(self.parquet_path):
                return pl.read_parquet(self.parquet_path)
            return pl.DataFrame()

        to_date = date.today()
        from_date = to_date - timedelta(days=lookback_days)

        all_bars = []
        logger.info(f"Downloading daily bars for {len(instruments)} NSE cash equities from {from_date} to {to_date}...")

        for idx, inst in enumerate(instruments, 1):
            sym = inst["symbol"]
            tok = inst["instrument_token"]
            bars = self.fetch_historical_daily_bars(tok, sym, from_date, to_date)
            all_bars.extend(bars)
            time.sleep(rate_limit_delay_sec)
            if idx % 50 == 0:
                logger.info(f"Progress: {idx}/{len(instruments)} symbols ingested...")

        if not all_bars:
            logger.warning("No historical bars retrieved.")
            return pl.DataFrame()

        df_new = pl.DataFrame(all_bars)
        # Standardize schema: symbol, time, open, high, low, close, volume
        df_new = df_new.select([
            pl.col("symbol").cast(pl.Utf8),
            pl.col("date").alias("time").cast(pl.Datetime),
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Int64)
        ])

        if os.path.exists(self.parquet_path):
            existing_df = pl.read_parquet(self.parquet_path)
            combined = pl.concat([existing_df, df_new]).unique(subset=["symbol", "time"]).sort(["symbol", "time"])
        else:
            combined = df_new.sort(["symbol", "time"])

        os.makedirs(os.path.dirname(self.parquet_path) or ".", exist_ok=True)
        combined.write_parquet(self.parquet_path)
        logger.info(f"Successfully saved {len(combined)} daily records across {combined['symbol'].n_unique()} symbols to {self.parquet_path}")
        return combined

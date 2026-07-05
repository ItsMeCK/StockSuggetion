import os
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import logging
import json
import csv
import email.utils
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List
from openai import OpenAI
from core.state import SovereignState
from langsmith import wrappers
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class NewsCatalystAgent:
    """
    Scrapes Google News RSS for candidate stocks and runs an OpenAI audit
    to identify upcoming or recent high-impact corporate news catalysts.
    Integrates headline recency auditing and compares news to real-time price reaction
    to filter out old/consumed news or negative consensus shocks.
    """
    def __init__(self, master_universe_path: str = "pipeline/master_universe.csv"):
        self.client = wrappers.wrap_openai(OpenAI(api_key=os.getenv("OPENAI_API_KEY")))
        default_model = "gpt-4o-mini" if os.getenv("TRADING_MODE") == "HISTORICAL" else "gpt-4o"
        self.model = os.getenv("OPENAI_MODEL", default_model)
        self.company_map = self._load_company_map(master_universe_path)
        self.cache_path = Path(__file__).parent.parent / "news_catalyst_cache.json"
        self.cache = self._load_cache()

    def _load_cache(self) -> Dict[str, Any]:
        if self.cache_path.exists():
            try:
                with open(self.cache_path, 'r') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_cache(self):
        try:
            with open(self.cache_path, 'w') as f:
                json.dump(self.cache, f, indent=4)
        except Exception as e:
            logging.error(f"Failed to save news catalyst cache: {e}")

    def _load_company_map(self, path: str) -> Dict[str, str]:
        company_map = {}
        if not os.path.exists(path):
            logging.warning(f"Master universe file not found at {path}, using symbols only.")
            return company_map
        try:
            with open(path, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    symbol = row.get("Symbol")
                    company_name = row.get("Company Name")
                    if symbol and company_name:
                        # Clean up suffixes like "Ltd." or "Enterprise" to improve query matching
                        clean_name = company_name.replace("Ltd.", "").replace("Limited", "").replace("Enterprise", "").strip()
                        company_map[symbol] = clean_name
        except Exception as e:
            logging.error(f"Error loading company names from {path}: {e}")
        return company_map

    def fetch_recent_headlines(self, symbol: str, target_date: str = None) -> List[Dict[str, Any]]:
        """
        Queries Google News RSS feed for the symbol and clean company name.
        Returns a list of dictionaries with 'title' and 'age_hours' relative to reference datetime.
        """
        company_name = self.company_map.get(symbol, "")
        query = f'"{symbol}" stock OR "{symbol}" share'
        if company_name:
            query = f'("{symbol}" OR "{company_name}") stock news'

        # Reference datetime for age calculations and preventing look-ahead bias
        from datetime import timedelta
        if target_date:
            try:
                ref_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                after_str = (ref_dt - timedelta(days=7)).strftime("%Y-%m-%d")
                before_str = (ref_dt + timedelta(days=1)).strftime("%Y-%m-%d")
                query += f" after:{after_str} before:{before_str}"
            except Exception:
                ref_dt = datetime.now(timezone.utc)
        else:
            ref_dt = datetime.now(timezone.utc)

        logging.info(f"Fetching Google News for {symbol} using query: {query} (ref: {ref_dt.date()})")
        try:
            encoded_query = urllib.parse.quote(query)
            # HL=en-IN, GL=IN, CEID=IN:en for Indian markets news focus
            url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-IN&gl=IN&ceid=IN:en"
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=10) as response:
                xml_data = response.read()
            
            root = ET.fromstring(xml_data)
            items = root.findall('.//item')
            headlines = []
            for item in items:
                title = item.find('title')
                pub_date_node = item.find('pubDate')
                
                if title is not None and title.text and pub_date_node is not None and pub_date_node.text:
                    try:
                        pub_dt = email.utils.parsedate_to_datetime(pub_date_node.text)
                        age_hours = (ref_dt - pub_dt).total_seconds() / 3600.0
                    except Exception:
                        age_hours = 0.0 # fallback

                    # Prevent look-ahead bias: ignore articles published after our target simulation date
                    if target_date and age_hours < -24.0:
                        continue
                        
                    headlines.append({
                        "title": title.text,
                        "age_hours": round(max(0.0, age_hours), 1)
                    })
                    
            return headlines[:10]  # Return top 10 relevant headlines
        except Exception as e:
            logging.error(f"Failed to fetch news for {symbol}: {e}")
            return []

    def fetch_technical_context(self, symbol: str, target_date: str = None) -> Dict[str, Any]:
        """
        Fetches the recent 20 daily candles for a stock from Postgres database
        to calculate real-time momentum metrics (ROC, volume ratio, close range).
        """
        import psycopg2
        import polars as pl
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "quant"),
                password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                database=os.getenv("POSTGRES_DB", "market_data")
            )
            date_filter = f"AND time <= '{target_date} 23:59:59+00'" if target_date else ""
            query = f"""
                SELECT time, close, volume, high, low 
                FROM daily_ohlcv 
                WHERE symbol = '{symbol}' {date_filter}
                ORDER BY time DESC
                LIMIT 20
            """
            df = pl.read_database(query, conn)
            conn.close()
            
            if len(df) < 2:
                return {}
                
            df = df.sort("time")
            latest = df.tail(1).to_dicts()[0]
            prev = df.tail(2).head(1).to_dicts()[0]
            
            today_close = latest["close"]
            prev_close = prev["close"]
            today_high = latest["high"]
            today_low = latest["low"]
            today_volume = latest["volume"]
            
            vol_avg_20 = df["volume"].mean()
            
            price_change_pct = ((today_close - prev_close) / prev_close) * 100 if prev_close else 0.0
            volume_ratio = today_volume / vol_avg_20 if vol_avg_20 else 1.0
            close_range_pct = (today_close - today_low) / (today_high - today_low) if today_high > today_low else 1.0
            
            return {
                "price_change_pct": round(price_change_pct, 2),
                "volume_ratio": round(volume_ratio, 2),
                "close_range_pct": round(close_range_pct, 2)
            }
        except Exception as e:
            logging.error(f"Error fetching tech context for news agent on {symbol}: {e}")
            return {}

    def evaluate_news_catalysts(self, symbol: str, target_date: str = None, bypass_prefilter: bool = False) -> Dict[str, Any]:
        """
        Downloads headlines, fetches technical metrics context, and evaluates
        priced-in / expectation risks using OpenAI.
        """
        date_str = target_date if target_date else datetime.now(timezone.utc).strftime("%Y-%m-%d")
        cache_key = f"{symbol}_{date_str}"
        if cache_key in self.cache:
            logging.info(f"NEWS CACHE HIT: Retrieving catalyst evaluation for {symbol} on {date_str}")
            return self.cache[cache_key]

        tech_ctx = self.fetch_technical_context(symbol, target_date)
        if tech_ctx and not bypass_prefilter:
            price_change = abs(tech_ctx.get("price_change_pct", 0.0))
            vol_ratio = tech_ctx.get("volume_ratio", 1.0)
            if price_change < 1.5 and vol_ratio < 1.2:
                logging.info(f"Pre-filter triggered for {symbol}: ROC={price_change}%, VolRatio={vol_ratio}x. Skipping news catalyst search.")
                result = {
                    "catalyst_detected": False,
                    "catalyst_type": "NONE",
                    "summary": "Skipped news search (no significant daily price/volume breakout).",
                    "score_boost": 0.0
                }
                self.cache[cache_key] = result
                self._save_cache()
                return result

        headlines = self.fetch_recent_headlines(symbol, target_date)
        if not headlines:
            result = {
                "catalyst_detected": False,
                "catalyst_type": "NONE",
                "summary": "No recent headlines found.",
                "score_boost": 0.0
            }
            self.cache[cache_key] = result
            self._save_cache()
            return result

        tech_text = ""
        if tech_ctx:
            tech_text = f"""
            --- TODAY'S MARKET REACTION ---
            Price Change Today: {tech_ctx.get('price_change_pct')}%
            Volume Ratio (vs 20d avg): {tech_ctx.get('volume_ratio')}x
            Close Range (0=low, 1=high): {tech_ctx.get('close_range_pct')}
            """

        headlines_text = "\n".join([f"- [Age: {h['age_hours']} hours ago] Headline: {h['title']}" for h in headlines])
        prompt = f"""
        Analyze the following recent news headlines and the today's price reaction for the Indian stock {symbol}.
        Your goal is to detect if there is a high-impact corporate catalyst that occurred recently, or is officially scheduled in the next 7 days.
        
        CATALYSTS OF INTEREST:
        1. EARNINGS_BEAT: Q4/Q3/Q2/Q1 earnings release showing strong profit/revenue growth.
        2. DIVIDEND: Final or interim dividend declaration.
        3. BOARD_MEETING: Upcoming board meeting scheduled to discuss earnings/dividends/buyback.
        4. ORDER_WIN: Massive new contract, order win, or government project.
        5. CORPORATE_ACTION: Merger, acquisition, stock split, or bonus shares.
        6. REGULATORY: FDA approvals, patent grants, or positive regulatory clearances.

        --- NEWS HEADLINES ---
        {headlines_text}
        {tech_text}
        
        CRITICAL ANALYSIS CHECKS:
        1. EXPECTATIONS RISK: Did the news actually meet/beat market expectations? If a headline indicates a "miss", "profit drop", or "lower margins", or if the positive news is described as "below estimates", tag as missed expectations.
        2. PRICED-IN RISK: Is the news already fully consumed? If the news is >24-48 hours old and the stock has already run up, it might be priced in.
        3. DIVERGENT PRICE REACTION: Compare the news sentiment with today's price action. If the news is positive but today's price is down (negative change), it indicates a "sell the news" reaction or a miss. Do not approve/boost.

        Return a JSON object:
        - "catalyst_detected": boolean (true/false)
        - "catalyst_type": one of [EARNINGS_BEAT, DIVIDEND, BOARD_MEETING, ORDER_WIN, CORPORATE_ACTION, REGULATORY, NONE]
        - "summary": A 1-sentence summary of the catalyst.
        - "meets_expectations": "EXCEEDED | MET | MISSED | UNKNOWN"
        - "priced_in_status": "PRICED_IN | CONSUMED | FRESH_REACTION | UNKNOWN"
        - "score_boost": float (from 0.0 to 20.0, indicating the strength of the catalyst. Capped at 20.0. Reduce to 0.0 or negative if news is priced-in, missed expectations, or has divergent reaction)
        - "reasoning": A brief explanation of expectations, recency, and reaction alignment.
        """

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                timeout=30.0
            )
            result = json.loads(response.choices[0].message.content)
            logging.info(f"News catalyst evaluation for {symbol}: {result}")
            self.cache[cache_key] = result
            self._save_cache()
            return result
        except Exception as e:
            logging.error(f"OpenAI error during news catalyst analysis for {symbol}: {e}")
            raise RuntimeError(f"OpenAI News Catalyst failed: {e}") from e

def run_news_catalyst_node(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node. Executes News Catalyst analysis for candidates.

    Note: this runs *before* critic_agent in the graph (critic_agent reads
    news_catalysts to compute its own catalyst_boost score), so we can't gate
    on critic approval here without breaking that dependency. Instead we gate
    on the same meta-gate veto list pattern_agent already uses, plus the DTW
    heuristic's requires_vision_validation flag - both already computed
    upstream at zero extra cost - to shrink the set of symbols that reach the
    paid OpenAI call.
    """
    candidates = state.get("candidates", [])
    experience_warnings = state.get("experience_warnings", {})
    heuristic_flags = state.get("heuristic_flags", {})
    target_date = state.get("target_date")
    news_catalysts = {}

    agent = NewsCatalystAgent()
    for symbol in candidates:
        if symbol in experience_warnings and len(experience_warnings[symbol]) > 0:
            logging.info(f"Skipping News Catalyst analysis for {symbol} due to Meta-Gate VETO.")
            continue

        if not heuristic_flags.get(symbol, {}).get("requires_vision_validation", False):
            logging.info(f"Skipping News Catalyst analysis for {symbol}: DTW heuristic found no qualifying pattern.")
            continue

        result = agent.evaluate_news_catalysts(symbol, target_date)
        if result.get("catalyst_detected", False):
            news_catalysts[symbol] = result

    return {"news_catalysts": news_catalysts}

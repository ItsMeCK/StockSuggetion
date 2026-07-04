import os
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import logging
import json
import csv
import email.utils
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List
from openai import OpenAI
from langsmith import wrappers

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SectorMacroAgent:
    """
    AI-driven Sectoral/Macro Link Agent.
    Scrapes broader Indian sector and macro policy news, uses LLM (OpenAI) to analyze
    if there's a high-impact catalyst, and maps that catalyst to beneficiary symbols 
    from the master universe.
    """
    def __init__(self, master_universe_path: str = "pipeline/master_universe.csv"):
        self.client = wrappers.wrap_openai(OpenAI(api_key=os.getenv("OPENAI_API_KEY")))
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o")
        self.master_universe_path = master_universe_path
        self.symbols = self._load_universe_symbols()

    def _load_universe_symbols(self) -> List[str]:
        symbols = []
        if not os.path.exists(self.master_universe_path):
            logging.warning(f"Master universe file not found at {self.master_universe_path}")
            return symbols
        try:
            with open(self.master_universe_path, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    symbol = row.get("Symbol")
                    if symbol:
                        symbols.append(symbol)
        except Exception as e:
            logging.error(f"Error loading universe symbols: {e}")
        return symbols

    def fetch_sector_headlines(self, query: str, target_date: str = None, lookback_days: int = 5, max_results: int = 15) -> List[str]:
        """
        Fetches Google News RSS feed for the given macro/sectoral query.
        """
        ref_dt = datetime.now(timezone.utc)
        if target_date:
            try:
                ref_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                after_str = (ref_dt - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
                before_str = (ref_dt + timedelta(days=1)).strftime("%Y-%m-%d")
                query += f" after:{after_str} before:{before_str}"
            except Exception:
                pass

        logging.info(f"SectorMacroAgent: Querying Google News for: {query}")
        try:
            encoded_query = urllib.parse.quote(query)
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
                        age_hours = 0.0
                        
                    if target_date and age_hours < -24.0:
                        continue
                    
                    headlines.append(title.text)
            return headlines[:max_results]
        except Exception as e:
            logging.error(f"SectorMacroAgent: Failed to fetch sector news for '{query}': {e}")
            return []

    def identify_macro_catalysts(self, target_date: str = None) -> List[str]:
        """
        Scans news for general sectors, calls LLM to map catalysts to universe symbols.
        """
        queries = [
            "Indian railway wagon tender order",
            "Indian defense orders exports sector",
            "Indian solar green energy subsidy PLI",
            "power transmission asset acquisition India"
        ]
        
        all_headlines = []
        for q in queries:
            headlines = self.fetch_sector_headlines(q, target_date)
            all_headlines.extend(headlines)
            
        if not all_headlines:
            return []
            
        headlines_text = "\n".join([f"- {h}" for h in list(set(all_headlines))])
        
        prompt = f"""
        Analyze the following Indian stock market news headlines to identify high-impact sectoral or macro catalysts 
        (e.g., massive government tenders like wagon procurements, major PLI schemes, or regulatory shifts).
        
        If you find a catalyst, determine which specific stock symbols from our universe are the direct beneficiaries.
        
        --- UNIVERSE OF SYMBOLS ---
        {", ".join(self.symbols[:150])} ... [Additional standard NSE symbols include: TITAGARH, JWL, ARE&M, EMMVEE, HFCL, ADANIPOWER, WIPRO, VBL, ONGC, HINDCOPPER]
        
        --- SECTOR/MACRO HEADLINES ---
        {headlines_text}
        
        Return a JSON object:
        - "catalysts": A list of objects, each containing:
          - "theme": A brief description of the catalyst theme (e.g. "Railway Wagon Tender")
          - "beneficiary_symbols": A list of uppercase NSE symbols from the universe that benefit (e.g. ["TITAGARH", "JWL"])
          - "reasoning": 1 sentence explaining why they benefit.
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            result = json.loads(response.choices[0].message.content)
            logging.info(f"SectorMacroAgent catalyst detection: {result}")
            
            injected_symbols = []
            for cat in result.get("catalysts", []):
                syms = cat.get("beneficiary_symbols", [])
                logging.info(f"🚀 SectorMacroAgent: Detected catalyst '{cat.get('theme')}' mapping to: {syms}")
                injected_symbols.extend(syms)
                
            # Filter to ensure they are valid universe symbols (or known ones)
            valid_symbols = [s for s in injected_symbols if s in self.symbols or s in ["TITAGARH", "JWL", "ARE&M", "EMMVEE", "HFCL", "ADANIPOWER"]]
            return list(set(valid_symbols))
        except Exception as e:
            logging.error(f"SectorMacroAgent: OpenAI error during analysis: {e}")
            return []

    def identify_upcoming_board_meetings(self, target_date: str = None) -> List[str]:
        """
        Scrapes news for upcoming board meetings (earnings/dividends) and extracts symbols.
        """
        # Run a general query to find all scheduled board meetings
        general_query = '("board meeting to consider" OR "results date" OR "earnings board meeting" OR "dividend") stock India'
        general_headlines = self.fetch_sector_headlines(general_query, target_date, lookback_days=14, max_results=80)
        
        watchlist_symbols = ["CUMMINSIND", "WIPRO", "VBL", "INDIGO", "COALINDIA", "SAIL", "VEDL", "RELIANCE", "HDFCBANK", "SBIN", "TITAGARH", "JWL", "AXISBANK", "EXIDEIND", "CGPOWER", "POWERINDIA", "DALBHARAT", "SUZLON"]
        
        # Query Google News RSS in parallel for each watchlist symbol to bypass search query complexity limits
        import concurrent.futures
        
        def fetch_for_symbol(symbol):
            friendly_names = {
                "CUMMINSIND": '("CUMMINSIND" OR "Cummins India")',
                "WIPRO": '("WIPRO" OR "Wipro")',
                "VBL": '("VBL" OR "Varun Beverages")',
                "INDIGO": '("INDIGO" OR "InterGlobe Aviation" OR "IndiGo")',
                "COALINDIA": '("COALINDIA" OR "Coal India")',
                "SAIL": '("SAIL" OR "Steel Authority")',
                "VEDL": '("VEDL" OR "Vedanta")',
                "RELIANCE": '("RELIANCE" OR "Reliance")',
                "HDFCBANK": '("HDFCBANK" OR "HDFC Bank")',
                "SBIN": '("SBIN" OR "State Bank of India" OR "SBI")',
                "TITAGARH": '("TITAGARH" OR "Titagarh Rail")',
                "JWL": '("JWL" OR "Jupiter Wagons")',
                "AXISBANK": '("AXISBANK" OR "Axis Bank")',
                "EXIDEIND": '("EXIDEIND" OR "Exide Industries")',
                "CGPOWER": '("CGPOWER" OR "CG Power")',
                "POWERINDIA": '("POWERINDIA" OR "Hitachi Energy")',
                "DALBHARAT": '("DALBHARAT" OR "Dalmia Bharat")',
                "SUZLON": '("SUZLON" OR "Suzlon Energy")'
            }
            term = friendly_names.get(symbol, f'"{symbol}"')
            query = f'{term} AND ("board meeting" OR "results" OR "earnings" OR "dividend" OR "conference call")'
            return self.fetch_sector_headlines(query, target_date, lookback_days=14, max_results=30)
            
        targeted_headlines = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(fetch_for_symbol, sym) for sym in watchlist_symbols]
            for future in concurrent.futures.as_completed(futures):
                try:
                    targeted_headlines.extend(future.result())
                except Exception as e:
                    logging.error(f"Error fetching board meeting news: {e}")
        
        # Merge and deduplicate headlines
        headlines = list(set(general_headlines + targeted_headlines))
        
        # Filter for relevant corporate/earnings keywords to reduce noise
        relevant_keywords = ["board meeting", "results", "dividend", "earnings", "performance", "q4", "financials", "quarterly", "call", "payout", "ex-date", "book closure", "consider"]
        filtered_headlines = []
        for h in headlines:
            h_lower = h.lower()
            if any(kw in h_lower for kw in relevant_keywords):
                filtered_headlines.append(h)
                
        headlines = filtered_headlines
        
        if not headlines:
            return []
            
        headlines_text = "\n".join([f"- {h}" for h in headlines])
        
        prompt = f"""
        Analyze the following Indian stock market news headlines to identify companies that have officially scheduled 
        an upcoming Board Meeting (typically to consider financial results, earnings, or dividends, or scheduled results conference call).
        
        Extract the uppercase NSE stock symbol for each identified company.
        
        --- UNIVERSE OF SYMBOLS ---
        {", ".join(self.symbols[:150])} ... [Additional standard NSE symbols include: TITAGARH, JWL, ARE&M, EMMVEE, HFCL, ADANIPOWER, WIPRO, VBL, ONGC, HINDCOPPER, CUMMINSIND, EXIDEIND, CGPOWER]
        
        --- NEWS HEADLINES ---
        {headlines_text}
        
        CRITICAL RULES:
        1. Be extremely thorough and precise. Do not miss any company that has an upcoming results announcement, board meeting, or results conference call.
        2. Specifically, look for companies in the headlines scheduled for results or meetings this week or next few days (e.g. Cummins India / CUMMINSIND, Wipro / WIPRO, etc.).
        3. Make sure to map each company to its correct uppercase NSE symbol (e.g. "Cummins India" to "CUMMINSIND", "Varun Beverages" to "VBL", "Titagarh Rail" to "TITAGARH", "Jupiter Wagons" to "JWL", "State Bank of India" to "SBIN", "Steel Authority of India" to "SAIL"). If not sure about the exact symbol, look it up in the UNIVERSE OF SYMBOLS or map it logically.
        
        Return a JSON object:
        - "meetings": A list of objects, each containing:
          - "company": The name of the company
          - "symbol": The uppercase NSE symbol from the universe (e.g. "CUMMINSIND")
          - "event_date": The scheduled date of the board meeting or results conference call if mentioned, or "UNKNOWN"
          - "catalyst_type": "EARNINGS" or "DIVIDEND" or "CORPORATE_ACTION"
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.0
            )
            result = json.loads(response.choices[0].message.content)
            logging.info(f"SectorMacroAgent board meeting calendar parse: {result}")
            
            injected_symbols = []
            for meeting in result.get("meetings", []):
                sym = meeting.get("symbol")
                if sym and sym != "UNKNOWN":
                    logging.info(f"📅 Corporate Calendar: Detected scheduled board meeting for {sym} | Event: {meeting.get('catalyst_type')}")
                    injected_symbols.append(sym)
                    
            # Filter to ensure they are valid universe symbols (or known ones)
            valid_symbols = [s for s in injected_symbols if s in self.symbols or s in ["TITAGARH", "JWL", "ARE&M", "EMMVEE", "HFCL", "ADANIPOWER", "CUMMINSIND", "EXIDEIND", "CGPOWER"]]
            return list(set(valid_symbols))
        except Exception as e:
            logging.error(f"SectorMacroAgent: OpenAI error during calendar parse: {e}")
            return []

if __name__ == "__main__":
    agent = SectorMacroAgent()
    res = agent.identify_macro_catalysts(target_date="2026-05-22")
    print("Identified Sectoral/Macro Injections:", res)

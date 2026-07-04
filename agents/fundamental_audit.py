import logging
import os
from typing import Dict, Any, List
from openai import OpenAI
from core.state import SovereignState
from langsmith import wrappers

class FundamentalAuditAgent:
    """
    Synthesizes web search results (news/fraud) using OpenAI to provide 
    a final fundamental grade and narrative for a stock.
    """
    def __init__(self):
        from pathlib import Path
        self.client = wrappers.wrap_openai(OpenAI(api_key=os.getenv("OPENAI_API_KEY")))
        self.model = os.getenv("OPENAI_MODEL", "gpt-4o")
        self.cache_path = Path(__file__).parent.parent / "audit_cache.json"
        self.cache = self._load_cache()

    def _load_cache(self) -> Dict[str, Any]:
        if self.cache_path.exists():
            try:
                import json
                with open(self.cache_path, 'r') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_cache(self):
        try:
            import json
            with open(self.cache_path, 'w') as f:
                json.dump(self.cache, f, indent=4)
        except Exception as e:
            logging.error(f"Failed to save audit cache: {e}")

    def analyze_sovereign_setup(self, symbol: str, news_data: str, fraud_data: str, tech_metrics: Dict[str, Any], target_date: str = None) -> Dict[str, Any]:
        import datetime
        date_str = target_date if target_date else datetime.datetime.now().strftime("%Y-%m-%d")
        cache_key = f"{symbol}_{date_str}"
        
        if cache_key in self.cache:
            logging.info(f"AUDIT CACHE HIT: Retrieving fundamental report for {symbol} on {date_str}")
            return self.cache[cache_key]
            
        logging.info(f"PERFORMING RUTHLESS SOVEREIGN AUDIT FOR {symbol}...")
        
        prompt = f"""
        Analyze the following data for the Indian stock {symbol}. 
        You are a Ruthless Hedge Fund Auditor. Your goal is to VETO any setup that has even a hint of weakness.
        
        --- TECHNICAL RIGOR DATA ---
        Distance to Overhead Resistance: {tech_metrics.get('dist_to_high', 0):.1f}%
        Momentum Velocity: {tech_metrics.get('velocity', 0):.2f}
        Current Phase: {tech_metrics.get('phase', 'Unknown')}
        
        --- FUNDAMENTAL/NEWS DATA ---
        {news_data}
        
        --- FRAUD/AUDIT DATA ---
        {fraud_data}
        
        CRITERIA FOR VETO:
        1. OVERHEAD WALL: If Distance to Resistance is < 5%, it is a 'Trap'. VETO.
        2. RSI DIVERGENCE: If Momentum Velocity is Negative while Price is high, it is a 'Divergence'. VETO.
        3. STATIC ACCUMULATION: If the phase is 'Static' and not 'Ignition', downgrade the grade.
        4. FRAUD/AUDIT: Any auditor resignation or SEBI fine is an immediate F.

        Provide your analysis in JSON format:
        - "narrative": A 1-sentence summary of the 'Truth' of this stock.
        - "red_flags": List any Technical or Fundamental red flags.
        - "grade": A letter grade (A, B, C, D, or F).
        - "sentiment": POSITIVE, NEUTRAL, or NEGATIVE.
        - "action": 'DEPLOY', 'ACCUMULATE', or 'AVOID'.
        """
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                timeout=30.0
            )
            import json
            result = json.loads(response.choices[0].message.content)
            
            # Save to cache
            self.cache[cache_key] = result
            self._save_cache()
            return result
        except Exception as e:
            logging.error(f"Fundamental Audit AI Error for {symbol}: {e}")
            raise RuntimeError(f"OpenAI Fundamental Audit failed: {e}") from e

def load_company_map(path: str = "pipeline/master_universe.csv") -> Dict[str, str]:
    import csv
    company_map = {}
    if not os.path.exists(path):
        return company_map
    try:
        with open(path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = row.get("Symbol")
                company_name = row.get("Company Name")
                if symbol and company_name:
                    clean_name = company_name.replace("Ltd.", "").replace("Limited", "").replace("Enterprise", "").strip()
                    company_map[symbol] = clean_name
    except Exception as e:
        logging.error(f"Error loading company names: {e}")
    return company_map

def fetch_rss_headlines(query: str) -> str:
    import urllib.request
    import urllib.parse
    import xml.etree.ElementTree as ET
    try:
        encoded_query = urllib.parse.quote(query)
        url = f"https://news.google.com/rss/search?q={encoded_query}&hl=en-IN&gl=IN&ceid=IN:en"
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=10) as response:
            xml_data = response.read()
        
        root = ET.fromstring(xml_data)
        items = root.findall('.//item')
        headlines = []
        for item in items[:10]:
            title = item.find('title')
            if title is not None and title.text:
                headlines.append(title.text)
        return "\n".join(headlines) if headlines else "No headlines found."
    except Exception as e:
        return f"Failed to fetch headlines: {e}"

def run_fundamental_audit_node(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node for Fundamental Audit. 
    Actively fetches news and corporate fraud/audit news and scores setups.
    """
    critic_results = state.get("critic_results", {})
    candidates = [sym for sym, eval_res in critic_results.items() if eval_res.get("approved", False)]
    
    if not candidates:
        candidates = state.get("candidates", [])
        
    if not candidates:
        return {"fundamental_reports": {}}

    company_map = load_company_map()
    agent = FundamentalAuditAgent()
    fundamental_reports = {}
    
    entry_trigger_results = state.get("entry_trigger_results", {})
    heuristic_flags = state.get("heuristic_flags", {})
    
    for symbol in candidates:
        company_name = company_map.get(symbol, symbol)
        
        # 1. Fetch news headlines
        news_query = f'("{symbol}" OR "{company_name}") news'
        news_data = fetch_rss_headlines(news_query)
        
        # 2. Fetch fraud/audit/SEBI regulatory headlines
        fraud_query = f'("{symbol}" OR "{company_name}") corporate governance OR fraud OR auditor OR SEBI OR penalty OR fine'
        fraud_data = fetch_rss_headlines(fraud_query)
        
        # 3. Construct tech metrics
        trigger_info = entry_trigger_results.get(symbol, {})
        pattern_info = heuristic_flags.get(symbol, {})
        
        # Query 50-SMA and 52-week High from Database
        dist_to_high = 10.0
        try:
            import psycopg2
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                user=os.getenv('POSTGRES_USER', 'quant'),
                password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
                dbname=os.getenv('POSTGRES_DB', 'market_data')
            )
            cur = conn.cursor()
            target_date = state.get("target_date")
            if target_date:
                cur.execute("SELECT close FROM daily_ohlcv WHERE symbol = %s AND time::date <= %s ORDER BY time DESC LIMIT 250", (symbol, target_date))
            else:
                cur.execute("SELECT close FROM daily_ohlcv WHERE symbol = %s ORDER BY time DESC LIMIT 250", (symbol,))
            closes = [float(r[0]) for r in cur.fetchall()]
            cur.close()
            conn.close()
            
            if closes:
                current_close = closes[0]
                high_250 = max(closes)
                dist_to_high = ((high_250 - current_close) / current_close) * 100
        except Exception as e:
            logging.error(f"Audit Tech metrics calculation error for {symbol}: {e}")
            
        tech_metrics = {
            "dist_to_high": dist_to_high,
            "velocity": trigger_info.get("entry_score", 50.0),
            "phase": pattern_info.get("identified_pattern", "Unknown")
        }
        
        # Run AI Auditing
        audit_res = agent.analyze_sovereign_setup(symbol, news_data, fraud_data, tech_metrics, target_date)
        logging.info(f"Fundamental Audit for {symbol}: Narrative: {audit_res.get('narrative')} | Grade: {audit_res.get('grade')} | Action: {audit_res.get('action')}")
        fundamental_reports[symbol] = audit_res
        
    return {"fundamental_reports": fundamental_reports}

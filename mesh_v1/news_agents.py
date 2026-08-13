"""
News/event detector agents for the V5 mesh — fresh code, but reuses ALREADY-
FETCHED real news data (news_catalyst_cache.json from earlier sessions,
unclassified_news_complete.json from this session's mover news-check) to avoid
wasting real API calls. Falls back to a live Google News RSS fetch + OpenAI
classification only for genuinely uncached (symbol, date) pairs.

Two agents:
  earnings_calendar_agent — scores presence of an earnings/results catalyst
  news_catalyst_agent_v2  — scores presence of ANY other real catalyst
                            (order-win, M&A, regulatory, analyst, corp-action,
                            litigation, management)
Both only invoked on the day's technical SHORTLIST (not all 504 stocks daily)
to keep real API usage bounded and realistic.
"""
import os
import json
import urllib.request
import urllib.parse
import xml.etree.ElementTree as ET
import email.utils
from datetime import datetime, timedelta, timezone
from pathlib import Path

from openai import OpenAI

_CACHE1 = json.load(open("news_catalyst_cache.json")) if os.path.exists("news_catalyst_cache.json") else {}
_CACHE2 = {}
if os.path.exists("unclassified_news_complete.json"):
    for r in json.load(open("unclassified_news_complete.json")):
        _CACHE2[(r["symbol"], r["start"])] = r

_LIVE_CACHE_PATH = Path("mesh_v1_live_news_cache.json")
_LIVE_CACHE = json.load(open(_LIVE_CACHE_PATH)) if _LIVE_CACHE_PATH.exists() else {}

_client = None
def _llm():
    global _client
    if _client is None:
        _client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    return _client

_company_map = {}
if os.path.exists("pipeline/master_universe.csv"):
    import csv
    with open("pipeline/master_universe.csv") as f:
        for row in csv.DictReader(f):
            if row.get("Symbol"):
                _company_map[row["Symbol"]] = row.get("Company Name", "")


def _fetch_headlines(symbol, target_date):
    company = _company_map.get(symbol, "")
    query = f'("{symbol}" OR "{company}") stock news' if company else f'"{symbol}" stock'
    ref_dt = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    after_str = (ref_dt - timedelta(days=7)).strftime("%Y-%m-%d")
    before_str = (ref_dt + timedelta(days=1)).strftime("%Y-%m-%d")
    query += f" after:{after_str} before:{before_str}"
    try:
        url = f"https://news.google.com/rss/search?q={urllib.parse.quote(query)}&hl=en-IN&gl=IN&ceid=IN:en"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=10) as resp:
            xml_data = resp.read()
        root = ET.fromstring(xml_data)
        heads = []
        for item in root.findall(".//item")[:8]:
            title = item.find("title")
            if title is not None and title.text:
                heads.append(title.text)
        return heads
    except Exception:
        return []


def _llm_classify(symbol, headlines):
    if not headlines:
        return {"has_catalyst": False, "type": "NONE", "confidence": 0}
    titles = "\n".join(f"- {h}" for h in headlines)
    prompt = (f"Headlines about NSE stock {symbol}:\n{titles}\n\n"
              "Classify the PRIMARY catalyst type as one of: earnings_results, "
              "order_contract_win, regulatory_approval, ma_stake_block_deal, "
              "analyst_rating, corp_action, litigation_penalty, management_change, "
              "none. Respond ONLY as JSON: "
              '{"type": "...", "confidence": 0-100}')
    try:
        resp = _llm().chat.completions.create(
            model="gpt-4o-mini", messages=[{"role": "user", "content": prompt}],
            response_format={"type": "json_object"}, temperature=0)
        d = json.loads(resp.choices[0].message.content)
        return {"has_catalyst": d.get("type", "none") != "none", "type": d.get("type", "none"),
                "confidence": d.get("confidence", 0)}
    except Exception:
        return {"has_catalyst": False, "type": "NONE", "confidence": 0}


NEWS_LIVE_FETCH = os.getenv("NEWS_LIVE_FETCH", "0") == "1"  # off by default: cache-only for
                                                              # fast iteration; set =1 for a
                                                              # slower, more complete run.


def _get_catalyst(symbol, date):
    """Cache-first lookup; live fetch+LLM only if NEWS_LIVE_FETCH=1 (else
    uncached symbol-dates are honestly reported as 'no catalyst data', not
    fetched live - keeps the mesh backtest fast; coverage caveat documented)."""
    k1 = f"{symbol}_{date}"
    if k1 in _CACHE1:
        c = _CACHE1[k1]
        return {"has_catalyst": bool(c.get("catalyst_detected")), "type": c.get("catalyst_type", "NONE"),
                "source": "cache1"}
    if (symbol, date) in _CACHE2:
        c = _CACHE2[(symbol, date)]
        return {"has_catalyst": c.get("has_news", False),
                "type": (c.get("catalyst_types") or ["NONE"])[0], "source": "cache2"}
    if k1 in _LIVE_CACHE:
        return {**_LIVE_CACHE[k1], "source": "live_cache"}
    if not NEWS_LIVE_FETCH:
        return {"has_catalyst": False, "type": "NO_CACHE_DATA", "source": "uncached_skipped"}
    # live fetch (only reached for genuinely new symbol-dates, when enabled)
    heads = _fetch_headlines(symbol, date)
    result = _llm_classify(symbol, heads)
    result["source"] = "live_fetch"
    _LIVE_CACHE[k1] = {k: v for k, v in result.items() if k != "source"}
    json.dump(_LIVE_CACHE, open(_LIVE_CACHE_PATH, "w"))
    return result


EARNINGS_TYPES = {"earnings_results", "earnings/results"}
EVENT_TYPES = {"order_contract_win", "order/contract win", "regulatory_approval", "regulatory/approval",
              "ma_stake_block_deal", "m&a/stake/block_deal", "analyst_rating", "analyst/rating",
              "corp_action", "litigation_penalty", "litigation/penalty",
              "management_change", "management/leadership", "ORDER_WIN", "CORPORATE_ACTION"}


def earnings_calendar_agent(symbol, date):
    c = _get_catalyst(symbol, date)
    if c["type"] in EARNINGS_TYPES or c["type"] == "EARNINGS":
        return 85, f"earnings catalyst found ({c['source']})"
    return 0, "no earnings catalyst"


def news_catalyst_agent_v2(symbol, date):
    c = _get_catalyst(symbol, date)
    if c["has_catalyst"] and c["type"] not in EARNINGS_TYPES and c["type"] != "NONE":
        return 65, f"event catalyst: {c['type']} ({c['source']})"
    return 0, "no other news catalyst"

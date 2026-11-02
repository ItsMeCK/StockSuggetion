"""
Google Search Grounding Advisor for Equity Stock Dossiers
Requirements EQ1, EQ12, EQ49
"""

import os
import re
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import Dict, Any, List, Tuple, Optional
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger("GroundingAdvisor")

DETERMINISTIC_TOXIC_KEYWORDS = [
    "sebi order", "sebi ban", "ed raid", "ed search", "forensic audit",
    "promoter pledge call", "debt default", "nclt", "insolvency",
    "accounting fraud", "cbi probe", "credit rating downgrade",
    "qip discount", "dilution", "promoter selling", "auditor resignation"
]

@dataclass
class GroundingFact:
    source: str
    date_str: str
    fact: str

@dataclass
class GroundingAdvice:
    symbol: str
    is_toxic_veto: bool
    conviction_tilt: float      # Bounded +/- 10.0 points
    evidence: List[GroundingFact]
    summary: str
    raw_response: str = ""

class EquityGroundingAdvisor:
    """
    Step 1 & Step 4 Live Intelligence Advisor:
    Uses Gemini with live Google Search Grounding to research:
    1. Regulatory/forensic actions (SEBI, ED, CBI, Auditor, Default)
    2. Corporate catalysts (Major order wins, capacity expansion, earnings guidance)
    3. Binary event risks (Upcoming earnings dates, board meetings)
    
    CRITICAL INVARIANT (EQ1):
    Advises with a bounded +/- 10% tilt. Only deterministic code approves/sizes orders.
    """
    def __init__(self, api_key: Optional[str] = None):
        key = (api_key or os.getenv("GEMINI_API_KEY", "")).strip("'\"")
        self.client = genai.Client(api_key=key) if key else None

    def evaluate_equity(self, symbol: str, company_name: Optional[str] = None) -> GroundingAdvice:
        name_str = f"({company_name})" if company_name else ""
        if not self.client:
            logger.info(f"Gemini client offline / no API key. Skipping live grounding for {symbol}.")
            return GroundingAdvice(
                symbol=symbol,
                is_toxic_veto=False,
                conviction_tilt=0.0,
                evidence=[],
                summary="Grounding API key unconfigured (Neutral 0.0 tilt)"
            )

        prompt = f"""You are an institutional forensic equity intelligence analyst for the National Stock Exchange of India (NSE).
Analyze the company: {symbol} {name_str}.
Search the live web and recent exchange filings for news, regulatory developments, order book wins, and auditor filings from the past 30 days.

Adhere strictly to the EVIDENCE-BEFORE-VERDICT protocol:
- First, extract verifiable dated evidence (source, date, description).
- Check for TOXIC RED FLAGS: SEBI ban, ED probe, CBI investigation, auditor resignation, debt default, NCLT insolvency, or promoter pledge invocation.
- Check for POSITIVE CATALYSTS: Sovereign order wins, capacity expansion, earnings beat, management growth guidance.

Respond ONLY with valid JSON in this exact structure:
{{
  "toxic_flag_detected": false,
  "toxic_rationale": "none",
  "evidence": [
    {{"source": "NSE/BSE/News", "date": "YYYY-MM-DD", "fact": "description"}}
  ],
  "catalyst_strength": 0.0,
  "summary": "1-2 sentence executive summary"
}}"""

        try:
            response = self.client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[{"google_search": {}}],
                    temperature=0.0
                )
            )
            raw = response.text.strip()
            # Clean markdown code fences if present
            if raw.startswith("```json"):
                raw = raw[7:]
            if raw.startswith("```"):
                raw = raw[3:]
            if raw.endswith("```"):
                raw = raw[:-3]

            data = json.loads(raw.strip())
            is_toxic = bool(data.get("toxic_flag_detected", False))
            toxic_rat = data.get("toxic_rationale", "")

            # Deterministic keyword safety net
            if not is_toxic:
                for kw in DETERMINISTIC_TOXIC_KEYWORDS:
                    if kw in raw.lower():
                        is_toxic = True
                        toxic_rat = f"Deterministic keyword matched: '{kw}'"
                        break

            evidence_items = [
                GroundingFact(
                    source=item.get("source", "Web"),
                    date_str=item.get("date", ""),
                    fact=item.get("fact", "")
                )
                for item in data.get("evidence", [])
                if isinstance(item, dict)
            ]

            # Bounded tilt between -10.0 and +10.0
            if is_toxic:
                conviction_tilt = -10.0
            else:
                raw_strength = float(data.get("catalyst_strength", 0.0)) # 0.0 to 1.0
                conviction_tilt = max(-10.0, min(10.0, round((raw_strength - 0.5) * 20.0, 1)))

            return GroundingAdvice(
                symbol=symbol,
                is_toxic_veto=is_toxic,
                conviction_tilt=conviction_tilt,
                evidence=evidence_items,
                summary=data.get("summary", toxic_rat or "Clean research"),
                raw_response=raw
            )

        except Exception as e:
            logger.warning(f"Grounding failed for {symbol}: {e}")
            return GroundingAdvice(
                symbol=symbol,
                is_toxic_veto=False,
                conviction_tilt=0.0,
                evidence=[],
                summary=f"Grounding query error ({e}); Neutral 0.0 tilt"
            )

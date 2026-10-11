import os
import re
import json
from datetime import datetime
from typing import Dict, Any, Tuple, Optional
from google import genai
from google.genai import types

# Frozen Immutable Constants (Cannot be modified by LLM prompt drift)
WEIGHT_PUBLIC_CATALYST = 0.30
WEIGHT_STEALTH_ACCUMULATION = 0.50
WEIGHT_TOXIC_VETO = 1.00
APPROVAL_THRESHOLD = 50.0

DETERMINISTIC_TOXIC_KEYWORDS = [
    "sebi order", "sebi ban", "ed raid", "ed search", "forensic audit",
    "promoter pledge call", "debt default", "nclt", "insolvency",
    "accounting fraud", "cbi probe", "credit rating downgrade",
    "qip discount", "dilution", "promoter selling"
]

class HardenedGroundingAgent:
    """
    Hardened 3D Grounding Intelligence Scout (v3.3.0-PROD)
    Empirically prevents LLM sycophancy, confirmation bias, and post-hoc rationalization:
    1. Evidence Before Verdict (LLM must extract dated facts before confidence).
    2. Structured JSON Output Only (LLM outputs raw confidences; code does the math).
    3. Deterministic Toxic Event Veto (SEBI/ED/Defaults trigger instant programmatic veto).
    4. Adversarial Devil's Advocate Pass (tests counter-thesis before accepting stealth accum).
    5. Temperature 0.0 with audit trail logging to data/grounding_audit.jsonl.
    """
    def __init__(self, client: Optional[genai.Client] = None):
        if client:
            self.client = client
        else:
            api_key = os.getenv("GEMINI_API_KEY", "").strip("'\"")
            self.client = genai.Client(api_key=api_key) if api_key else genai.Client()
        self.audit_log_path = "data/grounding_audit.jsonl"
        os.makedirs("data", exist_ok=True)

    def evaluate_symbol(self, symbol: str) -> Tuple[bool, float, str, Dict[str, Any]]:
        """
        Evaluates candidate stock at 3:15 PM IST.
        Returns: (is_approved, final_score, rationale, structured_audit_data)
        """
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        prompt = f"""You are a forensic institutional equity intelligence agent analyzing the National Stock Exchange of India (NSE).
The stock {symbol} is exhibiting massive unexplained trading volume and price surge at 3:15 PM IST today.

Search the live web and recent exchange filings for news, block deals, rumors, or regulatory updates from today.
You must adhere strictly to the EVIDENCE-BEFORE-VERDICT protocol:
- First, list verifiable, dated evidence for each dimension (sources, dates, facts).
- Only after listing facts, provide your confidence (0.0 to 1.0).

DIMENSION A: Known Public Catalyst (earnings beat, major order win, regulatory approval, merger).
DIMENSION B: Silent Institutional Accumulation (block deals, delivery % surge >60%, institutional co-movement).
DIMENSION C: Toxic Events (SEBI/ED probe, rating downgrade, promoter pledge sell-off, accounting fraud).

You MUST respond ONLY with valid JSON in this exact structure, with no markdown code fences or conversational text:
{{
  "public_catalyst": {{
    "evidence": [{{"source": "name", "date": "YYYY-MM-DD", "fact": "description"}}],
    "confidence": 0.0,
    "summary": "brief summary"
  }},
  "stealth_accumulation": {{
    "evidence": [{{"source": "name", "date": "YYYY-MM-DD", "fact": "description"}}],
    "confidence": 0.0,
    "summary": "brief summary"
  }},
  "toxic_catalyst": {{
    "evidence": [{{"source": "name", "date": "YYYY-MM-DD", "fact": "description"}}],
    "detected": false,
    "summary": "brief summary"
  }}
}}"""

        raw_response = ""
        try:
            response = self.client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[{"google_search": {}}],
                    temperature=0.0
                )
            )
            raw_response = response.text.strip()
            # Clean possible markdown fences
            if raw_response.startswith("```json"):
                raw_response = raw_response[7:]
            if raw_response.startswith("```"):
                raw_response = raw_response[3:]
            if raw_response.endswith("```"):
                raw_response = raw_response[:-3]
            parsed = json.loads(raw_response.strip())
        except Exception as e:
            # Fallback if search returns non-JSON or fails
            print(f"⚠️ Warning: Structured JSON parsing error for {symbol}: {e}")
            parsed = {
                "public_catalyst": {"evidence": [], "confidence": 0.0, "summary": "Parse error"},
                "stealth_accumulation": {"evidence": [], "confidence": 0.0, "summary": "Parse error"},
                "toxic_catalyst": {"evidence": [], "detected": False, "summary": "None"}
            }

        # 1. Deterministic Toxic Event Check with Negation Handling
        toxic_cat = parsed.get("toxic_catalyst", {})
        toxic_flag = self._is_toxic_confirmed(toxic_cat)

        if toxic_flag:
            final_score = 0.0
            verdict = False
            rationale = f"ABSOLUTE_VETO: Toxic regulatory/fraud catalyst detected: {toxic_cat.get('summary', 'Unknown')}"
            self._log_audit(symbol, now_str, raw_response, parsed, final_score, verdict, rationale)
            return verdict, final_score, rationale, parsed

        # 2. Adversarial Devil's Advocate Pass (if stealth accumulation claimed)
        stealth_conf = float(parsed.get("stealth_accumulation", {}).get("confidence", 0.0))
        pub_conf = float(parsed.get("public_catalyst", {}).get("confidence", 0.0))
        rebuttal = None

        if stealth_conf >= 0.50:
            rebuttal = self._run_devils_advocate(symbol, parsed.get("stealth_accumulation", {}).get("summary", ""))
            if "RETAIL_TRAP_CONFIRMED" in rebuttal or "DISTRIBUTION_DETECTED" in rebuttal:
                print(f"🕵️ Adversarial Rebuttal tripped for {symbol}: Discounting stealth confidence by 50%.")
                stealth_conf *= 0.50
                parsed["stealth_accumulation"]["confidence"] = stealth_conf

        # 3. Deterministic Code-Level Score Calculation (Frozen Weights)
        final_score = round((WEIGHT_PUBLIC_CATALYST * (pub_conf * 100)) + (WEIGHT_STEALTH_ACCUMULATION * (stealth_conf * 100)), 2)
        verdict = final_score >= APPROVAL_THRESHOLD

        if verdict:
            catalyst_type = "Public News" if pub_conf >= stealth_conf else "Silent Institutional Accumulation"
            summary_txt = parsed.get("public_catalyst", {}).get("summary") if pub_conf >= stealth_conf else parsed.get("stealth_accumulation", {}).get("summary")
            rationale = f"APPROVED ({catalyst_type}): {summary_txt} (Score: {final_score}/100)"
        else:
            rationale = f"REJECTED: Insufficient verified catalyst or accumulation footprint (Score: {final_score}/100 < {APPROVAL_THRESHOLD})"

        self._log_audit(symbol, now_str, raw_response, parsed, final_score, verdict, rationale, rebuttal)
        return verdict, final_score, rationale, parsed

    def _run_devils_advocate(self, symbol: str, accum_summary: str) -> str:
        """Adversarial check: actively tries to invalidate the accumulation thesis."""
        prompt = f"""You are a skeptical forensic short-seller examining NSE stock {symbol}.
An analyst claims this stock is undergoing silent institutional accumulation today based on: '{accum_summary}'.
Find the counter-thesis: Is this volume a retail bull trap, an operator pump-and-dump, illiquid ramp-up, or insider distribution before bad news?
Respond with exactly one paragraph. If you find credible evidence of a trap or offloading, start with 'RETAIL_TRAP_CONFIRMED:' or 'DISTRIBUTION_DETECTED:'. Otherwise start with 'CLEAN_ACCUMULATION:'."""
        try:
            resp = self.client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(temperature=0.0)
            )
            return resp.text.strip()
        except Exception as e:
            return f"CLEAN_ACCUMULATION: (Adversarial check error: {e})"

    def _is_toxic_confirmed(self, toxic_data: Dict[str, Any]) -> bool:
        """
        Determines if a toxic catalyst is confirmed, accounting for negations.
        E.g. 'No SEBI order found' or 'Company confirmed no SEBI order or ED raid'
        should NOT trigger toxic veto.
        """
        if not toxic_data or not isinstance(toxic_data, dict):
            return False

        if toxic_data.get("detected") is True:
            return True

        summary = str(toxic_data.get("summary", "")).lower()
        evidence_items = [
            str(item.get("fact", "")).lower()
            for item in toxic_data.get("evidence", [])
            if isinstance(item, dict)
        ]
        candidate_texts = [summary] + evidence_items

        contraction_expansions = {
            "didn't": "did not", "didn’t": "did not", "didnt": "did not",
            "wasn't": "was not", "wasn’t": "was not", "wasnt": "was not",
            "hasn't": "has not", "hasn’t": "has not", "hasnt": "has not",
            "haven't": "have not", "haven’t": "have not", "havent": "have not",
            "hadn't": "had not", "hadn’t": "had not", "hadnt": "had not",
            "isn't": "is not", "isn’t": "is not", "isnt": "is not",
            "aren't": "are not", "aren’t": "are not", "arent": "are not",
            "won't": "will not", "won’t": "will not", "wont": "will not",
            "wouldn't": "would not", "wouldn’t": "would not", "wouldnt": "would not",
            "couldn't": "could not", "couldnt": "could not",
            "shouldn't": "should not", "shouldnt": "should not",
            "can't": "cannot", "cant": "cannot",
            "cannot": "can not",
            "don't": "do not", "dont": "do not",
            "doesn't": "does not", "doesnt": "does not",
        }

        multi_word_negations = [
            "ruled out", "no evidence of", "no trace of", "never any",
            "free from", "no such", "not subject to", "no findings of",
            "found no", "no substance to", "without any", "devoid of",
            "dismissed as", "stayed by"
        ]

        negation_words = {
            "no", "not", "none", "neither", "nor", "never", "denied", "denies", "denying",
            "cleared", "clearing", "dismissed", "dismissing", "dismisses",
            "zero", "without", "unfounded", "clean", "false", "untrue", "refuted", "refutes",
            "baseless", "debunked", "disproved", "disproven", "fake", "hoax",
            "unsubstantiated", "cannot", "unproven", "quashed", "absolved", "exonerated",
            "vindicated", "dropped", "stayed"
        }

        clause_separators = [" but ", " however ", " yet ", ";", "."]

        for text in candidate_texts:
            clean_text = text.strip()
            if not clean_text or clean_text in ("none", "n/a", "nil", "false"):
                continue

            for kw in DETERMINISTIC_TOXIC_KEYWORDS:
                if kw in clean_text:
                    # Break text into clauses around adversative conjunctions
                    clauses = [clean_text]
                    for sep in clause_separators:
                        new_clauses = []
                        for cl in clauses:
                            new_clauses.extend(cl.split(sep))
                        clauses = new_clauses

                    # Find which clause contains kw
                    for clause in clauses:
                        if kw in clause:
                            # 1. Normalize curly apostrophes to straight apostrophes
                            c = clause.replace("’", "'").replace("‘", "'")
                            # 2. Check multi-word negation phrases
                            if any(mw in c for mw in multi_word_negations):
                                continue
                            # 3. Expand contractions
                            norm = c
                            for contr, exp in contraction_expansions.items():
                                norm = re.sub(rf"\b{re.escape(contr)}\b", exp, norm)
                            # 4. Tokenize cleanly without stripping apostrophes into isolated letters
                            words = set(re.findall(r"[a-z0-9]+", norm))
                            has_negation = any(neg in words for neg in negation_words)
                            if not has_negation:
                                return True

        return False

    def _log_audit(self, symbol: str, timestamp: str, raw_response: str, parsed: Dict[str, Any], score: float, approved: bool, rationale: str, rebuttal: Optional[str] = None):
        """Persists every audit entry for zero-drift reproducibility."""
        record = {
            "timestamp": timestamp,
            "symbol": symbol,
            "approved": approved,
            "final_score": score,
            "rationale": rationale,
            "parsed_dimensions": parsed,
            "rebuttal": rebuttal,
            "raw_completion": raw_response
        }
        try:
            with open(self.audit_log_path, "a") as f:
                f.write(json.dumps(record) + "\n")
        except Exception as e:
            print(f"Warning: Failed to log audit record to {self.audit_log_path}: {e}")

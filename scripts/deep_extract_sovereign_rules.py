import os
import json
import logging
import fitz  # PyMuPDF
from groq import Groq
from core.config import GROQ_API_KEY, GROQ_MODEL
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DeepRuleExtractor:
    """
    Sovereign Intelligence: Extracts elite trading rules and VETO criteria
    from entire technical books chunk-by-chunk.
    """
    def __init__(self):
        self.client = Groq(api_key=GROQ_API_KEY)
        self.downloads_path = Path.home() / "Downloads"
        self.output_path = Path(__file__).parent.parent / "core" / "context_rules_1.json"
        
        # Initialize the rulebook structure
        self.rulebook = {
            "metadata": {
                "version": "Sovereign-v1.1",
                "extracted_on": "",
                "sources": []
            },
            "disqualification_rules": {}, # "What NOT to buy"
            "pattern_geometries": {},      # "What to buy"
            "stage_transitions": {}        # Brian Shannon logic
        }

    def extract_chunk_rules(self, text: str, source_name: str, chunk_id: int) -> dict:
        logging.info(f"Neural Scan: Chunk {chunk_id} of {source_name}...")
        
        prompt = f"""
        Analyze this text from the trading book: "{source_name}".
        
        Focus on THREE things:
        1. ELITE ENTRY CRITERIA: Exact mathematical/visual setups that signify an institutional breakout.
        2. FORBIDDEN VETO RULES: Specific signs of failure, distribution, or "traps" that mean we MUST NOT BUY (even if it looks like a breakout).
        3. MARKET STAGES (Brian Shannon): Rules for Stage 1 (Accumulation), Stage 2 (Markup), Stage 3 (Distribution), Stage 4 (Markdown). Include the SMA and volume rules for each.
        
        Format the output as a JSON object:
        {{
            "buy_rules": [
                {{
                    "pattern": "Pattern Name",
                    "indicators": "SMA/Volume/RS requirements",
                    "institutional_footprint": "What big money is doing"
                }}
            ],
            "veto_rules": [
                {{
                    "trap_name": "Name of the failure pattern",
                    "warning_signs": ["Sign 1", "Sign 2"],
                    "reason": "Why this setup fails institutional scrutiny"
                }}
            ],
            "market_stages": [
                {{
                    "stage": "Stage 1/2/3/4",
                    "math": "SMA/Slope/Volume rules",
                    "action": "Hold/Buy/Sell/Avoid"
                }}
            ]
        }}
        
        TEXT:
        {text}
        """
        
        try:
            response = self.client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system", "content": "You are a Quantitative Strategist at a top-tier Sovereign Wealth Fund. Your goal is to extract rigid, non-discretionary trading rules."},
                    {"role": "user", "content": prompt}
                ],
                response_format={ "type": "json_object" }
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            logging.error(f"Chunk Scan failed: {e}")
            return {}

    def process_book(self, filename: str, display_name: str):
        pdf_path = self.downloads_path / filename
        if not pdf_path.exists():
            logging.error(f"Book not found: {pdf_path}")
            return
            
        logging.info(f"OPENING BOOK: {display_name}...")
        doc = fitz.open(pdf_path)
        total_pages = len(doc)
        chunk_size = 40 # Process in 40-page blocks
        
        if display_name not in self.rulebook["metadata"]["sources"]:
            self.rulebook["metadata"]["sources"].append(display_name)
        
        for i in range(0, total_pages, chunk_size):
            end_page = min(i + chunk_size, total_pages)
            text = ""
            for page_num in range(i, end_page):
                text += doc[page_num].get_text()
            
            # Extract rules from this chunk
            chunk_results = self.extract_chunk_rules(text, display_name, i // chunk_size + 1)
            self.merge_rules(chunk_results)
            
            # Save incrementally
            self.save_rulebook()
            
        doc.close()

    def merge_rules(self, results: dict):
        # Merge Buy Rules
        for r in results.get("buy_rules", []):
            name = r["pattern"].lower().replace(" ", "_")
            self.rulebook["pattern_geometries"][name] = {
                "logic": r.get("indicators", ""),
                "institutional_footprint": r.get("institutional_footprint", "")
            }
            
        # Merge Veto Rules (The "What Not To Buy" list)
        for v in results.get("veto_rules", []):
            name = v["trap_name"].lower().replace(" ", "_")
            self.rulebook["disqualification_rules"][name] = {
                "warning_signs": v.get("warning_signs", []),
                "rejection_logic": v.get("reason", "")
            }
            
        # Merge Market Stages
        for s in results.get("market_stages", []):
            name = s["stage"].lower().replace(" ", "_")
            self.rulebook["stage_transitions"][name] = {
                "math": s.get("math", ""),
                "action": s.get("action", "")
            }

    def save_rulebook(self):
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.output_path, "w") as f:
            json.dump(self.rulebook, f, indent=4)
        logging.info(f"Sovereign Rulebook updated at {self.output_path}")

def run_deep_extraction():
    extractor = DeepRuleExtractor()
    
    books = [
        {
            "name": "Pring on Price Patterns  The Definitive Guide to Price Pattern Analysis and Intrepretation by Martin Pring, Martin Pring (z-lib.org).pdf",
            "display": "Martin Pring - Price Patterns"
        },
        {
            "name": "Technical Analysis Using Multiple Timeframes - Understanding Market Structure and Profit from Trend Alignment by Brian Shannon (z-lib.org) (1).pdf",
            "display": "Brian Shannon - Multiple Timeframes"
        }
    ]
    
    for book in books:
        extractor.process_book(book["name"], book["display"])

if __name__ == "__main__":
    run_deep_extraction()

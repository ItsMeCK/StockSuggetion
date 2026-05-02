import os
import json
import logging
import fitz  # PyMuPDF
from groq import Groq
from core.config import GROQ_API_KEY, GROQ_MODEL
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class InstitutionalKnowledgeIngestor:
    """
    Parses PDF books and uses Groq to extract structured trading rules.
    """
    def __init__(self):
        self.client = Groq(api_key=GROQ_API_KEY)
        self.downloads_path = Path.home() / "Downloads"
        self.rules_path = Path(__file__).parent.parent / "core" / "context_rules.json"

    def extract_text_from_pdf(self, filename: str, start_page: int, end_page: int) -> str:
        pdf_path = self.downloads_path / filename
        if not pdf_path.exists():
            logging.error(f"PDF not found: {pdf_path}")
            return ""
        
        logging.info(f"Extracting text from {filename} (Pages {start_page}-{end_page})...")
        doc = fitz.open(pdf_path)
        text = ""
        for i in range(start_page, min(end_page, len(doc))):
            text += doc[i].get_text()
        doc.close()
        return text

    def extract_rules_with_llm(self, text: str, source_name: str) -> dict:
        logging.info(f"Invoking Groq to extract rules from {source_name}...")
        
        prompt = f"""
        Analyze the following text from a technical analysis book: "{source_name}".
        Extract all institutional trading setups, price patterns, and market stage rules.
        
        Format the output as a JSON object matching this schema:
        {{
            "patterns": [
                {{
                    "pattern_name": "String",
                    "suitable_regime": ["TRENDING", "VOLATILE", "MEAN_REVERTING"],
                    "risk_weight": 1-10,
                    "math_indicators": "Description of SMAs/Volumes",
                    "psychology": "Psychological state of market",
                    "adversarial_checks": ["Veto reason 1", "Veto reason 2"]
                }}
            ],
            "market_stages": [
                {{
                    "stage": "String",
                    "math": "SMA/Slope rules",
                    "action": "Strategy to deploy"
                }}
            ]
        }}
        
        TEXT:
        {text[:8000]} # Limit text to avoid token overflow
        """
        
        try:
            response = self.client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system", "content": "You are a senior institutional quantitative analyst."},
                    {"role": "user", "content": prompt}
                ],
                response_format={ "type": "json_object" }
            )
            return json.loads(response.choices[0].message.content)
        except Exception as e:
            logging.error(f"LLM Extraction failed: {e}")
            return {}

    def update_rulebook(self, new_data: dict):
        if not self.rules_path.exists():
            current_rules = {"metadata": {"version": "v3.0", "sources": []}, "shannon_stage_analysis": {}, "pring_pattern_geometries": {}}
        else:
            with open(self.rules_path, 'r') as f:
                current_rules = json.load(f)
        
        # Merge Patterns with advanced metadata
        if "patterns" in new_data:
            for p in new_data["patterns"]:
                name = p["pattern_name"].lower().replace(" ", "_")
                current_rules["pring_pattern_geometries"][name] = {
                    "structure": p.get("math_indicators", ""),
                    "psychology": p.get("psychology", ""),
                    "regime_fit": p.get("regime_fit", "UNKNOWN"),
                    "thematic_category": p.get("thematic_category", "UNKNOWN"),
                    "institutional_priority": p.get("institutional_priority", 5),
                    "vision_failure_markers": p.get("adversarial_veto_markers", [])
                }
        
        with open(self.rules_path, 'w') as f:
            json.dump(current_rules, f, indent=4)
        logging.info(f"Rulebook successfully updated at {self.rules_path}")

    def run_ingestion(self):
        sources = [
            {
                "name": "Brian Shannon - Stage Analysis",
                "file": "Technical Analysis Using Multiple Timeframes - Understanding Market Structure and Profit from Trend Alignment by Brian Shannon (z-lib.org) (1).pdf",
                "pages": (20, 80)
            },
            {
                "name": "Martin Pring - Price Patterns",
                "file": "Pring on Price Patterns  The Definitive Guide to Price Pattern Analysis and Intrepretation by Martin Pring, Martin Pring (z-lib.org).pdf",
                "pages": (40, 150)
            },
            {
                "name": "Al Brooks - Price Action",
                "file": "Trading Price Action Trading Ranges Technical Analysis of Price Charts Bar by Bar for the Serious Trader by Al Brooks (z-lib.org).pdf",
                "pages": (10, 100)
            }
        ]

        for source in sources:
            text = self.extract_text_from_pdf(source["file"], source["pages"][0], source["pages"][1])
            if text:
                # Optimized prompt for Regime-Aware Institutional Rules
                prompt = f"""
                Analyze the following text from: "{source['name']}".
                Extract the most elite institutional setups and patterns.
                
                For each pattern, categorize it by:
                1. Market Phase: [BULL_MARKET, BEAR_MARKET, CHOPPY_SIDEWAYS, VOLATILE_CRASH]
                2. Thematic Category: [WAR_HEDGE, MOMENTUM_IGNITION, MEAN_REVERSION, DEFENSIVE_CAPITULATION]
                
                Format as JSON:
                {{
                    "patterns": [
                        {{
                            "pattern_name": "String",
                            "regime_fit": "String from list 1",
                            "thematic_category": "String from list 2",
                            "institutional_priority": 1-10,
                            "math_indicators": "Detailed SMA/Volume/ATR rules",
                            "psychology": "Institutional sentiment",
                            "adversarial_veto_markers": ["Failure sign 1", "Failure sign 2"]
                        }}
                    ]
                }}
                
                TEXT:
                {text[:12000]}
                """
                
                try:
                    response = self.client.chat.completions.create(
                        model=GROQ_MODEL,
                        messages=[
                            {"role": "system", "content": "You are a Chief Strategist at a Sovereign Wealth Fund."},
                            {"role": "user", "content": prompt}
                        ],
                        response_format={ "type": "json_object" }
                    )
                    extracted_rules = json.loads(response.choices[0].message.content)
                    self.update_rulebook(extracted_rules)
                except Exception as e:
                    logging.error(f"Deep-Scan extraction failed for {source['name']}: {e}")

if __name__ == "__main__":
    ingestor = InstitutionalKnowledgeIngestor()
    ingestor.run_ingestion()

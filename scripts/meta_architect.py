import os
import json
from pathlib import Path
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

class MetaArchitect:
    """
    A meta-analysis system designed to ingest trading data, current architecture,
    and institutional literature (Martin Pring) to dynamically suggest LangGraph
    architectural changes (new agents, conditional edges, decision nodes).
    """
    def __init__(self):
        self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        self.base_dir = Path(__file__).parent.parent

    def load_context(self) -> dict:
        context = {}
        
        # 1. Load Missed Opportunities Data
        try:
            with open(self.base_dir / "docs" / "weekly_winner_miss_analysis.csv", "r") as f:
                context["missed_data"] = f.read()
        except FileNotFoundError:
            context["missed_data"] = "Data not found."

        # 2. Load Current Architecture (LangGraph State & Main Flow)
        try:
            with open(self.base_dir / "core" / "state.py", "r") as f:
                context["state_code"] = f.read()
            with open(self.base_dir / "main.py", "r") as f:
                context["main_flow"] = f.read()
        except Exception as e:
            context["architecture_code"] = str(e)

        # 3. Load Screener Logic (The Bottleneck)
        try:
            with open(self.base_dir / "pipeline" / "screener.py", "r") as f:
                context["screener_code"] = f.read()
        except Exception as e:
            context["screener_code"] = str(e)

        # 4. Load Pring Literature
        try:
            with open(self.base_dir / "scratch" / "pring_extracted_rules.txt", "r") as f:
                # Take up to 60k chars to give GPT-4o deep context
                context["pring_literature"] = f.read()[:60000] 
        except Exception as e:
            context["pring_literature"] = str(e)
            
        return context

    def analyze_and_architect(self):
        print("Loading context for Meta-Analysis...")
        ctx = self.load_context()
        
        system_prompt = """
        You are the 'Meta-Architect', an AI systems engineer specializing in LangGraph agentic workflows and quantitative finance.
        Your job is to analyze the provided trading data, the current Python/LangGraph architecture, and Martin Pring's technical analysis literature.
        
        OBJECTIVE:
        Determine the optimal architectural evolution for the 'Midnight Sovereign' trading system. The system recently missed massive momentum leaders (10-25% gains) because the static screener flagged them as 'Over-Extended' or 'ATR Squeeze Fail'.
        
        OUTPUT FORMAT (JSON ONLY):
        {
            "critique_of_current_system": "string explaining why the static pipeline failed",
            "architectural_decision": {
                "requires_new_agent": boolean,
                "requires_decision_node": boolean,
                "requires_conditional_edge": boolean
            },
            "proposed_components": [
                {
                    "name": "string (e.g., RegimeRouter Node or DynamicSizing Agent)",
                    "type": "agent|router|conditional_edge",
                    "responsibility": "string describing what it does",
                    "routing_parameters": ["param1", "param2"] // e.g., ['macro_regime', 'extension_pct']
                }
            ],
            "pring_integration_logic": "string detailing how specific rules from the Pring literature should govern the new routing/decision logic",
            "langgraph_workflow_changes": "string detailing exactly how the graph edges should be updated"
        }
        """

        user_prompt = f"""
        Analyze this data and propose the next evolution of our LangGraph architecture.
        
        1. MISSED WINNERS DATA:
        {ctx['missed_data']}
        
        2. CURRENT STATE SCHEMA:
        {ctx['state_code']}
        
        3. CURRENT MAIN ORCHESTRATION:
        {ctx['main_flow']}
        
        4. CURRENT SCREENER RULES:
        {ctx['screener_code']}
        
        5. MARTIN PRING LITERATURE EXTRACT:
        {ctx['pring_literature']}
        """

        print("Requesting deep architectural analysis from GPT-4o...")
        response = self.client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            temperature=0.2
        )

        result_json = response.choices[0].message.content
        output_path = self.base_dir / "docs" / "meta_architect_analysis.json"
        
        with open(output_path, "w") as f:
            f.write(result_json)
            
        print(f"Meta-Analysis complete. Results saved to {output_path}")
        return json.loads(result_json)

if __name__ == "__main__":
    architect = MetaArchitect()
    architect.analyze_and_architect()

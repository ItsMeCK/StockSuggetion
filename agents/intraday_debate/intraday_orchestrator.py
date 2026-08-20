import os
import asyncio
from dotenv import load_dotenv
from google import genai
from google.genai import types
from pydantic import BaseModel, Field

load_dotenv()

class DebateResult(BaseModel):
    bull_thesis: str
    bear_thesis: str
    arbiter_verdict: str
    conviction_score: int
    primary_catalyst: str

class IntradayDebateOrchestrator:
    def __init__(self):
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise ValueError("GEMINI_API_KEY not set")
        self.client = genai.Client(api_key=api_key)
        self.model = "gemini-3.1-pro-preview"
        self.fallback_model = "gemini-3.1-flash"
        
    async def _generate_with_fallback(self, prompt, config):
        models_to_try = [self.model, self.fallback_model]
        for idx, model_name in enumerate(models_to_try):
            try:
                response = await asyncio.to_thread(
                    self.client.models.generate_content,
                    model=model_name,
                    contents=prompt,
                    config=config
                )
                return response.text
            except Exception as e:
                if "429" in str(e) and idx < len(models_to_try) - 1:
                    print(f"⚠️ Rate limit hit on {model_name}, falling back to {models_to_try[idx+1]}...")
                    continue
                raise e
        
    async def get_bull_pitch(self, symbol, math_context):
        prompt = f"""
        You are the Institutional Bull. Your job is to pitch the upside for {symbol}.
        The math engine detected a massive Coiled Spring volume breakout.
        Context: {math_context}
        
        Search for real-world catalysts (earnings, contract wins, macro tailwinds) that justify this breakout.
        Output your aggressive Bull Thesis in 2-3 sentences.
        """
        config = types.GenerateContentConfig(
            system_instruction="You are a permabull options trader.",
            tools=[{"google_search": {}}],
            temperature=0.7
        )
        return await self._generate_with_fallback(prompt, config)
        
    async def get_bear_pitch(self, symbol, math_context):
        prompt = f"""
        You are the Billionaire Bear. Your only job is to destroy the Bull's thesis for {symbol} and save capital.
        The math engine detected a volume spike. 
        Context: {math_context}
        
        Search for negatives: insider selling, "sell the news" exhaustion, block deal traps, or overhead supply.
        Output your aggressive Bear Thesis in 2-3 sentences. Identify the exact reason this is a trap.
        """
        config = types.GenerateContentConfig(
            system_instruction="You are a ruthless risk manager hunting for false breakouts.",
            tools=[{"google_search": {}}],
            temperature=0.7
        )
        return await self._generate_with_fallback(prompt, config)
        
    async def evaluate_candidate(self, symbol, math_context) -> DebateResult:
        # Run Bull and Bear concurrently
        bull_thesis, bear_thesis = await asyncio.gather(
            self.get_bull_pitch(symbol, math_context),
            self.get_bear_pitch(symbol, math_context)
        )
        
        # Arbiter grades the debate
        prompt = f"""
        You are the Risk Arbiter. Evaluate the debate for {symbol}.
        
        BULL THESIS:
        {bull_thesis}
        
        BEAR THESIS:
        {bear_thesis}
        
        MATH CONTEXT:
        {math_context}
        
        Decide the final Conviction Score (0-100).
        Balance the Bear and Bull objectively. Do not hard-cap the score.
        If the Math Context is overwhelmingly bullish and the Bear's concerns are purely speculative, rely on the Math and award scores > 75.
        """
        config = types.GenerateContentConfig(
            system_instruction="You are the ultimate Risk Arbiter.",
            response_mime_type="application/json",
            response_schema=DebateResult,
            temperature=0.2
        )
        response_text = await self._generate_with_fallback(prompt, config)
        return DebateResult.model_validate_json(response_text)

    def evaluate_all_sync(self, candidates):
        """
        Runs the async debate for multiple candidates and returns a list of DebateResults.
        """
        async def run_all():
            tasks = [self.evaluate_candidate(c['symbol'], c['context']) for c in candidates]
            return await asyncio.gather(*tasks)
            
        return asyncio.run(run_all())

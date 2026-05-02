import os
from dotenv import load_dotenv

load_dotenv()

# --- INFRASTRUCTURE CONFIG ---
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = "llama-3.3-70b-versatile"

# --- SOVEREIGN RISK PARAMETERS ---
# Minimum conviction score required for trade entry (0-100)
COGNITIVE_THRESHOLD_DEFAULT = float(os.getenv("COGNITIVE_THRESHOLD", 70.0))
COGNITIVE_THRESHOLD_ELITE = float(os.getenv("COGNITIVE_THRESHOLD_ELITE", 65.0))

# --- CRITIC AGENT CONFIG ---
CRITIC_SMA_FAST = int(os.getenv("CRITIC_SMA_FAST", 10))
CRITIC_SMA_SLOW = int(os.getenv("CRITIC_SMA_SLOW", 50))
DISTRIBUTION_WINDOW = int(os.getenv("DISTRIBUTION_WINDOW", 5))
EXHAUSTION_VOLUME_MULT = float(os.getenv("EXHAUSTION_VOLUME_MULT", 3.0))

# --- VISION AGENT CONFIG ---
VISION_SCORE_THRESHOLD = float(os.getenv("VISION_SCORE_THRESHOLD", 70.0))
VISION_LOOKBACK_DAYS = int(os.getenv("VISION_LOOKBACK_DAYS", 60))

# --- DATABASE CONFIG ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("TIMESCALE_PORT", "5432"),
    "user": os.getenv("TIMESCALE_USER", "quant"),
    "password": os.getenv("TIMESCALE_PASSWORD", "quantpassword"),
    "database": os.getenv("TIMESCALE_DB", "market_data")
}

# --- RULEBOOK PATHS ---
MASTER_RULEBOOK = "core/context_rules_2.json"
BASELINE_RULEBOOK = "core/context_rules.json"

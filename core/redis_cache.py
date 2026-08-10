import os
import json
import logging
import hashlib
from datetime import datetime
import redis

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RedisCacheLayer:
    """
    High-performance Redis caching layer for the Institutional Options Engine.
    Provides sub-second access to debate outputs and hourly candidate states to 
    eliminate redundant LLM queries and reduce API billing.
    Includes in-memory dict fallback if Redis is unavailable.
    """
    
    def __init__(self):
        self.use_redis = False
        self.redis_client = None
        self.memory_cache = {}
        
        try:
            self.redis_client = redis.Redis(
                host=os.getenv("REDIS_HOST", "localhost"),
                port=int(os.getenv("REDIS_PORT", 6379)),
                db=0,
                decode_responses=True,
                socket_timeout=2
            )
            # Test connection
            self.redis_client.ping()
            self.use_redis = True
            logging.info("RedisCacheLayer initialized successfully. Connected to Redis.")
        except (redis.ConnectionError, redis.TimeoutError) as e:
            logging.warning(f"Redis connection failed ({e}). Falling back to in-memory caching.")
            self.use_redis = False

    def _generate_debate_key(self, symbol: str, target_date: str, macro_regime: str) -> str:
        """Generates a deterministic hash key for a debate session to ensure cache hits on identical setups."""
        key_str = f"{symbol}_{target_date}_{macro_regime}"
        return "debate_cache:" + hashlib.md5(key_str.encode()).hexdigest()
        
    def cache_llm_debate(self, symbol: str, target_date: str, macro_regime: str, debate_output: dict, ttl_seconds: int = 14400):
        """Caches the output of a Tri-Agent Debate for 4 hours (14400s)."""
        cache_key = self._generate_debate_key(symbol, target_date, macro_regime)
        payload = json.dumps(debate_output)
        
        if self.use_redis:
            try:
                self.redis_client.setex(cache_key, ttl_seconds, payload)
                return True
            except Exception as e:
                logging.error(f"Failed to cache debate in Redis: {e}")
                
        # In-memory fallback
        self.memory_cache[cache_key] = {
            "payload": debate_output,
            "expires_at": datetime.now().timestamp() + ttl_seconds
        }
        return True

    def get_cached_debate(self, symbol: str, target_date: str, macro_regime: str):
        """Retrieves a cached debate if it exists and hasn't expired."""
        cache_key = self._generate_debate_key(symbol, target_date, macro_regime)
        
        if self.use_redis:
            try:
                cached_data = self.redis_client.get(cache_key)
                if cached_data:
                    logging.info(f"CACHE HIT (Redis): Debate loaded for {symbol}")
                    return json.loads(cached_data)
            except Exception as e:
                logging.error(f"Failed to get debate from Redis: {e}")
                
        # In-memory fallback check
        if cache_key in self.memory_cache:
            entry = self.memory_cache[cache_key]
            if datetime.now().timestamp() < entry["expires_at"]:
                logging.info(f"CACHE HIT (Memory): Debate loaded for {symbol}")
                return entry["payload"]
            else:
                del self.memory_cache[cache_key] # Expired
                
        return None

    def store_hourly_candidate_state(self, pipeline_stage: str, candidates: list):
        """Stores the list of symbols currently residing in a specific intraday pipeline stage."""
        cache_key = f"pipeline_stage:{pipeline_stage}"
        payload = json.dumps(candidates)
        
        if self.use_redis:
            try:
                # Store for end of day (e.g. 12 hours)
                self.redis_client.setex(cache_key, 43200, payload)
            except Exception as e:
                logging.error(f"Failed to store candidate state in Redis: {e}")
                
        self.memory_cache[cache_key] = payload
        
    def get_hourly_candidate_state(self, pipeline_stage: str) -> list:
        """Retrieves the list of symbols in a specific stage."""
        cache_key = f"pipeline_stage:{pipeline_stage}"
        
        if self.use_redis:
            try:
                cached_data = self.redis_client.get(cache_key)
                if cached_data:
                    return json.loads(cached_data)
            except Exception as e:
                logging.error(f"Failed to get candidate state from Redis: {e}")
                
        if cache_key in self.memory_cache:
            return json.loads(self.memory_cache[cache_key])
            
        return []

# Singleton instance for global access
cache = RedisCacheLayer()

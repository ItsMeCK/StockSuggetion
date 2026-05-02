import os
import logging
import psycopg2
from typing import List, Dict, Any

class SovereignVectorStore:
    """
    Handles pattern embeddings and similarity searches using pgvector.
    The 'Memory' of the Sovereign Engine.
    """
    def __init__(self):
        self.conn_params = {
            "host": os.getenv('DB_HOST', 'localhost'),
            "port": os.getenv('TIMESCALE_PORT', '5432'),
            "user": os.getenv('TIMESCALE_USER', 'quant'),
            "password": os.getenv('TIMESCALE_PASSWORD', 'quantpassword'),
            "database": os.getenv('TIMESCALE_DB', 'market_data')
        }
        self._conn = None

    def _get_connection(self):
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(**self.conn_params)
        return self._conn

    def save_pattern(self, symbol: str, time: str, pattern_type: str, outcome: str, embedding: List[float]):
        """Saves a pattern embedding to the persistent memory."""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO pattern_embeddings (symbol, time, pattern_type, outcome, embedding) VALUES (%s, %s, %s, %s, %s)",
                (symbol, time, pattern_type, outcome, embedding)
            )
            conn.commit()
            cur.close()
            logging.info(f"VECTOR_STORE: Saved pattern memory for {symbol} ({pattern_type}).")
        except Exception as e:
            logging.error(f"VECTOR_STORE SAVE ERROR: {e}")

    def find_similar_traps(self, embedding: List[float], threshold: float = 0.85) -> List[Dict[str, Any]]:
        """
        Searches memory for patterns that were historical 'LOSS' outcomes (Traps).
        Returns matches with similarity > threshold.
        """
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            # L2 distance (<->) or Cosine distance (<=>). Using Cosine for similarity.
            cur.execute(
                "SELECT symbol, time, outcome, 1 - (embedding <=> %s::vector) AS similarity FROM pattern_embeddings WHERE outcome = 'LOSS' AND 1 - (embedding <=> %s::vector) > %s ORDER BY similarity DESC LIMIT 5",
                (embedding, embedding, threshold)
            )
            rows = cur.fetchall()
            cur.close()
            
            return [{"symbol": r[0], "time": r[1], "outcome": r[2], "similarity": r[3]} for r in rows]
        except Exception as e:
            logging.error(f"VECTOR_STORE SEARCH ERROR: {e}")
            return []

if __name__ == "__main__":
    # Test connection
    store = SovereignVectorStore()
    print("Sovereign Vector Store Initialized.")

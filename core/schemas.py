from pydantic import BaseModel, Field, validator
from typing import List, Dict, Optional, Any
from datetime import datetime
import os

class CandidateSchema(BaseModel):
    symbol: str
    price: float = Field(..., gt=0, description="Price must be positive and non-null")
    volume: float = Field(..., ge=0)
    volume_ratio: float = Field(..., gt=0)
    extension_pct: float = Field(..., description="Extension from 50 SMA")
    is_stage_2: bool
    last_updated: datetime

    @validator('price', 'volume_ratio', 'extension_pct')
    def prevent_nulls(cls, v):
        if v is None:
            raise ValueError("NULL_DATA_DETECTED: Critical financial metric is missing.")
        return v

    @validator('last_updated')
    def check_data_freshness(cls, v):
        # Skip freshness check for historical simulations
        if os.getenv("TRADING_MODE") == "HISTORICAL":
            return v

        is_live = os.getenv("TRADING_MODE") == "LIVE"
        threshold = 900 if is_live else 86400 # 15 mins or 24 hours
        
        diff = datetime.now(v.tzinfo) - v
        if diff.total_seconds() > threshold:
            raise ValueError(f"STALE_DATA_HALT: Latest data for this stock is {diff.total_seconds()/3600:.1f} hours old. Ingestion is likely broken.")
        return v

class LibrarianAuditSchema(BaseModel):
    ticker: str
    score: float = Field(..., ge=0, le=100)
    status: str = Field(..., pattern="^(SIGNALED|WATCHLIST|VETOED|REJECTED)$")
    passed: List[str]
    failed: List[str]
    price: float = Field(..., gt=0)
    reason: Optional[str] = None
    codex_version: str

class AllocationSchema(BaseModel):
    shares: int = Field(..., ge=0)
    entry: float = Field(..., gt=0)
    stop_loss: float = Field(..., gt=0)
    confidence: float = Field(..., ge=0, le=100)
    sizing_pct: float = Field(..., ge=0, le=100)

class ExecutionSchema(BaseModel):
    amo_order_id: str
    gtt_id: Optional[str]
    status: str = "QUEUED"
    execution_time: datetime = Field(default_factory=datetime.now)

class SovereignStateSchema(BaseModel):
    target_date: str
    macro_regime: str
    candidates: List[str]
    heuristic_flags: Dict[str, Dict[str, Any]]
    agent_scores: Dict[str, Dict[str, float]]
    approved_allocations: Dict[str, AllocationSchema]
    execution_telemetry: Dict[str, ExecutionSchema]
    error_log: List[str]

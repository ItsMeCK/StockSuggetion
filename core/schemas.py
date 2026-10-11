from pydantic import BaseModel, Field, validator
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum
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
        
        # Relax freshness check on weekends/Monday morning (Saturday, Sunday, Monday)
        now = datetime.now(v.tzinfo)
        if now.weekday() in (5, 6, 0):
            threshold = 345600 # 96 hours (4 days) to allow weekend/Monday runs
        else:
            threshold = 900 if is_live else 86400 # 15 mins or 24 hours
        
        diff = now - v
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

# =====================================================================
# PART 3: MASTER ROE ARCHITECTURE CONTRACTS (v3.3.0-PROD)
# =====================================================================

class HoldingTier(str, Enum):
    """
    Three immutable holding taxonomy classes:
    - TYPE_A_SCALP: 30 to 120 mins duration; hard exit by 15:00 IST sharp.
    - TYPE_B_DAY_TREND: 2 to 5 hours duration; hard exit by 15:15 IST sharp.
    - TYPE_C_BTST: Entry 15:15-15:20 IST; mandatory exit 09:15-09:30 IST on T+1.
    """
    TYPE_A_SCALP = "TYPE_A_SCALP"
    TYPE_B_DAY_TREND = "TYPE_B_DAY_TREND"
    TYPE_C_BTST = "TYPE_C_BTST"

class SetupArchetype(str, Enum):
    """Identified institutional setup archetypes."""
    STAGE_2_EXPANSION = "STAGE_2_EXPANSION"
    VOLATILITY_SQUEEZE = "VOLATILITY_SQUEEZE"
    STEALTH_ACCUM = "STEALTH_ACCUM"
    SWEEP_REVERSAL = "SWEEP_REVERSAL"

class TradeHypothesis(BaseModel):
    """
    Immutable Trade Hypothesis formulated by the Lead Planner before dispatch.
    Anchors technical entry bounds, taxonomy deadlines, and subagent mandate vectors.
    """
    symbol: str
    holding_tier: HoldingTier
    archetype: SetupArchetype
    anchor_avwap: float = Field(..., gt=0)
    proposed_entry: float = Field(..., gt=0)
    invalidation_price: float = Field(..., gt=0)
    target_price: float = Field(..., gt=0)
    conviction_score: float = Field(..., ge=0, le=100)
    mandates: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=datetime.now)

class AuditDoubtReport(BaseModel):
    """
    Typed feedback vector from a specialist subagent back to the Planner/Arbiter.
    Carries structured friction metrics, evidence, and concrete remedial proposals.
    """
    agent_id: str
    severity: str = Field(..., pattern="^(INFO|CAUTION|STRUCTURAL_CONFLICT|FATAL_INVALIDATION)$")
    confidence: float = Field(..., ge=0.0, le=1.0)
    weight: float = Field(..., ge=0.0)
    evidence: List[Dict[str, Any]] = Field(default_factory=list)
    remedial_proposal: Optional[str] = None
    created_at: datetime = Field(default_factory=datetime.now)

class GroundingEvaluationSchema(BaseModel):
    """
    Structured 3D Grounding Intelligence output hardened against sycophancy.
    Evidence must precede confidence; final score is computed by frozen code constants.
    """
    symbol: str
    public_catalyst: Dict[str, Any]
    stealth_accumulation: Dict[str, Any]
    toxic_catalyst: Dict[str, Any]
    final_score: float = Field(..., ge=0.0, le=100.0)
    verdict: str = Field(..., pattern="^(APPROVED|VETOED)$")
    devil_advocate_rebuttal: Optional[str] = None
    evaluated_at: datetime = Field(default_factory=datetime.now)


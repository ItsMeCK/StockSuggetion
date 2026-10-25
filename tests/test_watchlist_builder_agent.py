import pytest
import json
from unittest.mock import MagicMock, patch
from datetime import datetime
from agents.watchlist_builder_agent import GroundedWatchlistBuilder
from core.risk_gatekeeper import CapitalRiskGatekeeper
from core.watchlist_manager import WatchlistManager

def test_scan_market_themes():
    mock_client = MagicMock()
    mock_model = MagicMock()
    mock_client.models = mock_model
    
    mock_response = MagicMock()
    mock_response.text = json.dumps({
        "strong_sectors": ["Defence", "Automobile"],
        "weak_sectors": ["IT"],
        "macro_events": [{"event": "RBI Policy", "date": "2026-10-30", "impact": "Repo rate steady"}],
        "market_sentiment": "BULLISH",
        "narrative_summary": "Auto and defense sectors leading institutional inflow."
    })
    mock_model.generate_content.return_value = mock_response

    builder = GroundedWatchlistBuilder(client=mock_client)
    themes = builder.scan_market_themes()

    assert "Defence" in themes["strong_sectors"]
    assert "IT" in themes["weak_sectors"]
    assert themes["market_sentiment"] == "BULLISH"
    assert len(themes["macro_events"]) == 1

def test_check_upcoming_events_flags_iv_crush():
    mock_client = MagicMock()
    mock_model = MagicMock()
    mock_client.models = mock_model
    
    mock_response = MagicMock()
    mock_response.text = json.dumps({
        "symbol": "INFY",
        "earnings_due_soon": True,
        "earnings_date": "2026-10-26",
        "days_until_earnings": 1,
        "high_iv_risk": True,
        "other_events": [],
        "summary": "Q2 earnings announcement tomorrow"
    })
    mock_model.generate_content.return_value = mock_response

    builder = GroundedWatchlistBuilder(client=mock_client)
    events = builder.check_upcoming_events("INFY")

    assert events["high_iv_risk"] is True
    assert events["days_until_earnings"] == 1
    assert "Q2 earnings" in events["summary"]

def test_check_upcoming_events_clean():
    mock_client = MagicMock()
    mock_model = MagicMock()
    mock_client.models = mock_model
    
    mock_response = MagicMock()
    mock_response.text = json.dumps({
        "symbol": "RELIANCE",
        "earnings_due_soon": False,
        "earnings_date": "2026-11-20",
        "days_until_earnings": 26,
        "high_iv_risk": False,
        "other_events": [],
        "summary": "Earnings already reported"
    })
    mock_model.generate_content.return_value = mock_response

    builder = GroundedWatchlistBuilder(client=mock_client)
    events = builder.check_upcoming_events("RELIANCE")

    assert events["high_iv_risk"] is False
    assert events["days_until_earnings"] == 26

@patch("agents.watchlist_builder_agent.save_daily_focus_stock")
def test_build_daily_focus_watchlist_routes_iv_crush_to_review(mock_save_db):
    mock_client = MagicMock()
    builder = GroundedWatchlistBuilder(client=mock_client)

    # Mock upcoming events to return IV crush risk
    builder.check_upcoming_events = MagicMock(return_value={
        "high_iv_risk": True,
        "days_until_earnings": 1,
        "earnings_date": "2026-10-26",
        "summary": "Earnings tomorrow"
    })

    candidates = [{"symbol": "INFY", "direction": "LONG", "open": 1800.0, "prev_close": 1790.0}]
    approved, review = builder.build_daily_focus_watchlist(candidates)

    assert len(approved) == 0
    assert len(review) == 1
    assert review[0]["symbol"] == "INFY"
    assert review[0]["status"] == "REVIEW"
    assert "IV_CRUSH_RISK" in review[0]["route_reason"]
    mock_save_db.assert_called_once()

@patch("agents.watchlist_builder_agent.save_daily_focus_stock")
def test_build_daily_focus_watchlist_prunes_toxic_catalyst(mock_save_db):
    mock_client = MagicMock()
    builder = GroundedWatchlistBuilder(client=mock_client)

    # Clean upcoming events
    builder.check_upcoming_events = MagicMock(return_value={"high_iv_risk": False, "summary": "Clean"})
    # Toxic narrative
    builder.scout_symbol_narrative = MagicMock(return_value=(
        False, 0.0, "VETOED", "ABSOLUTE_VETO: SEBI order raid detected", {}
    ))

    candidates = [{"symbol": "ADANIENT", "direction": "LONG"}]
    approved, review = builder.build_daily_focus_watchlist(candidates)

    assert len(approved) == 0
    assert len(review) == 1
    assert review[0]["status"] == "PRUNED"
    assert "SEBI order" in review[0]["route_reason"]
    mock_save_db.assert_called_once()

@patch("agents.watchlist_builder_agent.save_daily_focus_stock")
def test_build_daily_focus_watchlist_ranks_by_conviction(mock_save_db):
    mock_client = MagicMock()
    builder = GroundedWatchlistBuilder(client=mock_client)

    builder.check_upcoming_events = MagicMock(return_value={"high_iv_risk": False, "summary": "Clean"})
    
    def mock_scout(symbol, direction_hint="LONG"):
        if symbol == "HAL":
            return True, 88.0, "LONG", "APPROVED: Massive defence contract win", {
                "stealth_accumulation": {"summary": "Strong FII block deal buy"}
            }
        elif symbol == "MARUTI":
            return True, 72.0, "LONG", "APPROVED: Strong monthly sales expansion", {
                "stealth_accumulation": {"summary": "High delivery volume 68%"}
            }
        elif symbol == "WIPRO":
            return True, 60.0, "SHORT", "APPROVED: Margin degradation guidance cut", {
                "stealth_accumulation": {"summary": "Distribution pattern detected"}
            }
        return False, 30.0, "LONG", "REJECTED", {}

    builder.scout_symbol_narrative = MagicMock(side_effect=mock_scout)

    candidates = [
        {"symbol": "MARUTI", "direction": "LONG"},
        {"symbol": "HAL", "direction": "LONG"},
        {"symbol": "WIPRO", "direction": "SHORT"}
    ]

    approved, review = builder.build_daily_focus_watchlist(candidates, max_symbols=2)

    # Out of 3, top 2 should be approved in rank order (HAL 88 > MARUTI 72)
    assert len(approved) == 2
    assert approved[0]["symbol"] == "HAL"
    assert approved[0]["conviction_score"] == 88.0
    assert approved[0]["direction"] == "LONG"

    assert approved[1]["symbol"] == "MARUTI"
    assert approved[1]["conviction_score"] == 72.0

    # WIPRO was pushed to review/pending due to max_symbols=2
    assert len(review) == 1
    assert review[0]["symbol"] == "WIPRO"
    assert review[0]["status"] == "PENDING_CAPACITY"

def test_daily_trades_limit_5_enforced():
    gatekeeper = CapitalRiskGatekeeper(max_daily_trades=5)
    
    # 4 trades executed today -> trade is approved
    approved, capital, reason = gatekeeper.evaluate_entry(
        engine_type="ENGINE_2_BTST",
        free_cash=10000.0,
        total_equity=27216.0,
        active_positions=[],
        trades_today_count=4
    )
    assert approved is True
    assert capital == 2500.0

    # 5 trades executed today -> trade is blocked
    approved, capital, reason = gatekeeper.evaluate_entry(
        engine_type="ENGINE_2_BTST",
        free_cash=10000.0,
        total_equity=27216.0,
        active_positions=[],
        trades_today_count=5
    )
    assert approved is False
    assert capital == 0.0
    assert "DAILY_TRADE_CAP_REACHED" in reason

def test_rules_only_sweep_promotes_without_llm():
    """Requirement WB2: Rules-only sweep promotes based on closed-bar volume and candle math without LLM."""
    builder = GroundedWatchlistBuilder(client=MagicMock())
    universe_bars = [
        # Candidate 1: Volume surge 2.5x, closing near HOD -> Promoted to HOT (LONG)
        {"symbol": "BHARTIARTL", "volume": 250000, "vol_avg_20": 100000, "open": 1600.0, "high": 1640.0, "low": 1595.0, "close": 1638.0},
        # Candidate 2: Normal volume 1.1x -> Ignored
        {"symbol": "TCS", "volume": 110000, "vol_avg_20": 100000, "open": 4100.0, "high": 4120.0, "low": 4090.0, "close": 4105.0},
        # Candidate 3: Bearish volume breakdown 2.1x, closing near LOD -> Promoted to HOT (SHORT)
        {"symbol": "TECHM", "volume": 210000, "vol_avg_20": 100000, "open": 1720.0, "high": 1725.0, "low": 1680.0, "close": 1682.0}
    ]

    promotions = builder.rules_only_sweep(universe_bars)
    symbols = [p["symbol"] for p in promotions]

    assert "BHARTIARTL" in symbols
    assert "TECHM" in symbols
    assert "TCS" not in symbols
    assert promotions[0]["tier"] == "HOT"

def test_soft_rumor_evidence_flags_and_discounts_does_not_remove():
    """Requirement WB4: SOFT evidence (social rumor, blog) flags and discounts conviction, never removes."""
    mock_client = MagicMock()
    builder = GroundedWatchlistBuilder(client=mock_client)

    # Mock evaluate_symbol returning an unverified social media rumor (SOFT evidence)
    builder.evaluate_symbol = MagicMock(return_value=(
        False, 80.0, "ABSOLUTE_VETO: Toxic regulatory/fraud catalyst detected: Twitter rumor", {
            "public_catalyst": {"evidence": [], "confidence": 0.8, "summary": "Contract win"},
            "stealth_accumulation": {"evidence": [], "confidence": 0.8, "summary": "Volume buildup"},
            "toxic_catalyst": {
                "evidence": [{"source": "Twitter", "date": "2026-10-25", "fact": "Unverified rumor on forum"}],
                "detected": False,
                "summary": "Social media forum rumor"
            }
        }
    ))

    approved, score, direction, rationale, audit_data = builder.scout_symbol_narrative("TATAMOTORS")

    # MUST NOT be removed! Kept under review with discounted score (80 * 0.70 = 56.0)
    assert approved is True
    assert score == 56.0
    assert "SOFT_FLAG" in rationale
    assert direction == "LONG"

def test_classify_preopen_gap_large_gap_stays_in_play():
    """Requirements WB8 & WB9: Gaps > 2.5% get tagged and stay in play with stricter entry rules."""
    builder = GroundedWatchlistBuilder(client=MagicMock())

    # Case 1: Normal gap +1.2% with scheduled earnings
    res_normal = builder.classify_preopen_gap("RELIANCE", prev_close=2800.0, preopen_open=2833.6, has_scheduled_event=True)
    assert res_normal["gap_pct"] == 1.2
    assert res_normal["gap_tag"] == "scheduled-event"
    assert res_normal["status"] == "ACTIVE"
    assert res_normal["requires_stricter_entry"] is False

    # Case 2: Large gap +3.5% with news -> Stays in play with HIGH_PRIORITY_STRICT_ENTRY
    res_large = builder.classify_preopen_gap("INFY", prev_close=1800.0, preopen_open=1863.0, has_news=True)
    assert res_large["gap_pct"] == 3.5
    assert res_large["gap_tag"] == "news-explained"
    assert res_large["status"] == "HIGH_PRIORITY_STRICT_ENTRY"
    assert res_large["requires_stricter_entry"] is True

def test_audit_missed_movers_labels_causes():
    """Requirement WB11: Miss audit correctly identifies and labels missed tradeable movers."""
    builder = GroundedWatchlistBuilder(client=MagicMock())
    tradeable_movers = [
        {"symbol": "COALINDIA", "move_pct": 5.2, "elimination_cause": "NOT_IN_WATCH"},
        {"symbol": "TATACHEM", "move_pct": 4.1, "elimination_cause": "FILTERED_BY_DTE"},
        {"symbol": "HAL", "move_pct": 3.8, "elimination_cause": "IN_HOT"}
    ]
    hot_symbols = {"HAL"} # HAL was captured

    misses = builder.audit_missed_movers("2026-10-25", tradeable_movers, hot_symbols)

    assert len(misses) == 2
    symbols_missed = [m["symbol"] for m in misses]
    assert "COALINDIA" in symbols_missed
    assert "TATACHEM" in symbols_missed
    assert "HAL" not in symbols_missed

def test_daily_query_budget_enforced():
    """Requirement WB12: Caps live search queries per day to prevent runaway billing or throttling."""
    builder = GroundedWatchlistBuilder(client=MagicMock(), max_daily_queries=2)
    builder.evaluate_symbol = MagicMock(return_value=(True, 80.0, "Approved", {}))

    # Query 1
    builder.scout_symbol_narrative("SYM1")
    assert builder.queries_used_today == 1

    # Query 2
    builder.scout_symbol_narrative("SYM2")
    assert builder.queries_used_today == 2

    # Query 3 -> Exceeds budget, falls back to rules baseline without calling evaluate_symbol
    builder.evaluate_symbol.reset_mock()
    approved, score, direction, rationale, data = builder.scout_symbol_narrative("SYM3")
    assert "BUDGET_CAP" in rationale
    builder.evaluate_symbol.assert_not_called()


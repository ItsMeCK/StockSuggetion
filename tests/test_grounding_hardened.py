import pytest
import json
from unittest.mock import MagicMock

from agents.grounding_intelligence import HardenedGroundingAgent, APPROVAL_THRESHOLD

def test_grounding_public_catalyst_approved():
    mock_client = MagicMock()
    mock_resp = MagicMock()
    
    mock_json = {
        "public_catalyst": {
            "evidence": [{"source": "NSE", "date": "2026-10-11", "fact": "Company won ₹5,000 Cr defense contract"}],
            "confidence": 0.90,
            "summary": "₹5,000 Cr major order win"
        },
        "stealth_accumulation": {
            "evidence": [{"source": "NSE", "date": "2026-10-11", "fact": "Sector co-movement"}],
            "confidence": 0.60,
            "summary": "Institutional co-movement"
        },
        "toxic_catalyst": {
            "evidence": [],
            "detected": False,
            "summary": "None"
        }
    }
    mock_resp.text = json.dumps(mock_json)
    
    rebuttal_resp = MagicMock()
    rebuttal_resp.text = "CLEAN_ACCUMULATION: Order win confirms accumulation."
    mock_client.models.generate_content.side_effect = [mock_resp, rebuttal_resp]

    agent = HardenedGroundingAgent(client=mock_client)
    approved, score, rationale, audit_data = agent.evaluate_symbol("HAL")

    assert approved is True
    # 0.30 * 90 + 0.50 * 60 = 27 + 30 = 57.0 >= 50.0
    assert score == 57.0
    assert "APPROVED (Public News)" in rationale

def test_grounding_stealth_accumulation_approved():
    mock_client = MagicMock()
    mock_resp = MagicMock()
    
    # Silent accumulation: zero public news, but massive delivery + block deal
    mock_json = {
        "public_catalyst": {
            "evidence": [],
            "confidence": 0.0,
            "summary": "No public news"
        },
        "stealth_accumulation": {
            "evidence": [{"source": "BSE", "date": "2026-10-11", "fact": "Block deal 2.5% equity at 3:15 PM, delivery 78%"}],
            "confidence": 1.0, # 0.50 * 100 = 50.0 == APPROVAL_THRESHOLD
            "summary": "Massive block deal and 78% delivery"
        },
        "toxic_catalyst": {
            "evidence": [],
            "detected": False,
            "summary": "None"
        }
    }
    mock_resp.text = json.dumps(mock_json)
    
    # Mock clean devil's advocate rebuttal
    rebuttal_resp = MagicMock()
    rebuttal_resp.text = "CLEAN_ACCUMULATION: Volume supported by institutional block transfer."
    
    mock_client.models.generate_content.side_effect = [mock_resp, rebuttal_resp]

    agent = HardenedGroundingAgent(client=mock_client)
    approved, score, rationale, audit_data = agent.evaluate_symbol("OIL")

    assert approved is True
    assert score >= APPROVAL_THRESHOLD
    assert "Silent Institutional Accumulation" in rationale

def test_grounding_deterministic_toxic_veto():
    mock_client = MagicMock()
    mock_resp = MagicMock()
    
    # Stock has high news, BUT toxic SEBI ban keyword is present
    mock_json = {
        "public_catalyst": {
            "evidence": [{"source": "Reuters", "date": "2026-10-11", "fact": "Strong earnings reported"}],
            "confidence": 0.95,
            "summary": "Record Q2 profits"
        },
        "stealth_accumulation": {
            "evidence": [],
            "confidence": 0.80,
            "summary": "Heavy buying"
        },
        "toxic_catalyst": {
            "evidence": [{"source": "SEBI", "date": "2026-10-11", "fact": "SEBI order against promoters for insider trading"}],
            "detected": True,
            "summary": "SEBI order issued against promoters"
        }
    }
    mock_resp.text = json.dumps(mock_json)
    mock_client.models.generate_content.return_value = mock_resp

    agent = HardenedGroundingAgent(client=mock_client)
    approved, score, rationale, audit_data = agent.evaluate_symbol("FRAUD_CORP")

    # MUST be absolute veto regardless of public catalyst score!
    assert approved is False
    assert score == 0.0
    assert "ABSOLUTE_VETO" in rationale
    assert "toxic" in rationale.lower()

def test_grounding_toxic_veto_negation_not_triggered():
    mock_client = MagicMock()
    mock_resp = MagicMock()

    # The AI explicitly reports "no SEBI order found" and detected: False
    mock_json = {
        "public_catalyst": {
            "evidence": [{"source": "Moneycontrol", "date": "2026-10-11", "fact": "Company reported robust Q2 numbers"}],
            "confidence": 0.85,
            "summary": "Strong Q2 growth"
        },
        "stealth_accumulation": {
            "evidence": [{"source": "NSE", "date": "2026-10-11", "fact": "Delivery volume 65%"}],
            "confidence": 0.60,
            "summary": "High delivery buying"
        },
        "toxic_catalyst": {
            "evidence": [{"source": "NSE", "date": "2026-10-11", "fact": "Company confirmed no sebi order or ed raid received"}],
            "detected": False,
            "summary": "No SEBI order or regulatory action found"
        }
    }
    mock_resp.text = json.dumps(mock_json)

    rebuttal_resp = MagicMock()
    rebuttal_resp.text = "CLEAN_ACCUMULATION: Clean volume and delivery expansion."
    mock_client.models.generate_content.side_effect = [mock_resp, rebuttal_resp]

    agent = HardenedGroundingAgent(client=mock_client)
    approved, score, rationale, audit_data = agent.evaluate_symbol("CLEAN_CORP")

    # Negation must NOT trigger toxic veto!
    assert approved is True
    assert score >= APPROVAL_THRESHOLD
    assert "ABSOLUTE_VETO" not in rationale

def test_grounding_toxic_veto_advanced_negations_never_refuted():
    mock_client = MagicMock()
    mock_resp = MagicMock()

    # The AI summarizes: "There was never any SEBI order" and "accounting fraud allegations were refuted as baseless"
    mock_json = {
        "public_catalyst": {
            "evidence": [{"source": "BSE", "date": "2026-10-11", "fact": "Major expansion approved"}],
            "confidence": 0.90,
            "summary": "Expansion greenlit"
        },
        "stealth_accumulation": {
            "evidence": [{"source": "NSE", "date": "2026-10-11", "fact": "Block deal 3% equity"}],
            "confidence": 0.60,
            "summary": "Block deal accumulation"
        },
        "toxic_catalyst": {
            "evidence": [
                {"source": "Exchange", "date": "2026-10-11", "fact": "Management clarified there was never any sebi order"},
                {"source": "Auditor", "date": "2026-10-11", "fact": "Rumors of accounting fraud were refuted as baseless"}
            ],
            "detected": False,
            "summary": "There was never any SEBI order, rumors refuted"
        }
    }
    mock_resp.text = json.dumps(mock_json)

    rebuttal_resp = MagicMock()
    rebuttal_resp.text = "CLEAN_ACCUMULATION: Clean institutional accumulation confirmed."
    mock_client.models.generate_content.side_effect = [mock_resp, rebuttal_resp]

    agent = HardenedGroundingAgent(client=mock_client)
    approved, score, rationale, audit_data = agent.evaluate_symbol("NEVER_VETO_CORP")

    # 'never' and 'refuted' must properly negate toxic keywords!
    assert approved is True
    assert score >= APPROVAL_THRESHOLD
    assert "ABSOLUTE_VETO" not in rationale

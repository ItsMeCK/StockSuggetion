# Midnight Sovereign Architecture v3.1 (Institutional Grade)

## 🏛️ Core Philosophy
Midnight Sovereign is an agentic quantitative engine designed to replicate the institutional auditing rigor of **Brian Shannon** (Stage Analysis) and **Martin Pring** (Momentum & Volume). It prioritizes **Veto Quality** over trade frequency, aiming for a high-conviction momentum capture.

## 🧠 The Cognitive Engine (LangGraph)
The system is orchestrated using an asynchronous graph of specialized agents:

### 1. The Screener (Deterministic)
*   **Engine**: Polars
*   **Role**: Filters the Nifty 500 for Stage 2 Markup candidates.
*   **Indicators**: SMA (10, 20, 50, 200), ROC (10, 20), ATR (3, 20), RSI, MFI.
*   **Safety**: Implements the **Continuity Guard** to reject trades if market data is >24h old.

### 2. The Sovereign Librarian (Heuristic Auditor)
*   **Codex**: Pring v4.0 Institutional Audit.
*   **Logic**: Performs an 8-point audit on every candidate.
*   **Key Guards**:
    *   **Intraday Fade Guard**: Rejects red candles (price dropping mid-day).
    *   **Anti-Chase Guard**: Rejects gap-ups > 4.5%.
    *   **Volume Integrity**: Validates the "Institutional Footprint" (Volume Thrust vs Dry-up).

### 3. The Execution Agent (Safety Gate)
*   **Platform**: Zerodha Kite API.
*   **Protocol**: AMO (After Market Orders) + Automated GTT Stop-Loss.
*   **Quality Gate**: Pydantic validation on every allocation to prevent zero/null quantity errors.

## 🛡️ Resilience & Safety
*   **Pydantic Enforcement**: Strict schema validation for all agent inputs and outputs.
*   **Kill-Switch**: Halts execution if `last_updated` timestamps are stale.
*   **Race-Condition Guard**: Execution is strictly downstream of the Librarian's final Veto.

## 📊 Infrastructure
*   **Database**: TimescaleDB (PostgreSQL) for OHLCV and Bitemporal Ledgers.
*   **Ledgers**:
    *   `decision_ledger`: Stores the "Why" (Agent opinions and scores).
    *   `trade_events`: Stores the "What" (Live order status and fills).
*   **Environment**: Managed via `.env` with strict credential separation.

# Operational Manual: Midnight Sovereign

## 🚦 Daily Workflow (IST Time)

### 1. Market Close (3:30 PM)
*   The system should automatically trigger the **EOD Ingestion**.
*   **Command**: `PYTHONPATH=. venv/bin/python3 pipeline/ingestion.py`
*   *Validation*: Check `daily_ohlcv` table in TimescaleDB. Ensure `MAX(time)` is today's date.

### 2. The Cognitive Run (10:30 PM)
*   Orchestrates the Screener, Librarian, and Execution.
*   **Command**: `PYTHONPATH=. venv/bin/python3 main.py`
*   *Validation*: Check the `run_history/` folder for the latest JSON state.

### 3. Order Verification (Next Morning 9:00 AM)
*   Verify that AMOs are correctly placed on Zerodha.
*   **Command**: `PYTHONPATH=. venv/bin/python3 execution/reconciliation_node.py`

## 🛠️ Maintenance & Debugging

### Data Stale Errors
If you see `STALE_DATA_HALT` in the logs:
1.  Check the Ingestion logs: `tail -n 100 logs/ingestion.log`
2.  Manually re-run ingestion for the specific date.

### Veto Audit
To understand why a stock was rejected:
1.  Query the `decision_ledger`:
    ```sql
    SELECT symbol, status, agent_opinions 
    FROM decision_ledger 
    WHERE symbol = 'TICKER' 
    ORDER BY time DESC LIMIT 1;
    ```

## 🔐 Credentials
All secrets are stored in the `.env` file. Never commit this file.
Required Keys:
- `KITE_API_KEY` / `KITE_ACCESS_TOKEN`
- `OPENAI_API_KEY` (for Pattern Agent backup)
- `POSTGRES_DB` / `POSTGRES_PASSWORD`
- `MAIL_USERNAME` / `MAIL_PASSWORD` (for Sovereign Emailer)

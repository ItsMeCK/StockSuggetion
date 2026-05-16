# Changelog: Midnight Sovereign

## [v3.1] - 2026-05-16
### 🛡️ Added (Security & Reliability)
- **Pydantic Validation**: Implemented strict schema validation for all agent inputs/outputs.
- **Stale Data Kill-Switch**: Added a 24h continuity guard in `core/schemas.py` to prevent "Ghost Trading."
- **Null-Data Protection**: Enforced `gt=0` checks on all price and volume metrics.

### 🏛️ Improved (Audit Logic)
- **8-Point Pring Audit**: Upgraded the Librarian from 5 points to 8 points (added Anti-Chase and Intraday Fade Guards).
- **Branch Merged**: Stabilized the `main_working_fix_latest_130` branch as the primary production branch.

### 🐛 Fixed (Critical Infrastructure)
- **Split-Brain Sync**: Identified the race condition between the mid-day emailer and the EOD database veto.
- **Ingestion Failure Recovery**: Diagnosed the May 12th ingestion crash that led to the "Ghost Trades" on May 14th.

## [v3.0] - 2026-05-11
- Initial migration to the **Sonnet/Librarian** regime.
- Implementation of the **Pring Codex** heuristic audit.

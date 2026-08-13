# Midnight Sovereign - Agent Guidelines

## 1. Strict Test-Driven Development Workflow

To maintain absolute stability across all core trading and analysis jobs, all agents must strictly adhere to the following development workflow:

1. **Test Execution**: After making *any* code modifications, you MUST run the test suite (`PYTHONPATH=. ./venv/bin/pytest tests/ -v`).
2. **Failure Analysis**: If a test breaks, **DO NOT** immediately modify the test case to make it pass.
3. **Business Logic Verification**: First, analyze *why* the test broke. You must determine if your code changes unintentionally broke critical business logic or state flow.
4. **Resolution**:
   - If the code unintentionally broke the business logic: **Rethink and revert/fix the code**. Do not touch the test.
   - If the code intentionally and correctly updated the business logic (and the user explicitly requested this behavioral change): **Only then should you update the test case to reflect the new logic.**

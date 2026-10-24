## Summary & Run Instructions
<!-- 3-bullet summary of what this PR builds and any commands needed to run/test it -->
-
-
-

**Related Issue:** Closes #<!-- Issue Number -->

## Branch & Governance Checklist
- [ ] **Branch Name:** Follows `feature/<issue-number>-<short-description>`, `bugfix/<issue-number>-<short-description>`, or `docs/<short-description>`
- [ ] **Formatting:** `make lint` (`pre-commit`) passes locally with zero errors.
- [ ] **Naming Conventions:** All SQL/data objects use `snake_case`, Python code follows PEP 8, and Airflow tasks use verb-first IDs.
- [ ] **No Secrets:** No passwords, API keys, or `.env` files are hardcoded or committed.

## Data Engineering Guardrails (If Applicable)
- [ ] **64-Bit Decimal Precision:** Price and volume columns use `DECIMAL(18, 8)` (or agreed `DECIMAL(18, x)` scale) — zero `float` or `DOUBLE` types used.
- [ ] **1-Second Timestamp Alignment:** Historical 16-digit microsecond CSVs (`/ 1e6`) and 13-digit millisecond WebSocket payloads (`/ 1e3`) are truncated to exact UTC seconds.
- [ ] **Cross-Database Compatibility (If dbt):** Uses cross-database dbt macros (e.g., `dbt.date_trunc`, `dbt.dateadd`) and Trino-compatible syntax for future Azure Synapse migration.
- [ ] **Streaming Idempotency:** Live WebSocket ingestion filters for `"x": true` (closed 1s candles only).
- [ ] **Testing:** Relevant `pytest` unit tests (`make test`) or `dbt test` checks pass locally.

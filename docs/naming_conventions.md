# 📖 Project Naming Conventions & Engineering Guardrails

This document defines the strict naming standards and data guardrails across all layers of the BTC Market Analytics platform. Consistency ensures our PySpark jobs, dbt transformations, and Airflow orchestrations integrate seamlessly.

## 1. Data Layer (SQL, dbt, Parquet, Delta Lake)
Data engines (Trino, Spark, and our future Synapse migration) handle casing differently. To prevent cross-engine bugs, all data structures MUST use `snake_case` exclusively. Never use `camelCase` or `PascalCase` for tables or columns.

### Tables and dbt Models (Medallion Architecture)
*   **Bronze (Raw):** Prefix with `raw_`.
    *   *Format:* `raw_<source>_<entity>`
    *   *Example:* `raw_binance_klines`
*   **Silver (Staging/Cleaned):** Prefix with `stg_`.
    *   *Format:* `stg_<source>_<entity>_<grain>`
    *   *Example:* `stg_binance_klines_1s`
*   **Gold (Facts & Dimensions):** Prefix with `fct_` (for time-series facts) or `dim_` (for descriptive dimensions).
    *   *Format:* `fct_<domain>_<entity>_<grain>` OR `dim_<entity>`
    *   *Example:* `fct_btc_klines_1s`, `dim_market_regime`

### Columns & Fields
*   **Surrogate & Foreign Keys:** Always suffix with `_key` (e.g., `date_key`, `market_regime_key`).
*   **Timestamps:** Suffix with `_time`. Must be converted to UTC.
    *   *Example:* `open_time`, `close_time`
*   **Dates (Calendar Day):** Suffix with `_date`.
    *   *Example:* `date_utc`, `date_eg`
*   **Booleans & Flags:** Prefix with `is_` or suffix with `_flag`.
    *   *Example:* `is_candle_closed`, `is_weekend`, `extreme_wick_flag`
*   **Metrics:** Use explicit, industry-standard names.
    *   *Example:* `quote_asset_volume`, `taker_sell_base_asset_volume`, `realized_volatility_60s`

### Core Data Guardrails (CRITICAL)
*   **Precision rule:** All price and volume metrics MUST be cast to `DECIMAL(18, 8)`. Do not use `FLOAT` or `DOUBLE`.
*   **Timestamp Truncation rule:** We operate on exactly 1-second boundaries. You must integer-divide historical 16-digit CSV timestamps by `1e6` and WebSocket 13-digit timestamps by `1e3` to yield exact 10-digit UTC seconds.

## 2. Infrastructure & Orchestration

### Airflow DAGs
DAG names must describe frequency, domain, and action.
*   *Format:* `<frequency>_<domain>_<action>`
*   *Example:* `daily_btc_batch_and_dbt`

### Airflow Tasks
Task IDs must be active verbs describing the exact operation.
*   *Format:* `<verb>_<entity>`
*   *Example:* `extract_csv_archive`, `transform_gold_star_schema`

### Kafka Topics
Topics must use dot notation to represent the hierarchy of the data.
*   *Format:* `<env>.<source>.<asset_grain>.<version>`
*   *Example:* `prod.binance.btc_usdt_1s.v1`

## 3. Application Code (Python & PySpark)
All Python scripts, PySpark jobs, and WebSocket code must strictly follow **PEP 8**. Formatting and linting are enforced via `ruff` in our pre-commit hooks.

*   **Variables, Functions, Methods:** `snake_case`
    *   *Example:* `def fetch_kline_data():`, `raw_payload = {}`
*   **Classes:** `PascalCase`
    *   *Example:* `class BinanceWebSocketClient:`
*   **Constants & Environment Variables:** `UPPER_SNAKE_CASE`
    *   *Example:* `MAX_RETRY_ATTEMPTS`, `KAFKA_BOOTSTRAP_SERVERS`
*   **File Names:** `snake_case.py`
    *   *Example:* `stream_bronze_to_silver.py` (Not `StreamBronzeToSilver.py`)

## 4. Git & Branching
Branch names must communicate the intent and link back to the GitHub Issue tracker.

*   **Features:** `feature/<issue-number>-<short-description>`
    *   *Example:* `feature/1-python-ws-producer`
*   **Bug Fixes:** `bugfix/<issue-number>-<short-description>`
    *   *Example:* `bugfix/3-checkpoint-offset-reset`
*   **Documentation:** `docs/<short-description>`
    *   *Example:* `docs/10-final-readme-and-docs`
*   **Commit Messages:** Use imperative mood (e.g., "Add Kafka producer", not "Added Kafka producer").

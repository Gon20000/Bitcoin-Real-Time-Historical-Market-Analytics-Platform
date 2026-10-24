
# BTC/USDT 1-Second Analytics Platform: Project Context

## 1. Project Overview & Status

**Goal:** Build an end-to-end, enterprise-grade unified analytics platform combining near real-time (1-second) continuous Bitcoin market data with 10 years of historical records.
**Team Size:** 5 developers.
**Current Status:** Planning and architecture phase completed. Repository structures, tooling choices, workflows, and exact data models have been decided. No code has been implemented yet beyond mental scaffolding and conceptual repository initialization.
**Target Market:** BTC/USDT 1-second candlesticks (`kline_1s`).

## 2. Architecture & Technology Stack

The platform operates on a containerized, orchestrated stack ensuring reproducible builds.

### Infrastructure (Local Development)

* **Containerization:** `docker-compose` managing a shared virtual network.

* **Orchestration:** Apache Airflow (`apache/airflow:2.8.1-python3.11`).

  * Database (Airflow Metadata only): PostgreSQL 13 (`postgres:13`).

* **Message Broker:** Apache Kafka (`bitnami/kafka:3.7.0`).

  * Running in **KRaft mode** (bypassing ZooKeeper).

  * Topic: `prod.binance.btc_usdt_1s.v1`.

  * Partitions: 3 (parallel consumption).

  * Retention: 24-hour.

  * Message Keys: Symbol (to strictly preserve chronological event order).

* **Stream Processing:** PySpark Structured Streaming (`bitnami/spark:3.5.0` running Python 3.11 internally).

### Transformation & Semantic Layer

* **Transformation:** dbt Core.

* **Airflow Integration:** Astronomer Cosmos (compiles dbt models into native Airflow Task Groups).

* **Semantic Layer:** dbt MetricFlow (standardized business metrics).

* **Query Engine (Local Dev):** **Trino** via `dbt-trino`.

  * *Decision rationale:* Favored over DuckDB due to its network client/server architecture, parallel and distributed system capabilities, and high structural compatibility with Azure Synapse Serverless, minimizing migration friction.

  * *Migration strategy:* Must use cross-database dbt macros (e.g., `dbt.date_trunc`, `dbt.dateadd`) to ensure smooth transition to Synapse T-SQL.

* **Consumption/Dashboarding:** Power BI (connecting to Trino locally or Synapse in production).

## 3. Data Sources & Ingestion

### Speed Layer (Near Real-Time)

* **Source:** Binance Live WebSocket API (`wss://fstream.binance.com/ws/btcusdt@kline_1s`).

* **Producer:** Custom Python WebSocket client with exponential backoff handling connection drops and pushing JSON payloads to Kafka.

### Batch Layer (Historical)

* **Source:** Binance Vision historical data archives (2017–2026).

* **Format:** Daily and monthly bulk CSV files (exact 1-second OHLCV records).

* **Processing:** Airflow sensors trigger PySpark batch jobs to extract CSVs and write them to the data lake.

## 4. Medallion Architecture & Storage

* **Storage Target:** Azure Data Lake Storage Gen2 (ADLS Gen2). Simulated locally as an ignored `data/` volume.

* **Format Upgrade Decision:** Initially Parquet, but upgraded to **Delta Lake** (or Apache Iceberg) to solve the "Small File" problem caused by 86,400 streaming micro-batches per day. Enables ACID transactions and daily `OPTIMIZE` commands.

### Bronze Zone (Raw Ingestion)

* Stores raw JSON payloads from Kafka and raw CSV dumps from Binance Vision without schema alteration.

### Silver Zone (Unified Schema & Cleaning)

* **Schema Mapping:** PySpark maps nested WS JSON (`k` object) to match the 12-column historic CSV:

  * Open Time, Open Price, High Price, Low Price, Close Price, Base Asset Volume, Close Time, Quote Asset Volume, Number of Trades, Taker Buy Base, Taker Buy Quote, Ignore.

* **Timestamps:** Converted to millisecond UTC Datetime.

  * *Constraint:* Historical CSVs use 16-digit microsecond UNIX (e.g., 1792540815000000); WS provides 13-digit milliseconds. Both must be explicitly standardized.

* **Data Type Precision (CRITICAL):** Prices and volumes are explicitly cast to `DECIMAL(18, 8)` to eliminate floating-point degradation. **No Python `float` or SQL `DOUBLE` allowed.**

* **Idempotency / The `k.x` Flag:** Binance WS sends continuous updates *during* a second. PySpark Structured Streaming must explicitly filter the live feed for `"x": true` (closed candles). This prevents partial-data duplicates and ensures perfect matching with static historical CSVs.

* **State Management:** PySpark must mount a durable volume for checkpointing (`/opt/bitnami/spark/checkpoints`) to resume offsets safely after crashes.

### Gold Zone (Feature Engineering via dbt)

dbt models generate advanced financial features:

* **Log Returns:** $r_t = \ln(P_t / P_{t-1})$

* **Realized Volatility:** Short-term (15s) and long-term (60s) rolling windows: $\sigma = \sqrt{\sum_{i=1}^{N} r_i^2}$

* **Trend & Momentum:** 5, 20, and 50-period SMA/EMA, RSI, Bollinger Bands ($SMA_N \pm (k \times \sigma_N)$).

* **Volume Analysis:** VWAP using Base and Quote asset volumes.

## 5. Repository Structure, Git Workflow & Governance

### Monorepo Structure (`btc-market-analytics/`)

```text
btc-market-analytics/
├── .github/
│   ├── CODEOWNERS                       # Auto-routes PR reviews based on folder paths
│   ├── PULL_REQUEST_TEMPLATE.md         # Forces developers to check boxes before merging
│   ├── ISSUE_TEMPLATE/                  # Standardized bug reports and feature requests
│   └── workflows/                       # (Future) CI/CD pipelines (e.g., dbt test on PR)
├── docs/                                # Centralized architecture and API documentation
│   ├── architecture_diagrams/
│   ├── data_dictionary.md               # Defines the 1-second Silver schema mappings
│   └── naming_conventions.md            # Naming conventions
├── airflow/
│   ├── dags/                            # Airflow DAGs (Batch & dbt-Cosmos triggers)
│   ├── plugins/                         # Custom Airflow hooks/operators
│   ├── Dockerfile                       # Custom image installing dbt, Cosmos, PySpark
│   └── requirements.txt                 # Python dependencies for Airflow
├── dbt-transformations/
│   ├── models/
│   │   ├── staging/                     # Bronze to Silver conversions
│   │   └── marts/                       # Silver to Gold financial indicators
│   ├── tests/                           # Custom SQL data quality tests
│   ├── dbt_project.yml
│   └── profiles.yml
├── kafka-streaming/
│   ├── producer/
│   │   ├── main.py                      # Binance WS -> Kafka Python script
│   │   └── requirements.txt
│   └── Dockerfile                       # Optional isolated container for the producer
├── spark-jobs/
│   ├── stream_bronze_to_silver.py       # PySpark Structured Streaming script
│   ├── batch_historical_ingest.py       # PySpark CSV batch ingestion script
│   └── requirements.txt
├── data/                                # (Git-ignored) Local Data Lake
│   ├── bronze/                          # Raw JSON and CSVs
│   ├── silver/                          # Cleaned, unified Delta tables
│   ├── gold/                            # Aggregated metrics (VWAP, volume)
│   └── checkpoints/                     # Spark streaming state
├── tests/                               # Python unit tests (pytest)
│   ├── test_kafka_producer.py
│   └── test_spark_transformations.py
├── .env.example                         # Template for environment variables (NO SECRETS)
├── .gitignore                           # Excludes .env, __pycache__, data/
├── .dockerignore
├── .pre-commit-config.yaml              # Enforces code formatting before a commit succeeds
├── docker-compose.yml                   # The unified infrastructure stack
├── Makefile                             # CLI shortcuts for the team
└── README.md                             # Project blueprint and setup guide
```

### Git & Team Workflow (5-Person Team)

* **Branching Strategy:** Feature Branching.
  * Features: `feature/<issue-number>-<short-description>`
  * Bug Fixes: `bugfix/<issue-number>-<short-description>`
  * Documentation: `docs/<short-description>`
* **Pull Request Governance:**
  * Branch protection enabled on `main` requiring PR reviews.
  * A `CODEOWNERS` file assigns team members to specific subfolders (e.g., Airflow, Spark, dbt) and guarantees the project leader has the final review and merge authority.
* **Task Tracking:** GitHub Issues combined with GitHub Projects Kanban boards (*Todo*, *In Progress*, *In Review*, *Done*).

## 6. Naming Conventions Summary

* **Data Objects & SQL:** Strictly `snake_case` (e.g., `raw_binance_klines`, `stg_binance_klines_1s`, `fct_btc_volatility_1m`).
* **Python Code:** PEP 8 standard (`snake_case` for variables/functions, `PascalCase` for classes, `UPPER_SNAKE_CASE` for constants).
* **Airflow DAGs & Tasks:** `<frequency>_<domain>_<action>` and verb-first task IDs (`extract_csv_archive`).
* **Kafka Topics:** Dot-notation hierarchical format (`prod.binance.btc_usdt_1s.v1`).

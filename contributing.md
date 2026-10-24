# Contributing to the BTC/USDT 1-Second Analytics Platform

Welcome to the team! This document outlines our local development setup, Docker workflows, branching rules, and engineering guardrails. Follow these steps to ensure smooth integration across all team members.

---

## 1. First-Time Local Setup

1. **Clone the repository:**
   ```bash
   git clone <repo-url>
   cd btc-market-analytics
   ```
2. **Initialize your environment:**
   Run the initialization shortcut to create your local `.env` file, git-ignored `data/` lake directories (`bronze/`, `silver/`, `gold/`, `checkpoints/`), and install `pre-commit` hooks:
   ```bash
   make init
   ```
3. **Build the Docker images:**
   ```bash
   make build
   ```

---

## 2. Running Only What You Need (Docker Profiles)

Do not spin up the entire stack if you only need to work on one layer. Use our `Makefile` shortcuts to save RAM and CPU:

* **Streaming Developers (Kafka + WS Producer + Spark Streaming):**
  ```bash
  make up-stream
  ```
* **Analytics / dbt Developers (Trino + dbt Workspace):**
  ```bash
  make up-analytics
  make dbt-shell   # Opens bash inside the dbt container
  ```
* **Orchestration Developers (Postgres + Airflow + Spark + Trino):**
  ```bash
  make up-orch     # Airflow UI: http://localhost:8080 | Spark UI: http://localhost:8081 | Trino UI: http://localhost:8082
  ```
* **Full Stack (End-to-End Integration Testing):**
  ```bash
  make up-full
  ```
* **Stop containers:**
  ```bash
  make down
  ```

---

## 3. Git & Kanban Workflow

We track all tasks on our **GitHub Projects Kanban Board** (*Todo* -> *In Progress* -> *In Review* -> *Done*). Issue assignment is managed strictly by the Project Lead to ensure infrastructure dependencies are built in the correct order.

1. **Receive Your Assignment:** The Project Lead will assign an issue to your GitHub handle. Once you begin working on it, move your card from *Todo* to *In Progress*.
2. **Create a Branch:** Branch off `main` using our strict naming convention:
   * Features: `feature/<issue-number>-<short-description>` (e.g., `feature/1-python-ws-producer`)
   * Bug Fixes: `bugfix/<issue-number>-<short-description>` (e.g., `bugfix/3-checkpoint-offset-reset`)
   * Docs: `docs/<short-description>` (e.g., `docs/10-final-readme-and-docs`)
3. **Format & Test Before Committing:**
   Run pre-commit checks (`ruff`) and unit tests locally before pushing:
   ```bash
   make lint
   make test
   ```
4. **Open a Pull Request:**
   * Push your branch and open a PR against `main`.
   * Fill out the PR template completely (including the 3-bullet summary so we can easily compile our final `README.md` later).
   * `CODEOWNERS` will automatically request a review from the relevant domain peer and the project lead. Move your card to *In Review*.

---

## 4. Core Data Engineering Guardrails

To ensure our Speed Layer (WebSocket -> Kafka -> Spark) and Batch Layer (CSV -> Spark) merge seamlessly into our Gold Star Schema (`fct_btc_klines_1s`, `dim_date`, `dim_time`, `dim_market_regime`), strictly follow these rules:

1. **1-Second Timestamp Truncation:**
   * Historical Binance CSVs provide 16-digit microsecond UNIX timestamps (e.g., `1792540815000000`). Divide by `1e6` (`1,000,000`) to get exact UTC seconds.
   * Binance WebSocket payloads provide 13-digit millisecond UNIX timestamps. Divide by `1e3` (`1,000`) to get exact UTC seconds.
   * All core processing must run in `UTC`. Local Cairo time (`Africa/Cairo`) is derived in the Gold layer via role-playing `date_key` and `time_key` foreign keys.
2. **No Floating-Point Math (`DECIMAL` Only):**
   * Never use Python `float` or SQL `DOUBLE` for prices or volumes.
   * Cast all price and volume fields to 64-bit `DECIMAL(18, 8)` to prevent floating-point drift and overflow during high-volume aggregations.
3. **Streaming Idempotency (`k.x` Flag):**
   * The Binance WebSocket pushes partial updates inside a 1-second window. PySpark Structured Streaming must explicitly filter for `"x": true` (closed candles only) before writing to Silver.
4. **Naming Conventions:**
   * See `docs/naming_conventions.md` for full rules (`snake_case` for all SQL/data objects, PEP 8 for Python, verb-first task IDs for Airflow).

"""
Daily Bitcoin Market Analytics Batch Ingestion & dbt Cosmos Orchestration DAG.
Pipeline Schedule: Daily at midnight UTC (0 0 * * *)
Domain: BTC/USDT 1-Second Analytics
Guardrails: Verb-first task IDs, UTC timezone alignment, PEP 8 compliance.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from airflow import DAG

try:
    from cosmos import (
        DbtTaskGroup,
        ExecutionConfig,
        ExecutionMode,
        ProfileConfig,
        ProjectConfig,
        RenderConfig,
    )

    COSMOS_AVAILABLE = True
except ImportError:
    COSMOS_AVAILABLE = False


DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/dbt-transformations")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/dbt-transformations")
DBT_VENV_BIN = os.getenv("DBT_VENV_BIN", "/opt/airflow/dbt_venv/bin/dbt")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def log_pipeline_start() -> None:
    """Log pipeline execution metadata."""
    print(
        "Starting Daily BTC Batch Ingestion and dbt Star Schema transformation"
        " pipeline."
    )


def register_silver_delta_table() -> None:
    """Ensure schemas exist and register physical Silver Delta table in Trino catalog."""
    try:
        from trino import dbapi

        conn = dbapi.connect(
            host=os.getenv("TRINO_HOST", "trino"),
            port=int(os.getenv("TRINO_PORT", "8080")),
            user="admin",
            catalog="delta",
            schema="default",
        )
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS delta.silver")
        cur.execute("CREATE SCHEMA IF NOT EXISTS delta.gold")
        cur.execute("SHOW TABLES FROM delta.silver LIKE 'binance_klines_1s'")
        if not cur.fetchall():
            cur.execute(
                "CALL delta.system.register_table("
                "schema_name => 'silver', "
                "table_name => 'binance_klines_1s', "
                "table_location => '/opt/spark/data/silver/stg_binance_klines_1s')"
            )
            print("Registered delta.silver.binance_klines_1s in Trino catalog.")
        else:
            print(
                "Table delta.silver.binance_klines_1s is already registered in Trino."
            )
        cur.close()
        conn.close()
    except ImportError:
        # Fallback to invoking the dedicated registration script in dbt_venv where trino is installed
        script_path = os.path.join(
            os.path.dirname(__file__), "scripts", "register_trino_silver.py"
        )
        cmd = ["/opt/airflow/dbt_venv/bin/python", script_path]
        subprocess.check_call(cmd)


with DAG(
    dag_id="daily_btc_batch_and_dbt",
    default_args=default_args,
    description=(
        "Extract daily historical BTC/USDT CSVs via Spark and transform into"
        " Gold Star Schema via dbt/Trino"
    ),
    schedule_interval="0 0 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["btc", "speed-batch", "dbt", "trino", "delta-lake"],
) as dag:
    start_pipeline = EmptyOperator(task_id="start_pipeline")

    emit_pipeline_start_log = PythonOperator(
        task_id="log_pipeline_metadata",
        python_callable=log_pipeline_start,
    )

    trigger_spark_historical_ingest = BashOperator(
        task_id="trigger_spark_historical_ingest",
        bash_command=(
            "if command -v /opt/spark/bin/spark-submit >/dev/null 2>&1; then "
            "/opt/spark/bin/spark-submit "
            "--master ${AIRFLOW_CONN_SPARK_DEFAULT:-spark://spark-master:7077} "
            "--packages io.delta:delta-spark_2.12:3.1.0 "
            "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
            "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
            "--conf spark.sql.session.timeZone=UTC "
            "--conf spark.driver.host=airflow-scheduler "
            "/opt/airflow/spark-jobs/batch_historical_ingest.py; "
            "else "
            "echo 'ERROR: /opt/spark/bin/spark-submit client not found. Cannot submit batch historical ingest.' >&2; "
            "exit 1; "
            "fi"
        ),
    )

    register_trino_silver_table = PythonOperator(
        task_id="register_trino_silver_table",
        python_callable=register_silver_delta_table,
    )

    if COSMOS_AVAILABLE:
        profile_config = ProfileConfig(
            profile_name="btc_market_analytics",
            target_name="dev",
            profiles_yml_filepath=f"{DBT_PROFILES_DIR}/profiles.yml",
        )

        project_config = ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR,
        )

        execution_config = ExecutionConfig(
            dbt_executable_path=DBT_VENV_BIN,
            execution_mode=ExecutionMode.LOCAL,
        )

        transform_dbt_models = DbtTaskGroup(
            group_id="transform_gold_star_schema",
            project_config=project_config,
            profile_config=profile_config,
            execution_config=execution_config,
            render_config=RenderConfig(
                emit_datasets=False,
            ),
        )
    else:
        transform_dbt_models = BashOperator(
            task_id="transform_gold_star_schema",
            bash_command=(
                f"cd {DBT_PROJECT_DIR} && "
                f"{DBT_VENV_BIN} run --profiles-dir {DBT_PROFILES_DIR} && "
                f"{DBT_VENV_BIN} test --profiles-dir {DBT_PROFILES_DIR}"
            ),
        )

    validate_gold_metrics = BashOperator(
        task_id="validate_gold_metrics",
        bash_command=(
            "/opt/airflow/dbt_venv/bin/python -c '"
            "from trino import dbapi; "
            'conn = dbapi.connect(host="trino", port=8080, user="admin", catalog="delta", schema="gold"); '
            "cur = conn.cursor(); "
            'cur.execute("SELECT COUNT(*) FROM fct_btc_klines_1s"); '
            "rows = cur.fetchone()[0]; "
            'assert rows > 0, "Validation Error: fct_btc_klines_1s is empty"; '
            'print(f"Validation SUCCESS: fct_btc_klines_1s contains {rows} verified gold records."); '
            "'"
        ),
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    (
        start_pipeline
        >> emit_pipeline_start_log
        >> trigger_spark_historical_ingest
        >> register_trino_silver_table
        >> transform_dbt_models
        >> validate_gold_metrics
        >> end_pipeline
    )

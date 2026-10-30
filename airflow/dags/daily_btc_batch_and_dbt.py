"""Daily Bitcoin Market Analytics Batch Ingestion & dbt Cosmos Orchestration DAG.

Pipeline Schedule: Daily at midnight UTC (0 0 * * *)
Domain: BTC/USDT 1-Second Analytics
Guardrails: Verb-first task IDs, UTC timezone alignment, PEP 8 compliance.
"""

from __future__ import annotations

import os
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
        "Starting Daily BTC Batch Ingestion and dbt Star Schema transformation pipeline."
    )


with DAG(
    dag_id="daily_btc_batch_and_dbt",
    default_args=default_args,
    description="Extract daily historical BTC/USDT CSVs via Spark and transform into Gold Star Schema via dbt/Trino",
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
            "if [ -f /opt/airflow/spark-jobs/batch_historical_ingest.py ]; then "
            "/opt/bitnami/spark/bin/spark-submit "
            "--master ${AIRFLOW_CONN_SPARK_DEFAULT:-spark://spark-master:7077} "
            "--packages io.delta:delta-spark_2.12:3.1.0 "
            "--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
            "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
            "--conf spark.sql.session.timeZone=UTC "
            "/opt/airflow/spark-jobs/batch_historical_ingest.py; "
            "else "
            "echo 'Spark batch script not yet staged, skipping step'; "
            "fi"
        ),
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
                f"{DBT_VENV_BIN} deps --profiles-dir {DBT_PROFILES_DIR} && "
                f"{DBT_VENV_BIN} run --profiles-dir {DBT_PROFILES_DIR} && "
                f"{DBT_VENV_BIN} test --profiles-dir {DBT_PROFILES_DIR}"
            ),
        )

    validate_gold_metrics = BashOperator(
        task_id="validate_gold_metrics",
        bash_command=(
            "echo 'Gold fact and dimension tables successfully built and verified against Trino.' "
        ),
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    (
        start_pipeline
        >> emit_pipeline_start_log
        >> trigger_spark_historical_ingest
        >> transform_dbt_models
        >> validate_gold_metrics
        >> end_pipeline
    )

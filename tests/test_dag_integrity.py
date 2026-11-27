"""DAG Integrity and Governance Unit Tests."""

from __future__ import annotations

import ast
from pathlib import Path


def test_airflow_dag_syntax():
    """Verify that all Python DAG files parse as valid Python AST without syntax errors."""
    dags_dir = Path("airflow/dags")
    dag_files = list(dags_dir.glob("*.py"))
    assert len(dag_files) > 0, "No DAG files discovered in airflow/dags"

    for dag_file in dag_files:
        content = dag_file.read_text(encoding="utf-8")
        parsed = ast.parse(content, filename=str(dag_file))
        assert parsed is not None, f"Failed to parse AST for {dag_file}"


def test_airflow_dag_naming_convention():
    """Verify that DAG IDs adhere to `<frequency>_<domain>_<action>` naming standard."""
    dag_path = Path("airflow/dags/daily_btc_batch_and_dbt.py")
    assert dag_path.exists(), "Expected DAG file daily_btc_batch_and_dbt.py not found"

    content = dag_path.read_text(encoding="utf-8")
    assert (
        'dag_id="daily_btc_batch_and_dbt"' in content
    ), "DAG ID does not adhere to naming conventions"


def test_airflow_dag_silver_registration_and_fail_fast():
    """Verify DAG handles silver table registration and fails fast if spark is unavailable."""
    dag_path = Path("airflow/dags/daily_btc_batch_and_dbt.py")
    content = dag_path.read_text(encoding="utf-8")

    assert (
        'task_id="register_trino_silver_table"' in content
    ), "DAG must include register_trino_silver_table task"
    assert (
        "exit 1" in content
    ), "DAG trigger_spark_historical_ingest must exit 1 when Spark is unavailable"
    assert (
        "register_silver_delta_table" in content
    ), "DAG must define register_silver_delta_table function"


def test_airflow_dag_scripts_exist():
    """Verify dedicated helper scripts referenced by DAG tasks exist and have valid syntax."""
    script_path = Path("airflow/dags/scripts/register_trino_silver.py")
    assert script_path.exists(), "register_trino_silver.py helper script not found"
    content = script_path.read_text(encoding="utf-8")
    parsed = ast.parse(content, filename=str(script_path))
    assert parsed is not None, "Failed to parse register_trino_silver.py"

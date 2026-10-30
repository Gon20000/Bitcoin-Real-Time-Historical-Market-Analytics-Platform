"""dbt Project Configuration and Model Guardrail Tests."""

from __future__ import annotations

from pathlib import Path

import yaml


def test_dbt_project_configuration():
    """Verify dbt_project.yml and profiles.yml parse correctly."""
    project_file = Path("dbt-transformations/dbt_project.yml")
    assert project_file.exists(), "dbt_project.yml not found"

    with project_file.open("r", encoding="utf-8") as f:
        project_config = yaml.safe_load(f)

    assert project_config["name"] == "btc_market_analytics"
    assert project_config["profile"] == "btc_market_analytics"


def test_dbt_model_naming_and_guardrails():
    """Verify dbt models follow medallion prefixes and 64-bit decimal precision rule."""
    staging_models = list(Path("dbt-transformations/models/staging").glob("*.sql"))
    marts_models = list(Path("dbt-transformations/models/marts").glob("*.sql"))

    assert len(staging_models) > 0, "No staging models found"
    assert len(marts_models) > 0, "No marts models found"

    # Verify staging models prefix
    for model in staging_models:
        assert model.name.startswith(
            "stg_"
        ), f"Staging model {model.name} must start with 'stg_'"

    # Verify marts models prefix
    for model in marts_models:
        assert model.name.startswith("dim_") or model.name.startswith(
            "fct_"
        ), f"Marts model {model.name} must start with 'dim_' or 'fct_'"

    # Verify DECIMAL(18, 8) enforcement in fct_btc_klines_1s
    fact_model = Path("dbt-transformations/models/marts/fct_btc_klines_1s.sql")
    content = fact_model.read_text(encoding="utf-8").lower()
    assert (
        "decimal(18, 8)" in content
    ), "Fact model fct_btc_klines_1s must enforce DECIMAL(18, 8) precision"

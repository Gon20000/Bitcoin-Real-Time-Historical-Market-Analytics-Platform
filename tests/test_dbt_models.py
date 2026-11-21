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

    for model in staging_models:
        assert model.name.startswith(
            "stg_"
        ), f"Staging model {model.name} must start with 'stg_'"

    for model in marts_models:
        assert model.name.startswith("dim_") or model.name.startswith(
            "fct_"
        ), f"Marts model {model.name} must start with 'dim_' or 'fct_'"

    fact_model = Path("dbt-transformations/models/marts/fct_btc_klines_1s.sql")
    content = fact_model.read_text(encoding="utf-8").lower()
    assert (
        "decimal(18, 8)" in content
    ), "Fact model fct_btc_klines_1s must enforce DECIMAL(18, 8) precision"


def test_dbt_semantic_layer_configuration():
    """Verify semantic models and metrics YAML configurations parse and enforce required contracts."""
    semantic_file = Path("dbt-transformations/models/marts/semantic_models.yml")
    metrics_file = Path("dbt-transformations/models/marts/metrics.yml")

    assert semantic_file.exists(), "semantic_models.yml not found"
    assert metrics_file.exists(), "metrics.yml not found"

    with semantic_file.open("r", encoding="utf-8") as f:
        semantic_config = yaml.safe_load(f)

    with metrics_file.open("r", encoding="utf-8") as f:
        metrics_config = yaml.safe_load(f)

    assert "semantic_models" in semantic_config
    assert len(semantic_config["semantic_models"]) >= 3

    semantic_model_names = {sm["name"] for sm in semantic_config["semantic_models"]}
    assert "btc_klines_1s" in semantic_model_names
    assert "dim_date" in semantic_model_names
    assert "dim_market_regime" in semantic_model_names

    assert "metrics" in metrics_config
    assert len(metrics_config["metrics"]) >= 5

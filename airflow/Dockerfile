FROM apache/airflow:2.8.1-python3.11

USER root

# Install OpenJDK-17 (Required for PySpark / SparkSubmitOperator) and git
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        git \
        curl && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Copy requirements file
COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

# 1. Install Airflow providers, Astronomer Cosmos, and PySpark into the main Airflow environment
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# 2. Create an isolated virtual environment for dbt-trino & MetricFlow (Cosmos best practice)
RUN python -m venv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core>=1.7.0,<1.8.0 \
        dbt-trino>=1.7.0,<1.8.0 \
        dbt-metricflow

# Add the dbt virtualenv to PATH so `dbt` CLI commands work directly in dbt-workspace
ENV PATH="/opt/airflow/dbt_venv/bin:${PATH}"

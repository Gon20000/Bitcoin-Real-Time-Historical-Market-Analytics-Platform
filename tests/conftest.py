"""Pytest configuration for unified test suite."""

import os
import tempfile

# Ensure Airflow initializes its home/logs in a writable temp directory during local/CI test runs.
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow_test_"))

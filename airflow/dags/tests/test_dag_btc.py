"""Unit tests for the Binance BTCUSDT historical-download Airflow DAG.

Run from the project root:
    docker compose exec airflow-scheduler python -m pytest /opt/airflow/dags/tests/test_dag_btc.py -v

These tests mock network access and use temporary directories. They do not contact
Binance or write to the project's real data directory.
"""

import hashlib
import importlib.util
import io
import sys
import zipfile
from datetime import timedelta
from pathlib import Path

import pendulum
import pytest


def _find_dag_file():
    """Locate DAG-BTC.py when this test is run from the repository or container."""
    here = Path(__file__).resolve()
    candidates = [
        here.parents[1] / "DAG-BTC.py",
        here.parents[2] / "dags" / "DAG-BTC.py",
        here.parents[3] / "airflow" / "dags" / "DAG-BTC.py",
        Path.cwd() / "airflow" / "dags" / "DAG-BTC.py",
        Path.cwd() / "dags" / "DAG-BTC.py",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    pytest.fail("Could not locate airflow/dags/DAG-BTC.py")


@pytest.fixture(scope="module")
def dag_module():
    """Import the DAG module by file path (its filename contains a hyphen)."""
    dag_path = _find_dag_file()
    spec = importlib.util.spec_from_file_location("dag_btc_under_test", dag_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def isolated_data_dir(monkeypatch, dag_module, tmp_path):
    monkeypatch.setattr(dag_module, "DATA_DIR", tmp_path)
    return tmp_path


class FakeResponse:
    def __init__(self, payload):
        self._stream = io.BytesIO(payload)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self._stream.close()

    def read(self, size=-1):
        return self._stream.read(size)


def make_zip_bytes(
    filename="BTCUSDT-1s-2026-11-01.csv", content=b"timestamp,open\n1,2\n"
):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(filename, content)
    return buffer.getvalue()


def test_dag_configuration_and_tasks(dag_module):
    dag = dag_module.dag

    assert dag.dag_id == "download_binance_btcusdt_historical"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.schedule_interval == timedelta(minutes=1)
    assert {"bitcoin", "binance", "historical", "backfill"}.issubset(set(dag.tags))
    assert set(dag.task_ids) == {"generate_file_list", "download_archive"}

    generator = dag.get_task("generate_file_list")
    downloader = dag.get_task("download_archive")

    assert generator.python_callable is dag_module.generate_file_list
    # In Airflow 2.8.1, a dynamically mapped PythonOperator is a
    # MappedOperator. Its original operator arguments are in partial_kwargs.
    assert downloader.partial_kwargs["python_callable"] is dag_module.download_archive
    assert downloader.__class__.__name__ == "MappedOperator"
    assert downloader.task_id == "download_archive"


def _set_context(monkeypatch, module, start_date, end_date):
    monkeypatch.setattr(
        module,
        "get_current_context",
        lambda: {"params": {"start_date": start_date, "end_date": end_date}},
    )


def test_file_list_uses_monthly_archives_for_previous_months(monkeypatch, dag_module):
    fixed_today = pendulum.datetime(2026, 11, 20, tz="UTC")
    monkeypatch.setattr(dag_module.pendulum, "now", lambda tz=None: fixed_today)
    _set_context(monkeypatch, dag_module, "2026-08-15", "2026-10-31")

    files = dag_module.generate_file_list()

    assert [item["filename"] for item in files] == [
        "BTCUSDT-1s-2026-08.zip",
        "BTCUSDT-1s-2026-09.zip",
        "BTCUSDT-1s-2026-10.zip",
    ]
    assert all(item["url"].startswith(dag_module.MONTHLY_URL) for item in files)
    assert [item["csv_filename"] for item in files] == [
        name.replace(".zip", ".csv")
        for name in [
            "BTCUSDT-1s-2026-08.zip",
            "BTCUSDT-1s-2026-09.zip",
            "BTCUSDT-1s-2026-10.zip",
        ]
    ]


def test_file_list_uses_daily_archives_for_current_month_only_through_yesterday(
    monkeypatch, dag_module
):
    fixed_today = pendulum.datetime(2026, 11, 20, tz="UTC")
    monkeypatch.setattr(dag_module.pendulum, "now", lambda tz=None: fixed_today)
    _set_context(monkeypatch, dag_module, "2026-11-18", "2026-11-20")

    files = dag_module.generate_file_list()

    assert [item["filename"] for item in files] == [
        "BTCUSDT-1s-2026-11-18.zip",
        "BTCUSDT-1s-2026-11-19.zip",
    ]
    assert all(item["url"].startswith(dag_module.DAILY_URL) for item in files)


def test_file_list_spans_monthly_then_daily_archives(monkeypatch, dag_module):
    fixed_today = pendulum.datetime(2026, 11, 20, tz="UTC")
    monkeypatch.setattr(dag_module.pendulum, "now", lambda tz=None: fixed_today)
    _set_context(monkeypatch, dag_module, "2026-10-30", "2026-11-03")

    files = dag_module.generate_file_list()

    # end_date is inclusive, so November 3 must be included.
    assert [item["filename"] for item in files] == [
        "BTCUSDT-1s-2026-10.zip",
        "BTCUSDT-1s-2026-11-01.zip",
        "BTCUSDT-1s-2026-11-02.zip",
        "BTCUSDT-1s-2026-11-03.zip",
    ]
    assert files[0]["url"].startswith(dag_module.MONTHLY_URL)
    assert all(item["url"].startswith(dag_module.DAILY_URL) for item in files[1:])


def test_file_list_returns_empty_when_range_is_today_only(monkeypatch, dag_module):
    fixed_today = pendulum.datetime(2026, 11, 20, tz="UTC")
    monkeypatch.setattr(dag_module.pendulum, "now", lambda tz=None: fixed_today)
    _set_context(monkeypatch, dag_module, "2026-11-20", "2026-11-20")

    assert dag_module.generate_file_list() == []


def test_file_list_rejects_start_after_end(monkeypatch, dag_module):
    _set_context(monkeypatch, dag_module, "2026-11-21", "2026-11-20")

    with pytest.raises(ValueError, match="start_date cannot be after end_date"):
        dag_module.generate_file_list()


def test_file_list_includes_first_supported_historical_month(monkeypatch, dag_module):
    fixed_today = pendulum.datetime(2026, 11, 20, tz="UTC")
    monkeypatch.setattr(dag_module.pendulum, "now", lambda tz=None: fixed_today)
    _set_context(monkeypatch, dag_module, "2017-08-17", "2017-08-17")

    files = dag_module.generate_file_list()

    assert len(files) == 1
    assert files[0]["filename"] == "BTCUSDT-1s-2017-08.zip"
    assert files[0]["url"].startswith(dag_module.MONTHLY_URL)


def test_download_verifies_checksum_extracts_csv_and_deletes_zip(
    monkeypatch, dag_module, isolated_data_dir
):
    archive_bytes = make_zip_bytes()
    checksum = hashlib.sha256(archive_bytes).hexdigest()
    calls = []

    def fake_urlopen(url, timeout):
        calls.append((url, timeout))
        if url.endswith(".CHECKSUM"):
            return FakeResponse(f"{checksum}  archive.zip\n".encode())
        return FakeResponse(archive_bytes)

    monkeypatch.setattr(dag_module, "urlopen", fake_urlopen)
    dag_module.download_archive(
        "https://example.test/archive.zip",
        "archive.zip",
        "BTCUSDT-1s-2026-11-01.csv",
    )

    csv_path = isolated_data_dir / "BTCUSDT-1s-2026-11-01.csv"
    assert csv_path.read_bytes() == b"timestamp,open\n1,2\n"
    assert not (isolated_data_dir / "archive.zip").exists()
    assert not (isolated_data_dir / "archive.zip.tmp").exists()
    assert calls == [
        ("https://example.test/archive.zip.CHECKSUM", 60),
        ("https://example.test/archive.zip", 120),
    ]


def test_existing_csv_skips_network_and_removes_leftover_zip(
    monkeypatch, dag_module, isolated_data_dir
):
    csv_path = isolated_data_dir / "existing.csv"
    zip_path = isolated_data_dir / "existing.zip"
    csv_path.write_text("already,there\n")
    zip_path.write_bytes(b"old zip")

    def unexpected_urlopen(*args, **kwargs):
        pytest.fail("Network should not be called when the CSV already exists")

    monkeypatch.setattr(dag_module, "urlopen", unexpected_urlopen)
    dag_module.download_archive(
        "https://example.test/existing.zip", "existing.zip", "existing.csv"
    )

    assert csv_path.read_text() == "already,there\n"
    assert not zip_path.exists()


def test_existing_zip_is_extracted_without_redownloading(
    monkeypatch, dag_module, isolated_data_dir
):
    archive_bytes = make_zip_bytes("existing.csv", b"local archive\n")
    zip_path = isolated_data_dir / "existing.zip"
    zip_path.write_bytes(archive_bytes)

    def unexpected_urlopen(*args, **kwargs):
        pytest.fail("Existing ZIP should be extracted without a download")

    monkeypatch.setattr(dag_module, "urlopen", unexpected_urlopen)
    dag_module.download_archive(
        "https://example.test/existing.zip", "existing.zip", "existing.csv"
    )

    assert (isolated_data_dir / "existing.csv").read_bytes() == b"local archive\n"
    assert not zip_path.exists()


def test_checksum_mismatch_raises_and_cleans_temporary_file(
    monkeypatch, dag_module, isolated_data_dir
):
    archive_bytes = make_zip_bytes()

    def fake_urlopen(url, timeout):
        if url.endswith(".CHECKSUM"):
            return FakeResponse(b"0" * 64 + b"  archive.zip\n")
        return FakeResponse(archive_bytes)

    monkeypatch.setattr(dag_module, "urlopen", fake_urlopen)

    with pytest.raises(ValueError, match="Checksum mismatch"):
        dag_module.download_archive(
            "https://example.test/archive.zip", "archive.zip", "archive.csv"
        )

    assert not (isolated_data_dir / "archive.zip").exists()
    assert not (isolated_data_dir / "archive.zip.tmp").exists()
    assert not (isolated_data_dir / "archive.csv").exists()


def test_download_failure_cleans_partial_temporary_file(
    monkeypatch, dag_module, isolated_data_dir
):
    class BrokenResponse(FakeResponse):
        def read(self, size=-1):
            if not hasattr(self, "_already_read"):
                self._already_read = True
                return b"partial"
            raise OSError("network interrupted")

    def fake_urlopen(url, timeout):
        if url.endswith(".CHECKSUM"):
            return FakeResponse(b"unused\n")
        return BrokenResponse(b"")

    monkeypatch.setattr(dag_module, "urlopen", fake_urlopen)

    with pytest.raises(OSError, match="network interrupted"):
        dag_module.download_archive(
            "https://example.test/archive.zip", "archive.zip", "archive.csv"
        )

    assert not (isolated_data_dir / "archive.zip.tmp").exists()
    assert not (isolated_data_dir / "archive.zip").exists()


def test_corrupt_zip_is_not_deleted_on_extraction_failure(
    monkeypatch, dag_module, isolated_data_dir
):
    archive_bytes = b"not a zip file"
    checksum = hashlib.sha256(archive_bytes).hexdigest()

    def fake_urlopen(url, timeout):
        if url.endswith(".CHECKSUM"):
            return FakeResponse(f"{checksum} archive.zip\n".encode())
        return FakeResponse(archive_bytes)

    monkeypatch.setattr(dag_module, "urlopen", fake_urlopen)

    with pytest.raises(zipfile.BadZipFile):
        dag_module.download_archive(
            "https://example.test/archive.zip", "archive.zip", "archive.csv"
        )

    # The verified ZIP is retained so an Airflow retry can retry extraction.
    assert (isolated_data_dir / "archive.zip").exists()
    assert not (isolated_data_dir / "archive.csv").exists()


def test_missing_expected_csv_raises_and_keeps_zip(
    monkeypatch, dag_module, isolated_data_dir
):
    archive_bytes = make_zip_bytes("unexpected-name.csv", b"wrong file\n")
    checksum = hashlib.sha256(archive_bytes).hexdigest()

    def fake_urlopen(url, timeout):
        if url.endswith(".CHECKSUM"):
            return FakeResponse(f"{checksum} archive.zip\n".encode())
        return FakeResponse(archive_bytes)

    monkeypatch.setattr(dag_module, "urlopen", fake_urlopen)

    with pytest.raises(FileNotFoundError, match="Expected CSV not found"):
        dag_module.download_archive(
            "https://example.test/archive.zip", "archive.zip", "expected.csv"
        )

    assert (isolated_data_dir / "unexpected-name.csv").exists()
    assert (isolated_data_dir / "archive.zip").exists()


def test_directory_entries_are_ignored(monkeypatch, dag_module, isolated_data_dir):
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("folder/", "")
        archive.writestr("folder/result.csv", "data\n")
    archive_bytes = buffer.getvalue()
    checksum = hashlib.sha256(archive_bytes).hexdigest()

    def fake_urlopen(url, timeout):
        if url.endswith(".CHECKSUM"):
            return FakeResponse(f"{checksum} archive.zip\n".encode())
        return FakeResponse(archive_bytes)

    monkeypatch.setattr(dag_module, "urlopen", fake_urlopen)
    dag_module.download_archive(
        "https://example.test/archive.zip", "archive.zip", "result.csv"
    )

    assert (isolated_data_dir / "result.csv").read_text() == "data\n"
    assert not (isolated_data_dir / "archive.zip").exists()


def test_download_creates_data_directory(monkeypatch, dag_module, tmp_path):
    nested_dir = tmp_path / "not-created-yet"
    monkeypatch.setattr(dag_module, "DATA_DIR", nested_dir)

    archive_bytes = make_zip_bytes("created.csv", b"created\n")
    checksum = hashlib.sha256(archive_bytes).hexdigest()

    def fake_urlopen(url, timeout):
        if url.endswith(".CHECKSUM"):
            return FakeResponse(f"{checksum} archive.zip\n".encode())
        return FakeResponse(archive_bytes)

    monkeypatch.setattr(dag_module, "urlopen", fake_urlopen)
    dag_module.download_archive(
        "https://example.test/archive.zip", "archive.zip", "created.csv"
    )

    assert (nested_dir / "created.csv").read_text() == "created\n"
    assert not (nested_dir / "archive.zip").exists()

"""Unit tests for the Binance BTCUSDT historical-download Airflow DAG.

Run from the project root:
    make test
    # or: pytest tests/batch/test_dag_btc.py -v

These tests mock network access and use temporary directories. They do not contact
Binance or write to the project's real data directory.
"""

import hashlib
import importlib.util
import io
import sys
import types
import zipfile
from pathlib import Path
from urllib.error import HTTPError

import pendulum
import pytest

BINANCE_TEST_BASE = "https://data.binance.vision/data/spot/daily/klines/BTCUSDT/1s"


def _find_dag_file() -> Path:
    """Locate DAG-BTC.py when this test is run from the repository or container."""
    here = Path(__file__).resolve()
    candidates = [
        here.parents[2] / "airflow" / "dags" / "DAG-BTC.py",
        Path.cwd() / "airflow" / "dags" / "DAG-BTC.py",
        Path.cwd() / "dags" / "DAG-BTC.py",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    pytest.fail("Could not locate airflow/dags/DAG-BTC.py")


def _install_airflow_stubs_if_missing() -> None:
    """Provide minimal Airflow stubs only when apache-airflow is not installed."""
    try:
        from airflow.models.param import Param  # noqa: F401
        from airflow.operators.python import (  # noqa: F401
            PythonOperator,
            get_current_context,
        )

        from airflow import DAG  # noqa: F401

        return
    except ModuleNotFoundError:
        pass

    class _StubParam:
        def __init__(self, default=None, **kwargs):
            self.value = default
            self.kwargs = kwargs

    class _StubOutput:
        def __init__(self, task_id: str):
            self.task_id = task_id

    class _StubPythonOperator:
        def __init__(self, task_id: str, python_callable, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.kwargs = kwargs
            self.output = _StubOutput(task_id)
            if _StubDAG.active_dag is not None:
                _StubDAG.active_dag._tasks[task_id] = self

        @classmethod
        def partial(cls, task_id: str, python_callable, **kwargs):
            partial_kwargs = {"python_callable": python_callable, **kwargs}

            class _PartialBuilder:
                def expand(self_builder, **expand_kwargs):
                    mapped_cls = type("MappedOperator", (), {})
                    mapped = mapped_cls()
                    mapped.task_id = task_id
                    mapped.partial_kwargs = partial_kwargs
                    mapped.max_active_tis_per_dag = kwargs.get("max_active_tis_per_dag")
                    mapped.expand_kwargs = expand_kwargs
                    if _StubDAG.active_dag is not None:
                        _StubDAG.active_dag._tasks[task_id] = mapped
                    return mapped

            return _PartialBuilder()

    class _StubDAG:
        active_dag = None

        def __init__(
            self,
            dag_id: str,
            description: str = "",
            start_date=None,
            schedule=None,
            catchup: bool = True,
            max_active_runs: int = 16,
            default_args=None,
            params=None,
            tags=None,
        ):
            self.dag_id = dag_id
            self.description = description
            self.start_date = start_date
            self.schedule_interval = schedule
            self.catchup = catchup
            self.max_active_runs = max_active_runs
            self.default_args = default_args or {}
            self.params = {
                key: val.value if isinstance(val, _StubParam) else val
                for key, val in (params or {}).items()
            }
            self.tags = tags or []
            self._tasks: dict[str, object] = {}

        def __enter__(self):
            _StubDAG.active_dag = self
            return self

        def __exit__(self, exc_type, exc, tb):
            _StubDAG.active_dag = None

        @property
        def task_ids(self) -> list[str]:
            return list(self._tasks.keys())

        def get_task(self, task_id: str):
            return self._tasks[task_id]

    def _stub_get_current_context():
        raise RuntimeError("get_current_context must be mocked in unit tests")

    airflow_mod = types.ModuleType("airflow")
    airflow_mod.DAG = _StubDAG

    models_mod = types.ModuleType("airflow.models")
    param_mod = types.ModuleType("airflow.models.param")
    param_mod.Param = _StubParam

    operators_mod = types.ModuleType("airflow.operators")
    python_op_mod = types.ModuleType("airflow.operators.python")
    python_op_mod.PythonOperator = _StubPythonOperator
    python_op_mod.get_current_context = _stub_get_current_context

    sys.modules["airflow"] = airflow_mod
    sys.modules["airflow.models"] = models_mod
    sys.modules["airflow.models.param"] = param_mod
    sys.modules["airflow.operators"] = operators_mod
    sys.modules["airflow.operators.python"] = python_op_mod


@pytest.fixture(scope="module")
def dag_module():
    """Import the DAG module by file path (its filename contains a hyphen)."""
    _install_airflow_stubs_if_missing()
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
    def __init__(self, payload: bytes):
        self._stream = io.BytesIO(payload)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self._stream.close()

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)


def make_zip_bytes(
    filename: str = "BTCUSDT-1s-2026-11-01.csv",
    content: bytes = b"timestamp,open\n1,2\n",
) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(filename, content)
    return buffer.getvalue()


def _set_context(monkeypatch, module, start_date: str, end_date: str | None):
    monkeypatch.setattr(
        module,
        "get_current_context",
        lambda: {"params": {"start_date": start_date, "end_date": end_date}},
    )


def test_dag_configuration_and_tasks(dag_module):
    dag = dag_module.dag

    assert dag.dag_id == "download_binance_btcusdt_historical"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.schedule_interval == "0 4 * * *"
    assert dag.params["start_date"] == "2017-08-17"
    assert dag.params["end_date"] is None
    assert {"bitcoin", "binance", "historical", "backfill"}.issubset(set(dag.tags))
    assert set(dag.task_ids) == {"generate_file_list", "download_archive"}

    generator = dag.get_task("generate_file_list")
    downloader = dag.get_task("download_archive")

    assert generator.python_callable is dag_module.generate_file_list
    # In Airflow 2.10.2, a dynamically mapped PythonOperator is a
    # MappedOperator. Its original operator arguments are in partial_kwargs.
    assert downloader.partial_kwargs["python_callable"] is dag_module.download_archive
    max_active_tis = getattr(
        downloader, "max_active_tis_per_dag", None
    ) or downloader.partial_kwargs.get("max_active_tis_per_dag")
    assert max_active_tis == 4
    assert downloader.__class__.__name__ == "MappedOperator"
    assert downloader.task_id == "download_archive"


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
        "BTCUSDT-1s-2026-08.csv",
        "BTCUSDT-1s-2026-09.csv",
        "BTCUSDT-1s-2026-10.csv",
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


def test_file_list_defaults_none_end_date_to_yesterday(monkeypatch, dag_module):
    fixed_today = pendulum.datetime(2026, 11, 20, tz="UTC")
    monkeypatch.setattr(dag_module.pendulum, "now", lambda tz=None: fixed_today)
    _set_context(monkeypatch, dag_module, "2026-11-18", None)

    files = dag_module.generate_file_list()

    assert [item["filename"] for item in files] == [
        "BTCUSDT-1s-2026-11-18.zip",
        "BTCUSDT-1s-2026-11-19.zip",
    ]


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


def test_file_list_falls_back_to_daily_when_previous_month_returns_404(
    monkeypatch, dag_module
):
    fixed_today = pendulum.datetime(2026, 11, 2, tz="UTC")
    monkeypatch.setattr(dag_module.pendulum, "now", lambda tz=None: fixed_today)
    _set_context(monkeypatch, dag_module, "2026-10-30", "2026-11-02")

    def fake_urlopen(request, timeout):
        raise HTTPError(request.full_url, 404, "Not Found", hdrs=None, fp=None)

    monkeypatch.setattr(dag_module, "urlopen", fake_urlopen)

    files = dag_module.generate_file_list()

    assert [item["filename"] for item in files] == [
        "BTCUSDT-1s-2026-10-30.zip",
        "BTCUSDT-1s-2026-10-31.zip",
        "BTCUSDT-1s-2026-11-01.zip",
    ]
    assert all(item["url"].startswith(dag_module.DAILY_URL) for item in files)


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
    archive_url = f"{BINANCE_TEST_BASE}/archive.zip"

    def fake_urlopen(url, timeout):
        calls.append((url, timeout))
        if url.endswith(".CHECKSUM"):
            return FakeResponse(f"{checksum}  archive.zip\n".encode())
        return FakeResponse(archive_bytes)

    monkeypatch.setattr(dag_module, "urlopen", fake_urlopen)
    dag_module.download_archive(
        archive_url,
        "archive.zip",
        "BTCUSDT-1s-2026-11-01.csv",
    )

    csv_path = isolated_data_dir / "BTCUSDT-1s-2026-11-01.csv"
    assert csv_path.read_bytes() == b"timestamp,open\n1,2\n"
    assert not (isolated_data_dir / "archive.zip").exists()
    assert not (isolated_data_dir / "archive.zip.tmp").exists()
    assert calls == [
        (f"{archive_url}.CHECKSUM", 60),
        (archive_url, 120),
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
        f"{BINANCE_TEST_BASE}/existing.zip", "existing.zip", "existing.csv"
    )

    assert csv_path.read_text() == "already,there\n"
    assert not zip_path.exists()


def test_existing_zip_verifies_checksum_and_extracts_without_redownloading(
    monkeypatch, dag_module, isolated_data_dir
):
    archive_bytes = make_zip_bytes("existing.csv", b"local archive\n")
    checksum = hashlib.sha256(archive_bytes).hexdigest()
    zip_path = isolated_data_dir / "existing.zip"
    zip_path.write_bytes(archive_bytes)
    archive_url = f"{BINANCE_TEST_BASE}/existing.zip"
    calls = []

    def fake_urlopen(url, timeout):
        calls.append(url)
        if url.endswith(".CHECKSUM"):
            return FakeResponse(f"{checksum}  existing.zip\n".encode())
        pytest.fail("Existing verified ZIP should not be re-downloaded")

    monkeypatch.setattr(dag_module, "urlopen", fake_urlopen)
    dag_module.download_archive(archive_url, "existing.zip", "existing.csv")

    assert calls == [f"{archive_url}.CHECKSUM"]
    assert (isolated_data_dir / "existing.csv").read_bytes() == b"local archive\n"
    assert not zip_path.exists()


def test_existing_zip_checksum_mismatch_deletes_corrupt_zip(
    monkeypatch, dag_module, isolated_data_dir
):
    zip_path = isolated_data_dir / "corrupt.zip"
    zip_path.write_bytes(b"corrupted local zip")

    def fake_urlopen(url, timeout):
        if url.endswith(".CHECKSUM"):
            return FakeResponse(b"0" * 64 + b"  corrupt.zip\n")
        pytest.fail("Should fail on mismatch before downloading")

    monkeypatch.setattr(dag_module, "urlopen", fake_urlopen)

    with pytest.raises(ValueError, match="Checksum mismatch"):
        dag_module.download_archive(
            f"{BINANCE_TEST_BASE}/corrupt.zip", "corrupt.zip", "corrupt.csv"
        )

    assert not zip_path.exists()
    assert not (isolated_data_dir / "corrupt.csv").exists()


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
            f"{BINANCE_TEST_BASE}/archive.zip", "archive.zip", "archive.csv"
        )

    assert not (isolated_data_dir / "archive.zip").exists()
    assert not (isolated_data_dir / "archive.zip.tmp").exists()
    assert not (isolated_data_dir / "archive.csv").exists()


def test_download_failure_cleans_partial_temporary_file(
    monkeypatch, dag_module, isolated_data_dir
):
    class BrokenResponse(FakeResponse):
        def read(self, size: int = -1) -> bytes:
            if not hasattr(self, "_already_read"):
                self._already_read = True
                return b"partial"
            raise OSError("network interrupted")

    def fake_urlopen(url, timeout):
        if url.endswith(".CHECKSUM"):
            return FakeResponse(b"0" * 64 + b"  archive.zip\n")
        return BrokenResponse(b"")

    monkeypatch.setattr(dag_module, "urlopen", fake_urlopen)

    with pytest.raises(OSError, match="network interrupted"):
        dag_module.download_archive(
            f"{BINANCE_TEST_BASE}/archive.zip", "archive.zip", "archive.csv"
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
            f"{BINANCE_TEST_BASE}/archive.zip", "archive.zip", "archive.csv"
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

    with pytest.raises(ValueError, match="Expected exactly one"):
        dag_module.download_archive(
            f"{BINANCE_TEST_BASE}/archive.zip", "archive.zip", "expected.csv"
        )

    assert not (isolated_data_dir / "unexpected-name.csv").exists()
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
        f"{BINANCE_TEST_BASE}/archive.zip", "archive.zip", "result.csv"
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
        f"{BINANCE_TEST_BASE}/archive.zip", "archive.zip", "created.csv"
    )

    assert (nested_dir / "created.csv").read_text() == "created\n"
    assert not (nested_dir / "archive.zip").exists()


@pytest.mark.parametrize(
    "bad_url",
    [
        "file:///etc/passwd",
        "http://data.binance.vision/archive.zip",
        "https://evil.example.com/archive.zip",
    ],
)
def test_rejects_non_binance_or_non_https_url(dag_module, isolated_data_dir, bad_url):
    with pytest.raises(ValueError, match="Invalid Binance URL"):
        dag_module.download_archive(bad_url, "archive.zip", "archive.csv")


@pytest.mark.parametrize("payload", [b"", b"   \n", b"not-a-sha256  archive.zip\n"])
def test_rejects_malformed_checksum_response(
    monkeypatch, dag_module, isolated_data_dir, payload
):
    monkeypatch.setattr(
        dag_module, "urlopen", lambda url, timeout: FakeResponse(payload)
    )

    with pytest.raises(ValueError, match="Invalid checksum response"):
        dag_module.download_archive(
            f"{BINANCE_TEST_BASE}/archive.zip", "archive.zip", "archive.csv"
        )


@pytest.mark.parametrize(
    "unsafe_name",
    ["../evil.csv", "/abs/evil.csv", "a/../evil.csv"],
)
def test_rejects_unsafe_zip_member_path(
    monkeypatch, dag_module, isolated_data_dir, unsafe_name
):
    archive_bytes = make_zip_bytes(unsafe_name, b"bad\n")
    checksum = hashlib.sha256(archive_bytes).hexdigest()

    def fake_urlopen(url, timeout):
        if url.endswith(".CHECKSUM"):
            return FakeResponse(f"{checksum} archive.zip\n".encode())
        return FakeResponse(archive_bytes)

    monkeypatch.setattr(dag_module, "urlopen", fake_urlopen)

    with pytest.raises(ValueError, match="Unsafe ZIP member path"):
        dag_module.download_archive(
            f"{BINANCE_TEST_BASE}/archive.zip", "archive.zip", "evil.csv"
        )

    assert not (isolated_data_dir / "evil.csv").exists()
    assert not (isolated_data_dir / "evil.csv.tmp").exists()


def test_rejects_zip_exceeding_max_extracted_bytes(
    monkeypatch, dag_module, isolated_data_dir
):
    monkeypatch.setattr(dag_module, "MAX_EXTRACTED_BYTES", 10)
    archive_bytes = make_zip_bytes("big.csv", b"0123456789abcdef")
    checksum = hashlib.sha256(archive_bytes).hexdigest()

    def fake_urlopen(url, timeout):
        if url.endswith(".CHECKSUM"):
            return FakeResponse(f"{checksum} archive.zip\n".encode())
        return FakeResponse(archive_bytes)

    monkeypatch.setattr(dag_module, "urlopen", fake_urlopen)

    with pytest.raises(ValueError, match="exceeds size limit"):
        dag_module.download_archive(
            f"{BINANCE_TEST_BASE}/archive.zip", "archive.zip", "big.csv"
        )

    assert not (isolated_data_dir / "big.csv").exists()
    assert not (isolated_data_dir / "big.csv.tmp").exists()


def test_monthly_extraction_removes_superseded_daily_csvs(
    monkeypatch, dag_module, isolated_data_dir
):
    oct_day1 = isolated_data_dir / "BTCUSDT-1s-2026-10-01.csv"
    oct_day31 = isolated_data_dir / "BTCUSDT-1s-2026-10-31.csv"
    nov_day1 = isolated_data_dir / "BTCUSDT-1s-2026-11-01.csv"
    oct_day1.write_text("day1\n")
    oct_day31.write_text("day31\n")
    nov_day1.write_text("nov1\n")

    archive_bytes = make_zip_bytes("BTCUSDT-1s-2026-10.csv", b"month\n")
    checksum = hashlib.sha256(archive_bytes).hexdigest()

    def fake_urlopen(url, timeout):
        if url.endswith(".CHECKSUM"):
            return FakeResponse(f"{checksum} BTCUSDT-1s-2026-10.zip\n".encode())
        return FakeResponse(archive_bytes)

    monkeypatch.setattr(dag_module, "urlopen", fake_urlopen)
    dag_module.download_archive(
        "https://data.binance.vision/data/spot/monthly/klines/BTCUSDT/1s/BTCUSDT-1s-2026-10.zip",
        "BTCUSDT-1s-2026-10.zip",
        "BTCUSDT-1s-2026-10.csv",
    )

    assert (isolated_data_dir / "BTCUSDT-1s-2026-10.csv").read_text() == "month\n"
    assert not oct_day1.exists()
    assert not oct_day31.exists()
    assert nov_day1.exists()

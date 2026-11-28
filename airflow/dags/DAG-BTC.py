import hashlib
import logging
import os
import re
import zipfile
from datetime import timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import pendulum
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, get_current_context

from airflow import DAG

logger = logging.getLogger(__name__)

DATA_DIR = Path(os.getenv("HISTORICAL_DATA_DIR", "/opt/spark/data/bronze/historical"))

SYMBOL = "BTCUSDT"
INTERVAL = "1s"

MONTHLY_URL = (
    f"https://data.binance.vision/data/spot/monthly/klines/{SYMBOL}/{INTERVAL}/"
)
DAILY_URL = f"https://data.binance.vision/data/spot/daily/klines/{SYMBOL}/{INTERVAL}/"

CHUNK_SIZE = 1024 * 1024
LOG_INTERVAL = 100 * 1024 * 1024
MAX_EXTRACTED_BYTES = 20 * 1024**3  # 20 GB
MONTH_BOUNDARY_GRACE_DAYS = 5

CHECKSUM_PATTERN = re.compile(r"^[0-9a-fA-F]{64}$")
MONTHLY_CSV_PATTERN = re.compile(
    rf"^{re.escape(SYMBOL)}-{re.escape(INTERVAL)}-(\d{{4}}-\d{{2}})\.csv$"
)


def validate_binance_url(url: str) -> None:
    """Ensure archive and checksum URLs target Binance Vision over HTTPS."""
    parsed_url = urlparse(url)

    if (
        parsed_url.scheme != "https"
        or parsed_url.hostname != "data.binance.vision"
        or parsed_url.port not in (None, 443)
        or parsed_url.username is not None
        or parsed_url.password is not None
    ):
        raise ValueError(f"Invalid Binance URL: {url}")


def _fetch_expected_checksum(checksum_url: str) -> str:
    """Download and validate the expected 64-character SHA-256 hex digest."""
    validate_binance_url(checksum_url)

    with urlopen(checksum_url, timeout=60) as response:
        parts = response.read().decode("utf-8").split()

    if not parts or not CHECKSUM_PATTERN.match(parts[0]):
        raise ValueError(f"Invalid checksum response from {checksum_url}")

    return parts[0].lower()


def _remote_archive_exists(url: str) -> bool:
    """Check whether a remote archive exists on Binance Vision via HTTP HEAD."""
    validate_binance_url(url)
    request = Request(url, method="HEAD")

    try:
        with urlopen(request, timeout=30):
            return True
    except HTTPError as exc:
        if exc.code == 404:
            return False
        raise


def _verify_existing_zip(file_path: Path, expected_hash: str) -> None:
    """Verify SHA-256 of an existing ZIP; delete it on mismatch so retries recover."""
    sha256 = hashlib.sha256()

    with file_path.open("rb") as archive:
        while chunk := archive.read(CHUNK_SIZE):
            sha256.update(chunk)

    if sha256.hexdigest().lower() != expected_hash:
        file_path.unlink()
        raise ValueError(f"Checksum mismatch: {file_path.name}")

    logger.info("Checksum verified for existing ZIP: %s", file_path.name)


def _download_verified_zip(url: str, file_path: Path, expected_hash: str) -> None:
    """Stream-download a ZIP into a temp file, hashing in a single pass."""
    logger.info("Downloading: %s", file_path.name)
    temp_path = file_path.with_suffix(".zip.tmp")
    sha256 = hashlib.sha256()

    try:
        with urlopen(url, timeout=120) as response, temp_path.open("wb") as output:
            while chunk := response.read(CHUNK_SIZE):
                sha256.update(chunk)
                output.write(chunk)

        if sha256.hexdigest().lower() != expected_hash:
            raise ValueError(f"Checksum mismatch: {file_path.name}")

        temp_path.replace(file_path)
        logger.info("Downloaded and verified: %s", file_path.name)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _extract_verified_csv(file_path: Path, csv_file: Path, csv_filename: str) -> None:
    """Extract only the expected CSV atomically with path and size guards."""
    logger.info("Starting extraction: %s", file_path.name)
    temp_csv_file = csv_file.with_name(csv_file.name + ".tmp")

    try:
        with zipfile.ZipFile(file_path, "r") as archive:
            matching_members = []

            for member in archive.infolist():
                if member.is_dir():
                    continue

                member_path = Path(member.filename)

                if (
                    not member.filename
                    or member_path.is_absolute()
                    or ".." in member_path.parts
                    or "\\" in member.filename
                    or member_path.name in ("", ".", "..")
                ):
                    raise ValueError(f"Unsafe ZIP member path: {member.filename}")

                if member_path.name == csv_filename:
                    matching_members.append(member)

            if len(matching_members) != 1:
                raise ValueError(
                    f"Expected exactly one {csv_filename} in archive, "
                    f"found {len(matching_members)}"
                )

            member = matching_members[0]

            if member.file_size > MAX_EXTRACTED_BYTES:
                raise ValueError(
                    f"Archive member exceeds size limit: {member.file_size} bytes"
                )

            extracted_size = 0
            total_size = member.file_size
            last_logged = 0

            with archive.open(member) as source, temp_csv_file.open("wb") as target:
                while chunk := source.read(CHUNK_SIZE):
                    extracted_size += len(chunk)

                    if extracted_size > MAX_EXTRACTED_BYTES:
                        raise ValueError(
                            f"Extracted file exceeds size limit: {csv_filename}"
                        )

                    target.write(chunk)

                    if (
                        extracted_size - last_logged >= LOG_INTERVAL
                        or extracted_size == total_size
                    ):
                        progress = (
                            extracted_size / total_size * 100 if total_size else 100
                        )
                        logger.info(
                            "%s: %.1f%% (%.2f GB)",
                            csv_filename,
                            progress,
                            extracted_size / (1024**3),
                        )
                        last_logged = extracted_size

            if extracted_size != total_size:
                raise ValueError(
                    f"Extracted size mismatch for {csv_filename}: "
                    f"expected {total_size}, got {extracted_size}"
                )

            temp_csv_file.replace(csv_file)
    finally:
        if temp_csv_file.exists():
            temp_csv_file.unlink()

    logger.info("Extraction completed: %s", csv_filename)


def _cleanup_superseded_daily_csvs(csv_filename: str) -> None:
    """Remove daily CSVs for a month once the consolidated monthly CSV lands."""
    match = MONTHLY_CSV_PATTERN.match(csv_filename)
    if not match:
        return

    month_prefix = f"{SYMBOL}-{INTERVAL}-{match.group(1)}-"
    for daily_csv in DATA_DIR.glob(f"{month_prefix}*.csv"):
        daily_csv.unlink()
        logger.info("Removed superseded daily CSV: %s", daily_csv.name)


def download_archive(url: str, filename: str, csv_filename: str) -> None:
    """Download, SHA-256 verify, and atomically extract a Binance Vision CSV."""
    validate_binance_url(url)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    file_path = DATA_DIR / filename
    csv_file = DATA_DIR / csv_filename

    if csv_file.exists():
        logger.info("Already extracted: %s", csv_filename)

        if file_path.exists():
            file_path.unlink()
            logger.info("Deleted ZIP: %s", filename)

        _cleanup_superseded_daily_csvs(csv_filename)
        return

    expected_hash = _fetch_expected_checksum(url + ".CHECKSUM")

    if file_path.exists():
        _verify_existing_zip(file_path, expected_hash)
    else:
        _download_verified_zip(url, file_path, expected_hash)

    _extract_verified_csv(file_path, csv_file, csv_filename)

    file_path.unlink()
    logger.info("Deleted ZIP: %s", filename)

    _cleanup_superseded_daily_csvs(csv_filename)


def _append_daily_files(
    files: list[dict[str, str]],
    range_start: pendulum.DateTime,
    range_end: pendulum.DateTime,
) -> None:
    """Append daily archive descriptors for an inclusive UTC date range."""
    current_day = range_start

    while current_day <= range_end:
        day = current_day.format("YYYY-MM-DD")
        filename = f"{SYMBOL}-{INTERVAL}-{day}.zip"
        csv_filename = f"{SYMBOL}-{INTERVAL}-{day}.csv"

        files.append(
            {
                "url": DAILY_URL + filename,
                "filename": filename,
                "csv_filename": csv_filename,
            }
        )

        current_day = current_day.add(days=1)


def generate_file_list() -> list[dict[str, str]]:
    """Build the ordered list of monthly and daily archives for the requested range."""
    context = get_current_context()
    params = context["params"]

    today = pendulum.now("UTC").start_of("day")
    yesterday = today.subtract(days=1)
    current_month_start = today.start_of("month")
    previous_month_start = current_month_start.subtract(months=1)

    start_date = pendulum.from_format(params["start_date"], "YYYY-MM-DD", tz="UTC")
    end_date = (
        pendulum.from_format(params["end_date"], "YYYY-MM-DD", tz="UTC")
        if params.get("end_date")
        else yesterday
    )

    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date")

    files: list[dict[str, str]] = []

    # 1. Previous months: monthly archives (fall back to daily if not yet published)
    if start_date < current_month_start:
        monthly_end = min(
            end_date,
            current_month_start.subtract(days=1),
        )

        if start_date <= monthly_end:
            current_month = start_date.start_of("month")
            last_month = monthly_end.start_of("month")

            while current_month <= last_month:
                month = current_month.format("YYYY-MM")
                filename = f"{SYMBOL}-{INTERVAL}-{month}.zip"
                csv_filename = f"{SYMBOL}-{INTERVAL}-{month}.csv"
                monthly_url = MONTHLY_URL + filename

                if (
                    current_month == previous_month_start
                    and today.day <= MONTH_BOUNDARY_GRACE_DAYS
                    and not _remote_archive_exists(monthly_url)
                ):
                    logger.info(
                        "Monthly archive not yet published for %s; "
                        "falling back to daily archives",
                        month,
                    )
                    fallback_start = max(start_date, current_month)
                    fallback_end = min(
                        monthly_end,
                        current_month.add(months=1).subtract(days=1),
                    )
                    _append_daily_files(files, fallback_start, fallback_end)
                else:
                    files.append(
                        {
                            "url": monthly_url,
                            "filename": filename,
                            "csv_filename": csv_filename,
                        }
                    )

                current_month = current_month.add(months=1)

    # 2. Current month: daily archives through yesterday
    daily_start = max(start_date, current_month_start)
    daily_end = min(end_date, yesterday)
    _append_daily_files(files, daily_start, daily_end)

    logger.info("Total files to process: %s", len(files))

    return files


with DAG(
    dag_id="download_binance_btcusdt_historical",
    description=(
        "Download monthly historical and daily current-month BTCUSDT 1-second archives"
    ),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    params={
        "start_date": Param(
            "2017-08-17",
            type="string",
            format="date",
        ),
        "end_date": Param(
            None,
            type=["null", "string"],
            format="date",
        ),
    },
    tags=["bitcoin", "binance", "historical", "backfill"],
) as dag:
    file_list = PythonOperator(
        task_id="generate_file_list",
        python_callable=generate_file_list,
    )

    download_tasks = PythonOperator.partial(
        task_id="download_archive",
        python_callable=download_archive,
        max_active_tis_per_dag=4,
    ).expand(op_kwargs=file_list.output)

from datetime import timedelta
from pathlib import Path
from urllib.request import urlopen
import hashlib
import zipfile

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, get_current_context


DATA_DIR = Path("/usr/local/airflow/dags/data/bronze/historical")

SYMBOL = "BTCUSDT"
INTERVAL = "1s"

MONTHLY_URL = (
    "https://data.binance.vision/data/spot/monthly/" f"klines/{SYMBOL}/{INTERVAL}/"
)

DAILY_URL = (
    "https://data.binance.vision/data/spot/daily/" f"klines/{SYMBOL}/{INTERVAL}/"
)

CHUNK_SIZE = 1024 * 1024
LOG_INTERVAL = 100 * 1024 * 1024


def download_archive(url, filename, csv_filename):
    file_path = DATA_DIR / filename
    csv_file = DATA_DIR / csv_filename

    # Skip existing CSV files
    if csv_file.exists():
        print(f"Already extracted: {csv_filename}")

        if file_path.exists():
            file_path.unlink()
            print(f"Deleted ZIP: {filename}")

        return

    # Download archive if it doesn't exist
    if not file_path.exists():
        print(f"Downloading: {filename}")

        # Download checksum
        with urlopen(url + ".CHECKSUM", timeout=60) as response:
            expected_hash = response.read().decode().split()[0]

        temp_path = file_path.with_suffix(".zip.tmp")

        try:
            # Download to temporary file
            with urlopen(url, timeout=120) as response:
                with temp_path.open("wb") as output:
                    while chunk := response.read(CHUNK_SIZE):
                        output.write(chunk)

            # Verify SHA-256
            sha256 = hashlib.sha256()

            with temp_path.open("rb") as archive:
                while chunk := archive.read(CHUNK_SIZE):
                    sha256.update(chunk)

            actual_hash = sha256.hexdigest()

            if actual_hash.lower() != expected_hash.lower():
                raise ValueError(f"Checksum mismatch: {filename}")

            temp_path.replace(file_path)

            print(f"Verified and saved: {filename}")

        finally:
            if temp_path.exists():
                temp_path.unlink()

    # Extract archive with progress logging
    print(f"Starting extraction: {filename}")

    with zipfile.ZipFile(file_path, "r") as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue

            target_path = DATA_DIR / Path(member.filename).name

            extracted_size = 0
            total_size = member.file_size
            last_logged = 0

            with archive.open(member) as source:
                with target_path.open("wb") as target:
                    while chunk := source.read(CHUNK_SIZE):
                        target.write(chunk)
                        extracted_size += len(chunk)

                        if (
                            extracted_size - last_logged >= LOG_INTERVAL
                            or extracted_size == total_size
                        ):
                            progress = (
                                extracted_size / total_size * 100 if total_size else 100
                            )

                            print(
                                f"{csv_filename}: "
                                f"{progress:.1f}% "
                                f"({extracted_size / (1024**3):.2f} GB)"
                            )

                            last_logged = extracted_size

    if not csv_file.exists():
        raise FileNotFoundError(f"Expected CSV not found: {csv_filename}")

    print(f"Extraction completed: {csv_filename}")

    # Delete ZIP after successful extraction
    file_path.unlink()

    print(f"Deleted ZIP: {filename}")


def download_data():
    context = get_current_context()
    params = context["params"]

    start_date = pendulum.from_format(params["start_date"], "YYYY-MM-DD", tz="UTC")

    end_date = pendulum.from_format(params["end_date"], "YYYY-MM-DD", tz="UTC")

    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    today = pendulum.now("UTC").start_of("day")
    current_month_start = today.start_of("month")
    yesterday = today.subtract(days=1)

    print("=" * 50)
    print("BINANCE HISTORICAL DATA EXTRACTION")
    print(f"Symbol: {SYMBOL}")
    print(f"Interval: {INTERVAL}")
    print(f"Start date: {start_date.to_date_string()}")
    print(f"End date: {end_date.to_date_string()}")
    print("=" * 50)

    # ------------------------------------------------
    # 1. Download previous months as monthly archives
    # ------------------------------------------------

    if start_date < current_month_start:
        monthly_end = min(end_date, current_month_start.subtract(days=1))

        if start_date <= monthly_end:
            current_month = start_date.start_of("month")
            last_month = monthly_end.start_of("month")

            while current_month <= last_month:
                month = current_month.format("YYYY-MM")

                filename = f"{SYMBOL}-{INTERVAL}-{month}.zip"

                csv_filename = f"{SYMBOL}-{INTERVAL}-{month}.csv"

                url = MONTHLY_URL + filename

                print(f"\nProcessing monthly archive: {month}")

                download_archive(url, filename, csv_filename)

                current_month = current_month.add(months=1)

    # ------------------------------------------------
    # 2. Download current month as daily archives
    # ------------------------------------------------

    daily_start = max(start_date, current_month_start)

    daily_end = min(end_date, yesterday)

    if daily_start <= daily_end:
        current_day = daily_start

        while current_day <= daily_end:
            day = current_day.format("YYYY-MM-DD")

            filename = f"{SYMBOL}-{INTERVAL}-{day}.zip"

            csv_filename = f"{SYMBOL}-{INTERVAL}-{day}.csv"

            url = DAILY_URL + filename

            print(f"\nProcessing daily archive: {day}")

            download_archive(url, filename, csv_filename)

            current_day = current_day.add(days=1)

    elif end_date >= current_month_start:
        print(
            "No completed daily archives available " "within the requested date range."
        )

    print("\nHistorical extraction completed successfully.")


with DAG(
    dag_id="download_binance_btcusdt_historical",
    description=(
        "Download monthly historical and daily current-month "
        "BTCUSDT 1-second archives"
    ),
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=timedelta(minutes=1),
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
            pendulum.now("UTC").to_date_string(),
            type="string",
            format="date",
        ),
    },
    tags=["bitcoin", "binance", "historical", "backfill"],
) as dag:
    download_task = PythonOperator(
        task_id="download_historical_archives",
        python_callable=download_data,
    )

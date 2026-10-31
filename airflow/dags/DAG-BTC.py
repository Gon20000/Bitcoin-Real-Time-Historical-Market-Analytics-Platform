
from datetime import timedelta
from pathlib import Path
from urllib.request import urlopen
from urllib.error import HTTPError
import hashlib
import zipfile

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, get_current_context

DATA_DIR = Path("/usr/local/airflow/dags/data/bronze/historical")

SYMBOL = "BTCUSDT"
INTERVAL = "1s"

BASE_URL = (
    "https://data.binance.vision/data/spot/monthly/"
    f"klines/{SYMBOL}/{INTERVAL}/"
)

CHUNK_SIZE = 1024 * 1024
LOG_INTERVAL = 100 * 1024 * 1024


def download_data():

    context = get_current_context()
    params = context["params"]

    start_date = pendulum.from_format(
        params["start_date"], "YYYY-MM-DD", tz="UTC"
    )

    end_date = pendulum.from_format(
        params["end_date"], "YYYY-MM-DD", tz="UTC"
    )

    if start_date > end_date:
        raise ValueError("start_date cannot be after end_date")

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    current_month = start_date.start_of("month")
    last_month = end_date.start_of("month")

    while current_month <= last_month:

        month = current_month.format("YYYY-MM")

        filename = f"{SYMBOL}-{INTERVAL}-{month}.zip"
        file_path = DATA_DIR / filename
        csv_file = DATA_DIR / filename.replace(".zip", ".csv")
        url = BASE_URL + filename

        print(f"Processing month: {month}")

        try:

            # Skip if CSV already exists
            if csv_file.exists():

                print(f"Already extracted: {csv_file.name}")

                if file_path.exists():
                    file_path.unlink()
                    print(f"Deleted ZIP: {filename}")

            else:

                # Download archive if it doesn't exist
                if not file_path.exists():

                    print(f"Downloading: {filename}")

                    with urlopen(
                        url + ".CHECKSUM", timeout=60
                    ) as response:
                        expected_hash = (
                            response.read().decode().split()[0]
                        )

                    temp_path = file_path.with_suffix(".zip.tmp")

                    try:
                        with urlopen(url, timeout=120) as response:
                            with temp_path.open("wb") as output:

                                while chunk := response.read(CHUNK_SIZE):
                                    output.write(chunk)

                        # Verify SHA-256 checksum
                        sha256 = hashlib.sha256()

                        with temp_path.open("rb") as archive:
                            while chunk := archive.read(CHUNK_SIZE):
                                sha256.update(chunk)

                        actual_hash = sha256.hexdigest()

                        if actual_hash.lower() != expected_hash.lower():
                            raise ValueError(
                                f"Checksum mismatch: {filename}"
                            )

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

                        target_path = (
                            DATA_DIR / member.filename
                        ).resolve()

                        if not target_path.is_relative_to(
                            DATA_DIR.resolve()
                        ):
                            raise ValueError(
                                f"Unsafe archive path: {member.filename}"
                            )

                        target_path.parent.mkdir(
                            parents=True, exist_ok=True
                        )

                        extracted_size = 0
                        total_size = member.file_size
                        last_logged = 0

                        with archive.open(member) as source:
                            with target_path.open("wb") as target:

                                while chunk := source.read(CHUNK_SIZE):

                                    target.write(chunk)
                                    extracted_size += len(chunk)

                                    if (
                                        extracted_size - last_logged
                                        >= LOG_INTERVAL
                                        or extracted_size == total_size
                                    ):

                                        progress = (
                                            extracted_size / total_size * 100
                                            if total_size else 100
                                        )

                                        print(
                                            f"Extraction progress: "
                                            f"{progress:.1f}% "
                                            f"({extracted_size / (1024**3):.2f} GB)"
                                        )

                                        last_logged = extracted_size

                if not csv_file.exists():
                    raise FileNotFoundError(
                        f"Expected CSV not found: {csv_file.name}"
                    )

                print(f"Extraction completed: {csv_file.name}")

                # Delete ZIP after successful extraction
                file_path.unlink()

                print(f"Deleted ZIP: {filename}")

        except HTTPError as error:
            print(
                f"Download failed: HTTP {error.code} for {url}"
            )
            raise

        # Move to next month
        current_month = current_month.add(months=1)

    print("Historical extraction completed successfully.")


with DAG(
    dag_id="download_binance_btcusdt_monthly",
    description="Download and extract monthly BTCUSDT 1-second historical archives",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
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
            "2026-09-30",
            type="string",
            format="date",
        ),
    },
    tags=["bitcoin", "binance", "backfill"],
) as dag:

    download_task = PythonOperator(
        task_id="download_monthly_zip",
        python_callable=download_data,
    )

"""PySpark Batch Ingestion & Cleaning Job.

Ingests raw Binance Vision historical CSV files,
standardizes schema, timestamps, and precision,
and saves partitioned Delta Lake tables into Silver layer.
"""

# Standard library imports for OS operations and CLI arguments
import os
import sys

# PySpark imports:
# DataFrame: Used for type hinting DataFrame inputs and outputs
# SparkSession: The unified entry point for reading, writing, and SQL execution
from pyspark.sql import DataFrame, SparkSession  # pyright: ignore[reportMissingImports]

# F: Provides built-in functions (when, col, year, month, to_timestamp, etc.)
from pyspark.sql import functions as F  # pyright: ignore[reportMissingImports]

# T: Provides explicit data types (StructType, LongType, DecimalType, etc.)
from pyspark.sql import types as T  # pyright: ignore[reportMissingImports]


def get_spark_session() -> SparkSession:
    """Initialize and configure a Delta Lake-enabled SparkSession in UTC.

    Returns:
        SparkSession: The configured SparkSession instance.
    """
    return (
        SparkSession.builder
        # Application name visible in the Spark Master Web UI (port 8081)
        .appName("Binance_Historical_Batch_Ingest")
        # 1. Enable Delta Lake specific SQL commands (e.g., MERGE, VACUUM, UPDATE)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        # 2. Register Delta Lake as the default catalog manager
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # 3. Dynamic overwrite: Overwrites only the touched partitions (year/month)
        # instead of deleting the entire table when writing in overwrite mode
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # 4. Enforce strict UTC timezone to prevent local machine time-zone shifts
        .config("spark.sql.session.timeZone", "UTC")
        # Build or retrieve the active SparkSession instance
        .getOrCreate()
    )


# -----------------------------------------------------------------------------
# 1. Raw Schema Contract for Historical Binance Vision CSVs (12 Columns)
# -----------------------------------------------------------------------------
# We enforce an explicit schema rather than using 'inferSchema=True'.
# Benefits:
# 1. Zero performance penalty (Spark doesn't scan the multi-GB dataset twice).
# 2. Prevents header corruption (Binance Vision CSVs contain no header row).
# 3. Prices and volumes are parsed as StringType to prevent automatic float coercion.
RAW_CSV_SCHEMA = T.StructType(
    [
        T.StructField("open_time", T.LongType(), nullable=False),
        T.StructField("open_price", T.StringType(), nullable=False),
        T.StructField("high_price", T.StringType(), nullable=False),
        T.StructField("low_price", T.StringType(), nullable=False),
        T.StructField("close_price", T.StringType(), nullable=False),
        T.StructField("base_asset_volume", T.StringType(), nullable=False),
        T.StructField("close_time", T.LongType(), nullable=False),
        T.StructField("quote_asset_volume", T.StringType(), nullable=False),
        T.StructField("number_of_trades", T.LongType(), nullable=False),
        T.StructField("taker_buy_base_volume", T.StringType(), nullable=False),
        T.StructField("taker_buy_quote_volume", T.StringType(), nullable=False),
        T.StructField("ignore", T.StringType(), nullable=True),
    ]
)

# Default I/O paths matching Docker volume mounts (/opt/spark/data)
DEFAULT_INPUT_DIR = os.getenv("INPUT_DIR", "/opt/spark/data/bronze/historical")
DEFAULT_OUTPUT_DIR = os.getenv(
    "OUTPUT_DIR", "/opt/spark/data/silver/stg_binance_klines_1s"
)


# -----------------------------------------------------------------------------
# 2. Data Cleaning and Transformation (Silver Layer Standardization)
# -----------------------------------------------------------------------------
def transform_to_silver(raw_df: DataFrame) -> DataFrame:
    """Transform raw Binance klines to unified Silver schema specifications.

    - Standardizes 16-digit (micro) and 13-digit (milli) timestamps to UTC seconds.
    - Explicitly casts all price and volume attributes to DECIMAL(18, 8).
    - Renames taker buy volumes with '_asset_' for Gold dbt/Trino parity.
    - Adds is_candle_closed=True flag for batch-streaming structural alignment.
    - Derives year and month columns for partition management.
    """
    decimal_type = T.DecimalType(18, 8)

    # 1. Standardize timestamps to 10-digit UTC seconds via integer division
    cleaned_df = (
        raw_df.withColumn(
            "open_time_sec",
            F.when(
                F.col("open_time") >= 100_000_000_000_000,
                (F.col("open_time") / 1_000_000).cast(T.LongType()),
            )
            .when(
                F.col("open_time") >= 100_000_000_000,
                (F.col("open_time") / 1_000).cast(T.LongType()),
            )
            .otherwise(F.col("open_time")),
        )
        .withColumn(
            "close_time_sec",
            F.when(
                F.col("close_time") >= 100_000_000_000_000,
                (F.col("close_time") / 1_000_000).cast(T.LongType()),
            )
            .when(
                F.col("close_time") >= 100_000_000_000,
                (F.col("close_time") / 1_000).cast(T.LongType()),
            )
            .otherwise(F.col("close_time")),
        )
        # Convert integer seconds to formal UTC TimestampType
        .withColumn("open_time", F.to_timestamp(F.from_unixtime("open_time_sec")))
        .withColumn("close_time", F.to_timestamp(F.from_unixtime(F.col("close_time_sec") + 1)))
    )

    # 2. Cast all price and volume attributes to DECIMAL(18, 8) & Rename for dbt parity
    silver_df = (
        cleaned_df.withColumn("open_price", F.col("open_price").cast(decimal_type))
        .withColumn("high_price", F.col("high_price").cast(decimal_type))
        .withColumn("low_price", F.col("low_price").cast(decimal_type))
        .withColumn("close_price", F.col("close_price").cast(decimal_type))
        .withColumn("base_asset_volume", F.col("base_asset_volume").cast(decimal_type))
        .withColumn(
            "quote_asset_volume", F.col("quote_asset_volume").cast(decimal_type)
        )
        # Add '_asset_' to match Gold Layer contracts exactly
        .withColumn(
            "taker_buy_base_asset_volume",
            F.col("taker_buy_base_volume").cast(decimal_type),
        )
        .withColumn(
            "taker_buy_quote_asset_volume",
            F.col("taker_buy_quote_volume").cast(decimal_type),
        )
        # Add boolean flag for streaming/batch schema parity
        .withColumn("is_candle_closed", F.lit(True))
    )

    # 3. Extract year and month columns for efficient Delta Lake partitioning
    silver_df = silver_df.withColumn("year", F.year("open_time")).withColumn(
        "month", F.month("open_time")
    )

    # 4. Data cleaning: filter corrupted records, deduplicate on timestamp, and sort
    silver_df = (
        silver_df.filter(F.col("open_time").isNotNull() & (F.col("open_price") > 0))
        .dropDuplicates(["open_time"])
        .orderBy("open_time")
    )

    # 5. Enforce exact column order as specified in the Silver data contracts
    final_columns = [
        "open_time",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "base_asset_volume",
        "close_time",
        "quote_asset_volume",
        "number_of_trades",
        "taker_buy_base_asset_volume",
        "taker_buy_quote_asset_volume",
        "is_candle_closed",
        "ignore",
        "year",
        "month",
    ]

    return silver_df.select(final_columns)


# -----------------------------------------------------------------------------
# 3. Idempotent Delta Lake Persistence
# -----------------------------------------------------------------------------
def write_to_silver_delta(df: DataFrame, output_path: str) -> None:
    """Write DataFrame as partitioned Delta Lake table with dynamic overwrite.

    Dynamic overwrite mode replaces only matching (year, month) partitions
    present in the current dataset, ensuring fully idempotent re-runs.
    """
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("year", "month")
        .save(output_path)
    )


# -----------------------------------------------------------------------------
# 4. Pipeline Execution & Orchestration
# -----------------------------------------------------------------------------
def main() -> None:
    """Execute the end-to-end historical batch ingestion pipeline."""
    # Allow CLI path overrides or fall back to standard container defaults
    input_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT_DIR
    output_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUTPUT_DIR

    print("[INFO] Initializing PySpark Batch Ingestion Pipeline...")
    print(f"[INFO] Input directory:  {input_path}")
    print(f"[INFO] Output directory: {output_path}")

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read bulk historical CSV files using explicit schema
    csv_wildcard = os.path.join(input_path, "*.csv")
    print(f"[INFO] Loading CSV files from: {csv_wildcard}")

    raw_df = spark.read.format("csv").schema(RAW_CSV_SCHEMA).load(csv_wildcard)

    total_records = raw_df.count()
    print(f"[INFO] Successfully loaded {total_records:,} raw records from Bronze.")

    if total_records == 0:
        print("[WARN] No records found to process. Exiting.")
        return

    # Clean and transform dataset to Silver specifications
    print("[INFO] Applying Silver schema transformations...")
    silver_df = transform_to_silver(raw_df)

    # Preview sample records and schema for operational verification
    print("[INFO] Transformed DataFrame preview:")
    silver_df.show(3, truncate=False)
    silver_df.printSchema()

    # Save to Silver Delta Lake
    print(f"[INFO] Persisting clean dataset to Silver Delta Lake: {output_path}...")
    write_to_silver_delta(silver_df, output_path)

    print("[INFO] Batch ingestion and cleaning completed successfully! [DONE]")


if __name__ == "__main__":
    main()

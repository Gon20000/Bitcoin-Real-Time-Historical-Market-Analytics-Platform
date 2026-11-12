"""PySpark Batch Ingestion & Cleaning Job.

Ingests raw Binance Vision historical CSV files,
standardizes schema, timestamps, and precision,
and writes partitioned Delta Lake tables into Silver layer.
"""

import os
import sys

from pyspark.sql import DataFrame, SparkSession  # pyright: ignore[reportMissingImports]
from pyspark.sql import functions as F  # pyright: ignore[reportMissingImports]
from pyspark.sql import types as T  # pyright: ignore[reportMissingImports]


def get_spark_session() -> SparkSession:
    """Initialize and configure a Delta Lake-enabled SparkSession."""
    return (
        SparkSession.builder.appName("Binance_Historical_Batch_Ingest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"),
        )
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .getOrCreate()
    )


# Raw Schema for Binance Vision 1s historical CSV files (no header row)
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

DEFAULT_INPUT_DIR = os.getenv("INPUT_DIR", "/opt/spark/data/bronze/historical")
DEFAULT_OUTPUT_DIR = os.getenv(
    "OUTPUT_DIR", "/opt/spark/data/silver/stg_binance_klines_1s"
)


def transform_to_silver(raw_df: DataFrame) -> DataFrame:
    """Standardize timestamps, types, validation, and partitions for Silver."""
    decimal_type = T.DecimalType(18, 8)

    # 1. Standardize 16-digit (micro) and 13-digit (milli) epochs to 10-digit UTC seconds
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
        .withColumn("open_time", F.to_timestamp(F.from_unixtime("open_time_sec")))
        .withColumn(
            "close_time",
            F.to_timestamp(F.from_unixtime(F.col("close_time_sec") + 1)),
        )
    )

    # 2. Enforce DECIMAL(18, 8) and align field names with Gold layer contracts
    silver_df = (
        cleaned_df.withColumn("open_price", F.col("open_price").cast(decimal_type))
        .withColumn("high_price", F.col("high_price").cast(decimal_type))
        .withColumn("low_price", F.col("low_price").cast(decimal_type))
        .withColumn("close_price", F.col("close_price").cast(decimal_type))
        .withColumn(
            "base_asset_volume", F.col("base_asset_volume").cast(decimal_type)
        )
        .withColumn(
            "quote_asset_volume", F.col("quote_asset_volume").cast(decimal_type)
        )
        .withColumn(
            "taker_buy_base_asset_volume",
            F.col("taker_buy_base_volume").cast(decimal_type),
        )
        .withColumn(
            "taker_buy_quote_asset_volume",
            F.col("taker_buy_quote_volume").cast(decimal_type),
        )
        .withColumn("is_candle_closed", F.lit(True))
    )

    # 3. Derive partition columns
    silver_df = silver_df.withColumn("year", F.year("open_time")).withColumn(
        "month", F.month("open_time")
    )

    # 4. Rigorous data quality filtering, deduplication, and partition-local sorting
    validation_filter = (
        F.col("open_time").isNotNull()
        & (F.col("open_price") > 0)
        & (F.col("close_price") > 0)
        & (F.col("high_price") >= F.col("low_price"))
        & (F.col("base_asset_volume") >= 0)
    )

    silver_df = (
        silver_df.filter(validation_filter)
        .dropDuplicates(["open_time"])
        .sortWithinPartitions("open_time")
    )

    # 5. Enforce explicit column order
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


def write_to_silver_delta(df: DataFrame, output_path: str) -> None:
    """Persist clean dataset to partitioned Delta Lake with dynamic overwrite."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy("year", "month")
        .save(output_path)
    )


def main() -> None:
    """Execute single-pass historical batch ingestion pipeline."""
    input_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT_DIR
    output_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUTPUT_DIR

    print("[INFO] Initializing PySpark Batch Ingestion Pipeline...")
    print(f"[INFO] Input directory:  {input_path}")
    print(f"[INFO] Output directory: {output_path}")

    spark = None
    try:
        spark = get_spark_session()
        spark.sparkContext.setLogLevel("WARN")

        csv_wildcard = os.path.join(input_path, "*.csv")
        print(f"[INFO] Reading historical CSV files from: {csv_wildcard}")
        raw_df = spark.read.format("csv").schema(RAW_CSV_SCHEMA).load(csv_wildcard)

        # Single-pass execution: transform and persist without intermediate actions
        print("[INFO] Applying Silver transformations and persisting to Delta Lake...")
        silver_df = transform_to_silver(raw_df)
        write_to_silver_delta(silver_df, output_path)

        print("[INFO] Batch ingestion and cleaning completed successfully! [DONE]")

    except Exception as e:
        print(f"[ERROR] Pipeline execution failed: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        if spark is not None:
            print("[INFO] Shutting down Spark session...")
            spark.stop()


if __name__ == "__main__":
    main()
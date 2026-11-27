import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

KLINE_FIELDS = {
    "t": "open_time",
    "o": "open_price",
    "h": "high_price",
    "l": "low_price",
    "c": "close_price",
    "v": "base_asset_volume",
    "T": "close_time",
    "q": "quote_asset_volume",
    "n": "number_of_trades",
    "V": "taker_buy_base_volume",
    "Q": "taker_buy_quote_volume",
    "B": "ignore",
}

KLINE_SCHEMA = T.StructType(
    [
        T.StructField(
            source, T.LongType() if source in {"t", "T", "n"} else T.StringType()
        )
        for source in KLINE_FIELDS
    ]
    + [T.StructField("x", T.BooleanType())]
)

MESSAGE_SCHEMA = T.StructType([T.StructField("k", KLINE_SCHEMA)])

DEFAULT_OUTPUT_DIR = os.getenv(
    "OUTPUT_DIR", "/opt/spark/data/silver/stg_binance_klines_1s"
)

# The previous Delta append sink used a different checkpoint.
CHECKPOINT_DIR = "/opt/spark/checkpoints/streaming_silver_merge"


def get_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("Binance_Streaming_Silver_Ingest")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.shuffle.partitions",
            os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"),
        )
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .getOrCreate()
    )


def normalize_to_silver(raw_df: DataFrame) -> DataFrame:
    decimal_type = T.DecimalType(18, 8)

    cleaned_df = (
        raw_df.withColumn(
            "open_time_sec",
            F.when(
                F.col("open_time") >= 100_000_000_000_000,
                F.expr("open_time DIV 1000000"),
            )
            .when(
                F.col("open_time") >= 100_000_000_000,
                F.expr("open_time DIV 1000"),
            )
            .otherwise(F.col("open_time")),
        )
        .withColumn(
            "close_time_sec",
            F.when(
                F.col("close_time") >= 100_000_000_000_000,
                F.expr("close_time DIV 1000000"),
            )
            .when(
                F.col("close_time") >= 100_000_000_000,
                F.expr("close_time DIV 1000"),
            )
            .otherwise(F.col("close_time")),
        )
        .withColumn("open_time", F.to_timestamp(F.from_unixtime("open_time_sec")))
        .withColumn(
            "close_time",
            F.to_timestamp(F.from_unixtime(F.col("close_time_sec") + 1)),
        )
    )

    silver_df = (
        cleaned_df.withColumn("open_price", F.col("open_price").cast(decimal_type))
        .withColumn("high_price", F.col("high_price").cast(decimal_type))
        .withColumn("low_price", F.col("low_price").cast(decimal_type))
        .withColumn("close_price", F.col("close_price").cast(decimal_type))
        .withColumn("base_asset_volume", F.col("base_asset_volume").cast(decimal_type))
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

    silver_df = silver_df.withColumn("year", F.year("open_time")).withColumn(
        "month", F.month("open_time")
    )

    validation_filter = (
        F.col("open_time").isNotNull()
        & (F.col("open_price") > 0)
        & (F.col("close_price") > 0)
        & (F.col("high_price") >= F.col("low_price"))
        & (F.col("base_asset_volume") >= 0)
    )

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

    return silver_df.filter(validation_filter).select(final_columns)


def transform_kafka_to_silver(kafka_df: DataFrame) -> DataFrame:
    klines = kafka_df.select(
        F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA)
        .getField("k")
        .alias("k")
    ).filter(F.col("k.x"))

    raw_df = klines.select(
        *[
            F.col("k").getField(source).alias(target)
            for source, target in KLINE_FIELDS.items()
        ]
    )
    return normalize_to_silver(raw_df)


def merge_into_silver(batch_df: DataFrame, _batch_id: int) -> None:
    candles = batch_df.dropDuplicates(["open_time"])
    (
        candles.limit(0)
        .write.format("delta")
        .mode("ignore")
        .partitionBy("year", "month")
        .save(DEFAULT_OUTPUT_DIR)
    )

    spark = candles.sparkSession
    output_path = DEFAULT_OUTPUT_DIR.replace("`", "``")
    candles.createOrReplaceTempView("streaming_silver_candles")
    try:
        spark.sql(
            f"""
            MERGE INTO delta.`{output_path}` AS target
            USING streaming_silver_candles AS source
            ON target.year = source.year
                AND target.month = source.month
                AND target.open_time = source.open_time
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    finally:
        spark.catalog.dropTempView("streaming_silver_candles")


def main() -> None:
    spark = get_spark_session()
    try:
        spark.conf.set("spark.sql.caseSensitive", "true")
        spark.sparkContext.setLogLevel("WARN")

        kafka_df = (
            spark.readStream.format("kafka")
            .option(
                "kafka.bootstrap.servers",
                os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
            )
            .option(
                "subscribe", os.getenv("KAFKA_TOPIC", "prod.binance.btc_usdt_1s.v1")
            )
            .option("startingOffsets", "earliest")
            .load()
        )
        query = (
            transform_kafka_to_silver(kafka_df)
            .writeStream.outputMode("append")
            .foreachBatch(merge_into_silver)
            .option("checkpointLocation", CHECKPOINT_DIR)
            .start()
        )
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

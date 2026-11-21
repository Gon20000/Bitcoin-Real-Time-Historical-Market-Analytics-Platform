"""Append closed Binance candles from Kafka to the Silver Delta table."""

import os

from pyspark.sql import DataFrame  # pyright: ignore[reportMissingImports]
from pyspark.sql import functions as F  # pyright: ignore[reportMissingImports]
from pyspark.sql import types as T  # pyright: ignore[reportMissingImports]

from batch_historical_ingest import (
    DEFAULT_OUTPUT_DIR,
    RAW_CSV_SCHEMA,
    get_spark_session,
    normalize_to_silver,
)

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
        T.StructField(source, RAW_CSV_SCHEMA[target].dataType)
        for source, target in KLINE_FIELDS.items()
    ]
    + [T.StructField("x", T.BooleanType())]
)
MESSAGE_SCHEMA = T.StructType([T.StructField("k", KLINE_SCHEMA)])
CHECKPOINT_DIR = "/opt/spark/checkpoints/streaming_silver"


def transform_kafka_to_silver(kafka_df: DataFrame) -> DataFrame:
    """Decode Kafka values and map closed klines onto the historical schema."""
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


def main() -> None:
    """Read Kafka continuously and resume from the durable checkpoint on restart."""
    spark = get_spark_session("Binance_Streaming_Silver_Ingest")
    try:
        # Binance distinguishes open/close timestamps (t/T) and volumes (v/V).
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
            .writeStream.format("delta")
            .outputMode("append")
            .partitionBy("year", "month")
            .option("checkpointLocation", CHECKPOINT_DIR)
            .start(DEFAULT_OUTPUT_DIR)
        )
        query.awaitTermination()
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

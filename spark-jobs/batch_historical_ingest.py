from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, month, to_timestamp, year
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


def main():
    # 1. إنشاء الـ SparkSession مع دعم الـ Delta Lake
    spark = (
        SparkSession.builder.appName("BinanceHistoricalBronzeToSilver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    print(">>> Spark Session Initialized with Delta Lake Support.")

    # 2. تحديد Schema ملف الـ Bronze الخام (لأنه بدون Header)
    raw_schema = StructType(
        [
            StructField("open_time_raw", LongType(), True),
            StructField("open_price_raw", DoubleType(), True),
            StructField("high_price_raw", DoubleType(), True),
            StructField("low_price_raw", DoubleType(), True),
            StructField("close_price_raw", DoubleType(), True),
            StructField("base_asset_volume_raw", DoubleType(), True),
            StructField("close_time_raw", LongType(), True),
            StructField("quote_asset_volume_raw", DoubleType(), True),
            StructField("number_of_trades_raw", LongType(), True),
            StructField("taker_buy_base_raw", DoubleType(), True),
            StructField("taker_buy_quote_raw", DoubleType(), True),
            StructField("ignore_raw", StringType(), True),
        ]
    )

    # 3. قراءة كل ملفات الـ CSV من مجلد البرونز
    bronze_path = "/opt/spark/data/bronze/historical/*.csv"
    silver_output_path = "/opt/spark/data/silver/stg_binance_klines_1s"

    print(f">>> Reading raw CSV from {bronze_path}...")
    df_raw = (
        spark.read.format("csv")
        .schema(raw_schema)
        .option("header", "false")
        .load(bronze_path)
    )

    # 4. تطبيق الـ Data Contract والتنظيف (Casting & Conversions)
    df_silver = (
        df_raw
        # أ) تحويل الـ Timestamp من Milliseconds إلى TIMESTAMP بتوقيت UTC
        .withColumn("open_time", to_timestamp(col("open_time_raw") / 1000))
        .withColumn("close_time", to_timestamp(col("close_time_raw") / 1000))
        # ب) التحويل الصريح للأسعار إلى DECIMAL(18, 8)
        .withColumn("open_price", col("open_price_raw").cast("decimal(18,8)"))
        .withColumn("high_price", col("high_price_raw").cast("decimal(18,8)"))
        .withColumn("low_price", col("low_price_raw").cast("decimal(18,8)"))
        .withColumn("close_price", col("close_price_raw").cast("decimal(18,8)"))
        # ج) التحويل الصريح للأحجام إلى DECIMAL(18, 8) بالأسماء المتفق عليها
        .withColumn(
            "base_asset_volume",
            col("base_asset_volume_raw").cast("decimal(18,8)"),
        )
        .withColumn(
            "quote_asset_volume",
            col("quote_asset_volume_raw").cast("decimal(18,8)"),
        )
        .withColumn("number_of_trades", col("number_of_trades_raw"))
        .withColumn(
            "taker_buy_base_asset_volume",
            col("taker_buy_base_raw").cast("decimal(18,8)"),
        )
        .withColumn(
            "taker_buy_quote_asset_volume",
            col("taker_buy_quote_raw").cast("decimal(18,8)"),
        )
        # د) إضافة الـ Flag: الشموع التاريخية كلها مغلقة (True)
        .withColumn("is_candle_closed", lit(True))
        # هـ) استخراج أعمدة الـ Partitioning (year و month)
        .withColumn("year", year(col("open_time")))
        .withColumn("month", month(col("open_time")))
        # اختيار الأعمدة النهائية وحذف الأعمدة الخام
        .select(
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
            "year",
            "month",
        )
    )

    # 5. كتابة البيانات في Delta Lake مقسمة حسب (year, month)
    print(f">>> Writing clean Delta table to {silver_output_path}...")
    (
        df_silver.write.format("delta")
        .mode("overwrite")
        .partitionBy("year", "month")
        .save(silver_output_path)
    )

    print(">>> Batch Ingestion Completed Successfully!")
    spark.stop()


if __name__ == "__main__":
    main()

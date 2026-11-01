from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("card_number", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("merchant_name", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("transaction_type", StringType(), True),
    StructField("payment_channel", StringType(), True),
    StructField("device_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("transaction_timestamp", TimestampType(), True),
    StructField("is_international", BooleanType(), True),
    StructField("status", StringType(), True)
])

@dp.table(
    name = "financial_guard.silver.Transactions",
    comment = "Parsed and Cleaned Transaction Data")
@dp.expect_or_fail("valid_transaction_id", "transaction_id IS NOT NULL")
@dp.expect_or_fail("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_amount", "amount > 0")
@dp.expect_or_drop("valid_transaction_timestamp", "transaction_timestamp IS NOT NULL")
@dp.expect_or_drop("valid_card_number", "card_number IS NOT NULL")
@dp.expect("valid_status", "status IN ('APPROVED', 'DECLINED', 'FAILED', 'PENDING')")
@dp.expect("valid_currency", "currency IN ('INR', 'USD', 'EUR', 'GBP', 'JPY', 'AUD', 'CAD', 'CNY')")

def transactions_silver() -> DataFrame:
    bronze_df = spark.readStream.table("financial_guard.bronze.Transactions")

    transformed_df = bronze_df.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("Ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        F.from_json(col("value"), transaction_schema).alias("data")
    ).select(
        col("data.*"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        col("bronze_ingestion_timestamp"),
        F.current_timestamp().alias("silver_ingestion_timestamp"),
    )

    return transformed_df
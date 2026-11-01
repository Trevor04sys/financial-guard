from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as f

@dp.table(
    name = "financial_guard.gold.transactions_count_by_minute")

def transactions_count_by_minute() -> DataFrame:
  transactions = spark.readStream.table("financial_guard.silver.transactions")
  transactions_with_watermark = transactions.withWatermark("transaction_timestamp", "5 minutes")

  transaction_count_df = transactions_with_watermark.groupBy(f.window("transaction_timestamp", "1 minute")).agg(f.count("*").alias("transaction_count")).select(f.col("window.start"), f.col("window.end"), f.col("transaction_count"))
  return transaction_count_df

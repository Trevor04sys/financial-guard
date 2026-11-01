from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name="financial_guard.gold.high_value_transaction_alert", 
    comment="Alert details Where Transactions have been performed with value higher than what is configured by customers"
)
def high_value_transaction_alert() -> DataFrame:
    transactions = spark.readStream.table("financial_guard.silver.transactions")
    customers = spark.read.table("financial_guard.silver.customers")

    joined_df = (
        transactions.join(customers, on="customer_id", how="left")
        .filter(F.col("amount") > F.col("transaction_limit"))
                .select(
                    F.concat_ws("-", F.lit("ALERT"),F.col("transaction_id")).alias("alert_id"),
                    F.lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),
                    F.current_timestamp().alias("alert_timestamp"),
                    transactions.transaction_id,
                    transactions.customer_id,
                    customers.email.alias("customer_email"),
                    F.concat_ws(" ", customers.first_name, customers.last_name).alias("customer_name"),

                    transactions.amount.alias("transaction_amount"),
                    transactions.transaction_type,
                    customers.transaction_limit,
                    transactions.currency,
                    transactions.merchant_name,
                    transactions.merchant_category,
                    transactions.payment_channel,
                    transactions.city,
                    transactions.country,
                    transactions.is_international,
                    transactions.transaction_timestamp,
                    transactions.status
                    )
    )

    return joined_df


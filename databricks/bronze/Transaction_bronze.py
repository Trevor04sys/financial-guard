from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import col
from pyspark.sql import functions as F

@dp.table(
    name = "financial_guard.bronze.Transactions",
    comment = "Transactions Raw Stream Data Ingested From KAFKA")

def transactions_bronze() -> DataFrame:
    kafka_connection_details = dbutils.secrets.get(scope = "Financial-guard-scope", key = "kafka_connection_details")
    kafka_config = json.loads(kafka_connection_details)
    BOOTSTRAP_SERVERS = kafka_config['bootstrap_servers']
    API_KEY = kafka_config['api_key']
    API_SECRET = kafka_config['api_secret']
    TOPIC_NAME = kafka_config['topic']

    streaming_df = (spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers",BOOTSTRAP_SERVERS)
                .option("subscribe",TOPIC_NAME)
                .option("startingOffsets", "earliest")
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config", f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{API_KEY}' password='{API_SECRET}';")
                .load())
    
    parsed_stream_df = streaming_df.select(col("key").cast("string"),
                                   col("value").cast("string"),
                                   col("topic"),
                                   col("partition"),
                                   col("offset"),
                                   col("timestamp"),
                                   col("timestampType"),
                                   F.current_timestamp().alias("Ingestion_timestamp"))
    
    return parsed_stream_df
    

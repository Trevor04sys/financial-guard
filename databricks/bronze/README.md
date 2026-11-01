
# Bronze Layer

The Bronze layer stores raw ingested data before applying business transformations.

Expected notebooks:

- `Transaction_bronze.py` – Ingests transaction events from the Kafka topic.
- `fraud_watchlist_bronze.py` – Ingests fraud watchlist JSON files using Databricks Auto Loader.

The raw ingestion layer preserves source information and metadata for downstream processing.

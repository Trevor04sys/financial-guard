# Databricks

This directory contains the Databricks notebooks and SQL/PySpark source files used in the Financial Guard streaming pipeline.

The project follows a Medallion Architecture:

- `bronze/` – Raw ingestion from Kafka, customer data, and the fraud watchlist.
- `silver/` – Data parsing, cleansing, standardisation, and validation.
- `gold/` – Business-level fraud alerts and transaction aggregations.
- `alerts/` – Email notification sinks.
- `customer-ingestion/` – Customer master data ingestion and transformation.
- `dashboard/` – Lakeview dashboard definition.

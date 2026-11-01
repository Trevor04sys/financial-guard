# Silver Layer

The Silver layer contains cleaned, parsed, standardised, and validated data.

Expected notebooks:

- `Transaction_silver.py` – Parses and transforms Bronze Kafka transaction data.
- `fraud_watchlist_silver.py` – Standardises and transforms fraud watchlist records.

This layer prepares the data for downstream business rules, streaming joins, and aggregations.

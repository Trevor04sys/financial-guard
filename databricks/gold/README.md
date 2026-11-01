# Gold Layer

The Gold layer contains business-level outputs for fraud monitoring and transaction analytics.

Expected notebooks:

- `high_value_transaction_alert.py` – Identifies transactions exceeding customer transaction limits.
- `fraud_card_alert.py` – Matches transaction card numbers against the fraud watchlist.
- `transaction_count_by_minute.py` – Produces transaction counts using one-minute tumbling windows.
- `transaction_count_by_minute_sliding-window.py` – Produces transaction counts using five-minute sliding windows with a one-minute slide.

These outputs support downstream email alerts and dashboard reporting.

# Alert Sinks

This directory contains the streaming email notification sinks.

Expected notebooks:

- `email_alert_sink.py` – Sends email notifications for high-value transaction alerts.
- `fraud_email_alert_sink.py` – Sends email notifications for fraud watchlist matches.

Email credentials should be configured securely using Databricks secrets. Never commit actual credentials or passwords.

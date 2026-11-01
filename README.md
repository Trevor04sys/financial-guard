# Financial Guard
### Real-Time Credit Card Transaction Monitoring & Fraud Detection

Financial Guard is an end-to-end data engineering project that demonstrates real-time credit card transaction monitoring, rule-based fraud watchlist matching, automated email alerts, and interactive analytics using Apache Kafka and Azure Databricks.

The solution integrates customer master data, streaming transactions, and fraud watchlist records into a unified data processing pipeline using the Medallion Architecture.

---

## Project Overview

Financial Guard monitors incoming credit card transactions and identifies potentially suspicious activity through configurable business rules and fraud watchlist matching.

The project demonstrates three independent data ingestion paths:

1. **Customer Data:** Customer master records are ingested from Tiger Cloud PostgreSQL into Databricks.
2. **Transaction Streaming:** Python-based producers generate transaction events and publish them to Confluent Cloud Kafka.
3. **Fraud Watchlist:** Watchlist CSV records are converted into individual JSON files and ingested using Databricks Auto Loader.

The data is processed through Bronze, Silver, and Gold layers. Gold outputs feed automated email notification sinks and a Databricks Lakeview dashboard.

## Architecture

![Financial Guard Architecture](docs/architecture/financial-guard-architecture.png)

### High-Level Data Flow

```text
Tiger Cloud PostgreSQL
          |
          v
   Customer Ingestion
          |
          v
   Bronze / Silver Customers
          |
          +------------------------------+
                                         |
Python Transaction Producers             |
          |                              |
          v                              |
   Confluent Cloud Kafka                 |
          |                              |
          v                              |
   Bronze Transactions                   |
          |                              |
          v                              |
   Silver Transactions                   |
          |                              |
          +---------------+--------------+
                          |
                          v
                    Gold Layer
                          |
             +------------+------------+
             |                         |
             v                         v
      High-Value Alerts          Fraud Card Alerts
             |                         |
             v                         v
      Email Alert Sink          Fraud Email Sink


Fraud Watchlist CSV
          |
          v
   Python JSON Generator
          |
          v
   Unity Catalog Volume
          |
          v
     Auto Loader
          |
          v
   Bronze Watchlist
          |
          v
   Silver Watchlist
          |
          +------> Gold Fraud Card Alerts


Gold Aggregations
          |
          v
   Databricks Lakeview Dashboard
```

## Technology Stack

| Category | Technologies |
|---|---|
| Programming | Python, SQL, PySpark |
| Streaming | Apache Kafka, Confluent Cloud |
| Data Engineering | Azure Databricks, Lakeflow Spark Declarative Pipelines |
| Database | Tiger Cloud PostgreSQL |
| Storage | Unity Catalog Volumes |
| Ingestion | Databricks Auto Loader |
| Architecture | Medallion Architecture |
| Processing | Structured Streaming |
| Visualization | Databricks Lakeview Dashboard |
| Notifications | Gmail SMTP with STARTTLS |

## Key Features

- Real-time transaction event ingestion through Kafka.
- Synthetic normal and fraud-scenario transaction generation.
- Customer master data ingestion from PostgreSQL.
- File-based fraud watchlist ingestion using Auto Loader.
- Bronze, Silver, and Gold data processing layers.
- Data quality expectations in Silver transaction processing.
- High-value transaction alert generation.
- Fraud watchlist card matching.
- Tumbling and sliding-window transaction aggregations.
- Automated email notifications.
- Interactive fraud monitoring dashboard.

---

## Data Ingestion

### 1. Customer Master Data

Customer records are maintained in Tiger Cloud PostgreSQL and ingested into Databricks through a configured ingestion pipeline.

The customer data is processed into the Silver layer and used to enrich transaction and fraud alert records.

### 2. Transaction Streaming

Python producers generate synthetic credit card transaction events and publish them to a Confluent Cloud Kafka topic.

The Databricks pipeline consumes Kafka messages and stores the original event payloads in the Bronze layer.

The Silver layer parses the JSON payload and applies data quality expectations.

### 3. Fraud Watchlist Ingestion

Fraud watchlist records are maintained in CSV format.

A Python utility converts individual CSV records into JSON files and places them in a Unity Catalog Volume.

Databricks Auto Loader detects and ingests the JSON files into the Bronze watchlist table. The Silver layer applies standardisation and type conversion.

---

## Medallion Architecture

### Bronze Layer

The Bronze layer stores incoming source data with minimal transformation.

Main datasets:

- `financial_guard.bronze.transactions`
- `financial_guard.bronze.fraud_watchlist`

The transaction Bronze table preserves Kafka metadata, including message key, payload, topic, partition, offset, timestamp, and ingestion timestamp.

### Silver Layer

The Silver layer transforms raw records into structured datasets.

Main datasets:

- `financial_guard.silver.transactions`
- `financial_guard.silver.fraud_watchlist`
- `financial_guard.silver.customers`

Processing includes:

- JSON parsing and schema application.
- Data type conversion.
- Column standardisation.
- Data quality expectations.
- Ingestion metadata.

### Gold Layer

The Gold layer contains business-oriented outputs.

| Dataset | Purpose |
|---|---|
| `high_value_transaction_alert` | Identifies transactions exceeding configured customer transaction limits |
| `fraud_card_alert` | Identifies transactions matching fraud watchlist card entries |
| `transactions_count_by_minute` | Calculates transaction counts using one-minute tumbling windows |
| `transactions_count_by_minute_sliding_window` | Calculates transaction counts using five-minute windows sliding every minute |

---

## Fraud Monitoring and Alerting

### 1. High-Value Transaction Detection

Transactions are compared against the configured transaction limit associated with the customer.

When a transaction exceeds the configured limit, the Gold processing logic generates a high-value transaction alert.

The generated alert is passed to the email notification sink.

### 2. Fraud Watchlist Matching

Transaction card numbers are matched against card entries in the Silver fraud watchlist.

When a match is identified, the Gold processing logic generates an enriched fraud alert containing transaction and watchlist information.

The alert includes details such as risk level, watchlist information, transaction details, and location comparison.

### 3. Automated Email Notifications

Two notification workflows are implemented:

- High-value transaction notification.
- Fraud watchlist match notification.

The notification sinks use Gmail SMTP with STARTTLS and retrieve credentials through Databricks secrets.

Card numbers in fraud notification emails are masked.

---

## Dashboard

Financial Guard includes a Databricks Lakeview dashboard with four pages.

### Executive Overview

Provides an overview of transaction activity, transaction volume, fraud alerts, high-value alerts, merchant categories, payment channels, and transaction trends.

### Fraud Analytics

Displays risk-level distribution, location mismatch analysis, merchant-category insights, watchlist reasons, and recent fraud alerts.

### Customer & Transaction Insights

Provides customer transaction analysis, top transaction cities, top customers by transaction volume, and high-value alert details.

### Watchlist Monitoring

Displays active watchlist records, risk distribution, reporting sources, reason codes, reporting teams, and recent watchlist activity.

---

## Project Screenshots

Screenshots and demonstration recordings are organised under `docs/screenshots/` and `docs/videos/`.

| Component | Evidence |
|---|---|
| Pipeline execution | Databricks pipeline graph |
| Continuous processing | Pipeline execution recording |
| Kafka streaming | Confluent topic and message records |
| Bronze and Silver | Transaction table screenshots |
| Gold processing | Alert table screenshots |
| Email notifications | High-value and fraud email examples |
| Analytics | Four Lakeview dashboard pages |

---

## Repository Structure

```text
financial-guard/
│
├── README.md
├── LICENSE
├── .gitignore
│
├── docs/
│   ├── architecture/
│   ├── screenshots/
│   │   ├── pipeline/
│   │   ├── kafka/
│   │   ├── databricks/
│   │   ├── dashboard/
│   │   └── email-alerts/
│   ├── videos/
│   ├── WALKTHROUGH.md
│   └── FILE_GUIDE.md
│
├── kafka-producer/
│   ├── producer_normal.py
│   ├── producer_fraud_transaction.py
│   ├── producer_fraud_card.py
│   ├── consumer.py
│   ├── config.py
│   ├── fraud_engine.py
│   ├── models.py
│   ├── utils.py
│   ├── requirements.txt
│   ├── .env.example
│   └── README.md
│
└── databricks/
    ├── bronze/
    ├── silver/
    ├── gold/
    ├── alerts/
    ├── customer-ingestion/
    └── dashboard/
```

*The repository structure may evolve as additional source files and documentation are added.*

---

## Setup and Configuration

### Prerequisites

- Python 3.x
- Confluent Cloud Kafka account or compatible Kafka cluster
- Azure Databricks workspace
- Unity Catalog enabled
- PostgreSQL source database
- Gmail account configured for SMTP notifications

### Python Producer Setup

Clone the repository:

```bash
git clone https://github.com/<your-github-username>/financial-guard.git
cd financial-guard/kafka-producer
```

Create a virtual environment:

```bash
python -m venv venv
```

Activate it on Windows:

```bash
venv\Scripts\activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Create a local `.env` file using `.env.example` as a template and configure the required Kafka connection settings.

Do not commit `.env` or any credentials.

### Databricks Setup

1. Configure the required source connections.
2. Create the required Unity Catalog catalog, schemas, and volume.
3. Configure the customer ingestion pipeline.
4. Import the Databricks notebooks or pipeline source files.
5. Configure Kafka connection details using Databricks secrets.
6. Configure email credentials securely.
7. Update source paths and table references for your workspace.
8. Configure and run the Lakeflow pipeline.
9. Import the Lakeview dashboard.

The original Databricks workspace configuration, connections, secrets, and deployed pipeline settings may need to be recreated manually.

---

## Important Project Notes

- This is an educational and portfolio data engineering project using synthetic data.
- Fraud identification is based on rule-based watchlist matching and configured business conditions, not a trained machine learning model.
- The Confluent Cloud environment used during development was a free trial. The historical screenshots and recording demonstrate earlier successful streaming activity; the environment may need a new Kafka cluster and credentials to reproduce it.
- Dashboard metrics are example results from the captured environment and are not live production metrics.
- Email delivery depends on valid SMTP credentials and configuration.
- This project is not a production banking or payment security system.

## Limitations and Future Improvements

- Implement alert deduplication and idempotent notification handling.
- Improve watchlist record deduplication and update handling.
- Add retry and failure recovery mechanisms for email notifications.
- Improve schema evolution and error handling.
- Add automated deployment and testing.
- Integrate additional fraud detection rules and model-based scoring.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Author

**Raj Pramalick**

Data Engineering | Azure Databricks | Apache Kafka | PySpark | Python

